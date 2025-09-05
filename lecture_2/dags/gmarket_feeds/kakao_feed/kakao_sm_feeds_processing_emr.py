from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3

# --- 사용자 설정 변수 ---
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"
EMR_EC2_KEY_NAME = "test"

# S3 경로 정의
GMARKET_FEED_KEY = "feeds/general/combined_gmarket_feed.csv.gz"
AUCTION_FEED_KEY = "feeds/general/combined_auction_feed.csv.gz"
SCRIPT_S3_KEY = "scripts/kakao_sm_process_gmarket_feeds.py"
LOG_S3_PATH = f"s3://{S3_BUCKET}/emr-logs/"

# --- DAG 기본 설정 ---
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 30),
    "aws_conn_id": AWS_CONN_ID,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG 정의 ---
with DAG(
    dag_id="kakao_gmarket_auction_emr_with_staging",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["emr", "gmarket", "auction", "kakao", "staging"],
    doc_md="[최종] G마켓/옥션 피드를 EMR Spark로 처리하고 Staging 후 최종 위치로 옮깁니다.",
) as dag:
    # --- Python 함수 정의 ---
    def create_emr_cluster(**kwargs):
        """Boto3를 사용하여 EMR 클러스터를 생성하는 함수"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )
        response = client.run_job_flow(
            Name="gmarket-auction-feed-processing-cluster",
            ReleaseLabel="emr-6.15.0",
            Applications=[{"Name": "Spark"}],
            Instances={
                "InstanceGroups": [
                    {
                        "Name": "Master node",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": "m5.xlarge",
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Worker nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": "m5.xlarge",
                        "InstanceCount": 2,
                    },
                ],
                "Ec2KeyName": EMR_EC2_KEY_NAME,
                "KeepJobFlowAliveWhenNoSteps": True,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            LogUri=LOG_S3_PATH,
            AutoTerminationPolicy={"IdleTimeout": 600},
            VisibleToAllUsers=True,
        )
        kwargs["ti"].xcom_push(key="emr_cluster_id", value=response["JobFlowId"])
        print(f"✅ Created EMR cluster: {response['JobFlowId']}")

    def submit_spark_job(**kwargs):
        """EMR 클러스터에 Spark 작업을 제출하는 함수"""
        ti = kwargs["ti"]
        cluster_id = ti.xcom_pull(task_ids="create_emr_cluster_task", key="emr_cluster_id")
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )
        spark_submit_args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            f"s3://{S3_BUCKET}/{SCRIPT_S3_KEY}",
        ]
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Process_Gmarket_Auction_Feeds_to_Staging",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args
                    },
                }
            ],
        )
        ti.xcom_push(key="spark_step_id", value=response["StepIds"][0])
        print(f"✅ Submitted Spark job with step ID: {response['StepIds'][0]}")

    def swap_s3_data_from_staging(**kwargs):
        """[수정됨] 5GB 이상 대용량 파일을 지원하는 데이터 바꿔치기 함수"""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        
        # 대용량 파일 처리를 위해 boto3의 'S3 Resource' 객체를 생성
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )

        markets = ["gmarket", "auction"]
        for market in markets:
            prod_prefix = f"feeds/kakao/{market}/"
            staging_prefix = f"feeds/kakao/{market}_staging/"
            
            print(f"--- '{market}' 데이터 바꿔치기 시작 ---")

            # 1. 기존 Production 폴더 내용 삭제
            keys_to_delete = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prod_prefix)
            if keys_to_delete:
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=keys_to_delete)

            # 2. Staging 폴더 내용을 Production 폴더로 복사
            staging_keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=staging_prefix)
            if not staging_keys:
                raise ValueError(f"Staging 경로 '{staging_prefix}'에 데이터가 없습니다!")
            
            for key in staging_keys:
                dest_key = key.replace("_staging", "", 1)
                
                copy_source = {'Bucket': S3_BUCKET, 'Key': key}
                dest_object = s3_resource.Object(S3_BUCKET, dest_key)
                dest_object.copy(copy_source)
                print(f"Copied {key} to {dest_key}")
            
            # 3. Staging 폴더 내용 삭제
            s3_hook.delete_objects(bucket=S3_BUCKET, keys=staging_keys)
            print(f"'{market}' 데이터 바꿔치기 완료.")

    # --- 📝 Airflow 태스크 정의 ---
    wait_for_gmarket_feed_task = S3KeySensor(
        task_id="wait_for_gmarket_feed_task",
        bucket_name=S3_BUCKET,
        bucket_key=GMARKET_FEED_KEY,
        poke_interval=60,
        timeout=60 * 10,
    )

    wait_for_auction_feed_task = S3KeySensor(
        task_id="wait_for_auction_feed_task",
        bucket_name=S3_BUCKET,
        bucket_key=AUCTION_FEED_KEY,
        poke_interval=60,
        timeout=60 * 10,
    )

    create_emr_cluster_task = PythonOperator(
        task_id="create_emr_cluster_task",
        python_callable=create_emr_cluster,
    )

    wait_for_emr_cluster_task = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        target_states=["WAITING"],
    )

    submit_spark_job_task = PythonOperator(
        task_id="submit_spark_job_task",
        python_callable=submit_spark_job,
    )

    wait_for_spark_step_task = EmrStepSensor(
        task_id="wait_for_spark_step_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        step_id="{{ ti.xcom_pull(task_ids='submit_spark_job_task', key='spark_step_id') }}",
        target_states=["COMPLETED"],
    )

    swap_data_task = PythonOperator(
        task_id="swap_data_from_staging_task",
        python_callable=swap_s3_data_from_staging,
    )
    
    terminate_emr_cluster_task = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        trigger_rule="all_done",
    )

    # --- 🚀 DAG 실행 순서 정의 ---
    [wait_for_gmarket_feed_task, wait_for_auction_feed_task] >> create_emr_cluster_task

    create_emr_cluster_task >> wait_for_emr_cluster_task >> submit_spark_job_task >> \
    wait_for_spark_step_task >> swap_data_task >> terminate_emr_cluster_task