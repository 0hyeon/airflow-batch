from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3

# --- 사용자 설정 변수 ---#
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"
EMR_EC2_KEY_NAME = "test"

# S3 경로 정의
# G마켓/옥션 피드 파일 경로를 입력으로 지정
GMARKET_FEED_KEY = "feeds/general/combined_gmarket_feed.csv.gz"
AUCTION_FEED_KEY = "feeds/general/combined_auction_feed.csv.gz"

# ER에서 실행할 새로운 Spark 스크립트 경로
SCRIPT_S3_KEY = "scripts/kakao_process_gmarket_feeds.py"
# EMR 처리 결과가 저장될 경로
OUTPUT_S3_PATH = f"s3://{S3_BUCKET}/processed-feeds/gmarket_auction/"
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
# DAG ID와 태그를 새로운 목적에 맞게 수정
with DAG(
    dag_id="kakao_gmarket_auction_emr_processing",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["emr", "gmarket", "auction", "kakao"],
    doc_md="G마켓과 옥션 피드 파일을 EMR Spark로 처리합니다.",
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

        # [변경] EMR 클러스터 이름 수정
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
        cluster_id = ti.xcom_pull(
            task_ids="create_emr_cluster_task", key="emr_cluster_id"
        )
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )

        # [변경] Spark 스크립트에 입력 및 출력 파일 경로를 인자로 전달
        spark_submit_args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            f"s3://{S3_BUCKET}/{SCRIPT_S3_KEY}",
            "--gmarket-input",
            f"s3://{S3_BUCKET}/{GMARKET_FEED_KEY}",
            "--auction-input",
            f"s3://{S3_BUCKET}/{AUCTION_FEED_KEY}",
            "--output",
            OUTPUT_S3_PATH,
        ]

        # [변경] EMR Step 이름 수정
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Process_Gmarket_Auction_Feeds",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args,
                    },
                }
            ],
        )
        ti.xcom_push(key="spark_step_id", value=response["StepIds"][0])
        print(f"✅ Submitted Spark job with step ID: {response['StepIds'][0]}")

    # --- 📝 Airflow 태스크 정의 ---

    # 1. [변경] G마켓 피드 파일 대기 센서
    wait_for_gmarket_feed_task = S3KeySensor(
        task_id="wait_for_gmarket_feed_task",
        bucket_name=S3_BUCKET,
        bucket_key=GMARKET_FEED_KEY,
        poke_interval=60,
        timeout=60 * 10,
    )

    # 2. [추가] 옥션 피드 파일 대기 센서
    wait_for_auction_feed_task = S3KeySensor(
        task_id="wait_for_auction_feed_task",
        bucket_name=S3_BUCKET,
        bucket_key=AUCTION_FEED_KEY,
        poke_interval=60,
        timeout=60 * 10,
    )

    # 3. EMR 클러스터 생성 태스크
    create_emr_cluster_task = PythonOperator(
        task_id="create_emr_cluster_task",
        python_callable=create_emr_cluster,
    )

    # 4. EMR 클러스터 준비 완료 대기
    wait_for_emr_cluster_task = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        target_states=["WAITING"],
    )

    # 5. Spark 작업 제출
    submit_spark_job_task = PythonOperator(
        task_id="submit_spark_job_task",
        python_callable=submit_spark_job,
    )

    # 6. Spark 작업 완료 대기
    wait_for_spark_step_task = EmrStepSensor(
        task_id="wait_for_spark_step_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        step_id="{{ ti.xcom_pull(task_ids='submit_spark_job_task', key='spark_step_id') }}",
        target_states=["COMPLETED"],
    )

    # 7. EMR 클러스터 종료
    terminate_emr_cluster_task = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster_task",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster_task', key='emr_cluster_id') }}",
        trigger_rule="all_done",
    )

    # --- 🚀 DAG 실행 순서 정의 ---
    # 두 피드 파일이 모두 준비되면 EMR 클러스터 생성 시작
    [wait_for_gmarket_feed_task, wait_for_auction_feed_task] >> create_emr_cluster_task

    create_emr_cluster_task >> wait_for_emr_cluster_task >> submit_spark_job_task
    submit_spark_job_task >> wait_for_spark_step_task >> terminate_emr_cluster_task
