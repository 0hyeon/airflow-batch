from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import time
from airflow.models import Variable
# from plugins import slack 
import botocore.exceptions

# ❗️버킷 경로 수정
S3_BUCKET = "gyoung0-test"
# ❗️connections s3 이름 설정 
AWS_CONN_ID = "aws_conn_id"

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,  # 🚨🚨📢Slack 알림 추가
}

dag = DAG(
    "jabko_iceberg_to_s3_emr",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# ✅ 1. EMR 클러스터 생성
def create_emr_cluster(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")

    response = client.run_job_flow(
        Name="csv-to-iceberg-emr-cluster",
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
            "Ec2KeyName": "test",
            "KeepJobFlowAliveWhenNoSteps": True,  # 모든 Step이 완료되는 즉시 EMR 클러스터가 즉시 자동
            "TerminationProtected": False,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BUCKET}/emr-logs/",
        AutoTerminationPolicy={"IdleTimeout": 600}, # ✅ 10분 대기 후 자동 종료
        VisibleToAllUsers=True,
    )

    cluster_id = response["JobFlowId"]
    kwargs["ti"].xcom_push(key="emr_cluster_id", value=cluster_id)
    print(f"✅ EMR 클러스터 생성 완료: {cluster_id}")

create_emr = PythonOperator(
    task_id="create_emr",
    python_callable=create_emr_cluster,
    provide_context=True,
    dag=dag,
)

# ✅ 2. 클러스터가 `WAITING` 상태가 될 때까지 대기
def wait_for_emr_cluster(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr", key="emr_cluster_id")

    print(f"🔄 EMR 클러스터 {cluster_id} 활성화 대기 중...")

    while True:
        response = client.describe_cluster(ClusterId=cluster_id)
        state = response["Cluster"]["Status"]["State"]

        if state == "WAITING":
            print(f"✅ EMR 클러스터 {cluster_id} 활성화 완료!")
            break
        elif state in ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]:
            raise Exception(f"❌ 클러스터 {cluster_id}가 비정상 종료됨! 상태: {state}")

        time.sleep(30) # ✅ 30초 간격으로 상태 확인

wait_for_cluster = PythonOperator(
    task_id="wait_for_cluster",
    python_callable=wait_for_emr_cluster,
    provide_context=True,
    dag=dag,
)

# ✅ 3. S3 파일 존재 여부 확인 
def check_s3_file_with_hook(**kwargs):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    target_date = kwargs["dag_run"].conf.get("target_date")
    key = f"deduction-csv/date={target_date}/final_attachment.csv"

    if not hook.check_for_key(key, bucket_name=S3_BUCKET):
        raise FileNotFoundError(f"❌ S3에 파일이 존재하지 않음: s3://{S3_BUCKET}/{key}")

    print(f"✅ S3에 파일 존재 확인 완료: s3://{S3_BUCKET}/{key}")

check_s3_files = PythonOperator(
    task_id="check_s3_files",
    python_callable=check_s3_file_with_hook,
    provide_context=True,
    dag=dag,
)
# ✅ 4. EMR에서 PySpark 작업 실행
def submit_spark_job(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    target_date = kwargs["dag_run"].conf.get("target_date")
    print(f"⏰⏰⏰⏰⏰⏰⏰⏰ target_date: {target_date}")
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr", key="emr_cluster_id")

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "Process Parquet with PySpark",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "--conf", "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
                        "--conf", "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                        "--conf", "spark.sql.catalog.glue.warehouse=s3a://gyoung0-test/iceberg_warehouse/",
                        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                        "--conf", f"spark.hadoop.yesterday={target_date}",
                        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
                        "s3://gyoung0-test/scripts/csv_to_iceberg.py"  # ✅ PySpark 코드 위치
                    ],
                },
            }
        ],
    )

    step_id = response["StepIds"][0]
    kwargs["ti"].xcom_push(key="spark_step_id", value=step_id)
    print(f"✅ PySpark 작업 제출 완료: Step ID = {step_id}")

run_spark_job = PythonOperator(
    task_id="run_spark_job",
    python_callable=submit_spark_job,
    provide_context=True,
    dag=dag,
)

# ✅ 5. 작업 완료 후 클러스터 자동 종료
def wait_for_spark_job(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr", key="emr_cluster_id")
    step_id = kwargs["ti"].xcom_pull(task_ids="run_spark_job", key="spark_step_id")

    print(f"🔄 Spark 작업 {step_id} 실행 대기 중...")

    retries = 0
    max_retries = 5

    while True:
        try:
            response = client.describe_step(ClusterId=cluster_id, StepId=step_id)
            state = response["Step"]["Status"]["State"]

            if state == "COMPLETED":
                print("✅ Spark 작업 완료!")
                client.terminate_job_flows(JobFlowIds=[cluster_id])
                print(f"🛑 클러스터 {cluster_id} 종료 요청 완료")
                break
            elif state in ["FAILED", "CANCELLED"]:
                raise Exception(f"❌ Spark 작업 실패! 상태: {state}")
            else:
                print(f"⌛ 현재 상태: {state}, 30초 후 재확인")
                time.sleep(30)

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                if retries < max_retries:
                    sleep_time = 2 ** retries
                    print(f"🚨 Throttling 발생. {sleep_time}초 후 재시도 ({retries + 1}/{max_retries})")
                    time.sleep(sleep_time)
                    retries += 1
                    continue
                else:
                    raise Exception("❌ 최대 재시도 횟수 초과: ThrottlingException")
            else:
                raise


wait_for_spark = PythonOperator(
    task_id="wait_for_spark",
    python_callable=wait_for_spark_job,
    provide_context=True,
    dag=dag,
)

check_s3_files >> create_emr >> wait_for_cluster >> run_spark_job >> wait_for_spark
