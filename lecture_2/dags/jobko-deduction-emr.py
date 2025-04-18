from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
from datetime import datetime, timedelta
import time
from airflow.models import Variable

# 날짜 설정
# yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
yesterday = "2025-04-15"
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/"
AWS_REGION = "ap-northeast-2"

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "jobko_deduction_to_s3_emr",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)
session = boto3.Session(
    aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
    region_name=Variable.get("AWS_DEFAULT_REGION"),
)


# ✅ 1. EMR 클러스터 생성
def create_emr_cluster(**kwargs):
    client = session.client("emr")
    response = client.run_job_flow(
        Name="jobko-emr-cluster",
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
        AutoTerminationPolicy={"IdleTimeout": 600},  # ✅ 10분 대기 후 자동 종료
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

        time.sleep(30)  # ✅ 30초 간격으로 상태 확인


wait_for_cluster = PythonOperator(
    task_id="wait_for_cluster",
    python_callable=wait_for_emr_cluster,
    provide_context=True,
    dag=dag,
)


# ✅ 3. S3 파일 존재 여부 확인
def check_all_s3_files():
    s3 = session.client("s3")
    files = [
        "data_aos_onepick_retarget.parquet",
        "data_aos_onepick_ua.parquet",
        "data_aos_retarget.parquet",
        "data_ios_onepick_retarget.parquet",
        "data_ios_onepick_ua.parquet",
        "data_ios_retarget.parquet",
        "data_ios_ua.parquet",
    ]

    for file in files:
        s3_path = f"{S3_INPUT_PREFIX}{file}"
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=s3_path)
        except Exception:
            raise ValueError(f"❌ S3 파일이 누락됨: {s3_path}")

    print(f"✅ S3에 모든 파일이 존재합니다.")


check_s3_files = PythonOperator(
    task_id="check_s3_files",
    python_callable=check_all_s3_files,
    dag=dag,
)


# ✅ 4. EMR에서 PySpark 작업 실행
def submit_spark_job(**kwargs):
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
                        "spark-submit",  # Spark 실행 명령어
                        "--deploy-mode",
                        "cluster",  # 클러스터 모드에서 실행 (마스터 노드에서 실행)
                        f"s3://{S3_BUCKET}/scripts/jobko_deduction.py",  # 실행할 PySpark 스크립트 경로
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
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr", key="emr_cluster_id")
    step_id = kwargs["ti"].xcom_pull(task_ids="run_spark_job", key="spark_step_id")

    print(f"🔄 Spark 작업 {step_id} 실행 대기 중...")

    while True:
        response = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response["Step"]["Status"]["State"]

        if state == "COMPLETED":
            print(f"✅ Spark 작업 완료!")
            break
        elif state in ["FAILED", "CANCELLED"]:
            raise Exception(f"❌ Spark 작업 실패! 상태: {state}")

        time.sleep(30)


wait_for_spark = PythonOperator(
    task_id="wait_for_spark",
    python_callable=wait_for_spark_job,
    provide_context=True,
    dag=dag,
)

# # 6. 이전 DAG 실행
# trigger_previous_dag = TriggerDagRunOperator(
#     task_id="trigger_previous_dag",
#     trigger_dag_id="jobko_apps_deduction_daily_to_s3_storage_sequential",
#     dag=dag,
# )

# # 7. 이전 DAG 완료 확인
# wait_for_previous_dag = ExternalTaskSensor(
#     task_id="wait_for_previous_dag",
#     external_dag_id="jobko_apps_deduction_daily_to_s3_storage_sequential",
#     timeout=3600,
#     mode="poke",
#     poke_interval=60,
#     dag=dag,
# )

# 8. DAG 실행 순서 설정

# trigger_previous_dag >> wait_for_previous_dag >> check_s3_files >> create_emr >> wait_for_cluster  >> run_spark_job >> wait_for_spark
check_s3_files >> create_emr >> wait_for_cluster >> run_spark_job >> wait_for_spark
