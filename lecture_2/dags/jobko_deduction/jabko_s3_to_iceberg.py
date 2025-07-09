from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from datetime import datetime, timedelta
from airflow.models import Variable
# from plugins import slack # 필요시 slack 임포트

# S3 버킷 경로 및 Airflow Connection ID
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,
}

dag = DAG(
    # 이전 DAG에서 호출한 ID와 동일하게 설정
    "jabko_iceberg_to_s3_emr",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# 1. EMR 클러스터 생성
def create_emr_cluster(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")
    response = client.run_job_flow(
        Name="jabko-csv-to-iceberg-cluster", # 클러스터 이름
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [
                {"Name": "Master node", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
                {"Name": "Worker nodes", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
            ],
            "Ec2KeyName": "test",
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BUCKET}/emr-logs/",
        AutoTerminationPolicy={"IdleTimeout": 600}, # 10분 후 자동 종료
        VisibleToAllUsers=True,
    )
    cluster_id = response["JobFlowId"]
    kwargs["ti"].xcom_push(key="emr_cluster_id", value=cluster_id)
    print(f"EMR 클러스터 생성 완료: {cluster_id}")

# 2. EMR 클러스터가 'WAITING' 상태가 될 때까지 대기
wait_for_emr = EmrJobFlowSensor(
    task_id='wait_for_emr_cluster_ready',
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='emr_cluster_id') }}",
    target_states=['WAITING'],
    failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=30,
    timeout=60 * 10,
    mode='reschedule',
    dag=dag,
)

# 3. S3에 잡코리아 CSV 파일이 존재하는지 확인
def check_s3_file(**kwargs):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    target_date = kwargs["dag_run"].conf.get("target_date")
    
    # 잡코리아 파일 경로
    key = f"deduction-csv/date={target_date}/final_attachment.csv"
    
    if not hook.check_for_key(key, bucket_name=S3_BUCKET):
        raise FileNotFoundError(f"S3에 해당 파일이 존재하지 않습니다: s3://{S3_BUCKET}/{key}")
    print(f"S3 파일 존재 확인 완료: s3://{S3_BUCKET}/{key}")

check_s3_file_task = PythonOperator(
    task_id="check_s3_file_for_jabko",
    python_callable=check_s3_file,
    dag=dag,
)

# 4. EMR에 PySpark 작업 제출
def submit_spark_job(**kwargs):
    import boto3
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    target_date = kwargs["dag_run"].conf.get("target_date")
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr_cluster", key="emr_cluster_id")
    
    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": "Process Jabko CSV to Iceberg", # Step 이름
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "--conf", "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
                    "--conf", "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                    "--conf", "spark.sql.catalog.glue.warehouse=s3a://gyoung0-test/iceberg_warehouse/",
                    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "--conf", f"spark.hadoop.yesterday={target_date}",
                    "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                    "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
                    # 잡코리아 PySpark 스크립트 경로
                    "s3://gyoung0-test/scripts/csv_to_iceberg.py"
                ],
            },
        }],
    )
    step_id = response["StepIds"][0]
    kwargs["ti"].xcom_push(key="spark_step_id", value=step_id)
    print(f"PySpark 작업 제출 완료: Step ID = {step_id}")

# 5. Spark 작업이 완료될 때까지 대기
wait_for_spark_job = EmrStepSensor(
    task_id="wait_for_spark_job_to_complete",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='emr_cluster_id') }}",
    step_id="{{ ti.xcom_pull(task_ids='submit_spark_job', key='spark_step_id') }}",
    target_states=["COMPLETED"],
    failed_states=["FAILED", "CANCELLED"],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=30,
    timeout=60 * 20,
    mode="reschedule",
    dag=dag,
)

# 6. EMR 클러스터 종료
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='emr_cluster_id') }}",
    aws_conn_id=AWS_CONN_ID,
    trigger_rule="all_done",
    dag=dag,
)

# Operator 인스턴스화
create_emr_cluster_task = PythonOperator(
    task_id="create_emr_cluster",
    python_callable=create_emr_cluster,
    dag=dag,
)

submit_spark_job_task = PythonOperator(
    task_id="submit_spark_job",
    python_callable=submit_spark_job,
    dag=dag,
)

# 태스크 의존성 설정
create_emr_cluster_task >> check_s3_file_task >> wait_for_emr >> submit_spark_job_task >> wait_for_spark_job >> terminate_emr_cluster