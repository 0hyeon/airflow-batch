from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models import Variable
# from plugins import slack
import boto3

S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": slack.on_failure_callback,
}

dag = DAG(
    "ssg_parquet_to_s3_emr",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# 1. Create EMR cluster
def create_emr_cluster(**kwargs):
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")

    response = client.run_job_flow(
        Name="txt-to-iceberg-emr-cluster",
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
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BUCKET}/emr-logs/",
        AutoTerminationPolicy={"IdleTimeout": 600},
        VisibleToAllUsers=True,
    )

    cluster_id = response["JobFlowId"]
    kwargs["ti"].xcom_push(key="emr_cluster_id", value=cluster_id)
    print(f"Created EMR cluster: {cluster_id}")

create_emr = PythonOperator(
    task_id="create_emr",
    python_callable=create_emr_cluster,
    provide_context=True,
    dag=dag,
)

# 2. Check S3 file existence
def check_s3_files(**kwargs):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    required_keys = [
        "ssg-raw-data/ssg_all_full.txt",
        "ssg-raw-data/e_all_full.txt"
    ]
    for key in required_keys:
        if not hook.check_for_key(key, bucket_name=S3_BUCKET):
            raise FileNotFoundError(f"Missing file in S3: s3://{S3_BUCKET}/{key}")
        print(f"Confirmed file exists: s3://{S3_BUCKET}/{key}")

check_s3_file_task = PythonOperator(
    task_id="check_s3_files",
    python_callable=check_s3_files,
    provide_context=True,
    dag=dag,
)

# 3. Wait for EMR ready
wait_for_emr = EmrJobFlowSensor(
    task_id="wait_for_emr_cluster",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr', key='emr_cluster_id') }}",
    target_states=["WAITING"],
    failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=30,
    timeout=600,
    mode="reschedule",
    dag=dag,
)

# 4. Submit Spark job
def submit_spark_job(**kwargs):
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    client = session.client("emr")
    cluster_id = kwargs["ti"].xcom_pull(task_ids="create_emr", key="emr_cluster_id")

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "Convert TXT to Iceberg",
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
                        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
                        "s3://gyoung0-test/scripts/txt_to_parquet.py",
                    ],
                },
            }
        ],
    )

    step_id = response["StepIds"][0]
    kwargs["ti"].xcom_push(key="spark_step_id", value=step_id)
    print(f"Submitted Spark job with step ID: {step_id}")

run_spark_job = PythonOperator(
    task_id="run_spark_job",
    python_callable=submit_spark_job,
    provide_context=True,
    dag=dag,
)

# 5. Wait for Spark step
wait_for_spark = EmrStepSensor(
    task_id="wait_for_spark_step",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr', key='emr_cluster_id') }}",
    step_id="{{ ti.xcom_pull(task_ids='run_spark_job', key='spark_step_id') }}",
    target_states=["COMPLETED"],
    failed_states=["FAILED", "CANCELLED"],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=60,
    timeout=60 * 40,
    mode="reschedule",
    dag=dag,
)

# 6. Terminate cluster
terminate_emr = EmrTerminateJobFlowOperator(
    task_id="terminate_emr",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr', key='emr_cluster_id') }}",
    aws_conn_id=AWS_CONN_ID,
    trigger_rule="all_done",
    dag=dag,
)

# DAG chain
create_emr >> check_s3_file_task >> wait_for_emr >> run_spark_job >> wait_for_spark >> terminate_emr



trigger_rename_dag = TriggerDagRunOperator(
    task_id="trigger_rename_parquet_dag",
    trigger_dag_id="ssg_rename_parquet_with_multipart",
    wait_for_completion=False,
    dag=dag,
)

terminate_emr >> trigger_rename_dag