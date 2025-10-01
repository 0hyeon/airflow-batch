from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
import boto3
import os
import time
from urllib.parse import urlparse

# --- 사용자 설정 변수 ---
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"
EMR_EC2_KEY_NAME = "test"
SCRIPT_S3_KEY = "scripts/gmc_process_feeds_final.py"
LOG_S3_PATH = f"s3://{S3_BUCKET}/emr-logs/"
CUSTOM_LABEL_4_SCRIPT_S3_KEY = "scripts/custom_label_4_update.py"
COMBINE_SCRIPT_S3_KEY = "scripts/gmc_combine_files.py"


@dag(
    dag_id="gmc_gmarket_auction_emr_process_and_rename_final_v2",
    start_date=pendulum.datetime(2025, 8, 29, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["emr", "gmc", "final"],
    default_args={"owner": "airflow", "aws_conn_id": AWS_CONN_ID},
)
def emr_process_and_rename_final_dag():

    @task
    def create_emr_cluster(dag_run: DagRun):
        market = dag_run.conf.get("market", "gmarket")
        cluster_name = f"process-{market}-feeds-cluster-final"
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )
        response = client.run_job_flow(
            Name=cluster_name,
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
                        "InstanceType": "m5.2xlarge",
                        "InstanceCount": 8,
                    },
                ],
                "Ec2KeyName": EMR_EC2_KEY_NAME,
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
            },
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            LogUri=LOG_S3_PATH,
            VisibleToAllUsers=True,
        )
        return response["JobFlowId"]

    @task
    def run_conditional_custom_label_4_job(cluster_id: str, dag_run: DagRun):
        market = dag_run.conf.get("market", "gmarket").lower()
        now = pendulum.now("Asia/Seoul")

        if not (
            (market == "gmarket" and 10 <= now.hour < 13)
            or (market == "auction" and 11 <= now.hour < 14)
        ):
            print("Time condition NOT MET. Skipping custom_label_4 job.")
            return

        print("Time condition MET. Submitting and waiting for custom_label_4 job...")
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )

        input_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC"
        output_s3_path = (
            f"s3://{S3_BUCKET}/feeds/filtering_rules/{market}/custom_label_4_output"
        )
        spark_submit_args = [
            "spark-submit",
            f"s3://{S3_BUCKET}/{CUSTOM_LABEL_4_SCRIPT_S3_KEY}",
            "--input-path",
            input_s3_path,
            "--output-path",
            output_s3_path,
        ]
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"Conditional_Update_CL4_{market}",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args,
                    },
                }
            ],
        )
        step_id = response["StepIds"][0]
        print(f"Submitted step with ID: {step_id}. Polling for completion...")

        while True:
            step_status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
            status = step_status["Step"]["Status"]["State"]
            if status in ["COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]:
                break
            time.sleep(30)

        if status != "COMPLETED":
            raise AirflowException(f"EMR step {step_id} failed with status: {status}")

        print(f"Step {step_id} completed successfully.")
        return

    @task(multiple_outputs=True)
    def submit_spark_job(cluster_id: str, dag_run: DagRun):
        market = dag_run.conf.get("market", "gmarket")
        input_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC"
        temp_output_path_1 = (
            f"s3://{S3_BUCKET}/feeds/google/{market}/GMC_processed_temp_1"
        )
        whitelist_s3_path = f"s3://{S3_BUCKET}/feeds/white_list/{market}/"
        previous_output_path = (
            f"s3://{S3_BUCKET}/feeds/google/{market}/GMC_processed_final"
        )
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=s3_hook.conn_region_name,
        )
        spark_configs = [
            "--conf",
            "spark.driver.memory=8g",
            "--conf",
            "spark.executor.memory=20g",
            "--conf",
            "spark.executor.cores=4",
            "--conf",
            "spark.default.parallelism=800",
            "--conf",
            "spark.sql.shuffle.partitions=800",
            "--conf",
            "spark.sql.adaptive.enabled=true",
            "--conf",
            "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf",
            "spark.sql.adaptive.skewJoin.enabled=true",
            "--conf",
            "spark.driver.maxResultSize=4g",
        ]
        spark_submit_args = [
            "spark-submit",
            *spark_configs,
            f"s3://{S3_BUCKET}/{SCRIPT_S3_KEY}",
            "--market",
            market,
            "--full-feed-path",
            input_s3_path,
            "--whitelist-path",
            whitelist_s3_path,
            "--previous-output-path",
            previous_output_path,
            "--output-path",
            temp_output_path_1,
        ]
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Main_Processing_Job",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args,
                    },
                }
            ],
        )
        return {
            "step_id": response["StepIds"][0],
            "temp_path_1": temp_output_path_1,
            "market": market,
        }

    @task(multiple_outputs=True)
    def submit_combine_job(cluster_id: str, temp_path_1: str, market: str):
        temp_output_path_2 = (
            f"s3://{S3_BUCKET}/feeds/google/{market}/GMC_processed_temp_2"
        )
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
            f"s3://{S3_BUCKET}/{COMBINE_SCRIPT_S3_KEY}",
            "--input",
            temp_path_1,
            "--output",
            temp_output_path_2,
            "--market",
            market,
        ]
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Combine_Files_Job",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args,
                    },
                }
            ],
        )
        return {"step_id": response["StepIds"][0], "temp_path_2": temp_output_path_2}

    @task
    def rename_final_files(output_path: str, market: str):
        print(f"Final renaming process started for: {output_path}")
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_client = s3_hook.get_conn()

        parsed_url = urlparse(output_path)
        bucket_name = parsed_url.netloc
        final_base_prefix = f"feeds/google/{market}/GMC_processed_final"

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket_name, Prefix=f"{parsed_url.path.lstrip('/')}/", Delimiter="/"
        ):
            if page.get("CommonPrefixes") is None:
                continue
            for subdir in page.get("CommonPrefixes", []):
                subdir_prefix = subdir.get("Prefix")
                part_file_key = None
                for obj in s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=subdir_prefix
                ).get("Contents", []):
                    if "part-" in obj["Key"]:
                        part_file_key = obj["Key"]
                        break
                if not part_file_key:
                    continue
                partition_id = subdir_prefix.split("=")[1].rstrip("/")
                new_filename = f"{market}_feed_{partition_id.zfill(3)}.txt.gz"
                new_key = os.path.join(final_base_prefix, new_filename)
                print(f"  - Renaming '{part_file_key}' to '{new_key}'")
                s3_client.copy_object(
                    CopySource={"Bucket": bucket_name, "Key": part_file_key},
                    Bucket=bucket_name,
                    Key=new_key,
                )

        print(f"  - Deleting original temp folder: {output_path}")
        keys_to_delete = s3_hook.list_keys(
            bucket_name=bucket_name, prefix=parsed_url.path.lstrip("/")
        )
        if keys_to_delete:
            s3_hook.delete_objects(bucket=bucket_name, keys=keys_to_delete)
        print("✅ Final renaming and cleanup complete.")

    # --- Task 인스턴스 생성 및 의존성 설정 ---

    trigger_sftp_upload_dag = TriggerDagRunOperator(
        task_id="trigger_sftp_upload_dag",
        trigger_dag_id="upload_feeds_to_sftp_dynamically",
        conf={"market": "{{ ti.xcom_pull(task_ids='submit_spark_job')['market'] }}"},
        wait_for_completion=False,
    )

    cluster_id = create_emr_cluster()
    wait_for_cluster = EmrJobFlowSensor(
        task_id="wait_for_cluster",
        job_flow_id=cluster_id,
        target_states=["WAITING"],
        aws_conn_id=AWS_CONN_ID,
    )

    conditional_job = run_conditional_custom_label_4_job(cluster_id)

    main_spark_job_info = submit_spark_job(cluster_id)
    wait_for_main_step = EmrStepSensor(
        task_id="wait_for_main_step",
        job_flow_id=cluster_id,
        step_id=main_spark_job_info["step_id"],
        aws_conn_id=AWS_CONN_ID,
    )

    combine_job_info = submit_combine_job(
        cluster_id=cluster_id,
        temp_path_1=main_spark_job_info["temp_path_1"],
        market=main_spark_job_info["market"],
    )
    wait_for_combine_step = EmrStepSensor(
        task_id="wait_for_combine_step",
        job_flow_id=cluster_id,
        step_id=combine_job_info["step_id"],
        aws_conn_id=AWS_CONN_ID,
    )

    rename_files = rename_final_files(
        output_path=combine_job_info["temp_path_2"],
        market=main_spark_job_info["market"],
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=cluster_id,
        trigger_rule="all_done",
        aws_conn_id=AWS_CONN_ID,
    )

    # --- 의존성 설정 ---
    wait_for_cluster >> conditional_job >> main_spark_job_info
    (
        main_spark_job_info
        >> wait_for_main_step
        >> combine_job_info
        >> wait_for_combine_step
    )
    wait_for_combine_step >> [terminate_cluster, rename_files]
    rename_files >> trigger_sftp_upload_dag


emr_process_and_rename_final_dag()
