# -*- coding: utf-8 -*-
"""
feed_emr_multi_cluster_pipeline.py
- 100개 파일을 8개 샤드로 나눠 '여러 개'의 EMR 클러스터를 병렬 실행
- 각 클러스터는 Spark step 완료 후 자동 종료
- Spark 스크립트: s3://{CODE_BUCKET}/scripts/feeds_transform_sharded.py
"""

from datetime import timedelta
import math
import boto3
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

# ===== 환경값 =====
AWS_CONN_ID = "aws_conn_id"
REGION = Variable.get("AWS_DEFAULT_REGION", default_var="ap-northeast-2")

S3_BUCKET = "gyoung0-test"  # 원본/로그 버킷(예시)
CODE_BUCKET = "gyoung0-test"  # PySpark 스크립트 저장 버킷
OUTPUT_BUCKET = "gyoung0-test"  # 결과 저장 버킷

# 입력 위치: s3://{S3_BUCKET}/feeds/google/{MARKET}/feed_00000.tsv.gz ...
INPUT_PREFIX = "feeds/google"
MARKET = "gmarket"  # 필요하면 동적으로 바꾸세요

# PySpark 스크립트 경로
SPARK_SCRIPT_S3 = f"s3://{CODE_BUCKET}/scripts/feeds_transform_sharded.py"

# EMR 설정
EMR_RELEASE = "emr-6.15.0"
MASTER_INSTANCE = "m6i.xlarge"
CORE_INSTANCE = "m6i.2xlarge"
CORE_COUNT = 4  # 필요 시 4→6으로 점진 상향 권장
EMR_KEY_NAME = Variable.get("EMR_KEY_NAME", default_var="test")
EMR_EC2_ROLE = Variable.get("EMR_EC2_ROLE", default_var="EMR_EC2_DefaultRole")
EMR_SERVICE_ROLE = Variable.get("EMR_SERVICE_ROLE", default_var="EMR_DefaultRole")

# Iceberg/Glue 카탈로그가 필요 없으면 ICEBERG_CONF를 제거하세요.
ICEBERG_CONF = [
    "--conf",
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf",
    "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
    "--conf",
    "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf",
    f"spark.sql.catalog.glue.warehouse=s3a://{S3_BUCKET}/iceberg_warehouse/",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="feed_emr_multi_cluster_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["feeds", "emr", "spark", "sharded", "parallel"],
)


# ---------- 유틸: 100개를 N개 샤드로 균등 분할 ----------
def build_shards(total_files: int = 100, num_shards: int = 8):
    size = math.ceil(total_files / num_shards)  # 100/8 = 13
    shards = []
    start = 0
    for i in range(num_shards):
        end = min(total_files - 1, start + size - 1)
        shards.append({"name": f"shard{i}", "start": start, "end": end})
        start = end + 1
        if start >= total_files:
            break
    return shards


SHARDS = build_shards(100, 8)  # ← 여기서 샤드 수 변경 가능


# ---------- 0. 사전 체크 ----------
@task(dag=dag)
def check_s3_inputs():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    # 샘플 파일 1개만 확인 (원하면 전체 목록 점검 로직으로 확장)
    key = f"{INPUT_PREFIX}/{MARKET}/feed_00000.tsv.gz"
    if not hook.check_for_key(key, bucket_name=S3_BUCKET):
        raise FileNotFoundError(f"Missing: s3://{S3_BUCKET}/{key}")
    return True


# ---------- 1. 샤드별 EMR 클러스터 생성 ----------
@task(dag=dag)
def create_emr_for_shard(shard: dict) -> str:
    client = boto3.client("emr", region_name=REGION)
    resp = client.run_job_flow(
        Name=f"feeds-emr-{shard['name']}",
        ReleaseLabel=EMR_RELEASE,
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": MASTER_INSTANCE,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": CORE_INSTANCE,
                    "InstanceCount": CORE_COUNT,
                },
            ],
            "Ec2KeyName": EMR_KEY_NAME,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        JobFlowRole=EMR_EC2_ROLE,
        ServiceRole=EMR_SERVICE_ROLE,
        LogUri=f"s3://{S3_BUCKET}/emr-logs/{shard['name']}/",
        AutoTerminationPolicy={"IdleTimeout": 600},
        VisibleToAllUsers=True,
    )
    return resp["JobFlowId"]


# ---------- 2. EMR 대기 ----------
wait_ready = EmrJobFlowSensor.partial(
    task_id="wait_emr_ready",
    target_states=["WAITING"],
    failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=30,
    timeout=60 * 20,
    mode="reschedule",
    dag=dag,
)


# ---------- 3. 샤드별 Spark step 제출 ----------
@task(dag=dag)
def submit_spark_step(cluster_id: str, shard: dict) -> str:
    client = boto3.client("emr", region_name=REGION)
    # 결과 경로 (날짜 경로는 필요 시 KST/실행일자 등으로 교체)
    output_path = f"s3://{OUTPUT_BUCKET}/feeds/{MARKET}/{shard['name']}/"

    args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",
        # 동적 할당 & 튜닝
        "--conf",
        "spark.dynamicAllocation.enabled=true",
        "--conf",
        "spark.dynamicAllocation.initialExecutors=4",
        "--conf",
        "spark.dynamicAllocation.minExecutors=4",
        "--conf",
        "spark.dynamicAllocation.maxExecutors=64",
        "--conf",
        "spark.executor.cores=4",
        "--conf",
        "spark.executor.memory=8g",
        "--conf",
        "spark.sql.adaptive.enabled=true",
        "--conf",
        "spark.sql.files.maxPartitionBytes=134217728",
        "--conf",
        "spark.sql.shuffle.partitions=800",
        # (아이스버그 필요 없으면 제거)
        *ICEBERG_CONF,
        # 제출 스크립트 & 파라미터
        SPARK_SCRIPT_S3,
        "--bucket",
        S3_BUCKET,
        "--input-prefix",
        f"{INPUT_PREFIX}/{MARKET}",
        "--start-idx",
        str(shard["start"]),
        "--end-idx",
        str(shard["end"]),
        "--output",
        output_path,
        "--max-records",
        "400000",
        "--dedupe-key",
        "itemId,ordNo",
        "--format",
        "parquet",
        "--target-files",
        "8",
    ]

    resp = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": f"feeds-transform-{shard['name']}",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": args},
            }
        ],
    )
    return resp["StepIds"][0]


# ---------- 4. Spark step 완료 대기 ----------
wait_steps = EmrStepSensor.partial(
    task_id="wait_spark_steps",
    target_states=["COMPLETED"],
    failed_states=["FAILED", "CANCELLED"],
    aws_conn_id=AWS_CONN_ID,
    poke_interval=60,
    timeout=60 * 60,
    mode="reschedule",
    dag=dag,
)

# ---------- 5. 샤드별 클러스터 종료 ----------
terminate = EmrTerminateJobFlowOperator.partial(
    task_id="terminate_emr_clusters",
    aws_conn_id=AWS_CONN_ID,
    trigger_rule="all_done",
    dag=dag,
)

# ===== DAG 의존성 =====
chk = check_s3_inputs()

cluster_ids = create_emr_for_shard.expand(shard=SHARDS)
chk >> cluster_ids

wait_ready_eachs = wait_ready.expand(job_flow_id=cluster_ids)

step_ids = submit_spark_step.expand(cluster_id=cluster_ids, shard=SHARDS)
step_ids.set_upstream(wait_ready_eachs)

wait_done_eachs = wait_steps.expand(job_flow_id=cluster_ids, step_id=step_ids)

term_eachs = terminate.expand(job_flow_id=cluster_ids)
term_eachs.set_upstream(wait_done_eachs)
