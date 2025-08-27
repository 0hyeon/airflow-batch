# -*- coding: utf-8 -*-
"""
feed_emr_multi_cluster_pipeline.py
- 100개 URL을 병렬로 받아 S3에 업로드(스트리밍)
- 8개 샤드로 나눠 '여러 개'의 EMR 클러스터에서 Spark 처리 (샤드 개수 조절 가능)
- 마지막에 단일 EMR로 '전역 40만 cap & publish' 실행(.tsv.gz 확장자까지 보장)
"""

import math
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

# ========= 환경 =========
AWS_CONN_ID = "aws_conn_id"
REGION = Variable.get("AWS_DEFAULT_REGION", default_var="ap-northeast-2")

S3_BUCKET = "gyoung0-test"  # 원본/로그 버킷
CODE_BUCKET = "gyoung0-test"  # Spark 스크립트 저장 버킷
OUTPUT_BUCKET = "gyoung0-test"  # 처리 결과 버킷

MARKET = "gmarket"  # 필요 시 'auction' 등 동적 선택 가능
INPUT_PREFIX = f"feeds/google/{MARKET}"  # s3://{S3_BUCKET}/feeds/google/gmarket/
SHARD_BASE = f"feeds/{MARKET}/shards"  # s3://{OUTPUT_BUCKET}/feeds/gmarket/shards/
FINAL_BASE = f"feeds/{MARKET}/final"  # s3://{OUTPUT_BUCKET}/feeds/gmarket/final/
SPARK_SCRIPT_S3 = f"s3://{CODE_BUCKET}/scripts/feeds_transform_sharded.py"

# 원본 URL(예시 경로) — 실제 YYYYMMDD/HHMM 규칙에 맞춰 build_urls()에서 생성
BASES = {
    "gmarket": "https://im-ep.gmarket.co.kr",
    "auction": "https://im-ep.auction.co.kr",
}

# EMR 설정
EMR_RELEASE = "emr-6.15.0"
MASTER_INSTANCE = "m6i.xlarge"
CORE_INSTANCE = "m6i.2xlarge"
CORE_COUNT = 4
EMR_KEY_NAME = Variable.get("EMR_KEY_NAME", default_var="test")
EMR_EC2_ROLE = Variable.get("EMR_EC2_ROLE", default_var="EMR_EC2_DefaultRole")
EMR_SERVICE_ROLE = Variable.get("EMR_SERVICE_ROLE", default_var="EMR_DefaultRole")

# 필요 없으면 ICEBERG_CONF 제거
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


# ---------- 유틸 ----------
def _emr_client():
    return AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="emr").get_client_type(
        "emr", region_name=REGION
    )


def build_shards(total_files: int = 100, num_shards: int = 8):
    size = math.ceil(total_files / num_shards)  # 100/8=13
    shards = []
    start = 0
    for i in range(num_shards):
        end = min(total_files - 1, start + size - 1)
        shards.append({"name": f"shard{i}", "start": start, "end": end})
        start = end + 1
        if start >= total_files:
            break
    return shards


SHARDS = build_shards(100, 8)


def decide_target_info(now_kst: pendulum.DateTime) -> dict:
    hour = now_kst.hour
    market = "gmarket" if not (11 <= hour < 16) else "auction"

    if 11 <= hour < 16:
        target_hour = "11"
    elif 4 <= hour < 10:
        target_hour = "04"
    elif 10 <= hour < 11:
        target_hour = "10"
    elif 16 <= hour < 22:
        target_hour = "16"
    else:
        target_hour = "22"

    return {
        "market": market,  # "gmarket" or "auction"
        "target_hour": target_hour,  # "04" | "10" | "11" | "16" | "22"
        "target_date": now_kst.format("YYYYMMDD"),  # "YYYYMMDD"
    }


# ---------- 0) 100개 URL 생성 ----------
@task
def build_urls() -> list[str]:
    info = decide_target_info(pendulum.now("Asia/Seoul"))
    base = BASES[info["market"]]
    date = info["target_date"]  # e.g. 20250827
    hour_str = f"{int(info['target_hour']):02d}00"  # "0400","1000","1100","1600","2200"
    urls = [f"{base}/google/{date}/{hour_str}/feed_{i:05d}.tsv.gz" for i in range(100)]
    return urls


# ---------- 1) 업로드(병렬) ----------
@task(retries=3, retry_delay=timedelta(minutes=2))
def upload_one_to_s3(url: str) -> str:
    import requests, boto3
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config

    filename = url.split("/")[-1]  # feed_00000.tsv.gz
    s3_key = f"{INPUT_PREFIX}/{filename}"  # feeds/google/gmarket/feed_00000.tsv.gz

    sess = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    # 빠른 404 체크
    h = sess.head(url, timeout=30)
    if h.status_code == 404:
        return f"skip-404:{filename}"
    h.raise_for_status()

    s3_client = boto3.client("s3", region_name=REGION, config=Config())
    tcfg = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=10,
        use_threads=True,
    )

    with sess.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        r.raw.decode_content = True
        s3_client.upload_fileobj(
            Fileobj=r.raw,
            Bucket=S3_BUCKET,
            Key=s3_key,
            Config=tcfg,
            ExtraArgs={
                "ContentType": "text/tab-separated-values",
                "ContentEncoding": "gzip",
            },
        )
    return f"s3://{S3_BUCKET}/{s3_key}"


# ---------- 2) 샤드별 EMR 생성 ----------
@task
def create_emr_for_shard(shard: dict) -> str:
    client = _emr_client()
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


# ---------- 3) 샤드별 Spark 제출 ----------
@task
def submit_shard_step(cluster_id: str, shard: dict) -> str:
    client = _emr_client()
    shard_out = f"s3://{OUTPUT_BUCKET}/{SHARD_BASE}/{shard['name']}/"

    args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",
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
        *ICEBERG_CONF,  # 불필요하면 제거
        SPARK_SCRIPT_S3,
        "--mode",
        "shard",
        "--bucket",
        S3_BUCKET,
        "--input-prefix",
        INPUT_PREFIX,  # feeds/google/gmarket
        "--start-idx",
        str(shard["start"]),
        "--end-idx",
        str(shard["end"]),
        "--output",
        shard_out,
        "--max-records",
        "400000",
        "--dedupe-key",
        "itemId,ordNo",
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


# ---------- 4) 최종 전역 40만 cap & publish ----------
@task
def create_emr_final() -> str:
    client = _emr_client()
    resp = client.run_job_flow(
        Name="feeds-emr-final-cap",
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
        LogUri=f"s3://{S3_BUCKET}/emr-logs/final/",
        AutoTerminationPolicy={"IdleTimeout": 600},
        VisibleToAllUsers=True,
    )
    return resp["JobFlowId"]


@task
def submit_final_step(final_cluster_id: str) -> str:
    client = _emr_client()
    final_out = f"s3://{OUTPUT_BUCKET}/{FINAL_BASE}/"

    args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",
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
        SPARK_SCRIPT_S3,
        "--mode",
        "final",
        "--bucket",
        OUTPUT_BUCKET,
        "--input-prefix",
        SHARD_BASE,  # shards/ 이하 *.tsv.gz 읽기
        "--output",
        final_out,
        "--max-records",
        "400000",
        "--dedupe-key",
        "itemId,ordNo",
        "--target-files",
        "8",
    ]
    resp = client.add_job_flow_steps(
        JobFlowId=final_cluster_id,
        Steps=[
            {
                "Name": "feeds-final-cap",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": args},
            }
        ],
    )
    return resp["StepIds"][0]


# ===== DAG 플로우 =====
with dag:
    urls = build_urls()
    staged = upload_one_to_s3.partial().expand(url=urls)

    # 샤드 클러스터 생성/대기/실행/대기/종료
    cluster_ids = create_emr_for_shard.expand(shard=SHARDS)

    wait_ready = EmrJobFlowSensor.partial(
        task_id="wait_emr_ready",
        target_states=["WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
    ).expand(job_flow_id=cluster_ids)

    step_ids = submit_shard_step.expand(cluster_id=cluster_ids, shard=SHARDS)
    step_ids.set_upstream(wait_ready)

    wait_steps = EmrStepSensor.partial(
        task_id="wait_shard_steps",
        target_states=["COMPLETED"],
        failed_states=["FAILED", "CANCELLED"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    ).expand(job_flow_id=cluster_ids, step_id=step_ids)

    terminate_shards = EmrTerminateJobFlowOperator.partial(
        task_id="terminate_shard_clusters",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    ).expand(job_flow_id=cluster_ids)

    # 최종 전역 40만 cap & publish (샤드 종료 후 실행)
    final_cluster = create_emr_final()
    wait_final_ready = EmrJobFlowSensor(
        task_id="wait_final_emr_ready",
        job_flow_id=final_cluster,
        target_states=["WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
    )
    final_step = submit_final_step(final_cluster)
    wait_final = EmrStepSensor(
        task_id="wait_final_step",
        job_flow_id=final_cluster,
        step_id=final_step,
        target_states=["COMPLETED"],
        failed_states=["FAILED", "CANCELLED"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )
    terminate_final = EmrTerminateJobFlowOperator(
        task_id="terminate_final_cluster",
        job_flow_id=final_cluster,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )

    # 의존성
    staged >> cluster_ids
    wait_final_ready.set_upstream(terminate_shards)
    final_step.set_upstream(wait_final_ready)
    wait_final.set_upstream(final_step)
    terminate_final.set_upstream(wait_final)
