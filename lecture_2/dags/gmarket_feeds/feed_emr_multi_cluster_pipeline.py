# -*- coding: utf-8 -*-
"""
feed_emr_multi_cluster_pipeline.py
- 100개 URL 병렬 수집 → S3 업로드(스트리밍)
- 8 샤드로 EMR 분산 처리 → 최종 결과 생성
- 한 번에 한 run만 실행(max_active_runs=1)하되, run 내부는 최대 병렬
- Pool/배치 옵션으로 파드 폭주 없이 오토스케일링과 잘 맞물리게 설계
"""

import math
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from kubernetes.client import models as k8s

# ── 기본 설정 ───────────────────────────────────────────────────────────
AWS_CONN_ID = "aws_conn_id"
REGION = Variable.get("AWS_DEFAULT_REGION", default_var="ap-northeast-2")

S3_BUCKET = "fc-practice2"  # 원본/로그
CODE_BUCKET = "fc-practice2"  # Spark 코드
OUTPUT_BUCKET = "fc-practice2"  # 결과물

DEFAULT_MARKET = "gmarket"
SPARK_SCRIPT_S3 = f"s3://{CODE_BUCKET}/scripts/feeds_transform_sharded.py"

BASES = {
    "gmarket": "https://im-ep.gmarket.co.kr",
    "auction": "https://im-ep.auction.co.kr",
}

# ── EMR 설정(필요 시 조정) ──────────────────────────────────────────────
EMR_RELEASE = "emr-6.15.0"
MASTER_INSTANCE = "m6i.xlarge"
CORE_INSTANCE = "m6i.2xlarge"
CORE_COUNT = 4
EMR_KEY_NAME = Variable.get("EMR_KEY_NAME", default_var="test")
EMR_EC2_ROLE = Variable.get("EMR_EC2_ROLE", default_var="EMR_EC2_DefaultRole")
EMR_SERVICE_ROLE = Variable.get("EMR_SERVICE_ROLE", default_var="EMR_DefaultRole")

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

# ── 동시성/배치/풀 ────────────────────────────────────────────────────────
# Pool 이름(Airflow UI → Admin → Pools). 비워두면 미적용.
S3_UPLOAD_POOL = Variable.get("S3_UPLOAD_POOL", default_var="s3_upload_pool")
# 배치 업로드: 0이면 단건 100개를 개별 파드로, >0이면 해당 크기로 묶어 파드 수 감축
FEEDS_UPLOAD_BATCH_SIZE = int(Variable.get("FEEDS_UPLOAD_BATCH_SIZE", default_var="0"))

# ── 경량 파드 오버라이드(모든 태스크 공통) ─────────────────────────────
# ⚠️ Airflow 2.6+ / K8sExecutor 기준의 정석 포맷(dict) 사용
EXECUTOR_CONFIG_LITE = {
    "KubernetesExecutor": {
        "pod_override": {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"labels": {"app": "airflow-task-lite"}},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        # ← 이 name을 템플릿의 메인 컨테이너 이름과 동일하게!
                        "name": "base",
                        "env": [
                            {
                                "name": "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT",
                                "value": "1800",
                            }
                        ],
                        "resources": {
                            "requests": {
                                "cpu": "100m",
                                "memory": "256Mi",
                                "ephemeral-storage": "1Gi",
                            },
                            "limits": {
                                "cpu": "500m",
                                "memory": "512Mi",
                                "ephemeral-storage": "2Gi",
                            },
                        },
                    }
                ],
            },
        }
    }
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="feed_emr_multi_cluster_pipeline",
    default_args=default_args,
    schedule_interval=None,  # 수동 트리거/외부에서만 실행
    catchup=False,
    concurrency=512,  # 한 run 안에서 동시에 돌 수 있는 TI 상한
    max_active_runs=1,  # ✅ run은 항상 1개만(겹치지 않게)
    tags=["feeds", "emr", "spark", "sharded", "parallel"],
)


# ── 유틸 ─────────────────────────────────────────────────────────────────
def _emr_client():
    return AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="emr").get_client_type(
        "emr", region_name=REGION
    )


def build_shards(total_files: int = 100, num_shards: int = 8):
    size = math.ceil(total_files / num_shards)
    shards, start = [], 0
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
    market = DEFAULT_MARKET if not (11 <= hour < 16) else "auction"
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
        "market": market,
        "target_hour": target_hour,
        "target_date": now_kst.format("YYYYMMDD"),
    }


# ── 0) URL/프리픽스 계산 ────────────────────────────────────────────────
@task
def build_urls_and_prefixes() -> dict:
    ctx = get_current_context()
    conf = (ctx.get("dag_run") and ctx["dag_run"].conf) or {}
    info = {**decide_target_info(pendulum.now("Asia/Seoul"))}
    for k in ("market", "target_date", "target_hour"):
        if k in conf:
            info[k] = conf[k]

    base = BASES[info["market"]]
    date = info["target_date"]
    hhmm = f"{int(info['target_hour']):02d}00"
    urls = [f"{base}/google/{date}/{hhmm}/feed_{i:05d}.tsv.gz" for i in range(100)]

    return {
        "info": info,
        "urls": urls,
        "input_prefix": f"feeds/google/{info['market']}",
        "shard_base": f"feeds/{info['market']}/shards",
        "final_base": f"feeds/{info['market']}/final",
    }


@task
def extract_urls(ctx: dict) -> list[str]:
    return ctx["urls"]


@task
def extract_input_prefix(ctx: dict) -> str:
    return ctx["input_prefix"]


@task
def extract_shard_base(ctx: dict) -> str:
    return ctx["shard_base"]


@task
def extract_final_base(ctx: dict) -> str:
    return ctx["final_base"]


# ── 1) 단건 업로드(스트리밍) ────────────────────────────────────────────
def _upload_one_to_s3_python(url: str) -> str:
    import requests
    from urllib.parse import urlparse
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from boto3.s3.transfer import TransferConfig

    filename = url.split("/")[-1]
    host = urlparse(url).netloc.lower()
    market = (
        "gmarket"
        if "gmarket" in host
        else ("auction" if "auction" in host else DEFAULT_MARKET)
    )
    s3_key = f"feeds/google/{market}/{filename}"

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

    h = sess.head(url, timeout=30)
    if h.status_code == 404:
        print(f"[SKIP 404] {url}")
        return f"skip-404:{filename}"
    h.raise_for_status()

    s3_client = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
    tcfg = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=10,
        use_threads=True,
    )
    print(f"[GET→S3] {url} -> s3://{S3_BUCKET}/{s3_key}")
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
    print(f"[DONE] s3://{S3_BUCKET}/{s3_key}")
    return f"s3://{S3_BUCKET}/{s3_key}"


@task(retries=3, retry_delay=timedelta(minutes=2))
def upload_one_to_s3(url: str) -> str:
    return _upload_one_to_s3_python(url)


# ── (옵션) 배치 업로드(파드 수 줄이기) ──────────────────────────────────
@task
def make_chunks(urls: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        # 배치 미사용 → 100개 각각의 단건 태스크가 되도록 "청크"는 url별 1개로 쪼갭니다
        return [[u] for u in urls]
    return [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]


@task(retries=3, retry_delay=timedelta(minutes=2))
def upload_batch_to_s3(urls: list[str]) -> list[str]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_upload_one_to_s3_python, u) for u in urls]
        for f in as_completed(futs):
            results.append(f.result())
    return results


# ── 2) 샤드 EMR 생성/스텝/종료 ──────────────────────────────────────────
@task
def create_emr_for_shard(shard: dict) -> str:
    client = _emr_client()
    resp = client.run_job_flow(
        StepConcurrencyLevel=8,
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


@task
def submit_shard_step(
    cluster_id: str, shard: dict, input_prefix: str, shard_base: str
) -> str:
    client = _emr_client()
    shard_out = f"s3://{OUTPUT_BUCKET}/{shard_base}/{shard['name']}/"
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
        *ICEBERG_CONF,
        SPARK_SCRIPT_S3,
        "--mode",
        "shard",
        "--bucket",
        S3_BUCKET,
        "--input-prefix",
        input_prefix,
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
def submit_final_step(final_cluster_id: str, shard_base: str, final_base: str) -> str:
    client = _emr_client()
    final_out = f"s3://{OUTPUT_BUCKET}/{final_base}/"
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
        shard_base,
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


with dag:
    # ─ URLs/프리픽스(경량 파드)
    ctx = build_urls_and_prefixes.override(executor_config=EXECUTOR_CONFIG_LITE)()
    urls = extract_urls.override(executor_config=EXECUTOR_CONFIG_LITE)(ctx)
    input_pref = extract_input_prefix.override(executor_config=EXECUTOR_CONFIG_LITE)(
        ctx
    )
    shard_base = extract_shard_base.override(executor_config=EXECUTOR_CONFIG_LITE)(ctx)
    final_base = extract_final_base.override(executor_config=EXECUTOR_CONFIG_LITE)(ctx)

    # ─ 업로드(배치 on/off). Pool 있으면 적용 → “가능한 만큼” 병렬로 돌아가다 노드 차면 대기/증설
    pool_arg = {"pool": S3_UPLOAD_POOL} if S3_UPLOAD_POOL else {}
    if FEEDS_UPLOAD_BATCH_SIZE > 0:
        chunks = make_chunks.override(executor_config=EXECUTOR_CONFIG_LITE)(
            urls, FEEDS_UPLOAD_BATCH_SIZE
        )
        staged = (
            upload_batch_to_s3.partial()
            .override(executor_config=EXECUTOR_CONFIG_LITE, **pool_arg)
            .expand(urls=chunks)  # chunks: list[list[str]]
        )
    else:
        staged = (
            upload_one_to_s3.partial()
            .override(executor_config=EXECUTOR_CONFIG_LITE, **pool_arg)
            .expand(url=urls)
        )

    # ─ 샤드 EMR: 업로드가 어느 정도 진행되면 병렬 생성/스텝 투입
    cluster_ids = create_emr_for_shard.override(
        executor_config=EXECUTOR_CONFIG_LITE
    ).expand(shard=SHARDS)
    wait_ready = EmrJobFlowSensor.partial(
        task_id="wait_emr_ready",
        target_states=["WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
        executor_config=EXECUTOR_CONFIG_LITE,
    ).expand(job_flow_id=cluster_ids)
    step_ids = submit_shard_step.override(executor_config=EXECUTOR_CONFIG_LITE).expand(
        cluster_id=cluster_ids,
        shard=SHARDS,
        input_prefix=input_pref,
        shard_base=shard_base,
    )
    wait_steps = EmrStepSensor.partial(
        task_id="wait_shard_steps",
        target_states=["COMPLETED"],
        failed_states=["FAILED", "CANCELLED"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        executor_config=EXECUTOR_CONFIG_LITE,
    ).expand(job_flow_id=cluster_ids, step_id=step_ids)
    terminate_shards = EmrTerminateJobFlowOperator.partial(
        task_id="terminate_shard_clusters",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
        executor_config=EXECUTOR_CONFIG_LITE,
    ).expand(job_flow_id=cluster_ids)

    # ─ 최종 EMR
    final_cluster = create_emr_final.override(executor_config=EXECUTOR_CONFIG_LITE)()

    # ✅ 추가 1: 최종 클러스터는 샤드 종료 후에 생성되도록
    final_cluster.set_upstream(terminate_shards)

    wait_final_ready = EmrJobFlowSensor(
        task_id="wait_final_emr_ready",
        job_flow_id=final_cluster,
        target_states=["WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    final_step = submit_final_step.override(executor_config=EXECUTOR_CONFIG_LITE)(
        final_cluster, shard_base, final_base
    )

    # ✅ 추가 2: WAITING 감지 후에만 최종 step 제출
    final_step.set_upstream(wait_final_ready)

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
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    terminate_final = EmrTerminateJobFlowOperator(
        task_id="terminate_final_cluster",
        job_flow_id=final_cluster,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    # ─ 의존성(명시 체인으로 깔끔하게)
    # terminate_shards → final_cluster → wait_final_ready → final_step → wait_final → terminate_final
    wait_final_ready.set_upstream(final_cluster)
    wait_final.set_upstream(final_step)
    terminate_final.set_upstream(wait_final)

    (
        terminate_shards
        >> final_cluster
        >> wait_final_ready
        >> final_step
        >> wait_final
        >> terminate_final
    )
