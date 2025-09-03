# -*- coding: utf-8 -*-
"""
G마켓/옥션 Google Feeds → S3 TSV 업로드 (직접 스트리밍)
- header.tsv + feed_00000..00099.tsv.gz
- URL 별로 매핑(expand)하여 파드 N개 동시 실행 (s3_upload_pool 슬롯으로 제어)
- 모든 태스크는 경량 파드로 오버라이드 (K8sExecutor)
- 완료 후 EMR DAG 트리거
"""

import logging
from datetime import timedelta
from typing import List

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ===================== 상수 =====================
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"
AWS_REGION = "ap-northeast-2"

# 네트워크/업로드 튜닝
HTTP_TIMEOUT_SEC = 300
HTTP_POOL_SIZE = 50  # requests Connection pool
RETRY_TOTAL = 3

log = logging.getLogger(__name__)

# ── 경량 파드 오버라이드 (모든 태스크 공통) ──────────────────────────────
# * 꼭 dict 형태로 "KubernetesExecutor" → "pod_override"
# ── imports ─────────────────────────────────────────────────────────
from kubernetes.client import (
    V1Pod,
    V1ObjectMeta,
    V1PodSpec,
    V1Affinity,
    V1PodAntiAffinity,
    V1WeightedPodAffinityTerm,
    V1PodAffinityTerm,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1Container,
    V1ResourceRequirements,
    V1EnvVar,
)

# 추가 import
from kubernetes import client as k8s

_pod = k8s.V1Pod(
    api_version="v1",
    kind="Pod",
    metadata=k8s.V1ObjectMeta(labels={"app": "airflow-task-lite", "role": "lite"}),
    spec=k8s.V1PodSpec(
        restart_policy="Never",
        affinity=k8s.V1Affinity(
            pod_anti_affinity=k8s.V1PodAntiAffinity(
                preferred_during_scheduling_ignored_during_execution=[
                    k8s.V1WeightedPodAffinityTerm(
                        weight=100,
                        pod_affinity_term=k8s.V1PodAffinityTerm(
                            label_selector=k8s.V1LabelSelector(
                                match_expressions=[
                                    k8s.V1LabelSelectorRequirement(
                                        key="role",
                                        operator="In",
                                        values=["lite", "heavy"],
                                    )
                                ]
                            ),
                            topology_key="kubernetes.io/hostname",
                        ),
                    )
                ]
            )
        ),
        containers=[
            k8s.V1Container(
                name="base",  # pod_template_file(/airflow-pod.yaml)의 컨테이너명과 동일해야 함
                resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": "300m",
                        "memory": "512Mi",
                        "ephemeral-storage": "1Gi",
                    },
                    limits={
                        "cpu": "1000m",
                        "memory": "1Gi",
                        "ephemeral-storage": "2Gi",
                    },
                ),
                env=[
                    k8s.V1EnvVar(
                        name="AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", value="1800"
                    )
                ],
            )
        ],
    ),
)

_pod_dict = k8s.ApiClient().sanitize_for_serialization(_pod)

EXECUTOR_CONFIG_LITE = {
    # Airflow 2.10에서는 이 키가 정식
    "kubernetes": {"pod_override": _pod_dict},
    # (옵션) 혹시 모를 하위호환 로그를 잠재우려면 같이 넣어도 무방
    "KubernetesExecutor": {"pod_override": _pod_dict},
}


# ===================== DAG =====================
@dag(
    dag_id="gmarket_auction_google_feeds_mpu_tsv_dag",
    start_date=pendulum.datetime(2025, 7, 27, tz="Asia/Seoul"),
    schedule="0 4,10,11,16,22 * * *",
    catchup=False,
    tags=["gmarket", "auction", "google", "feeds", "tsv", "gmc"],
    doc_md="G마켓/옥션 피드를 TSV 그대로 S3에 업로드(GMC 경로에 덮어쓰기) 후 EMR 처리 DAG를 트리거합니다.",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "queue": "kubernetes",
    },
    # 동시성(필요에 맞게 조절)
    concurrency=256,  # DAG 전체 동시 실행 TaskInstances 상한
    max_active_tasks=256,  # DAG 레벨 동시 Task 수 상한
    max_active_runs=1,  # 같은 DAG run 동시 1개(스케줄 5회/일)
)
def _dag():
    # ───────── 유틸 ─────────
    def _requests_session() -> requests.Session:
        sess = requests.Session()
        retry = Retry(
            total=RETRY_TOTAL,
            connect=RETRY_TOTAL,
            read=RETRY_TOTAL,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(
            pool_connections=HTTP_POOL_SIZE,
            pool_maxsize=HTTP_POOL_SIZE,
            max_retries=retry,
        )
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        return sess

    # ───────── 태스크들 ─────────
    @task(
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",  # 가벼운 선행 태스크 전용 풀(큰 값 권장)
    )
    def get_target_info() -> dict:
        now_kst = pendulum.now("Asia/Seoul")
        h = now_kst.hour
        if 11 <= h < 16:
            market = "auction"
            target_hour = "11"
        elif 4 <= h < 10:
            market = "gmarket"
            target_hour = "04"
        elif 10 <= h < 11:
            market = "gmarket"
            target_hour = "10"
        elif 16 <= h < 22:
            market = "gmarket"
            target_hour = "16"
        else:
            market = "gmarket"
            target_hour = "22"
        return {
            "market": market,
            "target_hour": target_hour,
            "target_date": now_kst.to_date_string().replace("-", ""),
        }

    @task(
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",
    )
    def build_urls(info: dict) -> List[str]:
        market = info["market"]
        base = f"https://im-ep.{market}.co.kr"
        hhmm = f"{int(info['target_hour']):02d}00"
        date = info["target_date"]
        header = f"{base}/google/{date}/{hhmm}/header.tsv"
        feeds = [f"{base}/google/{date}/{hhmm}/feed_{i:05d}.tsv.gz" for i in range(100)]
        urls = [header, *feeds]
        log.info(f"총 {len(urls)}개 URL")
        return urls

    @task(
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="s3_upload_pool",  # 실 업로드 동시성 제한 풀(노드 10개면 80~100 권장)
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    def upload_one_to_s3(url: str, market: str) -> str:
        """
        단일 URL을 S3로 스트리밍 업로드 (메모리 최소화)
        """
        sess = _requests_session()
        file_name = url.rsplit("/", 1)[-1]  # header.tsv or feed_00000.tsv.gz
        s3_key = f"feeds/google/{market}/GMC/{file_name.replace('.csv', '.tsv')}"  # 혹시 .csv면 .tsv로

        s3 = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()

        # 404면 skip
        h = sess.head(url, timeout=30)
        if h.status_code == 404:
            log.info(f"[SKIP 404] {url}")
            sess.close()
            return f"skip-404:{file_name}"
        h.raise_for_status()

        log.info(f"[GET→S3] {url} → s3://{S3_BUCKET}/{s3_key}")
        with sess.get(url, stream=True, timeout=HTTP_TIMEOUT_SEC) as r:
            r.raise_for_status()
            r.raw.decode_content = True
            extra_args = {}
            if url.endswith(".gz"):
                # 헤더가 gzip이면 내려보내기
                extra_args["ContentEncoding"] = "gzip"
            # TSV 컨텐츠 타입 지정(가능하면)
            if file_name.endswith(".tsv") or file_name.endswith(".tsv.gz"):
                extra_args["ContentType"] = "text/tab-separated-values"

            # boto3는 fileobj 스트리밍 가능
            s3.upload_fileobj(
                Fileobj=r.raw,
                Bucket=S3_BUCKET,
                Key=s3_key,
                ExtraArgs=extra_args if extra_args else None,
            )
        sess.close()
        return f"s3://{S3_BUCKET}/{s3_key}"

    # ───────── 플로우 ─────────
    info = get_target_info()
    urls = build_urls(info)

    # URL별로 파드 매핑 → 노드 10개 활용, s3_upload_pool 슬롯으로 동시성 관리
    uploaded = upload_one_to_s3.partial(
        market="{{ ti.xcom_pull(task_ids='get_target_info')['market'] }}"
    ).expand(url=urls)

    # 모두 성공 후 EMR DAG 트리거
    trigger_emr = TriggerDagRunOperator(
        task_id="trigger_emr_dag",
        trigger_dag_id="gmc_gmarket_auction_emr_process_and_rename",
        conf={"market": "{{ ti.xcom_pull(task_ids='get_target_info')['market'] }}"},
    )

    uploaded >> trigger_emr


dag = _dag()
