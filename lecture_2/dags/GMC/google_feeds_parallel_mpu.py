import io
import gzip
import time
import logging
import pendulum
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from boto3.s3.transfer import TransferConfig
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import boto3
from botocore.config import Config

# --- 상수 정의 ---
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"
AWS_REGION = "ap-northeast-2"
log = logging.getLogger(__name__)

# ★ 경량 파드 리소스 설정 (autoscaler 트리거용 requests/limits)
from kubernetes.client import (
    V1Pod,
    V1PodSpec,
    V1ObjectMeta,
    V1Container,
    V1ResourceRequirements,
)

EXECUTOR_CONFIG_LITE = {
    "KubernetesExecutor": {
        "pod_override": V1Pod(
            metadata=V1ObjectMeta(labels={"app": "airflow-task-lite"}),
            spec=V1PodSpec(
                restart_policy="Never",
                containers=[
                    V1Container(
                        name="base",
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "200m",
                                "memory": "512Mi",
                                "ephemeral-storage": "1Gi",
                            },
                            limits={
                                "cpu": "1000m",
                                "memory": "1Gi",
                                "ephemeral-storage": "2Gi",
                            },
                        ),
                    )
                ],
            ),
        )
    }
}


# --- DAG 정의 ---
@dag(
    dag_id="gmarket_auction_google_feeds_mpu_tsv_dag",
    start_date=pendulum.datetime(2025, 7, 27, tz="Asia/Seoul"),
    schedule="0 4,10,11,16,22 * * *",
    catchup=False,
    tags=["gmarket", "auction", "google", "feeds", "tsv", "gmc"],
    doc_md="G마켓/옥션 피드 TSV로 mpu (마켓별 GMC 경로에 덮어쓰기)",
    # ★ 전체 동시 Task 상한 (원하면 더 키워도 됨)
    concurrency=64,
)
def gmarket_google_feeds_tsv_direct_dag():
    """
    ### G마켓/옥션 피드 처리 DAG (TSV 직접 업로드)
    - 마켓별 고정 경로 아래 'GMC' 폴더에 덮어쓰기
    """

    @task
    def get_target_info() -> dict:
        now_kst = pendulum.now("Asia/Seoul")
        hour_kst = now_kst.hour
        market = "gmarket"
        if 11 <= hour_kst < 16:
            market = "auction"
            target_hour = "11"
        elif 4 <= hour_kst < 10:
            target_hour = "04"
        elif 10 <= hour_kst < 11:
            target_hour = "10"
        elif 16 <= hour_kst < 22:
            target_hour = "16"
        else:
            target_hour = "22"
        return {
            "market": market,
            "target_hour": target_hour,
            "target_date": now_kst.to_date_string().replace("-", ""),
        }

    # ★ 0) URL 목록/헤더 URL 구성 분리
    @task
    def build_urls(target_info: dict) -> dict:
        market_name = target_info["market"]
        url_prefix = f"https://im-ep.{market_name}.co.kr"
        hhmm = f"{int(target_info['target_hour']):02d}00"
        date = target_info["target_date"]
        header_url = f"{url_prefix}/google/{date}/{hhmm}/header.tsv"
        feed_urls = [
            f"{url_prefix}/google/{date}/{hhmm}/feed_{i:05d}.tsv.gz" for i in range(100)
        ]
        return {"header_url": header_url, "feed_urls": feed_urls}

    # ★ 1) 헤더는 단건 Task(1 파드)로 선 업로드 (실패시 바로 보이게)
    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def upload_header(target_info: dict, header_url: str) -> str:
        market_name = target_info["market"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        creds = s3_hook.get_credentials()
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=getattr(creds, "token", None),
            region_name=AWS_REGION,
            config=Config(max_pool_connections=50, retries={"max_attempts": 5}),
        )
        session = requests.Session()
        retry_strategy = Retry(
            total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            pool_connections=10, pool_maxsize=10, max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        s3_key = f"feeds/google/{market_name}/GMC/header.tsv"
        with session.get(header_url, stream=True, timeout=300) as r:
            if r.status_code == 404:
                log.warning("[header] 404 skip")
                return "skip"
            r.raise_for_status()
            s3_client.upload_fileobj(r.raw, S3_BUCKET, s3_key)
        return f"s3://{S3_BUCKET}/{s3_key}"

    # ★ 2) 100개를 청크로 분할 → 각 청크가 하나의 파드가 됨
    @task
    def make_chunks(feed_urls: list[str], chunk_size: int = 10) -> list[list[str]]:
        return [
            feed_urls[i : i + chunk_size] for i in range(0, len(feed_urls), chunk_size)
        ]

    # ★ 3) 청크 업로드용 Task (한 파드가 10개 내외 파일을 쓰레드로 처리)
    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def process_chunk(target_info: dict, urls: list[str]) -> dict:
        market_name = target_info["market"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        creds = s3_hook.get_credentials()
        transfer_config = TransferConfig(
            multipart_threshold=1024 * 1024 * 100,
            max_concurrency=10,
            multipart_chunksize=1024 * 1024 * 16,
            use_threads=True,
        )
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=getattr(creds, "token", None),
            region_name=AWS_REGION,
            config=Config(max_pool_connections=60, retries={"max_attempts": 5}),
        )

        def create_session():
            s = requests.Session()
            retry_strategy = Retry(
                total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(
                pool_connections=20, pool_maxsize=20, max_retries=retry_strategy
            )
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            return s

        def direct_stream_upload(url: str, idx: int, session: requests.Session) -> dict:
            MAX_ATTEMPTS = 3
            RETRY_DELAY_SECONDS = 8
            file_name = url.split("/")[-1].replace(".tsv.gz", "")
            base_s3_key = f"feeds/google/{market_name}/GMC/{file_name}"
            s3_key = f"{base_s3_key}.tsv.gz"

            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    with session.get(url, stream=True, timeout=300) as resp:
                        if resp.status_code == 404:
                            return {"status": "skipped", "file": file_name}
                        resp.raise_for_status()
                        s3_client.upload_fileobj(
                            resp.raw, S3_BUCKET, s3_key, Config=transfer_config
                        )
                        return {
                            "status": "success",
                            "file": file_name,
                            "s3_key": s3_key,
                        }
                except Exception as e:
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        return {
                            "status": "final_failure",
                            "file": file_name,
                            "error": str(e),
                        }

        successful, failed = [], []
        max_workers = min(12, max(2, len(urls)))  # 청크 크기에 맞춰 쓰레드 수 자동 조절
        sessions = [create_session() for _ in range(max_workers)]
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                fut2name = {}
                for i, u in enumerate(urls):
                    s = sessions[i % max_workers]
                    f = ex.submit(direct_stream_upload, u, i, s)
                    fut2name[f] = u
                for f in as_completed(fut2name):
                    r = f.result()
                    if r.get("status") == "success":
                        successful.append(r)
                    elif r.get("status") == "skipped":
                        pass
                    else:
                        failed.append(r)
        finally:
            for s in sessions:
                s.close()

        if failed:
            # 실패가 있어도 다른 청크들 진행되게 하고, 이 청크는 재시도를 태스크 레벨에서
            raise ValueError(f"chunk failed: {failed[:3]} ... ({len(failed)} fails)")

        return {"ok": len(successful)}

    # ── DAG 플로우 ──────────────────────────────────────────────────────
    ti = get_target_info()

    urls_obj = build_urls.override(executor_config=EXECUTOR_CONFIG_LITE)(ti)

    # 헤더 먼저 단건 업로드 (경량 파드)
    hdr = upload_header.override(executor_config=EXECUTOR_CONFIG_LITE)(
        target_info=ti,
        header_url=urls_obj["header_url"],
    )

    # 100개 URL을 10개 청크로 분할 → 각 청크가 "별도 파드"로 병렬 수행
    chunks = make_chunks.override(executor_config=EXECUTOR_CONFIG_LITE)(
        urls_obj["feed_urls"], chunk_size=10  # ★ 노드 10개면 10~20 청크 권장
    )

    # 동적 매핑: 청크별 파드 생성
    _ = (
        process_chunk.partial()
        .override(executor_config=EXECUTOR_CONFIG_LITE)
        .expand(
            target_info=[ti]
            * 0,  # Airflow 2.8+에선 키워드 확장 가능, 아래 방식으로 대체
        )
    )
    # ↑ 위 한줄은 문법 호환 이슈가 있을 수 있어, 아래처럼 safe하게 바꿉니다:

    mapped = process_chunk.override(executor_config=EXECUTOR_CONFIG_LITE).expand(
        target_info=[ti] * 1,  # 더미 (실제 값은 XCom pull로 씀)
        urls=chunks,
    )

    # 헤더 후 본문 업로드
    hdr >> mapped

    # 끝나면 EMR DAG 트리거
    trigger_emr_dag = TriggerDagRunOperator(
        task_id="trigger_emr_dag",
        trigger_dag_id="gmc_gmarket_auction_emr_process_and_rename",
        conf={"market": "{{ ti.xcom_pull(task_ids='get_target_info')['market'] }}"},
    )

    mapped >> trigger_emr_dag


gmarket_google_feeds_tsv_direct_dag()
