# -*- coding: utf-8 -*-
"""
GMC SFTP 업로드 파이프라인 (2단계, DAG 한정 경량 파드/안티어피니티 완화)
1) feed_00000..00099.tsv.gz 를 S3에 병렬 업로드
2) (옵션) 모두 완료 후 SFTP 병렬 업로드
- 이 DAG만 executor_config로 전역 템플릿의 강한 anti-affinity/무거운 리소스를 우회
- S3 업로드는 스트리밍(upload_fileobj)로 메모리 사용 최소화
"""

import logging
import pendulum
from datetime import timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

# ===== 환경 설정 =====
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"
S3_POOL = "s3_upload_pool"  # Pools에서 슬롯을 충분히 크게(예: 100)
SFTP_CONN_ID = "gmc_sftp"
SFTP_REMOTE_DIR = "/"
SFTP_POOL = "sftp_upload_pool"

CHUNK_SIZE = 1024 * 1024  # 1MB
REQUEST_TIMEOUT_SEC = 300

BASES = {
    "gmarket": "https://im-ep.gmarket.co.kr",
    "auction": "https://im-ep.auction.co.kr",
}

# ▶ 이 DAG만 경량 파드/안티어피니티 완화로 오버라이드
EXECUTOR_CONFIG_LITE = {
    "KubernetesExecutor": {
        "pod_override": {
            "metadata": {"labels": {"app": "airflow-task-lite"}},
            "spec": {
                "restartPolicy": "Never",
                "affinity": {
                    "podAntiAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 50,
                                "podAffinityTerm": {
                                    "labelSelector": {
                                        "matchLabels": {"app": "airflow-task-lite"}
                                    },
                                    "topologyKey": "kubernetes.io/hostname",
                                },
                            }
                        ]
                    }
                },
                "containers": [
                    {
                        "name": "base",
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "256Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                    }
                ],
            },
        }
    }
}


def decide_target_info(now_kst: pendulum.DateTime) -> dict:
    """실행시간 규칙 기반 market/hour/date 계산 (기존 규칙 유지)"""
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
        "market": market,
        "target_hour": target_hour,  # "04" | "10" | "11" | "16" | "22"
        "target_date": now_kst.to_date_string().replace("-", ""),  # "YYYYMMDD"
    }


@dag(
    dag_id="feeds_to_s3_then_sftp_parallel_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 4,10,11,16,22 * * *",
    catchup=False,
    tags=["gmarket", "auction", "feed", "s3", "sftp", "parallel", "barrier"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "queue": "kubernetes",  # ← 모든 태스크 기본 큐
    },
    # 이 DAG만 여유 있게 동시 태스크 허용 (Pool로 실제 동시성 제어)
    max_active_tasks=200,
    max_active_runs=1,
    doc_md="""
    ### 단계적 업로드
    1) feed_00000..00099.tsv.gz → S3 병렬 업로드  
    2) 모두 완료되면 → SFTP 병렬 업로드 시작(옵션)  
    ※ 이 DAG만 경량 파드/안티어피니티 완화로 빠른 병렬 수행
    """,
)
def pipeline():
    @task
    def get_target_info() -> dict:
        info = decide_target_info(pendulum.now("Asia/Seoul"))
        log.info(f"[target] {info}")
        return info

    @task
    def build_urls(target_info: dict) -> list[str]:
        market = target_info["market"]
        base = BASES[market]
        date = target_info["target_date"]
        hour_str = f"{int(target_info['target_hour']):02d}00"
        urls = [
            f"{base}/google/{date}/{hour_str}/feed_{i:05d}.tsv.gz" for i in range(100)
        ]
        log.info(f"총 {len(urls)}개 URL 생성")
        return urls

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def upload_one_to_s3(url: str, target_info: dict) -> str:
        """
        단일 .gz를 S3에 업로드 (스트리밍 업로드: 메모리 사용 최소화)
        - 404면 skip 반환
        """
        from botocore.config import Config

        filename = url.split("/")[-1]
        # 날짜/시간별로 키를 구성하려면 target_info 이용해서 경로 추가해도 됨
        s3_key = f"feeds/google/{target_info['market']}/{filename}"

        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SEC) as r:
                if r.status_code == 404:
                    log.info(f"[S3 skip-404] {url}")
                    return f"skip-404:{filename}"
                r.raise_for_status()
                r.raw.decode_content = True

                # boto3 client 얻기
                s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
                s3_client = s3_hook.get_conn()
                # (선택) 연결 풀/재시도 튜닝
                if hasattr(s3_client, "meta") and hasattr(s3_client.meta, "config"):
                    s3_client.meta.config = Config(max_pool_connections=50)

                # ▶ 스트리밍 업로드 (파일 전체를 메모리에 들고 있지 않음)
                s3_client.upload_fileobj(
                    Fileobj=r.raw,
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                )
                log.info(f"[S3 ok] s3://{S3_BUCKET}/{s3_key}")
                return f"s3:{s3_key}"

        except requests.exceptions.RequestException as e:
            log.warning(f"[S3 req-fail] {url} / {e}")
            raise
        except Exception as e:
            log.error(f"[S3 upload-fail] {url} / {e}")
            raise

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def upload_one_to_sftp(url: str) -> str:
        """
        (옵션) 단일 .gz를 SFTP에 업로드
        - S3 완료 이후 시작(배리어 뒤)
        - 원본 URL에서 다시 읽어서 SFTP로 전송
        """
        filename = url.split("/")[-1]
        remote_path = f"{SFTP_REMOTE_DIR}{filename}"

        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SEC) as r:
                if r.status_code == 404:
                    log.info(f"[SFTP skip-404] {url}")
                    return f"skip-404:{filename}"
                r.raise_for_status()
                r.raw.decode_content = True

                sftp = SFTPHook(ftp_conn_id=SFTP_CONN_ID).get_conn()
                with sftp.file(remote_path, "wb") as remote_f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            remote_f.write(chunk)

                log.info(f"[SFTP ok] {remote_path}")
                return f"sftp:{remote_path}"

        except requests.exceptions.RequestException as e:
            log.warning(f"[SFTP req-fail] {url} / {e}")
            raise
        except Exception as e:
            log.error(f"[SFTP upload-fail] {url} / {e}")
            raise

    # ----- DAG 흐름 -----
    ti = get_target_info.override(executor_config=EXECUTOR_CONFIG_LITE)()
    urls = build_urls.override(executor_config=EXECUTOR_CONFIG_LITE)(ti)

    s3_results = (
        upload_one_to_s3.partial(target_info=ti)
        .override(pool=S3_POOL, priority_weight=1, executor_config=EXECUTOR_CONFIG_LITE)
        .expand(url=urls)
    )

    s3_results

    # # 2) 배리어: S3 모든 매핑 태스크 완료 대기 (SFTP 켤 때 사용)
    # barrier = EmptyOperator(task_id="wait_all_s3_done")
    # s3_results >> barrier

    # # 3) SFTP 병렬 업로드 (옵션)
    # upload_one_to_sftp.partial().override(
    #     pool=SFTP_POOL,
    #     priority_weight=1,
    #     executor_config=EXECUTOR_CONFIG_LITE,   # SFTP도 동일하게 경량/완화 적용
    # ).expand(url=urls).set_upstream(barrier)


# DAG 인스턴스 생성
pipeline()
