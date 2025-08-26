# -*- coding: utf-8 -*-
"""
GMC SFTP 업로드 파이프라인 (2단계)
1) feed_00000..00099.tsv.gz 를 S3에 병렬 업로드
2) 모든 S3 업로드 완료 후, 동일 파일들을 SFTP에 병렬 업로드
- 동적 매핑 + 배리어(EmptyOperator)로 단계 순서 보장
"""

import logging
import pendulum
from datetime import timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

# ===== 환경 설정 =====
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"  # ← 본인 버킷으로 변경
S3_POOL = "s3_upload_pool"  # Airflow Pools에서 생성(예: slots=8)
SFTP_CONN_ID = "gmc_sftp"  # uploads.google.com:19321 (SSH key)
SFTP_REMOTE_DIR = "/"  # GMC 안내 경로로 조정
SFTP_POOL = "sftp_upload_pool"  # Airflow Pools에서 생성(예: slots=6)

CHUNK_SIZE = 1024 * 1024  # 1MB
REQUEST_TIMEOUT_SEC = 300

BASES = {
    "gmarket": "https://im-ep.gmarket.co.kr",
    "auction": "https://im-ep.auction.co.kr",
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
    },
    doc_md="""
    ### 단계적 업로드
    1) feed_00000..00099.tsv.gz → S3 병렬 업로드  
    2) 모두 완료되면 → SFTP 병렬 업로드 시작  
    동시성은 Pool로 제어하세요(`s3_upload_pool`, `sftp_upload_pool`).
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
        # 100개 gz 파일만 (header 제외)
        urls = [
            f"{base}/google/{date}/{hour_str}/feed_{i:05d}.tsv.gz" for i in range(100)
        ]
        log.info(f"총 {len(urls)}개 URL 생성")
        return urls

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def upload_one_to_s3(url: str, target_info: dict) -> str:
        """
        단일 .gz를 S3에 업로드 (그대로 바이너리 패스스루)
        - 404면 skip 반환
        """
        filename = url.split("/")[-1]
        s3_key = f"feeds/google/{target_info['market']}/{target_info['target_date']}/{target_info['target_hour']}/{filename}"

        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT_SEC) as r:
                if r.status_code == 404:
                    log.info(f"[S3 skip-404] {url}")
                    return f"skip-404:{filename}"
                r.raise_for_status()
                r.raw.decode_content = True

                s3 = S3Hook(aws_conn_id=S3_CONN_ID)
                # put_object는 bytes 필요 → 청크 모아 한번에 전달 (파일이 크면 Multipart 업로드로 바꿔도 됨)
                chunks = []
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        chunks.append(chunk)
                body = b"".join(chunks)

                s3.load_bytes(
                    bytes_data=body,
                    key=s3_key,
                    bucket_name=S3_BUCKET,
                    replace=True,
                )
                log.info(f"[S3 ok] s3://{S3_BUCKET}/{s3_key}")
                return f"s3:{s3_key}"
        except requests.exceptions.RequestException as e:
            log.warning(f"[S3 req-fail] {url} / {e}")
            raise
        except Exception as e:
            log.error(f"[S3 upload-fail] {url} / {e}")
            raise

    # 배리어(모든 S3 업로드 완료 대기) 전, SFTP 업로드용 태스크
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def upload_one_to_sftp(url: str) -> str:
        """
        단일 .gz를 SFTP에 업로드
        - S3 완료 이후 시작(배리어 뒤)
        - 원본 URL에서 다시 읽어서 SFTP로 전송 (요구사항: S3 완료 후 SFTP 진행)
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
    ti = get_target_info()
    urls = build_urls(ti)

    # 1) S3 병렬 업로드
    s3_results = (
        upload_one_to_s3.partial(target_info=ti)
        .override(
            pool=S3_POOL,
            priority_weight=1,
        )
        .expand(url=urls)
    )
    s3_results
    # # 2) 배리어: S3 모든 매핑 태스크 완료 대기
    # barrier = EmptyOperator(task_id="wait_all_s3_done")
    # s3_results >> barrier

    # # 3) SFTP 병렬 업로드 (배리어 이후 시작)
    # upload_one_to_sftp.partial().override(
    #     pool=SFTP_POOL,
    #     priority_weight=1,
    # ).expand(
    #     url=urls
    # ).set_upstream(barrier)


# DAG 인스턴스 생성
pipeline()
