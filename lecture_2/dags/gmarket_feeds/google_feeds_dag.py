import io
import gzip
import logging
import pendulum
import requests
import csv

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 상수 정의 ---
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"
MIN_PART_SIZE = 64 * 1024 * 1024  # 64MB

log = logging.getLogger(__name__)


# --- DAG 정의 (원래 이름으로 복원) ---
@dag(
    dag_id="gmarket_auction_google_feeds_mpu_dag",
    start_date=pendulum.datetime(2025, 7, 27, tz="Asia/Seoul"),
    schedule="0 4,10,11,16,22 * * *",
    catchup=False,
    tags=["gmarket", "auction", "google", "feeds"],
    doc_md="G마켓/옥션 피드를 **실제 실행 시간** 기준으로 처리합니다.",
)
def gmarket_google_feeds_dag():
    """
    ### G마켓/옥션 피드 처리 DAG (실행 시간 기준)

    - **실행 시간이 11시 ~ 15시 59분 사이일 경우:** 옥션
    - **그 외 스케줄 시간:** 지마켓
    """

    @task
    def get_target_info() -> dict:
        """실제 현재 시간을 기준으로 마켓, 날짜, 시간 정보를 결정"""
        now_kst = pendulum.now("Asia/Seoul")
        hour_kst = now_kst.hour

        market = "gmarket"
        target_hour = f"{now_kst.hour:02d}"

        if 11 <= hour_kst < 16:
            market = "auction"
            target_hour = "11"
        elif 4 <= hour_kst < 10:
            target_hour = "04"
        elif 10 <= hour_kst < 11:
            target_hour = "10"
        elif 16 <= hour_kst < 22:
            target_hour = "16"
        else:  # 22시 이후 ~ 4시 이전
            target_hour = "22"

        return {
            "market": market,
            "target_hour": target_hour,
            "target_date": now_kst.to_date_string().replace("-", ""),
        }

    @task
    def process_market_files(target_info: dict):
        """하나의 마켓에 대한 전체 처리 흐름을 실행합니다."""
        market_name = target_info["market"]

        url_prefix = f"https://im-ep.{market_name}.co.kr"
        target_hour, target_date = (
            target_info["target_hour"],
            target_info["target_date"],
        )
        hour_str = f"{int(target_hour):02d}00"
        urls = [f"{url_prefix}/google/{target_date}/{hour_str}/header.tsv"]
        urls.extend(
            [
                f"{url_prefix}/google/{target_date}/{hour_str}/feed_{i:05d}.tsv.gz"
                for i in range(100)
            ]
        )

        s3_key = f"feeds/google/{market_name}/combined_feed.csv.gz"
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_client = s3_hook.get_conn()

        log.info(f"S3 멀티파트 업로드를 시작합니다. 대상: s3://{S3_BUCKET}/{s3_key}")
        mpu = s3_client.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_key)
        upload_id = mpu["UploadId"]

        part_buffer = io.BytesIO()
        part_number = 1
        parts = []

        # gzip → text → csv writer (한 번만 래핑해서 계속 사용)
        gzip_compressor = gzip.GzipFile(fileobj=part_buffer, mode="wb")
        text_out = io.TextIOWrapper(gzip_compressor, encoding="utf-8-sig", newline="")
        writer = csv.writer(text_out, quoting=csv.QUOTE_MINIMAL)

        # --- 성능 미세 튜닝: 주기적 체크 기반 플러시/파트 분할 ---
        CHECK_INTERVAL = 1 * 1024 * 1024  # 1MB마다 확인
        written_since_check = 0

        def flush_buffer(final: bool = False):
            nonlocal part_number, part_buffer, gzip_compressor, text_out, writer
            # 현재 gzip 멤버를 종료하여 푸터까지 쓰기
            text_out.flush()
            gzip_compressor.close()

            if part_buffer.tell() > 0:
                log.info(
                    f"파트 {part_number} 업로드 시작 (버퍼 크기: {part_buffer.tell()} 바이트)"
                )
                part_buffer.seek(0)
                resp = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=part_buffer,
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                part_number += 1

            if not final:
                # 다음 파트 준비
                part_buffer = io.BytesIO()
                gzip_compressor = gzip.GzipFile(fileobj=part_buffer, mode="wb")
                text_out = io.TextIOWrapper(
                    gzip_compressor, encoding="utf-8-sig", newline=""
                )
                writer = csv.writer(text_out, quoting=csv.QUOTE_MINIMAL)

        def maybe_flush_part():
            nonlocal written_since_check
            if written_since_check >= CHECK_INTERVAL:
                text_out.flush()
                gzip_compressor.flush()
                written_since_check = 0
                if part_buffer.tell() >= MIN_PART_SIZE:
                    flush_buffer()

        try:
            for url in urls:
                try:
                    with requests.get(url, stream=True, timeout=300) as r:
                        if r.status_code == 404:
                            log.info(
                                f"파일 없음 (404), {market_name} 피드의 마지막으로 간주하여 중단합니다: {url}"
                            )
                            break
                        r.raise_for_status()

                        log.info(f"처리 중: {url}")
                        # 전송 레벨 압축(Content-/Transfer-Encoding)도 안전히 해제
                        r.raw.decode_content = True

                        if url.endswith(".gz"):
                            # .gz 파일은 GzipFile로 직접 스트리밍 해제
                            with gzip.GzipFile(fileobj=r.raw) as src:
                                for line_bytes in iter(lambda: src.readline(), b""):
                                    if not line_bytes:
                                        break
                                    fields = next(
                                        csv.reader(
                                            [line_bytes.decode("utf-8-sig")],
                                            delimiter="\t",
                                        )
                                    )
                                    writer.writerow(fields)

                                    # 성능 튜닝: 누적 바이트 기준 주기적 체크
                                    written_since_check += len(line_bytes)
                                    maybe_flush_part()
                        else:
                            # 평문 TSV는 iter_lines()로 안전하게 라인 단위 스트리밍
                            for line_bytes in r.iter_lines(
                                chunk_size=65536, decode_unicode=False
                            ):
                                if not line_bytes:
                                    continue
                                fields = next(
                                    csv.reader(
                                        [line_bytes.decode("utf-8-sig")], delimiter="\t"
                                    )
                                )
                                writer.writerow(fields)

                                written_since_check += len(line_bytes)
                                maybe_flush_part()

                except requests.exceptions.RequestException as e:
                    log.warning(f"URL 요청 실패, 건너뜁니다: {url}, 오류: {e}")
                    continue

            # 마지막 잔여 데이터 마감 (새 멤버 재오픈 없이 종료)
            flush_buffer(final=True)

            if not parts:
                raise ValueError("업로드할 데이터가 없습니다.")

            log.info("모든 파트 업로드 완료. 최종 파일 조립을 시작합니다.")
            s3_client.complete_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            log.info(f"성공: s3://{S3_BUCKET}/{s3_key} 파일 생성이 완료되었습니다.")

        except Exception as e:
            log.error(f"최종 오류 발생. 멀티파트 업로드를 중단합니다. 오류: {e}")
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id
            )
            raise

    # --- DAG 흐름 정의 ---
    target_info = get_target_info()
    process_market_files(target_info=target_info)


# DAG 인스턴스화
gmarket_google_feeds_dag()
