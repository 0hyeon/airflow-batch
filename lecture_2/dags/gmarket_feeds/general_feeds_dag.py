import pendulum
import requests
import io
import gzip
import csv  # csv 모듈 import
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 상수 정의 ---
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"


#
@dag(
    dag_id="gmarket_general_feeds_dag_sequential",
    start_date=pendulum.datetime(2025, 7, 16, tz="Asia/Seoul"),
    schedule="0 4,10,16,22 * * *",
    catchup=False,
    tags=["gmarket", "feed", "s3", "sequential"],
    doc_md="G마켓과 옥션 피드를 각각 별도의 파일로 합쳐 S3에 저장합니다. (csv 모듈 사용)",
)
def gmarket_general_feeds_dag_sequential():
    """
    Gmarket과 Auction 피드 원본(TSV)을 다운로드하여
    안전하게 CSV로 변환한 후, S3에 업로드하는 DAG입니다.
    """

    def _process_market(market_name: str, url_prefix: str):
        """특정 마켓의 모든 파일을 순차적으로 처리하고 S3에 업로드하는 공통 로직"""
        urls = [f"{url_prefix}/im_feed/feed_{i}.tsv.gz" for i in range(20)]
        s3_key = f"feeds/general/combined_{market_name}_feed.csv.gz"

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_client = s3_hook.get_conn()

        print(f"S3 멀티파트 업로드를 시작합니다. 대상: s3://{S3_BUCKET}/{s3_key}")
        mpu = s3_client.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_key)
        upload_id = mpu["UploadId"]
        parts = []

        # 멀티파트 제약: 마지막을 제외한 파트는 >= 5MB
        MIN_PART = 5 * 1024 * 1024  # 5MB
        CHUNK_1MB = 1024 * 1024
        buffer = bytearray()
        part_number = 1

        # 간단 백오프용
        def _sleep_backoff(attempt: int):
            import random, time

            time.sleep(min(30, 2**attempt) + random.uniform(0, 0.3))

        try:
            for index, url in enumerate(urls):
                print(f"--- {market_name} 파트 후보 #{index+1} 다운로드: {url} ---")

                # 1) 404 스킵 + 최대 3회 재시도(+지터)
                content = None
                for attempt in range(3):
                    try:
                        headers = {"User-Agent": "airflow-feed-fetcher/1.0"}
                        with requests.get(
                            url,
                            stream=True,
                            timeout=(10, 600),  # connect 10s, read 10분
                            headers=headers,
                        ) as r:
                            if r.status_code == 404:
                                print(f"[INFO] 404 스킵: {url}")
                                content = None
                                break
                            r.raise_for_status()
                            # 1MB 단위로 안전하게 수신
                            content = b"".join(r.iter_content(chunk_size=CHUNK_1MB))
                        break
                    except Exception as e:
                        if attempt == 2:
                            print(f"[ERROR] GET 실패(최종): {url} -> {e}")
                            raise
                        print(f"[WARN] GET 실패({attempt+1}/3): {e}; 잠시 후 재시도")
                        _sleep_backoff(attempt + 1)

                if not content:
                    # 404 등으로 스킵
                    continue

                # 2) Gzip 해제(깨진 파일 방어)
                try:
                    decompressed_content = gzip.decompress(content)
                except gzip.BadGzipFile as e:
                    print(f"[WARN] gzip 해제 실패(스킵): {url} -> {e}")
                    continue

                # 3) TSV -> CSV (안전 변환)
                try:
                    decoded_str = decompressed_content.decode("utf-8-sig")
                except UnicodeDecodeError:
                    decoded_str = decompressed_content.decode("cp949", errors="replace")
                decoded_str = decoded_str.replace("\x00", "")

                tsv_file = io.StringIO(decoded_str)
                csv_file = io.StringIO()
                tsv_reader = csv.reader(tsv_file, delimiter="\t")
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerows(tsv_reader)

                csv_bytes = csv_file.getvalue().encode("utf-8-sig")
                gz_chunk = gzip.compress(csv_bytes)  # 한 파일 단위로 gzip 멤버 생성

                buffer += gz_chunk
                # 5MB 이상이면 잘라서 업로드(마지막 파트 제외 규칙 충족)
                while len(buffer) >= MIN_PART:
                    to_upload = bytes(buffer[:MIN_PART])
                    del buffer[:MIN_PART]
                    part = s3_client.upload_part(
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=to_upload,
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1

            # 남은 버퍼는 마지막 파트로 업로드(5MB 미만도 허용)
            if buffer:
                part = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=bytes(buffer),
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            # 파트가 하나도 없으면 멀티파트 완료 불가 → abort
            if not parts:
                raise RuntimeError(
                    "업로드할 유효 파트가 없어 멀티파트를 완료할 수 없습니다."
                )

            print(f"{market_name}의 모든 파트 업로드 완료. 최종 파일을 조립합니다...")
            s3_client.complete_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            print(f"SUCCESS: s3://{S3_BUCKET}/{s3_key} 생성이 완료되었습니다.")

        except Exception as e:
            print(f"{market_name} 처리 중 오류 발생: {e}")
            # 업로드 세션이 열려있다면 abort 시도
            try:
                if upload_id:
                    s3_client.abort_multipart_upload(
                        Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id
                    )
            except Exception as abort_err:
                print(f"[WARN] abort 실패 무시: {abort_err}")
            raise

    @task
    def process_gmarket_files():
        """G마켓 피드 20개를 처리합니다."""
        _process_market(market_name="gmarket", url_prefix="https://im-ep.gmarket.co.kr")

    @task
    def process_auction_files():
        """옥션 피드 20개를 처리합니다."""
        _process_market(market_name="auction", url_prefix="https://im-ep.auction.co.kr")

    # --- DAG 흐름 정의 ---
    # 두 작업은 서로 의존성이 없으므로 병렬로 실행할 수 있지만,
    # 여기서는 코드의 단순성을 위해 순차적으로 정의합니다.
    process_gmarket_files()
    process_auction_files()


# DAG 인스턴스화
gmarket_general_feeds_dag_sequential()
