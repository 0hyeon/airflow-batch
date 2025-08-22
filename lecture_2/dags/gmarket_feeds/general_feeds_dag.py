import pendulum
import requests
import io
import gzip
import csv
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- 상수 정의 ---
S3_CONN_ID = "aws_conn_id"
S3_BUCKET = "gyoung0-test"


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
    1) Gmarket 20개 TSV.gz → CSV로 변환 → gzip 멤버로 이어붙여 멀티파트 업로드
    2) Auction도 동일 처리
    3) 둘 다 성공하면 kakao_gmarket_auction_emr_processing 트리거
    """

    def _process_market(market_name: str, url_prefix: str):
        urls = [f"{url_prefix}/im_feed/feed_{i}.tsv.gz" for i in range(20)]
        s3_key = f"feeds/general/combined_{market_name}_feed.csv.gz"

        s3_client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()

        print(f"S3 멀티파트 업로드 시작: s3://{S3_BUCKET}/{s3_key}")
        upload_id = None  # ← 중요: 예외 시 abort 분기에서 필요
        mpu = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key,
            # 브라우저/툴 호환 고려: 둘 중 택1
            ContentType="text/csv",
            ContentEncoding="gzip",
            # 또는 ContentType="application/gzip" 로 사용 가능(둘 다 문제 없음)
        )
        upload_id = mpu["UploadId"]
        parts = []

        MIN_PART = 5 * 1024 * 1024  # 멀티파트 규칙: 마지막 제외 5MB 이상
        CHUNK_1MB = 1024 * 1024
        buffer = bytearray()
        part_number = 1

        def _sleep_backoff(attempt: int):
            import random, time

            time.sleep(min(30, 2**attempt) + random.uniform(0, 0.3))

        try:
            for idx, url in enumerate(urls):
                print(f"--- {market_name} 후보 #{idx+1} 다운로드: {url}")
                content = None

                for attempt in range(3):
                    try:
                        with requests.get(
                            url,
                            stream=True,
                            timeout=(10, 600),  # connect 10s, read 10분
                            headers={"User-Agent": "airflow-feed-fetcher/1.0"},
                        ) as r:
                            if r.status_code == 404:
                                print(f"[INFO] 404 스킵: {url}")
                                content = None
                                break
                            r.raise_for_status()
                            # 주의: join은 누적 메모리 사용. 필요시 스트리밍 gzip 변환으로 개선 가능.
                            content = b"".join(r.iter_content(chunk_size=CHUNK_1MB))
                        break
                    except Exception as e:
                        if attempt == 2:
                            print(f"[ERROR] GET 실패(최종): {url} -> {e}")
                            raise
                        print(f"[WARN] GET 실패({attempt+1}/3): {e}; 재시도 대기")
                        _sleep_backoff(attempt + 1)

                if not content:  # 404 등
                    continue

                try:
                    decompressed_content = gzip.decompress(content)
                except gzip.BadGzipFile as e:
                    print(f"[WARN] gzip 해제 실패(스킵): {url} -> {e}")
                    continue

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
                gz_chunk = gzip.compress(csv_bytes)  # gzip 멤버 1개

                buffer += gz_chunk
                while len(buffer) >= MIN_PART:
                    to_upload = bytes(buffer[:MIN_PART])
                    del buffer[:MIN_PART]
                    res = s3_client.upload_part(
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=to_upload,
                    )
                    parts.append({"PartNumber": part_number, "ETag": res["ETag"]})
                    part_number += 1

            if buffer:  # 마지막 파트(5MB 미만 가능)
                res = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=bytes(buffer),
                )
                parts.append({"PartNumber": part_number, "ETag": res["ETag"]})

            if not parts:
                raise RuntimeError("업로드할 유효 파트가 없어 멀티파트 완료 불가")

            print(f"{market_name} 업로드 파트 완료 → 최종 조립")
            s3_client.complete_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            print(f"SUCCESS: s3://{S3_BUCKET}/{s3_key} 생성 완료")

        except Exception as e:
            print(f"{market_name} 처리 중 오류: {e}")
            try:
                if upload_id:
                    s3_client.abort_multipart_upload(
                        Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id
                    )
            except Exception as abort_err:
                print(f"[WARN] abort 실패 무시: {abort_err}")
            raise

    # 태스크 레벨 리트라이(네트워크/일시 오류 대비)
    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def process_gmarket_files():
        _process_market("gmarket", "https://im-ep.gmarket.co.kr")

    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def process_auction_files():
        _process_market("auction", "https://im-ep.auction.co.kr")

    # --- 의존성 ---
    g = process_gmarket_files()
    a = process_auction_files()

    both_done = EmptyOperator(task_id="both_done", trigger_rule="all_success")

    trigger = TriggerDagRunOperator(
        task_id="trigger_kakao_gmarket_auction_emr_processing",
        trigger_dag_id="kakao_gmarket_auction_emr_processing",
        conf={
            "run_ds": "{{ ds }}",
            "gmarket_key": "s3://gyoung0-test/feeds/general/combined_gmarket_feed.csv.gz",
            "auction_key": "s3://gyoung0-test/feeds/general/combined_auction_feed.csv.gz",
        },
        wait_for_completion=False,
    )

    [g, a] >> both_done >> trigger


dag = gmarket_general_feeds_dag_sequential()
