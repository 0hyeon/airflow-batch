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
MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB

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

    Airflow의 예약 시간(logical_date)이 아닌, 실제 코드가 실행되는
    현재 시간을 기준으로 처리할 마켓을 결정합니다.
    """

    @task
    def get_target_info() -> dict:
        """
        [수정됨] 실제 현재 시간을 기준으로 마켓, 날짜, 시간 정보를 결정합니다.
        """
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
        else: # 22시 이후 ~ 4시 이전
            target_hour = "22"
            
        return {
            "market": market,
            "target_hour": target_hour,
            "target_date": now_kst.to_date_string().replace("-", "")
        }

    @task
    def process_market_files(target_info: dict):
        """하나의 마켓에 대한 전체 처리 흐름을 실행합니다."""
        market_name = target_info["market"]
        
        url_prefix = f"https://im-ep.{market_name}.co.kr"
        target_hour, target_date = target_info["target_hour"], target_info["target_date"]
        hour_str = f"{int(target_hour):02d}00"
        urls = [f"{url_prefix}/google/{target_date}/{hour_str}/header.tsv"]
        urls.extend([f"{url_prefix}/google/{target_date}/{hour_str}/feed_{i:05d}.tsv.gz" for i in range(100)])

        s3_key = f"feeds/google/{market_name}/combined_feed.csv.gz"
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_client = s3_hook.get_conn()
        
        log.info(f"S3 멀티파트 업로드를 시작합니다. 대상: s3://{S3_BUCKET}/{s3_key}")
        mpu = s3_client.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_key)
        upload_id = mpu['UploadId']
        
        part_buffer = io.BytesIO()
        part_number = 1
        parts = []

        gzip_compressor = gzip.GzipFile(fileobj=part_buffer, mode="wb")

        def flush_buffer():
            nonlocal part_number, part_buffer, gzip_compressor
            gzip_compressor.close()
            
            if part_buffer.tell() > 0:
                log.info(f"파트 {part_number} 업로드 시작 (버퍼 크기: {part_buffer.tell()} 바이트)")
                part_buffer.seek(0)
                resp = s3_client.upload_part(
                    Bucket=S3_BUCKET, Key=s3_key, PartNumber=part_number,
                    UploadId=upload_id, Body=part_buffer.getvalue()
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                part_number += 1
            
            part_buffer = io.BytesIO()
            gzip_compressor = gzip.GzipFile(fileobj=part_buffer, mode="wb")

        try:
            for url in urls:
                try:
                    with requests.get(url, stream=True, timeout=300) as r:
                        if r.status_code == 404:
                            log.info(f"파일 없음 (404), {market_name} 피드의 마지막으로 간주하여 중단합니다: {url}")
                            break
                        r.raise_for_status()

                        log.info(f"처리 중: {url}")
                        source_stream = gzip.GzipFile(fileobj=r.raw) if url.endswith(".gz") else r.raw
                        
                        for line_bytes in source_stream:
                            line_str = line_bytes.decode('utf-8-sig')
                            reader = csv.reader([line_str], delimiter='\t')
                            fields = next(reader)
                            
                            output = io.StringIO()
                            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                            writer.writerow(fields)
                            
                            gzip_compressor.write(output.getvalue().encode('utf-8-sig'))
                        
                        if part_buffer.tell() >= MIN_PART_SIZE:
                            flush_buffer()

                except requests.exceptions.RequestException as e:
                    log.warning(f"URL 요청 실패, 건너뜁니다: {url}, 오류: {e}")
                    continue

            flush_buffer()

            if not parts:
                raise ValueError("업로드할 데이터가 없습니다.")

            log.info("모든 파트 업로드 완료. 최종 파일 조립을 시작합니다.")
            s3_client.complete_multipart_upload(
                Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
            log.info(f"성공: s3://{S3_BUCKET}/{s3_key} 파일 생성이 완료되었습니다.")

        except Exception as e:
            log.error(f"최종 오류 발생. 멀티파트 업로드를 중단합니다. 오류: {e}")
            s3_client.abort_multipart_upload(Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id)
            raise

    # --- DAG 흐름 정의 (호출 부분 수정) ---
    target_info = get_target_info() # 입력 변수 없이 호출
    process_market_files(target_info=target_info)

# DAG 인스턴스화
gmarket_google_feeds_dag()