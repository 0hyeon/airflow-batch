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

        try:
            for index, url in enumerate(urls):
                part_number = index + 1
                print(f"--- {market_name} 파트 {part_number} 처리 시작: {url} ---")

                # 1. 원본 TSV.GZ 파일 다운로드
                with requests.get(url, stream=True, timeout=300) as response:
                    response.raise_for_status()
                    content = response.content

                # 2. Gzip 압축 해제
                decompressed_content = gzip.decompress(content)

                # TSV 원본을 문자열로 변환

                # -- 추가 코드 --
                try:
                    decoded_str = decompressed_content.decode("utf-8-sig")
                except UnicodeDecodeError:
                    decoded_str = decompressed_content.decode("cp949", errors="replace")

                decoded_str = decoded_str.replace("\x00", "")
                # --- TSV to CSV 변환 (가장 안전한 방식) ---
                # 메모리상에서 파일처럼 다루기 위해 io.StringIO 사용
                tsv_file = io.StringIO(decoded_str)
                csv_file = io.StringIO()

                # TSV 리더(Reader)에게 입력 구분자가 탭('\t')임을 명시
                tsv_reader = csv.reader(tsv_file, delimiter="\t")

                # CSV 라이터(Writer)를 사용하여 새 CSV 내용 생성
                # 이 과정에서 라이브러리가 자동으로 따옴표 처리 등을 수행
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerows(tsv_reader)

                # 메모리에 생성된 최종 CSV 문자열 가져오기
                csv_content_str = csv_file.getvalue()

                # 3. CSV로 변환된 내용을 다시 Gzip으로 압축
                csv_content_bytes = csv_content_str.encode("utf-8-sig")
                final_part_body = gzip.compress(csv_content_bytes)

                # 4. S3에 멀티파트 업로드
                part = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=final_part_body,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            # 5. 모든 파트 업로드 완료 후 최종 파일 조립
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
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id
            )
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
