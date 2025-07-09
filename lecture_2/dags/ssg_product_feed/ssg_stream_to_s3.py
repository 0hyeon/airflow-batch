import csv
import io
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 기본 설정 ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'aws_conn_id': 'aws_conn_id'
}

# --- 상수 정의 ---
URLS = {
    "ssg_all": "http://ep2.ssgadm.com/channel/ssg/ssg_facebookAgenEpAll.txt",
    "e_all": "http://ep2.ssgadm.com/channel/emart/e_facebookAgenEpAll.txt",
}
S3_BUCKET = 'gyoung0-test'
S3_KEY_PREFIX = 'ssg-raw-data'
CHUNK_SIZE_BYTES = 64 * 1024 * 1024

# --- DAG 정의 ---
dag = DAG(
    dag_id='ssg_landing_raw_csv_to_s3',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    catchup=False,
    tags=['ssg', 's3', 'final-solution'],
)

def stream_and_convert_to_csv_correctly(name: str, url: str, bucket: str, s3_key: str, delimiter: str, **context):
    """
    가장 안정적인 디코딩 방식을 사용하여 S3 멀티파트 업로드를 수행합니다.
    """
    s3_hook = S3Hook(aws_conn_id=context['dag_run'].conf.get('aws_conn_id', default_args['aws_conn_id']))
    s3_client = s3_hook.get_conn()

    print(f"Starting FINAL Corrected Stream: {url} -> s3://{bucket}/{s3_key}")
    
    upload_id = None
    csv_buffer = io.StringIO()
    try:
        mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=s3_key)
        upload_id = mpu['UploadId']
        
        parts = []
        part_number = 1
        csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL)

        with requests.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()

            response.encoding = 'euc-kr'
            line_iterator = response.iter_lines(decode_unicode=True)

            try:
                header_line = next(line_iterator)
                csv_writer.writerow(header_line.split(delimiter))
            except StopIteration:
                print(f"Warning: File {url} is empty.")
                s3_client.complete_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={'Parts': []})
                return

            for line in line_iterator:
                if not line: continue
                
                csv_writer.writerow(line.split(delimiter))
                
                if csv_buffer.tell() > CHUNK_SIZE_BYTES:
                    encoding = 'utf-8-sig' if part_number == 1 else 'utf-8'
                    
                    part_body = csv_buffer.getvalue().encode(encoding)
                    part = s3_client.upload_part(Body=part_body, Bucket=bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_number)
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                    
                    csv_buffer.seek(0)
                    csv_buffer.truncate(0)
                    part_number += 1

            remaining_data = csv_buffer.getvalue()
            if remaining_data:
                encoding = 'utf-8-sig' if part_number == 1 else 'utf-8'
                
                part_body = remaining_data.encode(encoding)
                part = s3_client.upload_part(Body=part_body, Bucket=bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_number)
                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

            s3_client.complete_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
            print(f"✅ Finished FINAL Corrected Stream upload: s3://{bucket}/{s3_key}")

    except Exception as e:
        print(f"스트리밍 변환/복사 중 오류 발생: {e}")
        if upload_id: s3_client.abort_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id)
        raise
    finally:
        if 'csv_buffer' in locals() and not csv_buffer.closed:
            csv_buffer.close()

# --- 태스크 생성 ---
for name, url in URLS.items():
    s3_key = f"{S3_KEY_PREFIX}/{name}_full.csv"
    PythonOperator(
        task_id=f'land_and_convert_{name}_to_csv',
        python_callable=stream_and_convert_to_csv_correctly,
        op_kwargs={'name': name, 'url': url, 'bucket': S3_BUCKET, 's3_key': s3_key, 'delimiter': '\t'},
        dag=dag,
    )