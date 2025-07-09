import csv
import io
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    "ssg_brief": "http://ep2.ssgadm.com/channel/ssg/ssg_facebookAgenEpBrief.txt",
    "e_brief": "http://ep2.ssgadm.com/channel/emart/e_facebookAgenEpBrief.txt",
}
S3_BUCKET = 'gyoung0-test'
S3_KEY_PREFIX = 'ssg-raw-data'
CHUNK_SIZE_BYTES = 64 * 1024 * 1024
EMR_UPSERT_DAG_ID = 'ssg_pandas_deduplicate_brief_files_inplace'


def stream_and_convert_to_csv_final(name: str, url: str, **context):
    """
    [최종] 원본(euc-kr)을 읽어 표준(utf-8) CSV로 변환하며 S3에 스트리밍 업로드합니다.
    """
    s3_hook = S3Hook(aws_conn_id=default_args['aws_conn_id'])
    s3_client = s3_hook.get_conn()
    
    s3_key = f'{S3_KEY_PREFIX}/{name}_full.csv'
    delimiter = '\t'

    print(f"Starting Production Stream: {url} -> s3://{S3_BUCKET}/{s3_key}")
    
    upload_id = None
    csv_buffer = io.StringIO()
    try:
        mpu = s3_client.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_key)
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
                s3_client.complete_multipart_upload(Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id, MultipartUpload={'Parts': []})
                return

            for line in line_iterator:
                if not line: continue
                
                csv_writer.writerow(line.split(delimiter))
                
                if csv_buffer.tell() > CHUNK_SIZE_BYTES:
                    encoding = 'utf-8-sig' if part_number == 1 else 'utf-8'
                    part_body = csv_buffer.getvalue().encode(encoding)
                    
                    part = s3_client.upload_part(Body=part_body, Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id, PartNumber=part_number)
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                    
                    csv_buffer.seek(0)
                    csv_buffer.truncate(0)
                    part_number += 1

        remaining_data = csv_buffer.getvalue()
        if remaining_data:
            encoding = 'utf-8-sig' if part_number == 1 else 'utf-8'
            part_body = remaining_data.encode(encoding)
            
            part = s3_client.upload_part(Body=part_body, Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id, PartNumber=part_number)
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

        s3_client.complete_multipart_upload(Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
        print(f"✅ Finished Production Stream upload: s3://{S3_BUCKET}/{s3_key}")

    except Exception as e:
        print(f"스트리밍 변환 중 오류 발생: {e}")
        if upload_id: s3_client.abort_multipart_upload(Bucket=S3_BUCKET, Key=s3_key, UploadId=upload_id)
        raise
    finally:
        if 'csv_buffer' in locals() and not csv_buffer.closed:
            csv_buffer.close()


with DAG(
    dag_id='ssg_brief_convert_and_upload_to_s3_production',
    default_args=default_args,
    schedule_interval='30 17 * * *',
    catchup=False,
    tags=['ssg', 's3', 'etl', 'production'],
) as dag:
    tasks = []
    for name, url in URLS.items():
        task = PythonOperator(
            task_id=f'convert_and_upload_{name}',
            python_callable=stream_and_convert_to_csv_final,
            op_kwargs={'name': name, 'url': url},
        )
        tasks.append(task)

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_next_dag_task',
        trigger_dag_id=EMR_UPSERT_DAG_ID,
    )

    tasks >> trigger_next_dag