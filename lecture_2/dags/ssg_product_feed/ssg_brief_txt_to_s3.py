from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_LOCAL_PATH = '/data/ssg_txt'
FILES_TO_UPLOAD = {
    'ssg_brief': 'ssg_brief_full.csv',
    'e_brief': 'e_brief_full.csv'
}

dag = DAG(
    'ssg_brief_txt_upload_to_s3_sequential',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'upload', 'sequential']
)

def upload_single_file_to_s3(file_key: str, file_name: str, **context):
    # (함수 내용은 이전과 동일)
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    bucket = 'gyoung0-test'
    local_file_path = os.path.join(BASE_LOCAL_PATH, file_name)
    s3_key = f'ssg-raw-data/{file_name}'
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"로컬 파일이 존재하지 않습니다: {local_file_path}")
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )
    print(f"✅ 업로드 완료: s3://{bucket}/{s3_key}")

previous_task = None
for key, filename in FILES_TO_UPLOAD.items():
    task = PythonOperator(
        task_id=f'upload_{key}_to_s3',
        python_callable=upload_single_file_to_s3,
        op_kwargs={'file_key': key, 'file_name': filename},
        dag=dag,
    )
    
    # previous_task 변수에 태스크가 있다면, 현재 태스크와 순서를 연결합니다.
    if previous_task:
        previous_task >> task
    
    # 다음 루프를 위해 현재 태스크를 previous_task 변수에 할당합니다.
    previous_task = task


trigger_emr_dag = TriggerDagRunOperator(
    task_id='trigger_emr_iceberg_job',
    trigger_dag_id='ssg_brief_upsert_to_all_emr',
    dag=dag,
)

previous_task >> trigger_emr_dag