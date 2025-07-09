from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os

AWS_CONN_ID = 'aws_conn_id'
S3_BUCKET = 'gyoung0-test'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#
dag = DAG(
    dag_id='albamon_upload_to_s3',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['appsflyer', 'albamon', 's3', 'upload']
)

def upload_to_s3(**context):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # 이전 DAG로부터 동적으로 파일 경로와 날짜를 전달받습니다.
    target_date = context["dag_run"].conf.get("target_date")
    local_file_path = context["dag_run"].conf.get("file_path")

    if not target_date or not local_file_path:
        raise ValueError("'target_date' 또는 'file_path'를 전달받지 못했습니다.")
    
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"업로드할 파일을 찾을 수 없습니다: {local_file_path}")

    # S3에 저장될 파일명과 경로를 설정합니다.
    s3_key = f'deduction-csv/date={target_date}/final_attachment_albamon.csv'
    
    print(f"파일 업로드를 시작합니다...")
    print(f"로컬 경로: {local_file_path}")
    print(f"S3 경로: s3://{S3_BUCKET}/{s3_key}")

    s3_hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"업로드 완료: s3://{S3_BUCKET}/{s3_key}")

# S3 업로드 태스크
upload_task = PythonOperator(
    task_id='upload_final_csv_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

# EMR 처리 DAG를 호출하는 트리거 태스크
trigger_emr_processing = TriggerDagRunOperator(
    task_id='trigger_emr_processing_dag',
    trigger_dag_id='albamon_iceberg_to_s3_emr',  # EMR DAG의 ID
    conf={
        "target_date": "{{ dag_run.conf.get('target_date') }}"
    },
    dag=dag,
)

# 태스크 실행 순서: 업로드 후 EMR 트리거
upload_task >> trigger_emr_processing