from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
# from plugins import slack 
#
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,  # 🚨🚨📢Slack 알림 추가
}

dag = DAG(
    'albamon_process_and_upload_deduction_to_s3_with_hook',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


def upload_to_s3_with_hook(**context):
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')  # 연결 ID에 맞게 설정
    bucket = 'gyoung0-test'
    target_date = context["dag_run"].conf.get("target_date")
    print(f"📦 업로드 대상 날짜: {target_date}")
    s3_key = f'deduction-csv/date={target_date}/final_attachment_albamon.csv'
    local_file_path = '/dags/data/final_attachment_albamon.csv'

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"파일이 존재하지 않음: {local_file_path}")

    s3_hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )
    print(f"✅ 업로드 완료: s3://{bucket}/{s3_key}")

upload_task = PythonOperator(
    task_id='upload_deduction_csv_to_s3',
    python_callable=upload_to_s3_with_hook,
    provide_context=True,  # 이거 빠지면 context 못 씀
    dag=dag,
)

trigger_processing = TriggerDagRunOperator(
    task_id='trigger_process_dag',
    trigger_dag_id='albamon_iceberg_to_s3_emr',  # 이 DAG ID로 호출
    conf={
        "target_date": "{{ dag_run.conf['target_date'] }}"
    },
    dag=dag,
)

upload_task >> trigger_processing