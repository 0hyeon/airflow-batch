from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import os

SOURCE_AND_DEST_DIR = '/opt/airflow/data/ssg_txt'

# 처리할 파일 목록
FILES_TO_PROCESS = [
    'ssg_brief_full.txt',
    'e_brief_full.txt'
]
KEY_COLUMN = 'id'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# --- Python 함수 정의 ---

def deduplicate_files_in_place():
    """
    파일을 읽어 중복을 제거한 뒤, 같은 위치에 덮어쓰기합니다.
    """
    for file_name in FILES_TO_PROCESS:
        file_path = os.path.join(SOURCE_AND_DEST_DIR, file_name)

        print("-" * 50)
        print(f"Processing file: {file_path}")

        try:
            if not os.path.exists(file_path):
                print(f"WARNING: Source file not found, skipping: {file_path}")
                continue

            # 1. 파일을 메모리로 모두 읽어들입니다.
            df = pd.read_csv(file_path, sep='\t', dtype={KEY_COLUMN: str})
            original_count = len(df)
            print(f"INFO: Read {original_count} rows from source.")

            # 2. 메모리에서 중복 제거 작업을 수행합니다.
            deduplicated_df = df.drop_duplicates(subset=[KEY_COLUMN], keep='last')
            final_count = len(deduplicated_df)
            
            print(f"INFO: Deduplication complete. {original_count} -> {final_count} rows.")
            
            # 3. 원본 파일을 열어, 중복 제거된 새로운 내용으로 덮어씁니다.
            deduplicated_df.to_csv(file_path, sep='\t', index=False, header=True, encoding='utf-8-sig')
            
            print(f"SUCCESS: Overwrote deduplicated data to '{file_path}'")

        except Exception as e:
            print(f"ERROR processing file {file_path}: {e}")
            raise

# --- Airflow DAG 및 Operator 정의 ---

with DAG(
    dag_id="ssg_pandas_deduplicate_brief_files_inplace",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pandas', 'deduplication'],
) as dag:
    
    run_deduplication_task = PythonOperator(
        task_id='run_pandas_deduplication_inplace',
        python_callable=deduplicate_files_in_place,
    )


trigger_upload_dag = TriggerDagRunOperator(
    task_id='trigger_upload_to_s3_dag',
    trigger_dag_id='ssg_brief_txt_upload_to_s3_sequential',  # 트리거할 DAG ID
)

run_deduplication_task >> trigger_upload_dag
