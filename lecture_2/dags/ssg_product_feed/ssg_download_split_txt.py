from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import os
import glob
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG setting
dag = DAG(
    dag_id='ssg_all_daily_txt_download',
    default_args=default_args,
    schedule_interval='30 5 * * *',
    catchup=False,
    tags=['text_download']
)

# URL defines
URLS = {
    "ssg_all": "http://ep2.ssgadm.com/channel/ssg/ssg_facebookAgenEpAll.txt",
    "e_all": "http://ep2.ssgadm.com/channel/emart/e_facebookAgenEpAll.txt",
}

# save path
BASE_SAVE_DIR = "/data/ssg_txt"

def download_and_convert_safely(name: str, url: str):
    """
    스트리밍으로 데이터를 받아, 한 줄씩 탭으로 분리한 뒤,
    csv 라이브러리를 사용하여 안전한 CSV 형식으로 변환 후 저장합니다.
    """
    save_path = os.path.join(BASE_SAVE_DIR, f"{name}_full.csv")
    print(f"Start safe conversion from {url} to {save_path}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        response.encoding = "euc-kr"

        with open(save_path, "w", encoding="utf-8-sig", newline='') as f:
            csv_writer = csv.writer(f, delimiter=',')
            
            line_iterator = response.iter_lines(decode_unicode=True)
            
            try:
                # 헤더 처리
                header_line = next(line_iterator)
                header_fields = header_line.split('\t')
                csv_writer.writerow(header_fields)
            except StopIteration:
                print(f"Warning: File from {url} is empty.")
                return

            # 데이터 라인 처리
            total_lines = 0
            for line in line_iterator:
                fields = line.split('\t')
                csv_writer.writerow(fields)
                total_lines += 1
                if total_lines % 100000 == 0:
                    print(f"  ... {total_lines} lines processed for {name}")

        print(f"Finished: {name} (Total {total_lines} lines safely saved as CSV to {save_path})")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise
    except Exception as e:
        print(f"Error processing data for {name}: {e}")
        raise

def download_all():
    """모든 URL에 대해 다운로드 함수를 순차적으로 실행합니다."""
    print("Cleaning up directory")
    if os.path.exists(BASE_SAVE_DIR):
        files_to_delte = glob.glob(os.path.join(BASE_SAVE_DIR, '*'))
        for f_path in files_to_delte:
            try:
                if os.path.isfile(f_path):
                    os.remove(f_path)
                    print(f" Delted file: {f_path}")
            except Exception as e:
                print(f" Error deleting file {f_path}: {e}")
    for name, url in URLS.items():
        download_and_convert_safely(name, url)

# Airflow Operator 정의
download_task = PythonOperator(
    task_id='download_all_txt_files_as_single',
    python_callable=download_all,
    dag=dag,
)

trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_next_dag_task',
    trigger_dag_id='ssg_csv_upload_to_s3_sequential',
    dag=dag,
)

download_task >> trigger_next_dag