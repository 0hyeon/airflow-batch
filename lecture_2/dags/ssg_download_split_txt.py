from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import os
import glob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG setting
dag = DAG(
    dag_id='ssg_all_daily_txt_download', # dag_id를 더 명확하게 변경
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
BASE_SAVE_DIR = "/opt/airflow/data/ssg_txt"

def download_and_save_single_file(name: str, url: str):
    """
    하나의 URL에서 스트리밍으로 데이터를 받아, 단일 텍스트 파일로 저장합니다.
    """
    folder_path = BASE_SAVE_DIR
    os.makedirs(folder_path, exist_ok=True)
    # 저장될 파일 이름을 더 명확하게 변경 (예: ssg_all_full.txt)
    save_path = os.path.join(folder_path, f"{name}_full.txt")

    print(f"Start downloading from {url} to {save_path}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() # HTTP 오류 발생 시 예외 처리
        # 원본 소스의 인코딩을 지정
        response.encoding = "euc-kr"

        # 결과 파일을 쓰기 모드('w')로 한 번만 엽니다.
        with open(save_path, "w", encoding="utf-8-sig") as f:
            line_iterator = response.iter_lines(decode_unicode=True)
            
            # 첫 번째 줄(헤더)을 먼저 읽어서 씁니다.
            try:
                header = next(line_iterator)
                f.write(header + "\n")
            except StopIteration:
                print(f"Warning: File from {url} is empty.")
                return

            # 나머지 데이터 라인을 순차적으로 씁니다.
            total_lines = 0
            for line in line_iterator:
                f.write(line + "\n")
                total_lines += 1
                # 진행 상황을 확인하기 위한 로그 (10만 라인마다 출력)
                if total_lines % 100000 == 0:
                    print(f"  ... {total_lines} lines written for {name}")

        print(f"Finished: {name} (Total {total_lines} data lines saved to {save_path})")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
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
        download_and_save_single_file(name, url)

# Airflow Operator 정의
download_task = PythonOperator(
    task_id='download_all_txt_files_as_single',
    python_callable=download_all,
    dag=dag,
)

trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_next_dag_task',
    trigger_dag_id='ssg_txt_upload_to_s3_sequential',
    dag=dag,
)

download_task >> trigger_next_dag