from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd 
import pendulum # 1. pendulum import 추가

import requests
import os
from datetime import datetime, timedelta

SAVE_DIR = "/dags/data/appsflyer_albamon_csv"

# Default DAG setting
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,  # 🚨🚨📢Slack 알림 추가
}
dag = DAG(
    dag_id='albamon_fetch_appsflyer_csv_daily',
    default_args=default_args,
    schedule_interval="0 5 * * *", # KST 오전 5시는 UTC 20시
    catchup=False,
    tags=['appsflyer','albamon']
)

# API 호출
def fetch_appsflyer_csv(**kwargs):
    # 현재 서울 시간을 기준으로 어제 날짜를 계산
    target_date = pendulum.now('Asia/Seoul').subtract(days=1).strftime("%Y-%m-%d")
    print(f"✅ API 요청 대상 날짜: {target_date}")

    TOKEN = Variable.get("JOBKOREA_TOKEN")

    HEADERS = {
        "accept": "text/csv",
        "authorization": f"Bearer {TOKEN}",
    }
    
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    # 추가할 이벤트 목록
    new_events = ",experiences_views,experiences_comment_registration,experiences_registration"

    URLS = {
        "aos_리타겟팅": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.albamon.app/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Event_Flirting_completed3,Event_Participation_completed3,Event_subsidy_completed2,Event_Participation_completed2,Albatest_Attendance,Albatest_Gift,Albatest_Entry,Albatest_Area,Albatest_Share,ApplyCompletes,CallContactApply,CorpAccountCreations,EasyMessageContactApply,EmailContactApply,FreeJobRegist,HomepageContactApply,IndiviAccountCreations,JobRegist,MessageContactApply,ResumeNewCompletes,z_applycompletes,z_complete,z_jobresist,ResumeAddCompletes,25Event_gift_completed2{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.albamon.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Event_Flirting_completed3,Event_Participation_completed3,Event_subsidy_completed2,Event_Participation_completed2,Albatest_Attendance,Albatest_Gift,Albatest_Entry,Albatest_Area,Albatest_Share,ApplyCompletes,CallContactApply,CorpAccountCreations,EasyMessageContactApply,EmailContactApply,FreeJobRegist,HomepageContactApply,IndiviAccountCreations,JobRegist,MessageContactApply,ResumeNewCompletes,z_applycompletes,z_complete,z_jobresist,ResumeAddCompletes,25Event_gift_completed2{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_리타겟팅_bossmon": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.albamon.bossmon/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Boss_AccountCreations,Boss_signup_AM_complete,Boss_signup_apple,Boss_signup_kakao,Boss_signup_naver,Boss_invite,Boss_Invitationapproval,Boss_Deleteinvitation,Boss_Resignation,Boss_Withdrawal{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_ua_bossmon": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.albamon.bossmon/in_app_events_report/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Boss_AccountCreations,Boss_signup_AM_complete,Boss_signup_apple,Boss_signup_kakao,Boss_signup_naver,Boss_invite,Boss_Invitationapproval,Boss_Deleteinvitation,Boss_Resignation,Boss_Withdrawal{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_리타겟팅": f"https://hq1.appsflyer.com/api/raw-data/export/app/id382535825/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Event_Flirting_completed3,Event_Participation_completed3,Event_subsidy_completed2,Event_Participation_completed2,Albatest_Attendance,Albatest_Gift,Albatest_Entry,Albatest_Area,Albatest_Share,ApplyCompletes,CallContactApply,CorpAccountCreations,EasyMessageContactApply,EmailContactApply,FreeJobRegist,HomepageContactApply,IndiviAccountCreations,JobRegist,MessageContactApply,ResumeNewCompletes,z_applycompletes,z_complete,z_jobresist,ResumeAddCompletes,25Event_gift_completed2{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/id382535825/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&event_name=Event_Flirting_completed3,Event_Participation_completed3,Event_subsidy_completed2,Event_Participation_completed2,Albatest_Attendance,Albatest_Gift,Albatest_Entry,Albatest_Area,Albatest_Share,ApplyCompletes,CallContactApply,CorpAccountCreations,EasyMessageContactApply,EmailContactApply,FreeJobRegist,HomepageContactApply,IndiviAccountCreations,JobRegist,MessageContactApply,ResumeNewCompletes,z_applycompletes,z_complete,z_jobresist,ResumeAddCompletes,25Event_gift_completed2{new_events}&from={target_date}&to={target_date}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_리타겟팅_bossmon": f"https://hq1.appsflyer.com/api/raw-data/export/app/id6478902206/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Boss_AccountCreations,Boss_signup_AM_complete,Boss_signup_apple,Boss_signup_kakao,Boss_signup_naver,Boss_invite,Boss_Invitationapproval,Boss_Deleteinvitation,Boss_Resignation,Boss_Withdrawal{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_ua_bossmon": f"https://hq1.appsflyer.com/api/raw-data/export/app/id6478902206/in_app_events_report/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=Boss_AccountCreations,Boss_signup_AM_complete,Boss_signup_apple,Boss_signup_kakao,Boss_signup_naver,Boss_invite,Boss_Invitationapproval,Boss_Deleteinvitation,Boss_Resignation,Boss_Withdrawal{new_events}&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id"
    }

    for name, url in URLS.items():
        print(f"{name} 다운로드 중...")
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            save_path = os.path.join(SAVE_DIR, f"{name}.csv")
            with open(save_path, "w", encoding="utf-8-sig") as f:
                f.write(response.text)
            print(f"✅ 저장 완료 → {save_path}")
        else:
            print(f"❌ 실패: {name} → {response.status_code}")
        #target date를 XCom으로 전달
        kwargs['ti'].xcom_push(key='target_date', value=target_date)

def count_total_rows(**context):
    total_rows = 0

    for filename in os.listdir(SAVE_DIR):
        if filename.endswith(".csv"):
            file_path = os.path.join(SAVE_DIR, filename)
            df = pd.read_csv(file_path)
            row_count = len(df)
            total_rows += row_count
            print(f"{filename}: {row_count} rows")
    print(f"총 로우 합 {total_rows}")

    # XCom으로 다음 대그에 total_row를 전달해줌
    context['ti'].xcom_push(key='total_csv_rows', value=total_rows)
    
# 매일 실행 전 폴더 비우기
def cleanup_appsflyer_dir():
    if os.path.exists(SAVE_DIR):
        for filename in os.listdir(SAVE_DIR):
            file_path = os.path.join(SAVE_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        print(f"🧹 폴더 정리 완료: {SAVE_DIR}")
    else:
        os.makedirs(SAVE_DIR, exist_ok=True)
        print(f"📁 폴더 생성됨: {SAVE_DIR}")



# def fail_task():
#     raise ValueError("의도적인 에러 발생 테스트")

# 슬랙 메세지 실패 알림이 가는지 확인하기 위한 태스크
# test_fail = PythonOperator(
#     task_id='test_fail',
#     python_callable=fail_task,
#     dag=dag,
# )

# 실행전 SAVE_DIR 폴더내 파일 비우기 
cleanup_task = PythonOperator(
    task_id='cleanup_appsflyer_dir',
    python_callable=cleanup_appsflyer_dir,
    dag=dag,
)
# api호출후 파일저장
fetch_csv_task = PythonOperator(
    task_id='fetch_appsflyer_csv_files',
    python_callable=fetch_appsflyer_csv,
    dag=dag,
)

# 호출한 파일 로울 총합을 다음 task전달 
count_rows_task = PythonOperator(
    task_id='count_total_csv_rows',
    python_callable=count_total_rows,
    dag=dag,
)

# 디덕션 트리거
trigger_processing = TriggerDagRunOperator(
    task_id='trigger_process_dag',
    trigger_dag_id='albamon_process_appsflyer_data',  # 이 DAG ID로 호출
    conf={"fetch_row_count": "{{ti.xcom_pull(task_ids='count_total_csv_rows', key='total_csv_rows') }}",
        "target_date": "{{ ti.xcom_pull(task_ids='fetch_appsflyer_csv_files', key='target_date') }}"
        }, #XCom으로 다음 대그에 값 전달
    dag=dag,
)


# 기존플로우
# test_fail >> fetch_csv_task >> trigger_processing

#실행전 폴더비우기 추가
cleanup_task >> fetch_csv_task >> count_rows_task >> trigger_processing