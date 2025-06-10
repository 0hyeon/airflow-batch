from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# from plugins import slack
import pandas as pd


import requests
import os
from datetime import datetime, timedelta

SAVE_DIR = "/dags/data/appsflyer_csv"

# Default DAG setting
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,  # 🚨🚨📢Slack 알림 추가
}
dag = DAG(
    dag_id="jabko_fetch_appsflyer_csv_daily",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["appsflyer", "jbko"],
)


# API 호출
def fetch_appsflyer_csv(ds, **kwargs):
    execution_date = datetime.strptime(ds, "%Y-%m-%d")
    print("execution_date : ", execution_date)

    target_date = (execution_date + timedelta(days=1)).strftime("%Y-%m-%d")
    print("target_date : ", target_date)

    TOKEN = Variable.get("JOBKOREA_TOKEN")

    HEADERS = {
        "accept": "text/csv",
        "authorization": f"Bearer {TOKEN}",
    }

    os.makedirs(SAVE_DIR, exist_ok=True)

    URLS = {
        "aos_원픽": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=1pick_view_jobposting&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_원픽_2": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=1pick_view_jobposting&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_리타겟팅": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=contentslab_view,careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,job_apply_complete_homepage,job_apply_complete_mobile,signup_all,resume_complete_1st,resume_complete_re,1st_update_complete,re_update_complete,minikit_complete,1pick_direct_apply,jobposting_complete,1pick_offer_apply,1pick_start,profile_complete&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "aos_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={target_date}&to={target_date}&category=standard&event_name=contentslab_view,careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,1st_update_complete,signup_all,job_apply_complete_homepage,job_apply_complete_mobile,re_update_complete,resume_complete_1st,resume_complete_re,minikit_complete,jobposting_complete,1pick_direct_apply,business_signup_complete,1pick_offer_apply,1pick_start,profile_complete&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_원픽": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?timezone=Asia%2FSeoul&category=standard&from={target_date}&to={target_date}&event_name=1pick_view_jobposting&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_원픽_2": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&from={target_date}&to={target_date}&event_name=1pick_view_jobposting&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_리타겟팅": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?timezone=Asia%2FSeoul&category=standard&from={target_date}&to={target_date}&event_name=contentslab_view,careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,job_apply_complete_mobile,1st_update_complete,signup_all,resume_complete_1st,resume_complete_re,minikit_complete,re_update_complete,1pick_direct_apply,job_apply_complete_homepage,jobposting_complete,1pick_start,profile_complete&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
        "ios_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&from={target_date}&to={target_date}&event_name=contentslab_view,careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,1st_update_complete,job_apply_complete_homepage,job_apply_complete_mobile,resume_complete_re,signup_all,re_update_complete,minikit_complete,resume_complete_1st,jobposting_complete,1pick_direct_apply,business_signup_complete,1pick_offer_apply,1pick_start,profile_complete&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
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
        # target date를 XCom으로 전달
        kwargs["ti"].xcom_push(key="target_date", value=target_date)


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
    context["ti"].xcom_push(key="total_csv_rows", value=total_rows)


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
    task_id="cleanup_appsflyer_dir",
    python_callable=cleanup_appsflyer_dir,
    dag=dag,
)
# api호출후 파일저장
fetch_csv_task = PythonOperator(
    task_id="fetch_appsflyer_csv_files",
    provide_context=True,
    python_callable=fetch_appsflyer_csv,
    dag=dag,
)

# 호출한 파일 로울 총합을 다음 task전달
count_rows_task = PythonOperator(
    task_id="count_total_csv_rows",
    python_callable=count_total_rows,
    provide_context=True,
    dag=dag,
)

# 디덕션 트리거
trigger_processing = TriggerDagRunOperator(
    task_id="trigger_process_dag",
    trigger_dag_id="jabko_process_appsflyer_data",  # 이 DAG ID로 호출
    conf={
        "fetch_row_count": "{{ti.xcom_pull(task_ids='count_total_csv_rows', key='total_csv_rows') }}",
        "target_date": "{{ ti.xcom_pull(task_ids='fetch_appsflyer_csv_files', key='target_date') }}",
    },  # XCom으로 다음 대그에 값 전달
    dag=dag,
)


# 기존플로우
# test_fail >> fetch_csv_task >> trigger_processing

# 실행전 폴더비우기 추가
cleanup_task >> fetch_csv_task >> count_rows_task >> trigger_processing
