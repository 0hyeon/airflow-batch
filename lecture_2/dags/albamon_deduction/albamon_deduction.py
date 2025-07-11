from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'albamon_process_appsflyer_data',
    default_args=default_args,
    description='Process Albamon AppsFlyer data and trigger S3 upload.',
    schedule_interval=None,
    catchup=False,
    tags=['appsflyer', 'albamon', 'process']
)

def run_memory_optimized_processing():
    # 경로를 첫 번째 DAG의 저장 경로와 동일하게 수정합니다.
    BASE_PATH = "/opt/airflow/data/appsflyer_albamon_csv"
    os.makedirs(BASE_PATH, exist_ok=True)

    OUTPUT_CSV_PATH = os.path.join(BASE_PATH, "final_albamon_attachment.csv")
    
    FILES = [
        "aos_리타겟팅.csv", "aos_ua.csv", "aos_리타겟팅_bossmon.csv", "aos_ua_bossmon.csv",
        "ios_리타겟팅.csv", "ios_ua.csv", "ios_리타겟팅_bossmon.csv", "ios_ua_bossmon.csv"
    ]

    def process_ITET(df):
        df["Event Time"] = pd.to_datetime(df["Event Time"])
        df["Install Time"] = pd.to_datetime(df["Install Time"])
        df["Time Difference"] = (df["Event Time"] - df["Install Time"]).dt.total_seconds() / 3600
        is_over_24 = df["Time Difference"] >= 24 * 1.10
        return df[~is_over_24].copy(), df[is_over_24].copy()

    def process_CTIT(df):
        # 'Attributed Touch Time' 컬럼이 없으면 빈 데이터프레임 반환
        if 'Attributed Touch Time' not in df.columns or df['Attributed Touch Time'].isnull().all():
            return df, pd.DataFrame()
        df["Install Time"] = pd.to_datetime(df["Install Time"])
        df["Attributed Touch Time"] = pd.to_datetime(df["Attributed Touch Time"])
        df["Time Difference"] = (df["Install Time"] - df["Attributed Touch Time"]).dt.total_seconds() / 3600
        is_over_24 = df["Time Difference"] >= 24 * 1.10
        return df[~is_over_24].copy(), df[is_over_24].copy()

    def processProd(csv_data, osAdid, osMediaSource):
        if 'Retargeting Conversion Type' not in csv_data.columns:
            return csv_data, pd.DataFrame()
        re_engagement_df = csv_data[csv_data["Retargeting Conversion Type"] == "re-engagement"].copy()
        if re_engagement_df.empty:
            return csv_data, pd.DataFrame()
        
        if osAdid not in re_engagement_df.columns:
            return csv_data, pd.DataFrame()
            
        re_engagement_df = re_engagement_df.groupby(osAdid).filter(lambda x: len(x) >= 3)
        if re_engagement_df.empty:
            return csv_data, pd.DataFrame()

        re_engagement_df["Event Time"] = pd.to_datetime(re_engagement_df["Event Time"])
        
        target_indices = []
        for ad_id, group in re_engagement_df.groupby(osAdid):
            if not group["Media Source"].isin(osMediaSource).all():
                continue
            
            sorted_group = group.sort_values("Event Time")
            for i in range(len(sorted_group) - 2):
                start_time = sorted_group.iloc[i]["Event Time"]
                time_threshold = start_time + timedelta(minutes=1)
                if i + 2 < len(sorted_group) and sorted_group.iloc[i+2]["Event Time"] <= time_threshold:
                    target_indices.extend(group.index.tolist())
                    break
        
        if not target_indices:
            return csv_data, pd.DataFrame()

        target_indices = list(set(target_indices))
        dropped_rows_df = csv_data.loc[target_indices].copy()
        remaining_df = csv_data.drop(target_indices)
        return remaining_df, dropped_rows_df

    is_first_write = True
    columns_to_keep = [
        "Attributed Touch Type","Attributed Touch Time","Install Time","Event Time","Event Name","Event Value","Event Revenue","Event Revenue Currency",
        "Event Revenue USD","Event Source","Is Receipt Validated","Partner","Media Source","Channel","Keywords","Campaign","Campaign ID","Adset","Adset ID",
        "Ad","Ad ID","Ad Type","Site ID","Sub Site ID","Sub Param 1","Sub Param 2","Sub Param 3","Sub Param 4","Sub Param 5","Cost Model","Cost Value",
        "Cost Currency","Contributor 1 Partner","Contributor 1 Media Source","Contributor 1 Campaign","Contributor 1 Touch Type","Contributor 1 Touch Time",
        "Contributor 2 Partner","Contributor 2 Media Source","Contributor 2 Campaign","Contributor 2 Touch Type","Contributor 2 Touch Time",
        "Contributor 3 Partner","Contributor 3 Media Source","Contributor 3 Campaign","Contributor 3 Touch Type","Contributor 3 Touch Time",
        "Region","Country Code","State","City","Postal Code","DMA","IP","WIFI","Operator","Carrier","Language","AppsFlyer ID","Advertising ID",
        "IDFA","Android ID","Customer User ID","IMEI","IDFV","Platform","Device Type","OS Version","App Version","SDK Version","App ID","App Name",
        "Bundle ID","Is Retargeting","Retargeting Conversion Type","Attribution Lookback","Reengagement Window","Is Primary Attribution",
        "User Agent","HTTP Referrer","Original URL","Device Category","Match Type","Conversion Type","Campaign Type","Device Model","Keyword ID"
    ]

    if os.path.exists(OUTPUT_CSV_PATH):
        os.remove(OUTPUT_CSV_PATH)
        
    for fname in FILES:
        fpath = os.path.join(BASE_PATH, fname)
        if not os.path.exists(fpath):
            print(f"파일 없음, 건너뛰기: {fname}")
            continue
        
        print(f"파일 처리 시작: {fname}")
        df_source = pd.read_csv(fpath, encoding="utf-8-sig", low_memory=False)

        if df_source.empty:
            print(f"파일 내용 없음, 건너뛰기: {fname}")
            continue

        processed_chunks = []
        paramsData = {
            "aosMediaSource": ["appier_int", "cauly_int", "toastexchange_int", "tossa3u_int", "adisonofferwall_int", "OKPOS", "facebook_display"],
            "iosMediaSource": ["appier_int", "toastexchange_int", "tossa3u_int", "adisonofferwall_int", "OKPOS", "facebook_display"],
            "aosAdvertising": "Advertising ID", "iosIDFA": "IDFA",
        }

        # 'Is Primary Attribution' 컬럼이 없는 경우를 대비
        if 'Is Primary Attribution' in df_source.columns:
            df_false = df_source[df_source["Is Primary Attribution"] == False].copy()
            df_source = df_source[df_source["Is Primary Attribution"] != False].copy()
            if not df_false.empty:
                df_false.insert(0, "구분", "FALSE")
                processed_chunks.append(df_false)
        
        os_type = "aos" if "aos" in fname else "ios"
        adid_key, media_source_key = (paramsData["aosAdvertising"], paramsData["aosMediaSource"]) if os_type == "aos" else (paramsData["iosIDFA"], paramsData["iosMediaSource"])
        
        df_normal, df_prod = processProd(df_source, adid_key, media_source_key)
        df_normal, df_itet = process_ITET(df_normal)
        df_normal, df_ctit = process_CTIT(df_normal)

        if not df_normal.empty: df_normal.insert(0, "구분", "정상"); processed_chunks.append(df_normal)
        if not df_prod.empty: df_prod.insert(0, "구분", "프로드"); processed_chunks.append(df_prod)
        if not df_itet.empty: df_itet.insert(0, "구분", "ITET"); processed_chunks.append(df_itet)
        if not df_ctit.empty: df_ctit.insert(0, "구분", "CTIT"); processed_chunks.append(df_ctit)

        if not processed_chunks: continue

        df_chunk_total = pd.concat(processed_chunks, ignore_index=True)
        
        for col in columns_to_keep:
            if col not in df_chunk_total.columns:
                df_chunk_total[col] = None
        
        df_chunk_total = df_chunk_total[["구분"] + columns_to_keep]
        
        if is_first_write:
            df_chunk_total.to_csv(OUTPUT_CSV_PATH, mode='w', index=False, encoding='utf-8-sig')
            is_first_write = False
        else:
            df_chunk_total.to_csv(OUTPUT_CSV_PATH, mode='a', index=False, header=False, encoding='utf-8-sig')
            
        print(f"파일 처리 완료 및 추가: {fname}, {len(df_chunk_total)} rows")

    if is_first_write:
        print("경고: 모든 소스 파일을 처리했지만, 유효한 데이터가 한 건도 없어 빈 파일을 생성합니다.")
        pd.DataFrame(columns=["구분"] + columns_to_keep).to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')

process_task = PythonOperator(
    task_id='run_memory_optimized_processing',
    python_callable=run_memory_optimized_processing,
    dag=dag,
)

trigger_s3_upload = TriggerDagRunOperator(
    task_id='trigger_s3_upload_dag',
    trigger_dag_id='albamon_upload_to_s3',
    conf={
        "target_date": "{{ dag_run.conf.get('target_date') }}",
        "file_path": "/opt/airflow/data/appsflyer_albamon_csv/final_albamon_attachment.csv"
    },
    dag=dag,
)

process_task >> trigger_s3_upload