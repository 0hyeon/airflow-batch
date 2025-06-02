from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import asyncio
from io import StringIO, BytesIO
import aiofiles
import os
from datetime import timedelta as td
# from plugins import slack 
# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'on_failure_callback': slack.on_failure_callback,  # 🚨🚨📢Slack 알림 추가
}

# api 호출 후 실행되게 할 예정
dag = DAG(
    'albamon_process_appsflyer_data',
    default_args=default_args,
    description='albamon fetch appsflyer_data',
    schedule_interval=None,
    catchup=False,
)

def run_async_processing():
    import nest_asyncio
    nest_asyncio.apply()
    import pandas as pd
    from io import StringIO, BytesIO
    import aiofiles
    import os
    import asyncio
    from datetime import timedelta

    BASE_PATH = "/dags/data/appsflyer_albamon_csv"
    FILES = [
        "aos_리타겟팅.csv",
        "aos_ua.csv",
        "aos_리타겟팅_bossmon.csv",
        "aos_ua_bossmon.csv",
        "ios_리타겟팅.csv",
        "ios_ua.csv",
        "ios_리타겟팅_bossmon.csv",
        "ios_ua_bossmon.csv"
    ]


    files = {}
    for fname in FILES:
        fpath = os.path.join(BASE_PATH, fname)
        with open(fpath, "r", encoding="utf-8-sig") as f:
            files[fname] = f.read()
            
    async def process_file(file_key, file_obj):
        filename, _ = file_key.split(".")
        csv_data = StringIO(str(file_obj))
        csv_df = pd.read_csv(csv_data, encoding="utf-8-sig")
        if not os.path.exists("data"):
            os.makedirs("data")
        async with aiofiles.open(f"data/{filename}.json", "w") as f:
            await f.write(csv_df.to_json(orient="records", lines=True))

    async def process_ITET(df):
        df["Event Time"] = pd.to_datetime(df["Event Time"])
        df["Install Time"] = pd.to_datetime(df["Install Time"])
        df["Time Difference"] = (df["Event Time"] - df["Install Time"]).dt.total_seconds() / 3600
        df_over_24 = df[df["Time Difference"] >= 24 * 1.05].drop("Time Difference", axis=1)
        return df[df["Time Difference"] < 24 * 1.05], df_over_24

    async def process_CTIT(df):
        df["Install Time"] = pd.to_datetime(df["Install Time"])
        df["Attributed Touch Time"] = pd.to_datetime(df["Attributed Touch Time"])
        df["Time Difference"] = (df["Install Time"] - df["Attributed Touch Time"]).dt.total_seconds() / 3600
        df_over_24 = df[df["Time Difference"] >= 24 * 1.05].drop("Time Difference", axis=1)
        return df[df["Time Difference"] < 24 * 1.05], df_over_24
    
    async def process_dataframes():
        combined_df_ua = pd.DataFrame()
        combined_df_retarget_AOS = pd.DataFrame()
        combined_df_retarget_IOS = pd.DataFrame()
        dropped_ITET_rows_df_ua_total = pd.DataFrame()
        dropped_ITET_rows_df_ios_total = pd.DataFrame()
        dropped_ITET_rows_df_aos_total = pd.DataFrame()
        dropped_CTIT_rows_df_ua_total = pd.DataFrame()
        dropped_CTIT_rows_df_ios_total = pd.DataFrame()
        dropped_CTIT_rows_df_aos_total = pd.DataFrame()
        dropped_PROD_rows_df_aos = pd.DataFrame()
        dropped_PROD_rows_df_ios = pd.DataFrame()
        dropped_FALSE_rows_df_total = pd.DataFrame()
        total_df = pd.DataFrame()
        paramsData = {
            "aosMediaSource": ["appier_int", "cauly_int","toastexchange_int","tossa3u_int","adisonofferwall_int","OKPOS","facebook_display"],
            "iosMediaSource": ["appier_int","toastexchange_int","tossa3u_int","adisonofferwall_int","OKPOS","facebook_display"],
            "aosAdvertising": "Advertising ID",
            "iosIDFA": "IDFA",
        }
        for file_key, file_obj in files.items():
            filename, _ = file_key.split(".")
            csv_data = StringIO(str(file_obj))
            df = pd.read_csv(csv_data, encoding="utf-8-sig")
            if "리타겟팅" in filename:
                if "aos" in filename:
                    return_csv_df_aos, dropped_rows_df_aos = await processProd(
                        df, paramsData["aosAdvertising"], paramsData["aosMediaSource"]
                    )
                    processed_ITET_df, dropped_ITET_rows_df_data_aos = await process_ITET(return_csv_df_aos)
                    dropped_ITET_rows_df_aos_total = pd.concat([
                        dropped_ITET_rows_df_data_aos, dropped_ITET_rows_df_aos_total
                    ])
                    return_csv_df_aos = return_csv_df_aos[~return_csv_df_aos.index.isin(dropped_ITET_rows_df_data_aos.index)]

                    processed_CTIT_df, dropped_CTIT_rows_df_data_aos = await process_CTIT(return_csv_df_aos)
                    dropped_CTIT_rows_df_aos_total = pd.concat([
                        dropped_CTIT_rows_df_data_aos, dropped_CTIT_rows_df_aos_total
                    ])
                    return_csv_df_aos = return_csv_df_aos[~return_csv_df_aos.index.isin(dropped_CTIT_rows_df_data_aos.index)]

                    return_csv_df_aos = return_csv_df_aos.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    combined_df_retarget_AOS = pd.concat([combined_df_retarget_AOS, return_csv_df_aos])
                    dropped_PROD_rows_df_aos = pd.concat([dropped_PROD_rows_df_aos, dropped_rows_df_aos])

                elif "ios" in filename:
                    return_csv_df_ios, dropped_rows_df_ios = await processProd(
                        df, paramsData["iosIDFA"], paramsData["iosMediaSource"]
                    )
                    processed_ITET_df, dropped_ITET_rows_df_data_ios = await process_ITET(return_csv_df_ios)
                    dropped_ITET_rows_df_ios_total = pd.concat([
                        dropped_ITET_rows_df_data_ios, dropped_ITET_rows_df_ios_total
                    ])
                    return_csv_df_ios = return_csv_df_ios[~return_csv_df_ios.index.isin(dropped_ITET_rows_df_data_ios.index)]

                    processed_CTIT_df, dropped_CTIT_rows_df_data_ios = await process_CTIT(return_csv_df_ios)
                    dropped_CTIT_rows_df_ios_total = pd.concat([
                        dropped_CTIT_rows_df_data_ios, dropped_CTIT_rows_df_ios_total
                    ])
                    return_csv_df_ios = return_csv_df_ios[~return_csv_df_ios.index.isin(dropped_CTIT_rows_df_data_ios.index)]

                    return_csv_df_ios = return_csv_df_ios.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    combined_df_retarget_IOS = pd.concat([combined_df_retarget_IOS, return_csv_df_ios])
                    dropped_PROD_rows_df_ios = pd.concat([dropped_PROD_rows_df_ios, dropped_rows_df_ios])

            elif "ua" in filename:
                dropped_FALSE_rows_df = df[df["Is Primary Attribution"] == False]
                no_false_csv_df = df[df["Is Primary Attribution"] != False]
                processed_ITET_df, dropped_ITET_rows_df_data_ua = await process_ITET(no_false_csv_df)
                dropped_ITET_rows_df_ua_total = pd.concat([dropped_ITET_rows_df_data_ua, dropped_ITET_rows_df_ua_total])
                no_false_csv_df = no_false_csv_df[~no_false_csv_df.index.isin(dropped_ITET_rows_df_data_ua.index)]

                processed_CTIT_df, dropped_CTIT_rows_df_data_ua = await process_CTIT(no_false_csv_df)
                dropped_CTIT_rows_df_ua_total = pd.concat([dropped_CTIT_rows_df_data_ua, dropped_CTIT_rows_df_ua_total])
                no_false_csv_df = no_false_csv_df[~no_false_csv_df.index.isin(dropped_CTIT_rows_df_data_ua.index)]
                no_false_csv_df = no_false_csv_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                dropped_FALSE_rows_df_total = pd.concat([dropped_FALSE_rows_df_total, dropped_FALSE_rows_df])
                combined_df_ua = pd.concat([combined_df_ua, no_false_csv_df])
        
        columns_to_keep_ws1 = [ 
            "Attributed Touch Type",
            "Attributed Touch Time",
            "Install Time",
            "Event Time",
            "Event Name",
            "Partner",
            "Media Source",
            "Channel",
            "Keywords",
            "Campaign",
            "Adset",
            "Ad",
            "Ad Type",
            "Region",
            "Country Code",
            "Carrier",
            "Language",
            "AppsFlyer ID",
            "Android ID",
            "Advertising ID",
            "IDFA",
            "IDFV",
            "Device Category",
            "Platform",
            "OS Version",
            "App Version",
            "SDK Version",
            "Is Retargeting",
            "Retargeting Conversion Type",
            "Is Primary Attribution",
            "Attribution Lookback",
            "Reengagement Window",
            "Match Type",
            "User Agent",
            "Conversion Type",
            "Campaign Type",
            "Device Model",
            "Keyword ID",
            "Original URL",
        ]

        combined_df = pd.concat(
            [combined_df_ua, combined_df_retarget_AOS, combined_df_retarget_IOS],
            ignore_index=True,
        ).reset_index(drop=True)
        combined_df_ws1 = combined_df[columns_to_keep_ws1]
        combined_df_ws1["Event Name"] = combined_df_ws1["Event Name"].fillna("기타")
        combined_df_ws1.insert(0, "구분", "정상")

        prod_combined_df = pd.concat(
            [dropped_PROD_rows_df_ios, dropped_PROD_rows_df_aos], ignore_index=True
        )
        prod_combined_df_filter = prod_combined_df[columns_to_keep_ws1]
        prod_combined_df_filter["Event Name"] = prod_combined_df_filter["Event Name"].fillna("기타")
        prod_combined_df_filter.insert(0, "구분", "프로드")

        ITET_combined_df = pd.concat(
            [
                dropped_ITET_rows_df_aos_total,
                dropped_ITET_rows_df_ios_total,
                dropped_ITET_rows_df_ua_total,
            ],
            ignore_index=True,
        )
        ITET_combined_df_filter = ITET_combined_df[columns_to_keep_ws1]
        ITET_combined_df_filter["Event Name"] = ITET_combined_df_filter["Event Name"].fillna("기타")
        ITET_combined_df_filter.insert(0, "구분", "ITET")

        CTET_combined_df = pd.concat(
            [
                dropped_CTIT_rows_df_ua_total,
                dropped_CTIT_rows_df_aos_total,
                dropped_CTIT_rows_df_ios_total,
            ],
            ignore_index=True,
        )
        CTET_combined_df_filter = CTET_combined_df[columns_to_keep_ws1]
        CTET_combined_df_filter["Event Name"] = CTET_combined_df_filter["Event Name"].fillna("기타")
        CTET_combined_df_filter.insert(0, "구분", "CTIT")

        filter_false = dropped_FALSE_rows_df_total[columns_to_keep_ws1]
        filter_false["Event Time"] = pd.to_datetime(filter_false["Event Time"])
        filter_false["Event Name"] = filter_false["Event Name"].fillna("기타")
        filter_false.insert(0, "구분", "FALSE")

        
        total_df = pd.concat(
            [
                combined_df_ws1,
                prod_combined_df_filter,
                ITET_combined_df_filter,
                CTET_combined_df_filter,
                filter_false,
            ],
            ignore_index=True,
        )
        total_df["Event Time"] = pd.to_datetime(total_df["Event Time"])
        temp_csv_file = "./temp_file.csv"
        total_df.to_csv(temp_csv_file, index=False, encoding="utf-8-sig")

        async with aiofiles.open(temp_csv_file, "rb") as out_file:
            file_data = await out_file.read()

        with open("/dags/data/final_attachment_albamon.csv", "wb") as f:
            f.write(file_data)
            
    asyncio.run(process_dataframes())

async def processProd(csv_data, osAdid, osMediaSource):
    duplicate_advertising_ids = csv_data[
        csv_data["Retargeting Conversion Type"] == "re-engagement"
    ].copy()

    duplicate_advertising_ids["Event Time"] = pd.to_datetime(
        duplicate_advertising_ids["Event Time"]
    )

    duplicate_advertising_ids = duplicate_advertising_ids.groupby(osAdid).filter(
        lambda x: len(x) >= 3
    )

    grouped = duplicate_advertising_ids.groupby(
        duplicate_advertising_ids["Event Time"].dt.date
    )

    target_indices_per_group = []

    for _, group in grouped:
        for advertising_id in group[osAdid].unique():
            advertising_id_csv_df = group[group[osAdid] == advertising_id].sort_values(
                "Event Time"
            )
            retargeting_within_1_min = False

            if len(advertising_id_csv_df) >= 3:
                for i in range(len(advertising_id_csv_df) - 2):
                    start_time = advertising_id_csv_df.iloc[i]["Event Time"]
                    end_time1 = advertising_id_csv_df.iloc[i + 1]["Event Time"]
                    end_time2 = advertising_id_csv_df.iloc[i + 2]["Event Time"]
                    time_threshold = start_time + timedelta(minutes=1)

                    if end_time1 <= time_threshold and end_time2 <= time_threshold:
                        retargeting_within_1_min = True
                        break

            if (
                retargeting_within_1_min
                and (
                    advertising_id_csv_df["Retargeting Conversion Type"]
                    == "re-engagement"
                ).all()
                and (
                    advertising_id_csv_df["Media Source"].isin(osMediaSource)
                ).all()
            ):
                indices = advertising_id_csv_df[
                    (advertising_id_csv_df["Retargeting Conversion Type"] == "re-engagement")
                    & (advertising_id_csv_df["Media Source"].isin(osMediaSource))
                ].index.tolist()

                target_indices_per_group.extend(indices)

    dropped_rows_df = csv_data.loc[target_indices_per_group].copy()

    dropped_rows_df = dropped_rows_df[
        dropped_rows_df["Retargeting Conversion Type"] == "re-engagement"
    ].copy()

    csv_data = csv_data[~csv_data.index.isin(dropped_rows_df.index)]

    return csv_data.reset_index(drop=True), dropped_rows_df.reset_index(drop=True)

# XCom으로 전달받은 row 수와 디덕션 후 로우 수 체크
def check_row_count(**context):
    fetch_row_count= int(context['dag_run'].conf.get('fetch_row_count'))
    print(f"전달받은 row 수: {fetch_row_count}")
    df = pd.read_csv("/dags/data/final_attachment_albamon.csv")
    deduction_row_count = len(df)
    comp_row_count = fetch_row_count - deduction_row_count
    if comp_row_count != 0 :
        print(f"{comp_row_count}개 만큼 차이가 발생합니다!")
        raise ValueError("디덕션 과정 중 에러 발생")
    else :
        print("✅✅정상 처리")
    context['ti'].xcom_push(key='deduction_row_count', value = deduction_row_count)
    



def pass_target_date(**context):
    target_date = context['dag_run'].conf.get('target_date')
    print(f"✅ 전달할 target_date: {target_date}")
    return target_date

get_target_date = PythonOperator(
    task_id='get_target_date',
    python_callable=pass_target_date,
    provide_context=True,
    dag=dag,
)

def trigger_next_dag(**context):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.models import DagRun
    from airflow.utils.state import State
    from airflow.utils.trigger_rule import TriggerRule

    target_date = context['dag_run'].conf.get('target_date')
    print(f"🚀 Triggering with target_date: {target_date}")

    trigger_op = TriggerDagRunOperator(
        task_id='trigger_process_dag_actual',
        trigger_dag_id='albamon_process_and_upload_deduction_to_s3_with_hook',
        conf={"target_date": target_date},
        dag=dag,
    )
    trigger_op.execute(context=context)

trigger_processing = PythonOperator(
    task_id='trigger_process_dag',
    python_callable=trigger_next_dag,
    provide_context=True,
    dag=dag,
)


process_task = PythonOperator(
    task_id='run_async_processing',
    python_callable=run_async_processing,
    dag=dag,
)

count_rows_task = PythonOperator(
    task_id='count_deduction_csv_rows',
    python_callable=check_row_count,
    provide_context=True,
    dag=dag,
)

process_task >> count_rows_task >> trigger_processing