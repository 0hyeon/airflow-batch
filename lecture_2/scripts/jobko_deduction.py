from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, count, lit
import traceback

spark = SparkSession.builder.appName("deduction_processing_single_file").getOrCreate()

# 경로
yesterday = "2025-04-13"
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/{yesterday}_final_result.csv"
output_s3_path = f"s3://{S3_BUCKET}/{S3_OUTPUT_PREFIX}{yesterday}_final_result"

# 파일 리스트
files = [
    "data_aos_onepick_retarget.parquet",
    "data_aos_onepick_ua.parquet",
    "data_aos_retarget.parquet",
    "data_aos_ua.parquet",
    "data_ios_onepick_retarget.parquet",
    "data_ios_onepick_ua.parquet",
    "data_ios_retarget.parquet",
    "data_ios_ua.parquet",
]
columns_to_keep = ["Attributed_Touch_Type","Attributed_Touch_Time","Install_Time","Event_Time","Event_Name","Partner","Media_Source","Channel","Keywords","Campaign","Adset","Ad","Ad_Type","Region","Country_Code","Carrier","Language","AppsFlyer_ID","Android_ID","Advertising_ID","IDFA","IDFV","Device_Category","Platform","OS_Version","App_Version","SDK_Version","Is_Retargeting","Retargeting_Conversion_Type","Is_Primary_Attribution","Attribution_Lookback","Reengagement_Window","Match_Type","User_Agent","Conversion_Type","Campaign_Type","Device_Model","Keyword_ID","Original_URL",]


def normalize_column_names(df):
    new_columns = [col_name.strip().replace(" ", "_").replace("-", "_") for col_name in df.columns]
    return df.toDF(*new_columns)

AOS_MEDIA_SOURCE = ["appier_int", "adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]
IOS_MEDIA_SOURCE = ["adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]

# 카테고리별 결과 저장 리스트
combined_normal, combined_prod, combined_itet, combined_ctit, combined_false, combined_onepick = ([] for _ in range(6))

for file in files:
    try:
        print(f"🚀 파일 처리 시작: {file}")
        s3_path = f"s3a://{S3_BUCKET}/{S3_INPUT_PREFIX}{file}"

        df = spark.read.option("mergeSchema", "false").parquet(s3_path)
        df = normalize_column_names(df)

        for colname in df.columns:
            df = df.withColumn(colname, col(colname).cast("string"))

        
        # ✅ 드롭할 컬럼 제거
        cols_to_drop = [c for c in df.columns if c not in columns_to_keep]
        df = df.drop(*cols_to_drop)


        df = df.withColumn("Event_Time_ts", unix_timestamp("Event_Time"))
        df = df.withColumn("Install_Time_ts", unix_timestamp("Install_Time"))
        df = df.withColumn("Time_Diff", (col("Event_Time_ts") - col("Install_Time_ts")) / 3600)

        df_itet = df.filter(col("Time_Diff") >= 24 * 1.05).drop("Time_Diff")
        df = df.filter(col("Time_Diff") < 24 * 1.05).drop("Time_Diff")
        combined_itet.append(df_itet.withColumn("구분", lit("ITET")))

        if "Attributed_Touch_Time" in df.columns:
            df = df.withColumn("CTIT_Diff", (unix_timestamp("Install_Time") - unix_timestamp("Attributed_Touch_Time")) / 3600)
            df_ctit = df.filter(col("CTIT_Diff") >= 24 * 1.05).drop("CTIT_Diff")
            df = df.filter(col("CTIT_Diff") < 24 * 1.05).drop("CTIT_Diff")
            combined_ctit.append(df_ctit.withColumn("구분", lit("CTIT")))

        lower_name = file.lower()
        platform = "aos" if "aos" in lower_name else "ios"
        id_col = "Advertising_ID" if platform == "aos" else "IDFA"
        media_sources = AOS_MEDIA_SOURCE if platform == "aos" else IOS_MEDIA_SOURCE

        if "retarget" in lower_name:
            df_re = df.filter(col("Retargeting_Conversion_Type") == "re-engagement")
            grouped = df_re.groupBy(id_col, "Event_Time").agg(count("*").alias("Retargeting_Count"))
            df_re = df_re.join(grouped, on=[id_col, "Event_Time"], how="left")
            df_prod = df_re.filter((col("Retargeting_Count") >= 3) & col("Media_Source").isin(media_sources)).drop("Retargeting_Count")
            df = df.join(df_prod.select("Event_Time", id_col), on=["Event_Time", id_col], how="left_anti")
            combined_prod.append(df_prod.withColumn("구분", lit("프로드")))

        elif "ua" in lower_name:
            df_false = df.filter(col("Is_Primary_Attribution") == "false")
            combined_false.append(df_false.withColumn("구분", lit("FALSE")))
            df = df.filter(col("Is_Primary_Attribution") != "false")

        elif "onepick" in lower_name:
            combined_onepick.append(df.withColumn("구분", lit("정상")))
            continue

        combined_normal.append(df.withColumn("구분", lit("정상")))

    except Exception as e:
        print(f"❌ 처리 중 에러 - {file}: {e}")
        traceback.print_exc()

# 결과 병합 및 단일 파일로 저장
all_dataframes = combined_normal + combined_prod + combined_itet + combined_ctit + combined_false + combined_onepick

if all_dataframes:
    result_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        result_df = result_df.unionByName(df)
    
    # 단일 CSV로 저장
    result_df.coalesce(1).write.option("header", True).mode("overwrite").csv(f"s3a://{output_s3_path}")
    print("✅ 단일 CSV 저장 완료")
else:
    print("⚠️ 저장할 데이터 없음")

spark.stop()
