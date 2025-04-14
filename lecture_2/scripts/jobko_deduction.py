from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, count, lit
import traceback

spark = SparkSession.builder.appName("deduction_processing_v3").getOrCreate()

yesterday = "2025-04-13"
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_RENAMED_PREFIX = f"apps_flyer_jobko/renamed/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/"

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

AOS_MEDIA_SOURCE = ["appier_int", "adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]
IOS_MEDIA_SOURCE = ["adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]

combined_normal, combined_prod, combined_itet, combined_ctit, combined_false, combined_onepick = ([] for _ in range(6))

def normalize_column_names(df):
    new_columns = [col_name.replace(" ", "_").replace("-", "_") for col_name in df.columns]
    return df.toDF(*new_columns)

# ✅ Step 1: 컬럼 정규화 후 parquet 재저장
for file in files:
    try:
        s3_path = f"s3://{S3_BUCKET}/{S3_INPUT_PREFIX}{file}"
        print(f"📦 컬럼 정제 및 저장: {file}")
        df = spark.read.option("mergeSchema", "true").parquet(s3_path)
        df_clean = normalize_column_names(df)
        df_clean.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/{S3_RENAMED_PREFIX}{file}")
    except Exception as e:
        print(f"❌ 컬럼 정제 실패 - {file}: {e}")
        traceback.print_exc()

# ✅ Step 2: 필터링 로직 적용
for file in files:
    try:
        print(f"📂 처리 시작: {file}")
        s3_path = f"s3://{S3_BUCKET}/{S3_RENAMED_PREFIX}{file}"
        df = spark.read.parquet(s3_path)

        df = df.withColumn("Event_Time", col("Event_Time").cast("timestamp"))
        df = df.withColumn("Install_Time", col("Install_Time").cast("timestamp"))
        df = df.withColumn("Time_Diff", (unix_timestamp("Event_Time") - unix_timestamp("Install_Time")) / 3600)

        df_itet = df.filter(col("Time_Diff") >= 24 * 1.05).drop("Time_Diff")
        df = df.filter(col("Time_Diff") < 24 * 1.05).drop("Time_Diff")
        combined_itet.append(df_itet.withColumn("구분", lit("ITET")))

        if "Attributed_Touch_Time" in df.columns:
            df = df.withColumn("Attributed_Touch_Time", col("Attributed_Touch_Time").cast("timestamp"))
            df = df.withColumn("CTIT_Diff", (unix_timestamp("Install_Time") - unix_timestamp("Attributed_Touch_Time")) / 3600)
            df_ctit = df.filter(col("CTIT_Diff") >= 24 * 1.05).drop("CTIT_Diff")
            df = df.filter(col("CTIT_Diff") < 24 * 1.05).drop("CTIT_Diff")
            combined_ctit.append(df_ctit.withColumn("구분", lit("CTIT")))

        lower_name = file.lower()
        platform = "aos" if "aos" in lower_name else "ios"
        id_col = "Advertising_ID" if platform == "aos" else "IDFA"
        media_sources = AOS_MEDIA_SOURCE if platform == "aos" else IOS_MEDIA_SOURCE

        if "retarget" in lower_name:
            if "Retargeting_Conversion_Type" in df.columns:
                df_re = df.filter(col("Retargeting_Conversion_Type") == "re-engagement")
                grouped = df_re.groupBy(id_col, "Event_Time").agg(count("*").alias("Retargeting_Count"))
                df_re = df_re.join(grouped, on=[id_col, "Event_Time"], how="left")
                df_prod = df_re.filter(
                    (col("Retargeting_Count") >= 3) &
                    col("Media_Source").isin(media_sources)
                ).drop("Retargeting_Count")
                df = df.join(df_prod.select("Event_Time", id_col), on=["Event_Time", id_col], how="left_anti")
                combined_prod.append(df_prod.withColumn("구분", lit("프로드")))

        elif "ua" in lower_name:
            if "Is_Primary_Attribution" in df.columns:
                df_false = df.filter(col("Is_Primary_Attribution") == False)
                combined_false.append(df_false.withColumn("구분", lit("FALSE")))
                df = df.filter(col("Is_Primary_Attribution") != False)

        elif "onepick" in lower_name:
            combined_onepick.append(df.withColumn("구분", lit("정상")))
            continue

        combined_normal.append(df.withColumn("구분", lit("정상")))

    except Exception as e:
        print(f"❌ 오류 발생 - {file}: {e}")
        traceback.print_exc()

# ✅ Step 3: 통합 저장
all_dataframes = combined_normal + combined_prod + combined_itet + combined_ctit + combined_false + combined_onepick
output_s3_path = f"s3://{S3_BUCKET}/{S3_OUTPUT_PREFIX}{yesterday}_final_result"

if all_dataframes:
    result_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        result_df = result_df.unionByName(df)
    result_df.write.mode("overwrite").option("header", True).csv(output_s3_path)
    print("✅ S3 최종 저장 완료")
else:
    print("⚠️ 필터링된 데이터가 없습니다.")
    spark.createDataFrame([], schema="Event_Time timestamp").write.mode("overwrite").option("header", True).csv(output_s3_path)

spark.stop()
