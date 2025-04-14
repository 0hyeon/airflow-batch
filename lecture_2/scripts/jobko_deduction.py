from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, count, lit
from functools import reduce
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("deduction_processing_v2").getOrCreate()

# 날짜 기반 경로
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/"

# 대상 파일
files = [
    "data_aos_onepick_retarget.parquet",
    "data_aos_onepick_ua.parquet",
    "data_aos_retarget.parquet",
    "data_ios_onepick_retarget.parquet",
    "data_ios_onepick_ua.parquet",
    "data_ios_retarget.parquet",
    "data_ios_ua.parquet",
]

AOS_MEDIA_SOURCE = ["appier_int", "adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]
IOS_MEDIA_SOURCE = ["adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]

# 누적 저장용 리스트
combined_normal, combined_prod, combined_itet, combined_ctit, combined_false, combined_onepick = ([] for _ in range(6))

for file in files:
    try:
        df = spark.read.parquet(f"s3://{S3_BUCKET}/{S3_INPUT_PREFIX}{file}")
        df = reduce(lambda df, name: df.withColumnRenamed(name, name.replace(" ", "_")), df.columns, df)

        df = df.withColumn("Event_Time", col("Event_Time").cast("timestamp"))
        df = df.withColumn("Install_Time", col("Install_Time").cast("timestamp"))
        df = df.withColumn("Time_Diff", (unix_timestamp("Event_Time") - unix_timestamp("Install_Time")) / 3600)

        # ITET 필터
        df_itet = df.filter(col("Time_Diff") >= 24 * 1.05).drop("Time_Diff")
        df = df.filter(col("Time_Diff") < 24 * 1.05).drop("Time_Diff")
        combined_itet.append(df_itet.withColumn("구분", lit("ITET")))

        # CTIT 필터
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
            continue  # 조기 리턴

        combined_normal.append(df.withColumn("구분", lit("정상")))

    except Exception as e:
        print(f"❌ 오류 발생 - {file}: {e}")

# 모든 데이터 통합
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
    empty_schema = df.select("Event_Time").schema  # 최소 컬럼으로 빈 DataFrame 스키마 구성
    spark.createDataFrame([], empty_schema).write.mode("overwrite").option("header", True).csv(output_s3_path)


spark.stop()
