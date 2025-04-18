from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, unix_timestamp, lit, trim, udf, lag
from pyspark.sql.types import StringType
import traceback

spark = SparkSession.builder.appName("deduction_processing_final_exact").getOrCreate()

# 날짜 설정
yesterday = "2025-04-15"
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/{yesterday}/"
output_s3_path = f"s3a://{S3_BUCKET}/{S3_OUTPUT_PREFIX}"

files = [
    "data_aos_onepick_retarget.parquet", "data_aos_onepick_ua.parquet",
    "data_aos_retarget.parquet", "data_aos_ua.parquet",
    "data_ios_onepick_retarget.parquet", "data_ios_onepick_ua.parquet",
    "data_ios_retarget.parquet", "data_ios_ua.parquet"
]

columns_to_keep = [
    "Attributed_Touch_Type","Attributed_Touch_Time","Install_Time","Event_Time","Event_Name","Partner",
    "Media_Source","Channel","Keywords","Campaign","Adset","Ad","Ad_Type","Region","Country_Code",
    "Carrier","Language","AppsFlyer_ID","Android_ID","Advertising_ID","IDFA","IDFV","Device_Category",
    "Platform","OS_Version","App_Version","SDK_Version","Is_Retargeting","Retargeting_Conversion_Type",
    "Is_Primary_Attribution","Attribution_Lookback","Reengagement_Window","Match_Type","User_Agent",
    "Conversion_Type","Campaign_Type","Device_Model","Keyword_ID","Original_URL"
]

event_transfer = {
    "1st_update_complete": "7 이력서기본업데이트", "business_signup_complete": "4 기업회원가입",
    "job_apply_complete_homepage": "3 홈페이지지원", "job_apply_complete_mobile": "2 즉시지원",
    "minikit_complete": "9 MICT", "re_update_complete": "8 이력서추가업데이트",
    "resume_complete_1st": "5 이력서최초등록", "resume_complete_re": "6 이력서추가등록",
    "signup_all": "1 회원가입", "jobposting_complete": "90 채용공고등록",
    "1pick_direct_apply": "93 원픽직접지원", "1pick_offer_apply": "92 원픽제안지원",
    "1pick_view_jobposting": "94 원픽공고뷰", "nhis_resistration_complete": "91 건강보험_등록완료",
    "1pick_start": "95 원픽하기", "event_2411_tvc_apply": "96 TVC응모완료",
    "high_view_jobposting": "하이테크_공고조회", "high_scrap_notice": "하이테크_공고스크랩",
    "high_direct_apply": "하이테크_즉시지원", "high_homepage_apply": "하이테크_홈페이지지원",
    "careercheck_assess_complete": "커첵_참견하기",
}
map_event_udf = udf(lambda x: event_transfer.get(x, x), StringType())

def normalize_column_names(df):
    new_columns = [col_name.strip().replace(" ", "_").replace("-", "_") for col_name in df.columns]
    return df.toDF(*new_columns)

AOS_MEDIA_SOURCE = ["appier_int", "adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]
IOS_MEDIA_SOURCE = ["adisonofferwall_int", "cashfriends_int", "greenp_int", "buzzad_int"]

combined_rows = []
for file in files:
    try:
        print(f"🚀 처리중: {file}")
        s3_path = f"s3a://{S3_BUCKET}/{S3_INPUT_PREFIX}{file}"
        df = spark.read.option("mergeSchema", "true").parquet(s3_path)

        # 컬럼 정규화
        df = df.toDF(*[c.strip().replace(" ", "_").replace("-", "_") for c in df.columns])

        for c in df.columns:
            df = df.withColumn(c, trim(col(c).cast("string")))

        lower_name = file.lower()
        platform = "aos" if "aos" in lower_name else "ios"
        id_col = "Advertising_ID" if platform == "aos" else "IDFA"
        media_sources = AOS_MEDIA_SOURCE if platform == "aos" else IOS_MEDIA_SOURCE

        # 1. PROD
        if "retarget" in lower_name:
            window_spec = Window.partitionBy(id_col).orderBy(unix_timestamp("Event_Time", "yyyy-MM-dd HH:mm:ss.SSS"))
            df_re = df.filter(col("Retargeting_Conversion_Type") == "re-engagement") \
                      .withColumn("event_ts", unix_timestamp("Event_Time", "yyyy-MM-dd HH:mm:ss.SSS")) \
                      .withColumn("prev2", lag("event_ts", 2).over(window_spec)) \
                      .withColumn("delta2", col("event_ts") - col("prev2"))

            df_prod = df_re.filter((col("delta2") <= 60) & col("Media_Source").isin(media_sources)) \
                           .withColumn("구분", lit("\ud504\ub85c\ub4dc")) \
                           .withColumn("Event_Name", map_event_udf("Event_Name"))
                           
            df_prod = df_prod.drop("prev2", "delta2", "event_ts")
            combined_rows.append(df_prod)
            df = df.join(df_prod.select("Event_Time", id_col), on=["Event_Time", id_col], how="left_anti")

        # 2. UA FALSE
        if "ua" in lower_name:
            df_false = df.filter(col("Is_Primary_Attribution") == "false") \
                         .withColumn("구분", lit("FALSE")) \
                         .withColumn("Event_Name", map_event_udf("Event_Name"))
            combined_rows.append(df_false)
            df = df.filter(col("Is_Primary_Attribution") != "false")

        # 3. ITET
        df = df.withColumn("event_ts", unix_timestamp("Event_Time", "yyyy-MM-dd HH:mm:ss.SSS")) \
               .withColumn("install_ts", unix_timestamp("Install_Time", "yyyy-MM-dd HH:mm:ss.SSS")) \
               .withColumn("itet_diff", (col("event_ts") - col("install_ts")) / 3600.0)

        df_itet = df.filter(col("itet_diff") >= 24 * 1.05) \
                    .withColumn("구분", lit("ITET")) \
                    .withColumn("Event_Name", map_event_udf("Event_Name"))
        combined_rows.append(df_itet)
        df = df.filter(col("itet_diff") < 24 * 1.05)

        # 4. CTIT
        if "Attributed_Touch_Time" in df.columns:
            df = df.withColumn("touch_ts", unix_timestamp("Attributed_Touch_Time", "yyyy-MM-dd HH:mm:ss.SSS")) \
                   .withColumn("ctit_diff", (col("install_ts") - col("touch_ts")) / 3600.0)

            df_ctit = df.filter(col("ctit_diff") >= 24 * 1.05) \
                        .withColumn("구분", lit("CTIT")) \
                        .withColumn("Event_Name", map_event_udf("Event_Name"))
            combined_rows.append(df_ctit)
            df = df.filter(col("ctit_diff") < 24 * 1.05)

        # 5. 정상
        df_normal = df.withColumn("구분", lit("정상")) \
                      .withColumn("Event_Name", map_event_udf("Event_Name"))
        combined_rows.append(df_normal)

    except Exception as e:
        print(f"❌ 오류 발생 - {file}: {e}")
        traceback.print_exc()

# 저장
if combined_rows:
    result_df = combined_rows[0]
    for df in combined_rows[1:]:
        result_df = result_df.unionByName(df)

    # 🔧 여기서 중간 처리용 컬럼 제거 (drop은 존재하는 컬럼만 제거)
    drop_cols = ["event_ts", "install_ts", "touch_ts", "prev2", "delta2", "itet_diff", "ctit_diff"]
    result_df = result_df.drop(*[c for c in drop_cols if c in result_df.columns])

    # ✅ 저장
    result_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_s3_path)
    print("✅ 저장 완료:", output_s3_path)
else:
    print("⚠️ 저장할 데이터 없음")

spark.stop()
