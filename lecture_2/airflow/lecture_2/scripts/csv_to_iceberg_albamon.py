from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_date
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

print("===========start==============")

spark = SparkSession.builder \
    .appName("csv_to_iceberg_glue") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue.warehouse", "s3a://gyoung0-test/iceberg_warehouse/") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS glue.albamon_events")

yesterday = spark.sparkContext.getConf().get("spark.hadoop.yesterday")
print("✅ 전달받은 날짜:", yesterday)

csv_path = f"s3a://gyoung0-test/deduction-csv/date={yesterday}/final_attachment_albamon.csv"
df = spark.read.option("header", True).csv(csv_path)

# 컬럼 정리
rename_map = {c: c.lower().replace(" ", "_") for c in df.columns}
rename_map["구분"] = "data_flag"
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)

# 타입 캐스팅
cast_columns = {
    "event_time": "timestamp",
    "install_time": "timestamp",
    "attributed_touch_time": "timestamp",
    "is_retargeting": "boolean",
    "is_primary_attribution": "boolean",
}

def check_cast_nulls(df, col_name, target_type):
    if col_name not in df.columns:
        print(f"❓ 컬럼 없음: {col_name}")
        return df
    if target_type == "boolean":
        df = df.withColumn(col_name, lower(trim(col(col_name))))
    df = df.withColumn(col_name, col(col_name).cast(target_type))
    return df

for col_name, col_type in cast_columns.items():
    df = check_cast_nulls(df, col_name, col_type)

# partition_key 생성
df = df.withColumn("partition_key", to_date(col("event_time")))

# temp view 생성
df.createOrReplaceTempView("tmp_albamon_event_data")

# 테이블 조회
tables = spark.sql("SHOW TABLES IN glue.albamon_events").filter("tableName = 'albamon_event_data'").collect()

if len(tables) == 0:
    print("📌 테이블 없음 → 생성 진행")
    spark.sql("""
        CREATE TABLE glue.albamon_events.albamon_event_data
        USING iceberg
        PARTITIONED BY (days(partition_key))
        TBLPROPERTIES ('format-version' = '2')
        AS SELECT * FROM tmp_albamon_event_data
    """)
else:
    print("📌 테이블 존재함 → MERGE 진행")
    # 기준이 되는 unique key 지정 필요: 예시로 event_time + appsflyer_id
    spark.sql("""
        MERGE INTO glue.albamon_events.albamon_event_data t
        USING tmp_albamon_event_data s
        ON t.event_time = s.event_time AND t.appsflyer_id = s.appsflyer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Iceberg 테이블 병합 또는 생성 완료")
