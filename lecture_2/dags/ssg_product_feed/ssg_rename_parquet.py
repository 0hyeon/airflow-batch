from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import pendulum
import os

BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"

FILES_TO_RENAME = {
    "ssg-parquet-output/ssg_all_full": "ssg_all_full.parquet",
    "ssg-parquet-output/e_all_full": "e_all_full.parquet"
}

def multipart_copy_large_s3_file(s3, bucket, source_key, destination_key, part_size=100 * 1024 * 1024):
    obj = s3.head_object(Bucket=bucket, Key=source_key)
    total_size = obj['ContentLength']
    print(f"🔍 대상 파일 크기: {total_size / (1024**3):.2f} GB")

    mpu = s3.create_multipart_upload(Bucket=bucket, Key=destination_key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1

    try:
        for byte_position in range(0, total_size, part_size):
            last_byte = min(byte_position + part_size - 1, total_size - 1)
            print(f"📦 복사 파트 {part_number}: bytes={byte_position}-{last_byte}")

            part = s3.upload_part_copy(
                Bucket=bucket,
                Key=destination_key,
                PartNumber=part_number,
                UploadId=upload_id,
                CopySource={'Bucket': bucket, 'Key': source_key},
                CopySourceRange=f"bytes={byte_position}-{last_byte}"
            )

            parts.append({
                'PartNumber': part_number,
                'ETag': part['CopyPartResult']['ETag']
            })
            part_number += 1

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=destination_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print(f"✅ 이름 변경 완료: {source_key} → {destination_key}")

        s3.delete_object(Bucket=bucket, Key=source_key)
        print(f"🗑️ 원본 삭제 완료")

    except Exception as e:
        s3.abort_multipart_upload(Bucket=bucket, Key=destination_key, UploadId=upload_id)
        print(f"❌ 오류 발생: {e}")
        raise

@task
def rename_parquet_file(folder: str, fixed_filename: str):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3 = hook.get_conn()

    prefix = f"{folder}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    parquet_keys = [k for k in keys if k.endswith(".parquet") and "part-" in k]

    if not parquet_keys:
        raise FileNotFoundError(f"❌ part-*.parquet 파일을 찾을 수 없습니다: {prefix}")

    source_key = parquet_keys[0]
    destination_key = os.path.join(prefix, fixed_filename)

    print(f"🚀 복사 시작: {source_key} → {destination_key}")
    multipart_copy_large_s3_file(s3, BUCKET, source_key, destination_key)

@dag(
    dag_id="ssg_rename_parquet_with_multipart",
    start_date=pendulum.datetime(2025, 6, 25, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    tags=["s3", "rename", "multipart"]
)
def rename_parquet_with_multipart():
    for folder, filename in FILES_TO_RENAME.items():
        rename_parquet_file.override(task_id=f"rename_{folder.replace('/', '_')}")(
            folder=folder,
            fixed_filename=filename
        )

dag = rename_parquet_with_multipart()