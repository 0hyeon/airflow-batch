import pendulum
import pandas as pd
import os
from io import StringIO

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(
    dag_id="jobko_meta_upload_feed_to_s3",
    start_date=pendulum.datetime(2025, 6, 29, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["meta", "s3-upload", "final"],
)
def upload_final_feed_to_s3_dag():
    BASE_DIR = "/opt/airflow/data/meta_feed"
    INPUT_CSV_PATH = os.path.join(BASE_DIR, "jobkorea_ai_only_final.csv")
    EXCLUDE_URL = "https://img.jobkorea.kr/images/logo/200/l/o/Logo_None_200.png"

    S3_CONN_ID = "aws_conn_id"
    S3_BUCKET = "gyoung0-test"

    @task
    def filter_and_upload():
        # --- [수정] 임시 로컬 파일 경로 및 S3 최종 경로 정의 ---
        temp_local_path = os.path.join(BASE_DIR, "temp_final_feed_for_upload.csv")
        s3_key = "jobko_meta_ai/jobko_meta_final_feed.csv"

        print(f"Reading source CSV from: {INPUT_CSV_PATH}")
        try:
            df = pd.read_csv(INPUT_CSV_PATH, encoding="utf-8-sig")
        except FileNotFoundError:
            raise FileNotFoundError(f"Input file not found: {INPUT_CSV_PATH}")

        # 컬럼 이름 변경 로직
        rename_map = {
            "ProductId": "id",
            "상품 설명": "title",
            "메타 설명": "description",
            "최종 URL": "link",
            "이미지 URL": "image_link",
            "항목 제목": "brand",
            "항목 부제목": "extra_careerType",
            "Category2": "google_product_category",
            "Fb_product_category": "custom_label_2",
            "항목 카테고리": "quantity_to_sell_on_facebook",
            "ExtraDate": "custom_label_1",
            "항목 주소": "material",
            "Android 앱 링크": "applink.android_url",
        }
        df.rename(columns=rename_map, inplace=True)
        if "extra_careerType" in df.columns:
            df["custom_label_0"] = df["extra_careerType"]
        df["availability"] = "in stock"
        df["condition"] = "new"
        df["price"] = "1.00 KRW"

        if 'Extra_CareerType' in df.columns:
            df['custom_label_0'] = df['Extra_CareerType']
        
        if 'Extra_Date' in df.columns:
            df['custom_label_1'] = df['Extra_Date']
            
        if 'Extra_Jobtype' in df.columns:
            df['custom_label_3'] = df['Extra_Jobtype']

        cols_to_drop = ['Extra_CareerType', 'Extra_Jobtype', 'Extra_Date']

        # errors='ignore'는 혹시 원본 컬럼이 없더라도 에러를 내지 않게 함
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        # 필터링
        if "image_link" in df.columns:
            df_filtered = df[df["image_link"] != EXCLUDE_URL].copy()
        else:
            df_filtered = df

        if df_filtered.empty:
            print("No data left to upload after filtering.")
            return "No data to upload."

        # --- [수정] 1. 필터링된 데이터를 로컬 임시 파일로 먼저 저장 ---
        print(f"Saving filtered data to temporary local file: {temp_local_path}")
        df_filtered.to_csv(temp_local_path, index=False, encoding="utf-8-sig")

        # --- [수정] 2. S3Hook으로 메모리가 아닌, 로컬 파일을 업로드 ---
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        print(f"Uploading local file '{temp_local_path}' to S3 bucket '{S3_BUCKET}'")

        s3_hook.load_file(
            filename=temp_local_path,  # 업로드할 로컬 파일 경로
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        # --- [수정] 3. 작업 완료 후 임시 로컬 파일 삭제 ---
        try:
            os.remove(temp_local_path)
            print(f"Removed temporary local file: {temp_local_path}")
        except OSError as e:
            print(f"Error removing temporary file: {e}")

        final_s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Upload complete. Final path: {final_s3_path}")
        return final_s3_path
    
    @task
    def cleanup_intermediate_files():
        """
        delete files that are created 
        """
        files_to_delete = [
            os.path.join(BASE_DIR, "jobkorea_cietro.xml"),
            os.path.join(BASE_DIR, "jobkorea_cietro_all.csv"),
            os.path.join(BASE_DIR, "jobkorea_cietro_ai_only.csv"),
            os.path.join(BASE_DIR, "final_url_map.json"),
            os.path.join(BASE_DIR, "jobkorea_ai_only_final.csv"),
        ]

        for file_path in files_to_delete:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f" -> Removed: {file_path}")
            except OSError as e:
                print(f" -> Error removing file {file_path}: {e}")

    s3_upload_task = filter_and_upload()
    s3_upload_task >> cleanup_intermediate_files()


upload_final_feed_to_s3_dag()
