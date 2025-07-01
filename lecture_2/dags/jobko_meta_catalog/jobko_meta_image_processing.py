import pendulum
import requests
import csv
import os
import hashlib
import json
from io import BytesIO
from collections import defaultdict
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import time
import shutil

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="jobko_generate_meta_images_sequential",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["meta", "image", "caching", "production"]
)
def generate_meta_images_dag():
    BASE_DIR = "/opt/airflow/data"
    META_FEED_DIR = os.path.join(BASE_DIR, "meta_feed")

    @task
    def process_and_upload_by_brand() -> str:
        """
        로컬 캐시를 먼저 확인하여 API 호출을 최소화하고,
        이미지 처리 후 최종 URL 맵을 JSON 파일로 저장 및 반환합니다.
        """
        INPUT_CSV_PATH = os.path.join(META_FEED_DIR, "jobkorea_cietro_ai_only.csv")
        URL_MAP_PATH = os.path.join(META_FEED_DIR, "final_url_map.json")
        CACHE_PATH = os.path.join(META_FEED_DIR, "brand_url_cache.json")
        TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "Meta_Frame.jpg")
        FONT_PATH = os.path.join(BASE_DIR, "fonts", "GmarketSansTTFMedium.ttf")
        MEDIVISION_LOGO_PATH = os.path.join(BASE_DIR, "templates", "medivision.jpg")
        EXCLUDE_IMAGE_URL = "https://img.jobkorea.kr/images/logo/200/l/o/Logo_None_200.png"
        LOGO_WIDTH, LOGO_HEIGHT = 650, 240
        
        try:
            with open(INPUT_CSV_PATH, newline='', encoding='utf-8-sig') as csvfile:
                all_rows = list(csv.DictReader(csvfile))
        except FileNotFoundError:
            print(f"Input file not found: {INPUT_CSV_PATH}.")
            return ""

        brand_to_source_url = {}
        for row in all_rows:
            brand, image_url = row.get("Brand", "").strip(), row.get("Image_Link", "").strip()
            if brand and image_url and image_url != EXCLUDE_IMAGE_URL and brand not in brand_to_source_url:
                brand_to_source_url[brand] = image_url
        
        try:
            with open(CACHE_PATH, 'r', encoding='utf-8-sig') as f:
                brand_to_cf_url = json.load(f)
            print(f"Loaded {len(brand_to_cf_url)} brands from local cache.")
        except FileNotFoundError:
            brand_to_cf_url = {}
            print("Local cache file not found. Starting with an empty cache.")

        conn = HttpHook.get_connection('cloudflare_images_api')
        account_id, api_token = conn.extra_dejson.get('account_id'), conn.password
        auth_headers = {"Authorization": f"Bearer {api_token}"}
        
        unique_brands = list(brand_to_source_url.keys())
        error_logs = []
        
        def download_image(url, retries=3, timeout=30):
            headers = {"User-Agent": "Mozilla/5.0"}
            for i in range(retries):
                try:
                    response = requests.get(url, headers=headers, stream=True, timeout=timeout)
                    response.raise_for_status()
                    Image.open(BytesIO(response.content)).verify()
                    return Image.open(BytesIO(response.content)).convert("RGBA")
                except Exception as e:
                    if i < retries - 1: time.sleep(2)
                    else: raise

        def adjust_font_size(draw, text, font_path, image_width, base_font_ratio=0.073, min_size=20):
            max_width = image_width * 0.75
            font_size = int(image_width * base_font_ratio)
            while font_size > min_size:
                font = ImageFont.truetype(font_path, font_size)
                if font.getbbox(text)[2] <= max_width: return font
                font_size -= 2
            return ImageFont.truetype(font_path, min_size)

        def find_variant_url(variants_list: list, desired_variant: str = "AiMeta", fallback_variant: str = "public") -> str | None:
            for url in variants_list:
                if url.endswith(f'/{desired_variant}'): return url
            for url in variants_list:
                if url.endswith(f'/{fallback_variant}'): return url
            if variants_list: return variants_list[0]
            return None

        for i, brand in enumerate(unique_brands):
            if brand in brand_to_cf_url:
                print(f"Processing: {brand} -> Found in cache. Skipping API call.")
                continue

            brand_hash = hashlib.md5(brand.encode('utf-8-sig')).hexdigest()
            custom_id = f"jobko_brand_{brand_hash}"
            safe_brand_name = brand.replace("/", "_").replace("\\", "_")
            human_readable_filename = f"{safe_brand_name}_AI.jpg"
            
            print(f"Processing: {brand} (API ID: {custom_id}) -> Not in cache. Checking Cloudflare.")

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    check_hook = HttpHook(method='GET', http_conn_id='cloudflare_images_api')
                    response_check = check_hook.run(f"/client/v4/accounts/{account_id}/images/v1/{custom_id}", headers=auth_headers, extra_options={"check_response": False})

                    if response_check.status_code == 200:
                        variants = response_check.json().get('result', {}).get('variants', [])
                        result_url = find_variant_url(variants, "AiMeta")
                        if not result_url: raise ValueError("Found image but could not find a valid variant URL.")
                        brand_to_cf_url[brand] = result_url
                    elif response_check.status_code == 404:
                        source_image_url = brand_to_source_url[brand]
                        base = Image.open(TEMPLATE_PATH).convert("RGB"); draw = ImageDraw.Draw(base)
                        logo = Image.open(MEDIVISION_LOGO_PATH).convert("RGBA") if brand == "메디비전" else download_image(source_image_url)
                        logo = logo.resize((LOGO_WIDTH, LOGO_HEIGHT), Image.LANCZOS)
                        base.paste(logo, ((base.width - LOGO_WIDTH) // 2, 250), logo)
                        font = adjust_font_size(draw, brand, FONT_PATH, base.width)
                        text_bbox = font.getbbox(brand)
                        draw.text(((base.width - text_bbox[2]) / 2, 250 + LOGO_HEIGHT + 25), brand, font=font, fill="black")
                        final_image_buffer = BytesIO(); base.save(final_image_buffer, format='JPEG', quality=95); final_image_buffer.seek(0)
                        
                        upload_hook = HttpHook(method='POST', http_conn_id='cloudflare_images_api')
                        files = {'file': (human_readable_filename, final_image_buffer, 'image/jpeg')}
                        data = {'id': custom_id}
                        response_upload = upload_hook.run(f"/client/v4/accounts/{account_id}/images/v1", data=data, files=files, headers=auth_headers, extra_options={"timeout": 60})
                        variants = response_upload.json().get('result', {}).get('variants', [])
                        result_url = find_variant_url(variants, "AiMeta")
                        if not result_url: raise ValueError("Upload successful but could not find a valid variant URL.")
                        brand_to_cf_url[brand] = result_url
                    else:
                        response_check.raise_for_status()
                    break
                except Exception as e:
                    print(f"  -> Attempt {attempt+1}/{max_retries} failed for brand {brand}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
                    else:
                        error_logs.append(f"Failed to process brand {brand} after {max_retries} attempts: {e}")
        
        if error_logs:
            raise Exception(f"{len(error_logs)}건의 처리 실패가 발생했습니다:\n" + "\n".join(error_logs))

        print(f"Updating local cache file with {len(brand_to_cf_url)} brands at: {CACHE_PATH}")
        with open(CACHE_PATH, 'w', encoding='utf-8-sig') as f:
            json.dump(brand_to_cf_url, f, ensure_ascii=False, indent=4)

        final_url_map = {}
        for row in all_rows:
            product_id, brand = row.get("ProductId", "").strip(), row.get("Brand", "").strip()
            if brand in brand_to_cf_url:
                final_url_map[product_id] = brand_to_cf_url[brand]
        
        with open(URL_MAP_PATH, 'w', encoding='utf-8-sig') as f:
            json.dump(final_url_map, f, ensure_ascii=False, indent=4)
        return URL_MAP_PATH

    @task
    def update_ai_csv_with_new_urls(url_map_path: str):
        if not url_map_path or not os.path.exists(url_map_path): return

        with open(url_map_path, 'r', encoding='utf-8-sig') as f:
            url_map = json.load(f)
        if not url_map: return

        INPUT_CSV_PATH = os.path.join(META_FEED_DIR, "jobkorea_cietro_ai_only.csv")
        FINAL_CSV_PATH = os.path.join(META_FEED_DIR, "jobkorea_ai_only_final.csv")

        try:
            with open(INPUT_CSV_PATH, 'r', encoding='utf-8-sig') as infile:
                all_data = list(csv.DictReader(infile))
        except FileNotFoundError: return

        for row in all_data:
            product_id = row.get("ProductId")
            if product_id in url_map:
                row["Image_Link"] = url_map[product_id]
        
        if all_data:
            with open(FINAL_CSV_PATH, 'w', newline='', encoding='utf-8-sig') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=all_data[0].keys())
                writer.writeheader()
                writer.writerows(all_data)
            print(f"Final AI CSV created. Path: {FINAL_CSV_PATH}")

    trigger_s3_upload_dag = TriggerDagRunOperator(
        task_id="trigger_s3_upload_dag",
        trigger_dag_id="jobko_meta_upload_feed_to_s3", # 실행시킬 다음 DAG의 ID
        wait_for_completion=False,

    )
        
    url_map_file_path = process_and_upload_by_brand()
    final_csv_task = update_ai_csv_with_new_urls(url_map_file_path)

    final_csv_task >> trigger_s3_upload_dag

generate_meta_images_dag()