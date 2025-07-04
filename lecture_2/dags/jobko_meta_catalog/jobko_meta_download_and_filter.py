import pendulum
import requests
import xml.etree.ElementTree as ET
import csv
import os
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# XML URL 및 저장 경로

XML_URL = "https://m.jobkorea.co.kr/recruit/AgiFeed/1"
SAVE_DIR = "/opt/airflow/data/meta_feed"
XML_PATH = os.path.join(SAVE_DIR, "jobkorea_cietro.xml")
CSV_ALL_PATH = os.path.join(SAVE_DIR, "jobkorea_cietro_all.csv")
CSV_AI_PATH = os.path.join(SAVE_DIR, "jobkorea_cietro_ai_only.csv")

@dag(
    dag_id="jobko_meta_feed_download_and_filter",
    start_date=pendulum.datetime(2025, 6, 25, tz="Asia/Seoul"),
    schedule_interval="10 7 * * *",
    catchup=False,
    tags=["jobkorea", "meta_feed"]
)
def jobko_meta_feed_download_and_filter():

    @task()
    def fetch_and_save_xml():
        os.makedirs(SAVE_DIR, exist_ok=True)

        # WAF 우회
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.get(XML_URL, headers=headers,timeout=30)
        response.raise_for_status()
        with open(XML_PATH, "wb") as f:
            f.write(response.content)
        print(f"Saved XML to: {XML_PATH}")
        return XML_PATH

    @task()
    def parse_and_generate_csvs(xml_path: str):
        tree = ET.parse(xml_path)
        root = tree.getroot()

        rows = []
        for product in root.findall("product"):
            item = {}
            for elem in product:
                tag = elem.tag.strip()
                text = elem.text.strip() if elem.text else ""
                item[tag] = text
            rows.append(item)

        if not rows:
            raise ValueError("No product data found in XML.")

        headers = list(rows[0].keys())
        
        # 전체 CSV 저장
        with open(CSV_ALL_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Saved full CSV to: {CSV_ALL_PATH}")

        # AI 필터링
        ai_rows = [r for r in rows if r.get("custom_label_4", "") == "AI"]
        if ai_rows:
            with open(CSV_AI_PATH, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(ai_rows)
            print(f"Saved AI-only CSV to: {CSV_AI_PATH}")
        else:
            print("No AI-related rows found.")

    xml_downloaded = fetch_and_save_xml()
    csv_genrated = parse_and_generate_csvs(xml_downloaded)


    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_xml_download_dag",
        trigger_dag_id="jobko_generate_meta_images_sequential",
        wait_for_completion=False,
    )

    csv_genrated >> trigger_next_dag

jobko_meta_feed_download_and_filter()