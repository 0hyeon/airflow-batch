from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import glob
import time
import boto3
from botocore.exceptions import NoCredentialsError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


# 📌 1. Airflow 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 17),  # DAG 시작 날짜
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "jobko_mobile_index_crawler",
    default_args=default_args,
    description="Crawl mobile index data and upload to S3",
    schedule_interval="0 0 * * *",  # 매일 자정 실행
    catchup=False,
)


# 📌 2. 크롤링 및 S3 업로드 함수
def crawl_and_upload():
    # 쿠버네티스 환경에서 임시 디렉토리 사용
    download_path = "/tmp"

    # Chrome 옵션 설정 (쿠버네티스 환경)
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 백그라운드 실행
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        # 1. 로그인 페이지 접속
        driver.get("https://www.mobileindex.com/login")
        time.sleep(1.5)

        # 2. 이메일 입력
        email_input = driver.find_element(By.XPATH, "//input[@placeholder='계정 이메일을 입력하세요']")
        email_input.send_keys("mkt_gb@greenbricks.co.kr")
        time.sleep(1)

        # 3. 비밀번호 입력
        password_input = driver.find_element(By.XPATH, "//input[@placeholder='비밀번호를 입력하세요']")
        password_input.send_keys("Green0501@!")
        time.sleep(1.5)

        # 4. 로그인 버튼 클릭
        login_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".flake-ui.button.primary.reverse"))
        )
        driver.execute_script("arguments[0].click();", login_button)
        time.sleep(1)

        # 5. 개인 인증키 입력
        auth_input = driver.find_element(By.XPATH, "//input[@placeholder='개인 인증키를 입력해주세요.']")
        auth_input.send_keys("c459m")

        # 6. 기기 등록하기 클릭
        gigi_register = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".flake-ui.button.primary.reverse"))
        )
        driver.execute_script("arguments[0].click();", gigi_register)
        time.sleep(1.5)

        # 7. "변경하기" 버튼 클릭 (필요 시)
        try:
            alter_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".flake-ui.modal.modify-key-modal .flake-ui.button.primary"))
            )
            driver.execute_script("arguments[0].click();", alter_btn)
            time.sleep(1.5)
        except:
            print("기기 변경 팝업 없음 (최초 로그인 아님)")

        # 8. 사용량 인덱스 클릭
        try:
            use_index = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".item.usage-index"))
            )
            driver.execute_script("arguments[0].click();", use_index)
            time.sleep(1.5)
        except:
            print("사용량 인덱스 클릭 실패")

        # 9. 경쟁앱 비교분석 클릭
        try:
            competition_app_analy = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#lnb-menu-4 > label"))
            )
            driver.execute_script("arguments[0].click();", competition_app_analy)
            time.sleep(1.5)
        except:
            print("경쟁앱 비교분석 클릭 실패")

        # 10. CSV 다운로드 클릭
        try:
            csv_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#page-scroll-obj button"))
            )
            driver.execute_script("arguments[0].click();", csv_btn)
            time.sleep(3)
        except:
            print("CSV 다운로드 실패")

        # 11. 최신 다운로드된 CSV 찾기
        def get_latest_csv(download_dir, prefix="MI-INSIGHT"):
            files = glob.glob(os.path.join(download_dir, f"{prefix}*.csv"))
            if not files:
                return None
            return max(files, key=os.path.getctime)

        csv_file_path = get_latest_csv(download_path)

        if csv_file_path:
            print(f"✅ 최신 다운로드된 CSV 파일: {csv_file_path}")
        else:
            print("❌ CSV 파일을 찾을 수 없음")
            return

        # 12. S3 업로드 설정
        AWS_ACCESS_KEY = "your-access-key"
        AWS_SECRET_KEY = "your-secret-key"
        AWS_REGION = "your-region"
        S3_BUCKET = "fc-practice2"
        today = datetime.now().strftime("%Y-%m-%d")
        file_name = os.path.basename(csv_file_path)
        S3_OBJECT_KEY = f"jobko_mobile_index/date={today}/{file_name}"

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        )
        s3_client = session.client("s3")

        # 13. S3 업로드
        s3_client.upload_file(csv_file_path, S3_BUCKET, S3_OBJECT_KEY)
        print(f"✅ S3 업로드 성공: s3://{S3_BUCKET}/{S3_OBJECT_KEY}")

        # 14. 파일 삭제
        os.remove(csv_file_path)
        print(f"🗑️ 로컬 파일 삭제 완료: {csv_file_path}")

    finally:
        driver.quit()


# 📌 3. Airflow Task 등록
crawl_task = PythonOperator(
    task_id="crawl_and_upload",
    python_callable=crawl_and_upload,
    dag=dag,
)

crawl_task
