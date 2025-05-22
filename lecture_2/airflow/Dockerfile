# docker build -t 0hyeon/airflow-custom:2.11.5 .
# docker build --no-cache -t 0hyeon/airflow-custom:2.11.5 .

# docker push 0hyeon/airflow-custom:2.11.5
# 1️⃣ Airflow 기본 이미지 기반
# Airflow 기본 이미지 사용
FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir aiofiles
