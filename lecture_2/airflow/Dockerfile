# docker build -t 0hyeon/airflow-custom:2.11.6 .
# docker build --no-cache -t 0hyeon/airflow-custom:2.11.6 .

# docker push 0hyeon/airflow-custom:2.11.6
# 1️⃣ Airflow 기본 이미지 기반
FROM apache/airflow:2.8.1-python3.10

USER airflow

ENV PATH="/home/airflow/.local/bin:$PATH"

RUN pip install --user --no-cache-dir aiofiles
