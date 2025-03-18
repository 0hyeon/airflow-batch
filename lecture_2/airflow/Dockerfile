# Airflow 기본 이미지 기반
FROM apache/airflow:2.3.2

# 필요한 Python 라이브러리 설치
RUN pip install --no-cache-dir \
    pandas boto3 requests pyarrow confluent-kafka \
    apache-airflow-providers-cncf-kubernetes
