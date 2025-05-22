# docker build -t 0hyeon/airflow-custom:2.11.7 .
# docker build --no-cache -t 0hyeon/airflow-custom:2.11.7 .

# docker push 0hyeon/airflow-custom:2.11.7
# 1️⃣ Airflow 기본 이미지 기반
FROM apache/airflow:2.10.0-python3.10

USER root

# 필수 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    libxml2-dev libxmlsec1-dev libxmlsec1-openssl \
    pkg-config libtool libxml2-utils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    aiofiles \
    selenium \
    apache-airflow-providers-amazon==9.2.0 \
    apache-airflow-providers-cncf-kubernetes==10.1.0 \
    apache-airflow-providers-http==4.5.0 \
    apache-airflow-providers-sqlite==3.4.3
