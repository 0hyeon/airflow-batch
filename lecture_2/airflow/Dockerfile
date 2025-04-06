# docker build -t 0hyeon/airflow-custom:2.11.3 .
# docker build --no-cache -t 0hyeon/airflow-custom:2.11.3 .

# docker push 0hyeon/airflow-custom:2.11.3
# 1️⃣ Airflow 기본 이미지 기반
FROM apache/airflow:2.10.5

# 2️⃣ root 권한 활성화
USER root

# 3️⃣ 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    wget build-essential libssl-dev libffi-dev \
    libbz2-dev libsqlite3-dev libreadline-dev \
    zlib1g-dev libncurses5-dev libgdbm-dev liblzma-dev \
    libnss3-dev tk-dev libedit-dev libexpat1-dev \
    libxml2-dev libxmlsec1-dev libxmlsec1-openssl pkg-config \
    unzip curl xvfb software-properties-common \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/*

# 4️⃣ Python 3.10 소스 빌드 & 설치
WORKDIR /usr/src
RUN wget https://www.python.org/ftp/python/3.10.13/Python-3.10.13.tgz && \
    tar xvf Python-3.10.13.tgz && \
    cd Python-3.10.13 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.10 /usr/local/bin/python && \
    ln -sf /usr/local/bin/pip3.10 /usr/local/bin/pip

# 5️⃣ Python 3.10을 기본 경로로 설정
ENV PATH="/usr/local/bin:$PATH"
ENV PYTHONPATH="/usr/local/lib/python3.10/site-packages"

USER airflow 

# 6️⃣ Airflow 관련 패키지 및 Selenium 설치
RUN pip install --no-cache-dir --upgrade \
    apache-airflow-providers-amazon==9.2.0 \
    apache-airflow-providers-cncf-kubernetes==10.1.0 \
    apache-airflow-providers-common-compat==1.3.0 \
    apache-airflow-providers-common-io==1.5.0 \
    apache-airflow-providers-common-sql==1.20.0 \
    apache-airflow-providers-fab==1.5.3 \
    apache-airflow-providers-ftp==3.5.0 \
    apache-airflow-providers-http==4.5.0 \
    apache-airflow-providers-imap==3.3.0 \
    apache-airflow-providers-smtp==1.9.0 \
    apache-airflow-providers-sqlite==3.4.3 \
    selenium

# 7️⃣ Chrome 설치 (apt-key 대신 gpg 사용)
USER root
RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor --output /tmp/google-chrome-keyring.gpg && \
    mv /tmp/google-chrome-keyring.gpg /usr/share/keyrings/ && \
    echo "deb [signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y --no-install-recommends google-chrome-stable && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/*

# 8️⃣ ChromeDriver 설치 (Chrome 버전에 맞춰 다운로드)
RUN export CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1,2,3) && \
    echo "Detected Chrome version: $CHROME_VERSION" && \
    export CHROME_DRIVER_VERSION=$(curl -sS "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION") && \
    if [ -z "$CHROME_DRIVER_VERSION" ] || [[ "$CHROME_DRIVER_VERSION" == *"<Error>"* ]]; then \
        echo "ChromeDriver version not found! Trying fallback version..."; \
        export CHROME_VERSION_MAJOR=$(echo $CHROME_VERSION | cut -d '.' -f 1,2) && \
        export CHROME_DRIVER_VERSION=$(curl -sS "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION_MAJOR"); \
    fi && \
    if [ -z "$CHROME_DRIVER_VERSION" ] || [[ "$CHROME_DRIVER_VERSION" == *"<Error>"* ]]; then \
        echo "Fallback failed! Using latest ChromeDriver version."; \
        export CHROME_DRIVER_VERSION=$(curl -sS "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"); \
    fi && \
    echo "Using ChromeDriver version: $CHROME_DRIVER_VERSION" && \
    wget -N "https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip" -P /tmp && \
    if [ -f "/tmp/chromedriver_linux64.zip" ]; then \
        unzip /tmp/chromedriver_linux64.zip -d /usr/local/bin/ && \
        rm -f /tmp/chromedriver_linux64.zip; \
    else \
        echo "❌ ChromeDriver ZIP 파일 다운로드 실패"; \
        exit 1; \
    fi && \
    chmod +x /usr/local/bin/chromedriver
