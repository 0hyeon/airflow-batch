import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowException
import boto3
import pandas as pd
import io
import json
from airflow.operators.python import get_current_context
from pathlib import Path
import threading
import paramiko
import os

from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig

# ───────────────── Kubernetes 경량 파드 오버라이드 ─────────────────
from kubernetes.client import (
    V1Pod,
    V1ObjectMeta,
    V1PodSpec,
    V1Affinity,
    V1PodAntiAffinity,
    V1WeightedPodAffinityTerm,
    V1PodAffinityTerm,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1Container,
    V1ResourceRequirements,
    V1EnvVar,
)

EXECUTOR_CONFIG_LITE = {
    "pod_override": V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(labels={"app": "airflow-task-lite", "role": "lite"}),
        spec=V1PodSpec(
            restart_policy="Never",
            affinity=V1Affinity(
                pod_anti_affinity=V1PodAntiAffinity(
                    preferred_during_scheduling_ignored_during_execution=[
                        V1WeightedPodAffinityTerm(
                            weight=100,
                            pod_affinity_term=V1PodAffinityTerm(
                                label_selector=V1LabelSelector(
                                    match_expressions=[
                                        V1LabelSelectorRequirement(
                                            key="role",
                                            operator="In",
                                            values=["lite", "heavy"],
                                        )
                                    ]
                                ),
                                topology_key="kubernetes.io/hostname",
                            ),
                        )
                    ]
                )
            ),
            containers=[
                V1Container(
                    name="base",  # pod_template의 컨테이너명과 일치해야 적용됨
                    resources=V1ResourceRequirements(
                        requests={
                            "cpu": "300m",
                            "memory": "1Gi",
                            "ephemeral-storage": "1Gi",
                        },
                        limits={
                            "cpu": "1000m",
                            "memory": "2Gi",
                            "ephemeral-storage": "2Gi",
                        },
                    ),
                    env=[
                        V1EnvVar(
                            name="AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", value="1800"
                        ),
                    ],
                )
            ],
        ),
    )
}
# ───────────────────────────────────────────────────────────────────

S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"

# 마켓별 SFTP Connection ID 매핑
SFTP_CONN_MAP = {
    "gmarket": "gmarket_sftp",
    "auction": "auction_sftp",
}

# K8s에서 RW 보장되는 경로(로그 폴더). 필요 시 env ROLL_OUT_STATE_FILE로 경로 지정
SCHEDULE_FILE = Path(
    os.environ.get("ROLL_OUT_STATE_FILE", "/opt/airflow/logs/schedule.json")
)
file_lock = threading.Lock()


@dag(
    dag_id="upload_feeds_to_sftp_dynamically",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gmc", "sftp", "cumulative", "dynamic", "triggered"],
    default_args={
        "pool": "lite_pool",  # 기본 풀을 라이트로
        "executor_config": EXECUTOR_CONFIG_LITE,  # 기본 파드 스펙을 라이트로
        "queue": "kubernetes",  # KubernetesExecutor 큐(환경에 따라 생략 가능)
    },
)
def process_and_upload_feeds_to_sftp_cumulatively_dag():

    @task(
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",
    )
    def generate_s3_process_list_cumulatively():
        """
        schedule.json을 기반으로 점진적 배포 스케줄을 실행합니다.
        Trigger로 전달받은 market 값을 사용하여 S3 경로를 동적으로 설정합니다.
        market 값이 없으면 'gmarket'을 기본값으로 사용합니다.
        """
        # --- 1. Trigger로부터 market 값 가져오기 ---
        ctx = get_current_context()
        conf = (ctx.get("dag_run") and ctx["dag_run"].conf) or {}
        market = (conf.get("market") or "gmarket").lower()
        print(f"Executing cumulative rollout for market: {market}")

        # SFTP 연결 정보 확인
        sftp_conn_id = SFTP_CONN_MAP.get(market)
        if not sftp_conn_id:
            raise AirflowException(f"No SFTP connection ID found for market: {market}")
        print(f"Using SFTP connection: {sftp_conn_id}")

        # --- 2. 상태파일 준비 ---
        if not SCHEDULE_FILE.exists():
            SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(SCHEDULE_FILE, "w") as f:
                json.dump(
                    {
                        "is_paused": False,
                        "total_feeds": 100,
                        "rollout_start_date": pendulum.now(
                            "Asia/Seoul"
                        ).to_date_string(),
                        "last_run_date": "",
                        "daily_run_count": 0,
                    },
                    f,
                    indent=2,
                )
            print(f"Initialized schedule file at {SCHEDULE_FILE}")

        # --- 3. 스케줄 기반 배포율 계산 ---
        FIRST_DAY_SCHEDULE = [5, 10, 15, 20]
        SUBSEQUENT_DAYS_SCHEDULE = [40, 40, 40, 80, 100]

        today_str = pendulum.now("Asia/Seoul").to_date_string()
        current_percent = 0

        with file_lock:
            with open(SCHEDULE_FILE, "r") as f:
                schedule_data = json.load(f)

            is_paused = schedule_data.get("is_paused", False)

            total_feeds = schedule_data["total_feeds"]
            start_date_str = schedule_data["rollout_start_date"]
            last_run_date = schedule_data.get("last_run_date", "")
            daily_run_count = schedule_data.get("daily_run_count", 0)

            if is_paused:
                print("=======Rollout is PAUSED===========")
            elif (not last_run_date) or (today_str > last_run_date):
                print(f"New day detected. Resetting daily run count for {today_str}.")
                daily_run_count = 0
                last_run_date = today_str

            start_date = pendulum.parse(start_date_str)
            today = pendulum.parse(today_str)
            days_elapsed = (today - start_date).in_days()

            if days_elapsed < 0:
                print("Rollout has not started yet.")
                current_percent = 0
            elif days_elapsed == 0:
                print(f"Day 1: Processing run #{daily_run_count + 1}")
                if daily_run_count < len(FIRST_DAY_SCHEDULE):
                    current_percent = FIRST_DAY_SCHEDULE[daily_run_count]
                else:
                    current_percent = FIRST_DAY_SCHEDULE[-1]
            else:
                day_index = days_elapsed - 1
                print(f"Day {days_elapsed + 1}: Applying fixed daily percentage.")
                if day_index < len(SUBSEQUENT_DAYS_SCHEDULE):
                    current_percent = SUBSEQUENT_DAYS_SCHEDULE[day_index]
                else:
                    current_percent = SUBSEQUENT_DAYS_SCHEDULE[-1]

            if not is_paused:
                schedule_data["last_run_date"] = last_run_date
                schedule_data["daily_run_count"] = daily_run_count + 1

                with open(SCHEDULE_FILE, "w") as f:
                    json.dump(schedule_data, f, indent=2)
                print("State updated for the next run")
            else:
                if days_elapsed == 0:
                    print(
                        f"Day 1: Maintaining run #{daily_run_count + 1} at {current_percent}%"
                    )
                else:
                    print(
                        f"Day {days_elapsed + 1}: Maintaining fixed percentage at {current_percent}%"
                    )

        # --- 4. 동적 market 값을 사용하여 파일 목록 생성 ---
        end_index = int(total_feeds * (current_percent / 100))
        print(
            f"Applying {current_percent}%. Processing files from index 0 to {end_index - 1}."
        )

        if end_index == 0:
            return []

        source_prefix = f"feeds/google/{market}/GMC_processed_final"

        file_mappings = [
            {
                "s3_key": f"{source_prefix}/{market}_feed_{i:03d}.txt.gz",
                "remote_filename": f"{market}_feed_{i:03d}.txt.gz",
                "sftp_conn_id": sftp_conn_id,
            }
            for i in range(0, end_index)
        ]

        return file_mappings

    @task(
        task_id="process_and_upload_to_sftp",
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",
        retries=3,
        retry_delay=pendulum.duration(minutes=5),
    )
    def process_and_upload_to_sftp(
        s3_key: str, remote_filename: str, sftp_conn_id: str
    ):
        """
        S3에서 파일을 읽어 'updated_at' 컬럼을 제거한 후, SFTP로 스트리밍 업로드합니다.
        """
        # AWS S3 연결
        try:
            aws_conn = BaseHook.get_connection(AWS_CONN_ID)
            session = boto3.Session(
                aws_access_key_id=aws_conn.login,
                aws_secret_access_key=aws_conn.password,
            )
        except AirflowException:
            print("AWS Connection ID not found. Falling back to default boto3 session.")
            session = boto3.Session()

        # s3 클라이언트에 표준 재시도 정책 적용
        boto_cfg = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
        s3 = session.client("s3", config=boto_cfg)

        # SFTP 연결 정보 가져오기
        sftp_conn = BaseHook.get_connection(sftp_conn_id)
        host = sftp_conn.host
        port = int(sftp_conn.port or 22)
        username = sftp_conn.login
        password = sftp_conn.password

        transport = None
        sftp = None

        try:
            # S3에서 파일 읽기 및 처리
            print(f"Reading from S3: s3://{S3_BUCKET}/{s3_key}")
            s3_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            df = pd.read_csv(s3_response["Body"], sep="\t", compression="gzip")

            # updated_at 컬럼 제거
            if "updated_at" in df.columns:
                df = df.drop(columns=["updated_at"])
                print(f"Removed 'updated_at' column from {s3_key}")

            # 처리된 데이터를 메모리 버퍼에 저장
            output_buffer = io.BytesIO()
            df.to_csv(
                output_buffer,
                sep="\t",
                index=False,
                compression="gzip",
                encoding="utf-8",
            )
            output_buffer.seek(0)

            # SFTP 연결 및 업로드
            print(f"Connecting to SFTP: {host}:{port} as {username}")
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            print(f"Uploading to SFTP: {remote_filename}")
            with sftp.file(remote_filename, "wb") as remote_file:
                remote_file.set_pipelined(True)
                # 버퍼의 내용을 청크 단위로 업로드
                chunk_size = 1024 * 2048  # 2MB chunks
                while True:
                    chunk = output_buffer.read(chunk_size)
                    if not chunk:
                        break
                    remote_file.write(chunk)

            print(f"Successfully uploaded {remote_filename} to SFTP")

        except Exception as e:
            raise AirflowException(f"Failed to process and upload {s3_key}: {e}")

        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()

    # DAG 흐름 정의
    file_list = generate_s3_process_list_cumulatively()
    process_and_upload_to_sftp.expand_kwargs(file_list)


process_and_upload_feeds_to_sftp_cumulatively_dag()


# # -*- coding: utf-8 -*-
# import pendulum
# from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook
# from airflow.models.dagrun import DagRun
# from airflow.operators.python import ShortCircuitOperator
# from airflow.exceptions import AirflowException

# import boto3
# import paramiko

# # ───────────────── Kubernetes 경량 파드 오버라이드 ─────────────────
# from kubernetes.client import (
#     V1Pod,
#     V1ObjectMeta,
#     V1PodSpec,
#     V1Affinity,
#     V1PodAntiAffinity,
#     V1WeightedPodAffinityTerm,
#     V1PodAffinityTerm,
#     V1LabelSelector,
#     V1LabelSelectorRequirement,
#     V1Container,
#     V1ResourceRequirements,
#     V1EnvVar,
# )

# EXECUTOR_CONFIG_LITE = {
#     "pod_override": V1Pod(
#         api_version="v1",
#         kind="Pod",
#         metadata=V1ObjectMeta(labels={"app": "airflow-task-lite", "role": "lite"}),
#         spec=V1PodSpec(
#             restart_policy="Never",
#             affinity=V1Affinity(
#                 pod_anti_affinity=V1PodAntiAffinity(
#                     preferred_during_scheduling_ignored_during_execution=[
#                         V1WeightedPodAffinityTerm(
#                             weight=100,
#                             pod_affinity_term=V1PodAffinityTerm(
#                                 label_selector=V1LabelSelector(
#                                     match_expressions=[
#                                         V1LabelSelectorRequirement(
#                                             key="role",
#                                             operator="In",
#                                             values=["lite", "heavy"],
#                                         )
#                                     ]
#                                 ),
#                                 topology_key="kubernetes.io/hostname",
#                             ),
#                         )
#                     ]
#                 )
#             ),
#             containers=[
#                 V1Container(
#                     name="base",  # pod_template의 컨테이너명과 일치해야 적용됨
#                     resources=V1ResourceRequirements(
#                         requests={
#                             "cpu": "300m",
#                             "memory": "512Mi",
#                             "ephemeral-storage": "1Gi",
#                         },
#                         limits={
#                             "cpu": "1000m",
#                             "memory": "1Gi",
#                             "ephemeral-storage": "2Gi",
#                         },
#                     ),
#                     env=[
#                         V1EnvVar(
#                             name="AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", value="1800"
#                         ),
#                     ],
#                 )
#             ],
#         ),
#     )
# }
# # ───────────────────────────────────────────────────────────────────

# # --- 사용자 설정 ---
# S3_BUCKET = "gyoung0-test"
# AWS_CONN_ID = "aws_conn_id"

# # 마켓별 SFTP Connection ID 매핑
# SFTP_CONN_MAP = {
#     "gmarket": "gmarket_sftp",
#     "auction": "auction_sftp",
# }


# @dag(
#     dag_id="upload_feeds_to_sftp_dynamically",
#     start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
#     schedule=None,
#     catchup=False,
#     max_active_runs=1,
#     tags=["gmc", "sftp", "dynamic", "triggered"],
#     default_args={
#         "pool": "lite_pool",  # 기본 풀을 라이트로
#         "executor_config": EXECUTOR_CONFIG_LITE,  # 기본 파드 스펙을 라이트로
#         "queue": "kubernetes",  # KubernetesExecutor 큐(환경에 따라 생략 가능)
#     },
# )
# def upload_feeds_to_sftp_dag():

#     # 추후 삭제 (아래처럼도 개별 지정 가능하지만 default_args로 이미 적용됨)
#     do_nothing_and_succeed = ShortCircuitOperator(
#         task_id="do_nothing_and_succeed",
#         python_callable=lambda: False,  # 👈 False를 반환하여 뒷 단계를 건너뛰게 함
#         executor_config=EXECUTOR_CONFIG_LITE,
#         pool="lite_pool",
#     )

#     @task(
#         executor_config=EXECUTOR_CONFIG_LITE,
#         pool="lite_pool",
#     )
#     def generate_file_upload_list(dag_run: DagRun):
#         """
#         DAG 실행 시 conf로 전달받은 market 정보를 기반으로
#         업로드할 파일 목록과 SFTP 접속 정보를 동적으로 생성합니다.
#         """
#         market = dag_run.conf.get("market")
#         if not market:
#             raise AirflowException(
#                 "'market' not found in DAG run configuration. This DAG must be triggered with a market."
#             )

#         market = market.lower()
#         sftp_conn_id = SFTP_CONN_MAP.get(market)
#         if not sftp_conn_id:
#             raise AirflowException(f"No SFTP connection ID found for market: {market}")

#         print(
#             f"Generating file list for market: {market} using conn_id: {sftp_conn_id}"
#         )

#         s3_key_prefix = f"feeds/google/{market}/GMC_processed_final"

#         file_mappings = [
#             {
#                 "s3_key": f"{s3_key_prefix}/{market}_feed_{i:03d}.txt.gz",
#                 "remote_filename": f"{market}_feed_{i:03d}.txt.gz",
#                 "sftp_conn_id": sftp_conn_id,  # 사용할 conn_id를 각 task에 전달
#             }
#             for i in range(100)
#         ]
#         return file_mappings

#     @task(
#         task_id="s3_to_gmc_sftp_streaming_upload",
#         executor_config=EXECUTOR_CONFIG_LITE,
#         pool="lite_pool",
#     )
#     def s3_to_gmc_sftp_streaming_upload(
#         s3_key: str, remote_filename: str, sftp_conn_id: str
#     ):
#         """S3에서 SFTP로 파일을 스트리밍하여 업로드합니다."""
#         sftp_conn = BaseHook.get_connection(sftp_conn_id)
#         host, port, username, password = (
#             sftp_conn.host,
#             int(sftp_conn.port or 22),
#             sftp_conn.login,
#             sftp_conn.password,
#         )

#         # AWS 세션 (Airflow Connection 우선)
#         try:
#             aws_conn = BaseHook.get_connection(AWS_CONN_ID)
#             session = boto3.Session(
#                 aws_access_key_id=aws_conn.login,
#                 aws_secret_access_key=aws_conn.password,
#                 aws_session_token=(
#                     aws_conn.extra_dejson.get("aws_session_token")
#                     if aws_conn.extra
#                     else None
#                 ),
#                 region_name=(
#                     aws_conn.extra_dejson.get("region_name") if aws_conn.extra else None
#                 ),
#             )
#         except Exception:
#             session = boto3.Session()
#         s3 = session.client("s3")

#         transport = None
#         sftp = None
#         try:
#             transport = paramiko.Transport((host, port))
#             transport.connect(username=username, password=password)
#             sftp = paramiko.SFTPClient.from_transport(transport)

#             print(
#                 f"[{sftp_conn_id}] Streaming s3://{S3_BUCKET}/{s3_key} to sftp://{host}/{remote_filename}"
#             )

#             s3_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
#             s3_streaming_body = s3_response["Body"]

#             with sftp.file(remote_filename, "wb") as f:
#                 # 파이프라이닝으로 RTT 최소화
#                 try:
#                     f.set_pipelined(True)
#                 except Exception:
#                     pass
#                 for chunk in s3_streaming_body.iter_chunks(
#                     chunk_size=1024 * 2048
#                 ):  # 2MB
#                     if not chunk:
#                         continue
#                     f.write(chunk)

#             print(f"[{sftp_conn_id}] Streaming success: {remote_filename}")
#         finally:
#             try:
#                 if sftp:
#                     sftp.close()
#             finally:
#                 if transport:
#                     transport.close()

#     generated_list = generate_file_upload_list()
#     upload_tasks = s3_to_gmc_sftp_streaming_upload.expand_kwargs(generated_list)

#     # 태스크가 실행 안 되고 바로 스킵됨 (운영에서 제거 예정)
#     do_nothing_and_succeed >> generated_list


# upload_feeds_to_sftp_dag()
