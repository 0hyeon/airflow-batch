import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowException
import boto3
import pandas as pd
import io
from airflow.operators.python import get_current_context

#

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


@dag(
    dag_id="upload_feeds_to_sftp_dynamically",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gmc", "s3", "dynamic", "triggered"],
    default_args={
        "pool": "lite_pool",  # 기본 풀을 라이트로
        "executor_config": EXECUTOR_CONFIG_LITE,  # 기본 파드 스펙을 라이트로
        "queue": "kubernetes",  # KubernetesExecutor 큐(환경에 따라 생략 가능)
    },
)
def process_and_reupload_feeds_in_s3_dag():

    @task(
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",
    )
    def generate_s3_process_list():
        """
        Trigger로 전달받은 market 값을 사용하여 S3 경로 목록을 생성합니다.
        market 값이 없으면 'gmarket'을 기본값으로 사용합니다.
        """
        ctx = get_current_context()
        market = (ctx.get("dag_run") and ctx["dag_run"].conf.get("market")) or "gmarket"

        print(f"Generating file list for market: {market}")

        # 원본 파일들이 있는 S3 경로
        source_prefix = f"feeds/google/{market}/GMC_processed_final"
        # 처리 후 저장될 S3 경로
        destination_prefix = f"feeds/google/{market}/GMC_upload_ready"

        file_mappings = [
            {
                "source_s3_key": f"{source_prefix}/{market}_feed_{i:03d}.txt.gz",
                "destination_s3_key": f"{destination_prefix}/{market}_feed_{i:03d}.txt.gz",
            }
            for i in range(100)
        ]
        return file_mappings

    @task(
        task_id="process_and_upload_to_s3",
        executor_config=EXECUTOR_CONFIG_LITE,
        pool="lite_pool",
        retries=2,
        retry_delay=pendulum.duration(minutes=3),
    )
    def process_and_upload_to_s3(source_s3_key: str, destination_s3_key: str):
        """
        S3에서 파일을 안전하게(멀티파트+재시도) 내려받아 pandas로 처리한 후,
        다시 멀티파트 업로드로 저장합니다.
        """
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

        print(f"Processing s3://{S3_BUCKET}/{source_s3_key}")

        try:
            # 1) S3 → 메모리 버퍼(BytesIO)로 안전 다운로드 (멀티파트+재시도)
            dl_cfg = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,
                multipart_chunksize=16 * 1024 * 1024,
                max_concurrency=6,
            )
            download_buf = io.BytesIO()
            s3.download_fileobj(S3_BUCKET, source_s3_key, download_buf, Config=dl_cfg)
            download_buf.seek(0)

            # 2) pandas 처리 (gzip 입력)
            df = pd.read_csv(download_buf, sep="\t", compression="gzip")

            if "updated_at" in df.columns:
                df = df.drop(columns=["updated_at"])
                print("Successfully removed 'updated_at' column.")
            else:
                print("'updated_at' column not found, skipping removal.")

            # 3) 결과를 gzip으로 메모리 버퍼에 쓰기
            output_buffer = io.BytesIO()
            df.to_csv(
                output_buffer,
                sep="\t",
                index=False,
                compression="gzip",
                encoding="utf-8",
            )
            output_buffer.seek(0)

            # 4) 메모리 버퍼 → S3 멀티파트 업로드(재시도 내장)
            ul_cfg = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,
                multipart_chunksize=16 * 1024 * 1024,
                max_concurrency=6,
            )
            s3.upload_fileobj(
                output_buffer, S3_BUCKET, destination_s3_key, Config=ul_cfg
            )

            print(f"Successfully uploaded to s3://{S3_BUCKET}/{destination_s3_key}")

        except Exception as e:
            print(f"An error occurred: {e}")
            raise

    # DAG 흐름 정의
    file_list = generate_s3_process_list()
    upload_tasks = process_and_upload_to_s3.expand_kwargs(file_list)

    file_list >> upload_tasks


process_and_reupload_feeds_in_s3_dag()


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
