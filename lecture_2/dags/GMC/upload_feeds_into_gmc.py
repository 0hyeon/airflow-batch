# -*- coding: utf-8 -*-
import io
import os
import pendulum
import boto3
import pandas as pd
import paramiko
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

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
                        )
                    ],
                )
            ],
        ),
    )
}
# ───────────────────────────────────────────────────────────────────

# --- 사용자 설정 ---
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"

# 마켓별 SFTP Connection ID 매핑 (Airflow Connections에 정의되어 있어야 함)
SFTP_CONN_MAP = {
    "gmarket": "gmarket_sftp",
    "auction": "auction_sftp",
}

# ── 분할 업로드 설정 (2026-10-07 한정) ─────────────────────────────
# 9/28~10/2 피드 미갱신분이 한 번에 반영되면 변경량이 과도하므로,
# 100개 피드를 6등분해 10~15시 정각마다 구간별로 올려 부하를 분산한다.
#
# SPLIT_DATE 당일 scheduled run  → 분할 업로드
# SPLIT_START 이후 manual run    → 전체 업로드 (16·22시 배치 트리거)
# SPLIT_START 이전 manual run    → 차단 (04시 배치가 전체를 먼저 올리는 것 방지)
# SPLIT_DATE 경과 후 scheduled   → 빈 목록 → 평시 운영 자동 복귀 (원복 작업 불필요)
TOTAL_FEEDS = 100
SPLIT_COUNT = 6
SPLIT_DATE = "2026-10-07"
SPLIT_HOURS = [10, 11, 12, 13, 14, 15]
SPLIT_START = pendulum.datetime(2026, 10, 7, 10, 0, tz="Asia/Seoul")


@dag(
    dag_id="upload_feeds_to_sftp_dynamically",
    start_date=pendulum.datetime(2025, 10, 16, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gmc", "sftp", "split", "dynamic", "triggered"],
    default_args={
        "pool": "lite_pool",
        "queue": "kubernetes",
        "executor_config": EXECUTOR_CONFIG_LITE,
        "owner": "airflow",
    },
)
def process_and_upload_feeds_to_sftp_dag():
    """
    S3(GMC_processed_final) → (updated_at 제거) → SFTP 업로드
    - EXECUTOR_CONFIG_LITE 적용: 경량 파드로 병렬 처리
    - SPLIT_DATE 당일은 구간 분할, 그 외는 전체 업로드
    """

    # ───────────────────────── 유틸 ─────────────────────────
    def _s3_client():
        try:
            aws_conn = BaseHook.get_connection(AWS_CONN_ID)
            session = boto3.Session(
                aws_access_key_id=aws_conn.login,
                aws_secret_access_key=aws_conn.password,
            )
        except AirflowException:
            session = boto3.Session()
        # 표준 재시도 정책
        from botocore.config import Config as BotoConfig

        return session.client(
            "s3", config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
        )

    def _ensure_remote_dir(sftp, remote_dir: str):
        # 절대경로/상대경로 모두 지원
        # 존재하지 않으면 단계적으로 생성
        if not remote_dir or remote_dir == ".":
            return
        parts = []
        for p in remote_dir.split("/"):
            if p in ("", "."):
                continue
            parts.append(p)
            cur = (
                "/" + "/".join(parts) if remote_dir.startswith("/") else "/".join(parts)
            )
            try:
                sftp.stat(cur)
            except IOError:
                sftp.mkdir(cur)

    def _connect_sftp(sftp_conn_id: str):
        conn = BaseHook.get_connection(sftp_conn_id)
        host = conn.host
        port = int(conn.port or 22)
        username = conn.login
        # password OR private_key from extras
        pkey = None
        if conn.extra_dejson.get("private_key"):
            from io import StringIO

            key_str = conn.extra_dejson["private_key"]
            try:
                pkey = paramiko.RSAKey.from_private_key(StringIO(key_str))
            except Exception:
                pkey = paramiko.Ed25519Key.from_private_key(StringIO(key_str))
        transport = paramiko.Transport((host, port))
        if pkey:
            transport.connect(username=username, pkey=pkey)
        else:
            transport.connect(username=username, password=conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return transport, sftp, conn.extra_dejson  # extras로 remote_dir 등 받음

    # ───────────────────────── Tasks ─────────────────────────

    @task
    def generate_s3_process_list_by_part():
        """
        업로드 대상 목록 생성.

        [동작 모드]
          - 분할 업로드 : SPLIT_DATE 당일, SPLIT_HOURS 정각의 scheduled run
                          → 100개를 SPLITS 등분한 구간만 업로드
          - 전체 업로드 : 그 외 manual(EMR TriggerDagRunOperator) run
          - 미동작     : SPLIT_START 이전 manual run / 분할 대상 아닌 scheduled run

        SPLIT_START 이전 manual을 차단하는 이유:
          분할 시작 전에 04시 배치가 EMR을 거쳐 SFTP를 자동 트리거하면
          전체 100개가 먼저 올라가 분할 자체가 무의미해지기 때문.

        SPLIT_DATE 경과 후에는 scheduled run이 빈 목록을 반환하므로
        별도 원복 작업 없이 자동으로 전체 업로드(평시 운영)로 복귀한다.
        """
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf if dag_run else None) or {}
        run_type = dag_run.run_type if dag_run else "manual"

        market = str(conf.get("market") or "gmarket").lower()
        print(f"[generate] market={market} run_type={run_type}")

        # --- 'auction' 마켓일 경우, 여기서 실행을 중단하고 빈 리스트를 반환 ---
        if market == "auction":
            print(f"Market is '{market}'. Skipping process as intended.")
            return []

        sftp_conn_id = SFTP_CONN_MAP.get(market)
        if not sftp_conn_id:
            raise AirflowException(f"No SFTP connection ID for market: {market}")

        now = pendulum.now("Asia/Seoul")
        part = conf.get("part")  # 수동 지정이 최우선

        if part is None:
            if run_type == "scheduled":
                if now.to_date_string() == SPLIT_DATE and now.hour in SPLIT_HOURS:
                    part = SPLIT_HOURS.index(now.hour)
                else:
                    print(f"[generate] {now} is not a split slot -> skip")
                    return []
            elif now < SPLIT_START:
                print(f"[generate] before SPLIT_START({SPLIT_START}) -> skip")
                return []

        if part is None:
            indices = range(TOTAL_FEEDS)
            print(f"[generate] FULL upload: 000..{TOTAL_FEEDS - 1:03d}")
        else:
            part = int(part)
            if not 0 <= part < SPLIT_COUNT:
                raise AirflowException(
                    f"part must be 0..{SPLIT_COUNT - 1}, got {part}"
                )
            base, rem = divmod(TOTAL_FEEDS, SPLIT_COUNT)
            start = part * base + min(part, rem)
            end = start + base + (1 if part < rem else 0)
            indices = range(start, end)
            print(
                f"[generate] part={part}/{SPLIT_COUNT - 1} → "
                f"{start:03d}..{end - 1:03d} ({end - start} files)"
            )

        source_prefix = f"feeds/google/{market}/GMC_processed_final"
        # remote_dir은 Connection Extras로도 전달 가능 (없으면 기본 경로)
        remote_dir_default = f"/incoming/google/{market}"

        items = []
        for i in indices:
            items.append(
                {
                    "s3_key": f"{source_prefix}/{market}_feed_{i:03d}.txt.gz",
                    "remote_dir": remote_dir_default,  # 필요 시 connection.extra의 remote_dir로 override
                    "remote_filename": f"{market}_feed_{i:03d}.txt.gz",
                    "sftp_conn_id": sftp_conn_id,
                    "market": market,
                }
            )
        return items

    @task(
        task_id="process_and_upload_to_sftp",
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    def process_and_upload_to_sftp(
        s3_key: str,
        remote_dir: str,
        remote_filename: str,
        sftp_conn_id: str,
        market: str,
    ):
        """
        S3에서 파일 읽기 → updated_at 제거 → SFTP 원자적 업로드(.tmp → rename)
        """
        # 1) S3에서 읽기
        s3 = _s3_client()
        print(f"[s3→df] s3://{S3_BUCKET}/{s3_key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_csv(obj["Body"], sep="\t", compression="gzip")

        if "updated_at" in df.columns:
            df = df.drop(columns=["updated_at"])
            print(f"[df] removed 'updated_at' ({s3_key})")

        # 2) gzip으로 메모리 버퍼에 저장
        buf = io.BytesIO()
        df.to_csv(buf, sep="\t", index=False, compression="gzip", encoding="utf-8")
        buf.seek(0)

        # 3) SFTP 연결
        transport, sftp, extras = _connect_sftp(sftp_conn_id)
        try:
            # connection.extra에 remote_dir가 있으면 우선 사용
            remote_base = extras.get("remote_dir", remote_dir)
            target_dir = os.path.join(remote_base)  # market 하위는 위에서 포함된 버전
            _ensure_remote_dir(sftp, target_dir)

            remote_path = os.path.join(target_dir, remote_filename)
            print(f"[sftp] uploading(final) → {remote_path}")
            buf.seek(0)
            written = 0

            # 최종 파일명으로 바로 업로드
            with sftp.file(remote_path, "wb") as rf:
                rf.set_pipelined(True)
                chunk = buf.read(2 * 1024 * 1024)  # 2MB
                while chunk:
                    rf.write(chunk)
                    written += len(chunk)
                    chunk = buf.read(2 * 1024 * 1024)

            # 업로드 정합성 확인(선택이지만 권장)
            st = sftp.stat(remote_path)
            if st.st_size != written:
                try:
                    sftp.remove(remote_path)  # 불완전 파일 제거
                except Exception:
                    pass
                raise AirflowException(
                    f"Uploaded size mismatch: remote={st.st_size}, written={written}"
                )

            print(f"[sftp] uploaded(final): {remote_path} ({st.st_size} bytes)")

        except Exception as e:
            # 예외 시 반쯤 올라간 최종 파일 정리
            try:
                sftp.remove(remote_path)
            except Exception:
                pass
            raise AirflowException(f"SFTP upload failed for {remote_filename}: {e}")

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            try:
                transport.close()
            except Exception:
                pass

    # ───────────────────── DAG 흐름 ─────────────────────
    file_list = generate_s3_process_list_by_part()
    _ = process_and_upload_to_sftp.expand_kwargs(file_list)


process_and_upload_feeds_to_sftp_dag()
