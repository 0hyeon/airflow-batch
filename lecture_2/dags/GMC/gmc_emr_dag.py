from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.dagrun import DagRun
import pendulum
import boto3
import os
from urllib.parse import urlparse

# ===== 사용자 설정 변수 =====
S3_BUCKET = "gyoung0-test"
AWS_CONN_ID = "aws_conn_id"
AWS_REGION = "ap-northeast-2"  # ★ 리전 고정
EMR_RELEASE = "emr-7.1.0"  # ★ 실제 계정에 맞게
EMR_EC2_KEY_NAME = "test"
EMR_EC2_ROLE = "EMR_EC2_DefaultRole"
EMR_SERVICE_ROLE = "EMR_DefaultRole"
SCRIPT_S3_KEY = "scripts/gmc_process_feeds.py"
LOG_S3_PATH = f"s3://{S3_BUCKET}/emr-logs/"

# ★ 경량 파드 executor_config (오토스케일 트리거)
from kubernetes.client import (
    V1Pod,
    V1ObjectMeta,
    V1PodSpec,
    V1Container,
    V1ResourceRequirements,
    V1EnvVar,
    V1Affinity,
    V1PodAntiAffinity,
    V1WeightedPodAffinityTerm,
    V1PodAffinityTerm,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1TopologySpreadConstraint,
)

EXECUTOR_CONFIG_LITE = {
    "KubernetesExecutor": {
        "pod_override": V1Pod(
            metadata=V1ObjectMeta(labels={"app": "airflow-task-lite", "role": "lite"}),
            spec=V1PodSpec(
                restart_policy="Never",
                # ↓ 베이스의 required를 대체: '가능하면 분산'
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
                # ↓ 균등 분산 유도(막히진 않게)
                topology_spread_constraints=[
                    V1TopologySpreadConstraint(
                        max_skew=1,
                        topology_key="kubernetes.io/hostname",
                        when_unsatisfiable="ScheduleAnyway",
                        label_selector=V1LabelSelector(
                            match_expressions=[
                                V1LabelSelectorRequirement(
                                    key="role", operator="In", values=["lite"]
                                )
                            ]
                        ),
                    )
                ],
                containers=[
                    V1Container(
                        name="base",  # ← 반드시 베이스 컨테이너 이름과 동일해야 merge가 제대로 됨
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "300m",
                                "memory": "512Mi",
                                "ephemeral-storage": "1Gi",
                            },
                            limits={
                                "cpu": "1000m",
                                "memory": "1Gi",
                                "ephemeral-storage": "2Gi",
                            },
                        ),
                        env=[
                            V1EnvVar(
                                name="AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT",
                                value="1800",
                            )
                        ],
                    )
                ],
            ),
        )
    }
}


@dag(
    dag_id="gmc_gmarket_auction_emr_process_and_rename",
    start_date=pendulum.datetime(2025, 8, 29, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["emr", "gmc", "process", "feed", "gmarket"],
    doc_md="GMC 피드 처리 후 S3 출력 파일명을 변경하고 클러스터를 종료하는 최종 DAG",
    default_args={"owner": "airflow", "aws_conn_id": AWS_CONN_ID},
)
def emr_process_and_rename_final_dag():

    @task
    def create_emr_cluster(dag_run: DagRun):
        market = dag_run.conf.get("market", "default_market")
        cluster_name = f"process-{market}-feeds-cluster-final"

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        creds = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=getattr(creds, "token", None),
            region_name=AWS_REGION,  # ★ 고정
        )
        resp = client.run_job_flow(
            Name=cluster_name,
            ReleaseLabel=EMR_RELEASE,  # ★ 상수 사용
            Applications=[{"Name": "Spark"}],
            Instances={
                "InstanceGroups": [
                    {
                        "Name": "Master",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": "m5.xlarge",
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Core",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": "m5.2xlarge",
                        "InstanceCount": 4,
                    },
                ],
                "Ec2KeyName": EMR_EC2_KEY_NAME,
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
            },
            JobFlowRole=EMR_EC2_ROLE,  # ★ 상수
            ServiceRole=EMR_SERVICE_ROLE,  # ★ 상수
            LogUri=LOG_S3_PATH,
            VisibleToAllUsers=True,
        )
        return resp["JobFlowId"]

    @task(multiple_outputs=True)
    def submit_spark_job(cluster_id: str, dag_run: DagRun):
        market = dag_run.conf.get("market", "default_market")
        input_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC"
        output_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC_processed_output"

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        creds = s3_hook.get_credentials()
        client = boto3.client(
            "emr",
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=getattr(creds, "token", None),
            region_name=AWS_REGION,  # ★ 고정
        )

        spark_submit_args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            f"s3://{S3_BUCKET}/{SCRIPT_S3_KEY}",
            "--input-path",
            input_s3_path,
            "--output-path",
            output_s3_path,
        ]
        resp = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Process_GMC_Feeds",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": spark_submit_args,
                    },
                }
            ],
        )
        return {
            "step_id": resp["StepIds"][0],
            "output_path": output_s3_path,
            "market": market,
        }

    @task
    def rename_output_files(output_s3_path: str, market: str):
        """
        Spark 출력이 (A) 서브디렉토리(source_filename=...) 구조이거나,
                       (B) 평면(part-*.gz만) 구조일 때 모두 지원
        """
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
        parsed = urlparse(output_s3_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        # (A) 서브폴더 있는 경우
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        subdirs = [cp["Prefix"] for cp in listed.get("CommonPrefixes", [])]

        if subdirs:
            for sub in subdirs:
                objs = s3.list_objects_v2(Bucket=bucket, Prefix=sub).get("Contents", [])
                for o in objs:
                    k = o["Key"]
                    if "part-" in k and k.endswith(".gz"):
                        original = sub.split("=")[-1].rstrip("/")
                        base = original.replace(".tsv.gz", "")
                        new_key = os.path.join(prefix, f"{market}_{base}.txt.gz")
                        s3.copy_object(
                            CopySource={"Bucket": bucket, "Key": k},
                            Bucket=bucket,
                            Key=new_key,
                        )
                # 청소
                try:
                    s3.delete_object(Bucket=bucket, Key=os.path.join(sub, "_SUCCESS"))
                except:
                    pass
                # 서브디렉토리 내 파일 삭제
                objs = s3.list_objects_v2(Bucket=bucket, Prefix=sub).get("Contents", [])
                for o in objs:
                    s3.delete_object(Bucket=bucket, Key=o["Key"])
        else:
            # (B) 평면 구조: prefix/ 아래 part-*.gz만 존재
            objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
            for o in objs:
                k = o["Key"]
                if "part-" in k and k.endswith(".gz"):
                    # 파일명 알 수 없으면 타임스탬프 기반 이름
                    base = (
                        os.path.basename(k)
                        .replace("part-", "feed-")
                        .replace(".csv.gz", ".tsv.gz")
                    )
                    new_key = os.path.join(
                        prefix, f"{market}_{base.replace('.tsv.gz', '')}.txt.gz"
                    )
                    s3.copy_object(
                        CopySource={"Bucket": bucket, "Key": k},
                        Bucket=bucket,
                        Key=new_key,
                    )
                    s3.delete_object(Bucket=bucket, Key=k)

    # ===== DAG 플로우 =====
    # cluster_id = create_emr_cluster().override(executor_config=EXECUTOR_CONFIG_LITE)()
    cluster_id = create_emr_cluster.override(executor_config=EXECUTOR_CONFIG_LITE)()

    wait_for_cluster = EmrJobFlowSensor(
        task_id="wait_for_cluster",
        job_flow_id=cluster_id,
        target_states=["WAITING"],
        failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],  # ★
        poke_interval=30,  # ★
        timeout=60 * 20,  # ★
        mode="reschedule",  # ★
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    # spark_job_info = submit_spark_job(cluster_id).override(executor_config=EXECUTOR_CONFIG_LITE)()
    spark_job_info = submit_spark_job.override(executor_config=EXECUTOR_CONFIG_LITE)(
        cluster_id
    )

    wait_for_step = EmrStepSensor(
        task_id="wait_for_step",
        job_flow_id=cluster_id,
        step_id=spark_job_info["step_id"],
        target_states=["COMPLETED"],
        failed_states=["FAILED", "CANCELLED"],  # ★
        poke_interval=60,  # ★
        timeout=60 * 60,  # ★
        mode="reschedule",  # ★
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    # rename_files = rename_output_files(
    #     spark_job_info["output_path"], market=spark_job_info["market"]
    # ).override(executor_config=EXECUTOR_CONFIG_LITE)

    rename_files = rename_output_files.override(executor_config=EXECUTOR_CONFIG_LITE)(
        spark_job_info["output_path"], market=spark_job_info["market"]
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=cluster_id,
        trigger_rule="all_done",
        executor_config=EXECUTOR_CONFIG_LITE,
    )

    # terminate를 rename와 병렬로 둘지, rename 이후로 둘지 선택
    wait_for_cluster >> spark_job_info >> wait_for_step
    wait_for_step >> [
        rename_files,
        terminate_cluster,
    ]  # 필요시: wait_for_step >> rename_files >> terminate_cluster


emr_process_and_rename_final_dag()
