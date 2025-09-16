# from airflow.decorators import dag, task
# from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
# from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.models.dagrun import DagRun
# import pendulum
# import boto3
# import os
# from urllib.parse import urlparse

# # ===== 사용자 설정 변수 =====
# S3_BUCKET = "gyoung0-test"
# AWS_CONN_ID = "aws_conn_id"
# AWS_REGION = "ap-northeast-2"  # ★ 리전 고정
# EMR_RELEASE = "emr-7.1.0"  # ★ 실제 계정에 맞게
# EMR_EC2_KEY_NAME = "test"
# EMR_EC2_ROLE = "EMR_EC2_DefaultRole"
# EMR_SERVICE_ROLE = "EMR_DefaultRole"
# SCRIPT_S3_KEY = "scripts/gmc_process_feeds.py"
# LOG_S3_PATH = f"s3://{S3_BUCKET}/emr-logs/"

# # ★ 경량 파드 executor_config (오토스케일 트리거)
# # ── imports ─────────────────────────────────────────────────────────
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
#                     name="base",  # pod_template 컨테이너명과 동일해야 함
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
#                             name="AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT",
#                             value="1800",
#                         )
#                     ],
#                 )
#             ],
#         ),
#     )
# }


# @dag(
#     dag_id="gmc_gmarket_auction_emr_process_and_rename",
#     start_date=pendulum.datetime(2025, 8, 29, tz="Asia/Seoul"),
#     schedule=None,
#     catchup=False,
#     tags=["emr", "gmc", "process", "feed", "gmarket"],
#     doc_md="GMC 피드 처리 후 S3 출력 파일명을 변경하고 클러스터를 종료하는 최종 DAG",
#     default_args={"owner": "airflow", "aws_conn_id": AWS_CONN_ID},
# )
# def emr_process_and_rename_final_dag():

#     @task
#     def create_emr_cluster(dag_run: DagRun):
#         market = dag_run.conf.get("market", "default_market")
#         cluster_name = f"process-{market}-feeds-cluster-final"

#         s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
#         creds = s3_hook.get_credentials()
#         client = boto3.client(
#             "emr",
#             aws_access_key_id=creds.access_key,
#             aws_secret_access_key=creds.secret_key,
#             aws_session_token=getattr(creds, "token", None),
#             region_name=AWS_REGION,  # ★ 고정
#         )
#         resp = client.run_job_flow(
#             Name=cluster_name,
#             ReleaseLabel=EMR_RELEASE,  # ★ 상수 사용
#             Applications=[{"Name": "Spark"}],
#             Instances={
#                 "InstanceGroups": [
#                     {
#                         "Name": "Master",
#                         "Market": "ON_DEMAND",
#                         "InstanceRole": "MASTER",
#                         "InstanceType": "m5.xlarge",
#                         "InstanceCount": 1,
#                     },
#                     {
#                         "Name": "Core",
#                         "Market": "ON_DEMAND",
#                         "InstanceRole": "CORE",
#                         "InstanceType": "m5.2xlarge",
#                         "InstanceCount": 8,
#                     },
#                 ],
#                 "Ec2KeyName": EMR_EC2_KEY_NAME,
#                 "KeepJobFlowAliveWhenNoSteps": True,
#                 "TerminationProtected": False,
#             },
#             JobFlowRole=EMR_EC2_ROLE,  # ★ 상수
#             ServiceRole=EMR_SERVICE_ROLE,  # ★ 상수
#             LogUri=LOG_S3_PATH,
#             VisibleToAllUsers=True,
#         )
#         return resp["JobFlowId"]

#     @task(multiple_outputs=True)
#     def submit_spark_job(cluster_id: str, dag_run: DagRun):
#         market = dag_run.conf.get("market", "default_market")
#         input_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC"
#         output_s3_path = f"s3://{S3_BUCKET}/feeds/google/{market}/GMC_processed_output"

#         s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
#         creds = s3_hook.get_credentials()
#         client = boto3.client(
#             "emr",
#             aws_access_key_id=creds.access_key,
#             aws_secret_access_key=creds.secret_key,
#             aws_session_token=getattr(creds, "token", None),
#             region_name=AWS_REGION,  # ★ 고정
#         )

#         spark_submit_args = [
#             "spark-submit",
#             "--deploy-mode",
#             "cluster",
#             f"s3://{S3_BUCKET}/{SCRIPT_S3_KEY}",
#             "--input-path",
#             input_s3_path,
#             "--output-path",
#             output_s3_path,
#         ]
#         resp = client.add_job_flow_steps(
#             JobFlowId=cluster_id,
#             Steps=[
#                 {
#                     "Name": "Process_GMC_Feeds",
#                     "ActionOnFailure": "CONTINUE",
#                     "HadoopJarStep": {
#                         "Jar": "command-runner.jar",
#                         "Args": spark_submit_args,
#                     },
#                 }
#             ],
#         )
#         return {
#             "step_id": resp["StepIds"][0],
#             "output_path": output_s3_path,
#             "market": market,
#         }

#     @task
#     def rename_output_files(output_s3_path: str, market: str):
#         # 파일명 변경 태스크
#         print(f"S3 출력 파일명 변경 시작: {output_s3_path}")
#         s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
#         parsed_url = urlparse(output_s3_path)
#         bucket_name = parsed_url.netloc

#         s3_client = s3_hook.get_conn()

#         prefix = parsed_url.path.lstrip("/") + "/"

#         paginator = s3_client.get_paginator("list_objects_v2")
#         # Spark가 생성한 source_filename=... 형태의 하위 폴더 목록 가져오기
#         for page in paginator.paginate(
#             Bucket=bucket_name, Prefix=prefix, Delimiter="/"
#         ):
#             for subdir in page.get("CommonPrefixes", []):
#                 subdir_prefix = subdir["Prefix"]

#                 # 각 하위 폴더 안의 gz 파일(part- 파일) 찾기
#                 for obj in s3_client.list_objects_v2(
#                     Bucket=bucket_name, Prefix=subdir_prefix
#                 ).get("Contents", []):
#                     source_key = obj["Key"]
#                     if "part-" in source_key and source_key.endswith(".gz"):
#                         original_filename = subdir_prefix.split("=")[1].rstrip("/")
#                         base_filename = original_filename.replace(".tsv.gz", "")
#                         new_filename = f"{market}_{base_filename}.txt.gz"
#                         new_key = os.path.join(prefix, new_filename)

#                         print(f"  - Renaming: {source_key} -> {new_key}")
#                         copy_source = {"Bucket": bucket_name, "Key": source_key}
#                         s3_client.copy_object(
#                             CopySource=copy_source, Bucket=bucket_name, Key=new_key
#                         )
#                         s3_client.delete_object(Bucket=bucket_name, Key=source_key)

#                         success_key = os.path.join(subdir_prefix, "_SUCCESS")
#                         try:
#                             s3_client.delete_object(Bucket=bucket_name, Key=success_key)
#                         except:
#                             pass

#         print(f"✅ 파일명 변경 및 정리 완료.")

#     # ===== DAG 플로우 =====
#     # cluster_id = create_emr_cluster().override(executor_config=EXECUTOR_CONFIG_LITE)()
#     cluster_id = create_emr_cluster.override(executor_config=EXECUTOR_CONFIG_LITE)()

#     wait_for_cluster = EmrJobFlowSensor(
#         task_id="wait_for_cluster",
#         job_flow_id=cluster_id,
#         target_states=["WAITING"],
#         failed_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],  # ★
#         poke_interval=30,  # ★
#         timeout=60 * 20,  # ★
#         mode="reschedule",  # ★
#         executor_config=EXECUTOR_CONFIG_LITE,
#     )

#     # spark_job_info = submit_spark_job(cluster_id).override(executor_config=EXECUTOR_CONFIG_LITE)()
#     spark_job_info = submit_spark_job.override(executor_config=EXECUTOR_CONFIG_LITE)(
#         cluster_id
#     )

#     wait_for_step = EmrStepSensor(
#         task_id="wait_for_step",
#         job_flow_id=cluster_id,
#         step_id=spark_job_info["step_id"],
#         target_states=["COMPLETED"],
#         failed_states=["FAILED", "CANCELLED"],  # ★
#         poke_interval=60,  # ★
#         timeout=60 * 60,  # ★
#         mode="reschedule",  # ★
#         executor_config=EXECUTOR_CONFIG_LITE,
#     )

#     # rename_files = rename_output_files(
#     #     spark_job_info["output_path"], market=spark_job_info["market"]
#     # ).override(executor_config=EXECUTOR_CONFIG_LITE)

#     rename_files = rename_output_files.override(executor_config=EXECUTOR_CONFIG_LITE)(
#         spark_job_info["output_path"], market=spark_job_info["market"]
#     )

#     terminate_cluster = EmrTerminateJobFlowOperator(
#         task_id="terminate_cluster",
#         job_flow_id=cluster_id,
#         trigger_rule="all_done",
#         executor_config=EXECUTOR_CONFIG_LITE,
#     )

#     # terminate를 rename와 병렬로 둘지, rename 이후로 둘지 선택
#     wait_for_cluster >> spark_job_info >> wait_for_step
#     wait_for_step >> [
#         rename_files,
#         terminate_cluster,
#     ]  # 필요시: wait_for_step >> rename_files >> terminate_cluster


# emr_process_and_rename_final_dag()
