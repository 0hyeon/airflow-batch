"""
Airflow 로그 자동 정리 DAG

- NFS 로그 볼륨의 inode 소진 방지
- 14일 이전 로그 파일/디렉토리 자동 삭제
- 매일 03:00 KST 실행 (GMC DAG 04:00 전에 완료)

배경: 2026-09-28 ~ 10-02 NFS inode 100% 소진으로 GMC 피드 5일간 중단 사고 발생
문서: docs/gmc-airflow-nfs-inode-incident.md (terraform-provisioning 레포)
"""
import pendulum
import subprocess
from airflow.decorators import dag, task

LOG_RETENTION_DAYS = 14


@dag(
    dag_id="airflow_log_cleanup",
    description="NFS 로그 볼륨 inode 소진 방지 — 14일 이전 로그 자동 삭제",
    start_date=pendulum.datetime(2026, 10, 2, tz="Asia/Seoul"),
    schedule="0 3 * * *",  # 매일 03:00 KST
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
    },
    tags=["ops", "cleanup", "nfs"],
)
def airflow_log_cleanup():

    @task
    def cleanup_old_logs():
        """14일 이전 로그 파일/디렉토리 삭제."""
        # 삭제 전 inode 상태
        before = subprocess.run(["df", "-i", "/logs"], capture_output=True, text=True)
        print("=== 삭제 전 inode 상태 ===")
        print(before.stdout)

        # 14일 이전 파일/디렉토리 삭제
        result = subprocess.run(
            [
                "find", "/logs",
                "-mindepth", "1",
                "-mtime", f"+{LOG_RETENTION_DAYS}",
                "-delete",
            ],
            capture_output=True, text=True, timeout=1800,
        )
        if result.returncode != 0:
            print(f"[WARN] find 명령 일부 실패 (반환 코드 {result.returncode})")
            print(f"stderr: {result.stderr[:500]}")

        # 삭제 후 inode 상태
        after = subprocess.run(["df", "-i", "/logs"], capture_output=True, text=True)
        print("=== 삭제 후 inode 상태 ===")
        print(after.stdout)

        # 사용률 체크 — 80% 넘으면 FAIL (UI에서 보이도록)
        import re
        m = re.search(r"\s(\d+)%", after.stdout.splitlines()[-1])
        if m:
            usage = int(m.group(1))
            print(f"[INFO] inode 사용률: {usage}%")
            if usage > 80:
                raise RuntimeError(
                    f"🚨 로그 정리 후에도 inode 사용률 {usage}% — "
                    f"수동 확인 필요. 14일 이내 로그가 과도하게 쌓이고 있음."
                )

    cleanup_old_logs()


airflow_log_cleanup()
