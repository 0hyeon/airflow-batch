"""
Trip 어뷰징 감지 알람 DAG

30분마다 reward.trip_postback_received를 체크해서
동일 click_key에서 30분 내 한화 500만원 이상 취소가 3건 이상이면
상세 테이블 HTML 메일을 발송한다.
"""

import email.mime.multipart
import email.mime.text
import os
import smtplib
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

FX_TO_KRW = {
    "CNY": 193.0, "JPY": 9.3,   "TWD": 43.5,  "THB": 39.0,
    "USD": 1380.0,"EUR": 1500.0, "GBP": 1770.0,"HKD": 177.0,
    "SGD": 1030.0,"MYR": 315.0,  "KRW": 1.0,   "VND": 0.055,
    "RUB": 14.5,  "IDR": 0.088,  "BRL": 240.0, "CAD": 1000.0,
    "AUD": 900.0, "INR": 16.5,   "PHP": 24.0,  "KZT": 2.9,
    "AED": 376.0, "PLN": 335.0,  "TRY": 40.0,
}

CANCEL_STATUSES = (
    "HOTEL_CANCELLED", "FLIGHT_CANCELLED", "FLIGHT_UNSUBCRIBE_ALL",
    "TRAIN_CANCELLED", "CAR_CANCELLED", "ATTRACTION_CANCELLED",
)

QUERY = """
SELECT
    t.click_key,
    t.sid,
    t.order_id,
    t.order_status_id,
    t.order_amount,
    t.currency,
    t.order_date,
    t.order_type,
    t.media_code AS chnm,
    t.ingested_at
FROM reward.trip_postback_received t
JOIN reward.trip_fx_rate f ON t.currency = f.currency
WHERE t.order_status_id = ANY(%s)
  AND t.ingested_at > NOW() - INTERVAL '30 minutes'
  AND t.order_amount * f.rate_to_krw >= 5000000
  AND t.click_key IN (
    SELECT t2.click_key
    FROM reward.trip_postback_received t2
    JOIN reward.trip_fx_rate f2 ON t2.currency = f2.currency
    WHERE t2.order_status_id = ANY(%s)
      AND t2.ingested_at > NOW() - INTERVAL '30 minutes'
      AND t2.order_amount * f2.rate_to_krw >= 5000000
    GROUP BY t2.click_key
    HAVING COUNT(*) >= 3
  )
ORDER BY t.click_key, t.ingested_at DESC
"""


def _build_html(rows: list[dict], now_kst: datetime) -> tuple[str, str]:
    total_krw = sum(
        int(float(r["order_amount"]) * FX_TO_KRW.get(r["currency"], 0))
        for r in rows
    )
    subject = (
        f"[Trip 어뷰징 감지] 동일 클릭키 고가 취소 {len(rows)}건 발생 — "
        f"즉시 확인 필요 ({now_kst.strftime('%Y-%m-%d %H:%M')} KST)"
    )

    rows_html = ""
    for i, r in enumerate(rows, 1):
        amt = float(r["order_amount"])
        krw = int(amt * FX_TO_KRW.get(r["currency"], 0))
        ingested = str(r["ingested_at"])[:19]
        order_date = str(r["order_date"])[:10]
        click_short = r["click_key"][:40] + ("..." if len(r["click_key"]) > 40 else "")
        bg = "#fff" if i % 2 == 0 else "#f9f9f9"
        rows_html += f"""
        <tr style="background:{bg}">
          <td style="padding:8px;border:1px solid #ddd">{r['order_id']}</td>
          <td style="padding:8px;border:1px solid #ddd">{r['chnm']}</td>
          <td style="padding:8px;border:1px solid #ddd">{r['sid']}</td>
          <td style="padding:8px;border:1px solid #ddd">{r['order_status_id']}</td>
          <td style="padding:8px;border:1px solid #ddd">{r['order_type']}</td>
          <td style="padding:8px;border:1px solid #ddd">{r['currency']}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right">{amt:,.0f}</td>
          <td style="padding:8px;border:1px solid #ddd;text-align:right;font-weight:bold">{krw:,}원</td>
          <td style="padding:8px;border:1px solid #ddd">{order_date}</td>
          <td style="padding:8px;border:1px solid #ddd">{ingested}</td>
          <td style="padding:8px;border:1px solid #ddd;font-size:11px;color:#666">{click_short}</td>
        </tr>"""

    html = f"""
<html><body style="font-family:Arial,sans-serif;font-size:13px">
<h2 style="color:#c0392b">Trip 어뷰징 감지 — 동일 클릭키 30분 내 500만원+ 취소 {len(rows)}건</h2>
<p>감지 시각: <b>{now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST</b></p>
<p>취소 합계 (한화 환산): <b style="color:#c0392b">{total_krw:,}원</b></p>
<table style="border-collapse:collapse;width:100%;margin-top:12px">
  <thead>
    <tr style="background:#2c3e50;color:#fff">
      <th style="padding:8px;border:1px solid #ddd">orderid</th>
      <th style="padding:8px;border:1px solid #ddd">매체</th>
      <th style="padding:8px;border:1px solid #ddd">SID</th>
      <th style="padding:8px;border:1px solid #ddd">status</th>
      <th style="padding:8px;border:1px solid #ddd">ordertype</th>
      <th style="padding:8px;border:1px solid #ddd">통화</th>
      <th style="padding:8px;border:1px solid #ddd">금액</th>
      <th style="padding:8px;border:1px solid #ddd">한화</th>
      <th style="padding:8px;border:1px solid #ddd">결제일</th>
      <th style="padding:8px;border:1px solid #ddd">취소일시(UTC)</th>
      <th style="padding:8px;border:1px solid #ddd">click_key</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
<p style="margin-top:16px;color:#666;font-size:12px">
  * 환율 기준: 월 고정환율 적용<br>
  * 상세 조회: <a href="https://grafana.stcps.com">https://grafana.stcps.com</a>
</p>
</body></html>"""

    return subject, html


@dag(
    dag_id="trip_abuse_alert",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 11, 9, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["trip", "alert", "abuse"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def trip_abuse_alert():

    @task
    def check_and_alert():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = hook.get_records(QUERY, parameters=[list(CANCEL_STATUSES), list(CANCEL_STATUSES)])

        if not rows:
            print("이상 없음 — 조건 미충족")
            return

        col_names = ["click_key", "sid", "order_id", "order_status_id",
                     "order_amount", "currency", "order_date", "order_type",
                     "chnm", "ingested_at"]
        row_dicts = [dict(zip(col_names, r)) for r in rows]

        now_kst = datetime.now(timezone.utc) + timedelta(hours=9)
        subject, html = _build_html(row_dicts, now_kst)

        sender = os.environ["AIRFLOW_VAR_ALERT_EMAIL_SENDER"]
        password = os.environ["AIRFLOW_VAR_ALERT_EMAIL_PASSWORD"]
        recipients = os.environ["AIRFLOW_VAR_ALERT_EMAIL_RECIPIENTS"]

        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = recipients
        msg.attach(email.mime.text.MIMEText(html, "html", "utf-8"))

        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.ehlo()
            s.starttls()
            s.login(sender, password)
            s.sendmail(sender, recipients.split(","), msg.as_string())

        print(f"메일 발송 완료 — {len(row_dicts)}건 감지, 수신: {recipients}")

    check_and_alert()


trip_abuse_alert()
