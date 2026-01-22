# scripts/daily_hold_summary_email.py
import os, sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from db import state_conn
from emailer import send_email
from config import FULFILLMENT_EMAILS, STARPAK_EMAILS
from logger import get_logger
log = get_logger("daily_hold_summary")

CT = ZoneInfo("America/Chicago")


def fetch_active_hold_orders():
    conn = state_conn()
    rows = conn.execute("""
        SELECT
            sordernum,
            so_p2_num,
            po_p4_num,
            job_p4_code,
            job_p2_code,
            last_step,
            updated_ts,
            last_error_summary
        FROM lws_order_state
        WHERE status = 'HOLD'
          AND (
                last_step = 'SO4_QTY_CHANGED_WAIT_RECONFIRM'
                OR last_step LIKE 'P2_%'
          )
        ORDER BY updated_ts DESC
    """).fetchall()
    conn.close()
    return rows


def build_html(rows):
    today_ct = datetime.now(CT).strftime("%b %d, %Y %I:%M %p (CT)")

    polytex = [r for r in rows if r["last_step"] == "SO4_QTY_CHANGED_WAIT_RECONFIRM"]
    starpak = [r for r in rows if r["last_step"].startswith("P2_")]

    def table_block(title, items):
        if not items:
            return f"""
            <h3 style="margin-top:20px;">{title}</h3>
            <p style="color:#4caf50;"><b>✅ No active holds.</b></p>
            """

        rows_html = ""
        for r in items:
            rows_html += f"""
            <tr>
              <td style="padding:8px;border:1px solid #ddd;">{r['sordernum']}</td>
              <td style="padding:8px;border:1px solid #ddd;">{r['so_p2_num'] or "—"}</td>
              <td style="padding:8px;border:1px solid #ddd;">{r['po_p4_num'] or "—"}</td>
              <td style="padding:8px;border:1px solid #ddd;">{r['job_p4_code'] or "—"}</td>
              <td style="padding:8px;border:1px solid #ddd;">{r['job_p2_code'] or "—"}</td>
              <td style="padding:8px;border:1px solid #ddd;"><b>{r['last_step']}</b></td>
              <td style="padding:8px;border:1px solid #ddd;">{str(r['updated_ts'])}</td>
            </tr>
            """

        return f"""
        <h3 style="margin-top:20px;">{title} ({len(items)})</h3>
        <table style="border-collapse:collapse;width:100%;font-size:14px;margin-top:8px;">
          <thead>
            <tr style="background:#f5f5f5;">
              <th style="padding:8px;border:1px solid #ddd;">PolyTex SO</th>
              <th style="padding:8px;border:1px solid #ddd;">StarPak SO</th>
              <th style="padding:8px;border:1px solid #ddd;">PO</th>
              <th style="padding:8px;border:1px solid #ddd;">Job P4</th>
              <th style="padding:8px;border:1px solid #ddd;">Job P2</th>
              <th style="padding:8px;border:1px solid #ddd;">Hold Reason</th>
              <th style="padding:8px;border:1px solid #ddd;">Last Updated</th>
            </tr>
          </thead>
          <tbody>
            {rows_html}
          </tbody>
        </table>
        """

    html = f"""
    <div style="font-family:Segoe UI,Arial,sans-serif;max-width:900px;margin:auto;">
      <h2 style="color:#0b57d0;">📌 LWS Daily HOLD Summary</h2>
      <p style="color:#333;">Below is the current list of LWS orders still waiting for manual reconfirmation or completion.</p>
      <p><b>Generated:</b> {today_ct}</p>

      {table_block("✅ PolyTex – Waiting for Job Reconfirm", polytex)}
      {table_block("✅ StarPak – Waiting for Job Reconfirm + COMPLETE", starpak)}

      <hr style="margin-top:25px;">
      <p style="font-size:13px;color:#666;">
        This email is automatically generated daily at 8:00 PM CT by the LWS Workflow Monitor.
      </p>
    </div>
    """

    return html, len(polytex), len(starpak)


def main():
    rows = fetch_active_hold_orders()

    # ✅ NEW: Only send email if there are HOLD orders
    if not rows:
        log.info("No HOLD orders found. Daily summary email not sent.")
        return

    html, poly_cnt, sp_cnt = build_html(rows)

    subject = f"LWS Daily HOLD Summary – PolyTex={poly_cnt}, StarPak={sp_cnt}"

    recipients = list(set(FULFILLMENT_EMAILS + STARPAK_EMAILS))

    send_email(recipients, subject, html)
    log.info(f"Sent daily summary email to {recipients}")



if __name__ == "__main__":
    main()
