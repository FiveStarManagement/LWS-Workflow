# services/hold_reminder.py

from datetime import datetime, timezone
from db import get_active_hold_orders_for_reminder, state_conn
from emailer import send_email
from config import CSR_EMAILS, ADMIN_EMAILS
from logger import get_logger

log = get_logger("hold_reminder")


def _parse_iso(ts: str):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        # ✅ FIX: force timezone if missing (naive datetime)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt
    except Exception:
        return None



def send_hold_reminders_if_needed():
    """
    ✅ Sends reminder email if HOLD order > 2 days
    ✅ Escalates to admin if HOLD order > 5 days
    ✅ Sends only ONCE per day (per order)
    ✅ Returns counts: (reminder_count, escalated_count)
    """
    rows = get_active_hold_orders_for_reminder(limit=500)
    if not rows:
        return 0, 0

    now = datetime.now(timezone.utc)

    reminder_orders = []
    escalate_orders = []

    conn = state_conn()
    cur = conn.cursor()

    for r in rows:
        so = r["sordernum"]
        hold_since = _parse_iso(r["hold_since_ts"])
        last_reminder = _parse_iso(r["last_hold_reminder_ts"])
        escalated = _parse_iso(r["hold_escalated_ts"])

        if not hold_since:
            continue

        age_days = (now - hold_since).total_seconds() / 86400

        # ✅ Reminder threshold: 2 days (but only once per day)
        if age_days >= 2:
            if not last_reminder or (now - last_reminder).total_seconds() > 86400:
                reminder_orders.append((so, age_days, r["last_step"]))

                # update reminder timestamp
                cur.execute("""
                    UPDATE lws_order_state
                    SET last_hold_reminder_ts = ?
                    WHERE sordernum = ?
                """, (now.isoformat(), so))

        # ✅ Escalation threshold: 5 days (only once ever)
        if age_days >= 5 and not escalated:
            escalate_orders.append((so, age_days, r["last_step"]))

            # update escalated timestamp
            cur.execute("""
                UPDATE lws_order_state
                SET hold_escalated_ts = ?
                WHERE sordernum = ?
            """, (now.isoformat(), so))

    conn.commit()
    conn.close()

    # ✅ Send reminder email to CSR team if needed
    if reminder_orders:
        rows_html = "".join(
            f"<tr><td style='padding:8px;border:1px solid #ddd;'>{so}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;'>{step}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;'>{age:.1f} days</td></tr>"
            for so, age, step in reminder_orders
        )

        html = f"""
        <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px;">
          <h2 style="margin:0 0 10px;">⏳ HOLD Reminder — Orders Waiting for Reconfirmation</h2>
          <p>The following orders have been on HOLD for more than <b>2 days</b>:</p>
          <table style="border-collapse:collapse;width:100%; font-size:14px;">
            <tr style="background:#f3f4f6;">
              <th style="padding:8px;border:1px solid #ddd;">PolyTex SO</th>
              <th style="padding:8px;border:1px solid #ddd;">HOLD Step</th>
              <th style="padding:8px;border:1px solid #ddd;">Held For</th>
            </tr>
            {rows_html}
          </table>
          <p style="margin-top:12px;color:#555;">
            Please reconfirm these orders so the workflow can continue.
          </p>
        </div>
        """

        send_email(CSR_EMAILS, "LWS Workflow HOLD Reminder — Action Needed", html)
        log.info(f"Sent HOLD reminder email for {len(reminder_orders)} orders.")

    # ✅ Send escalation email to admins if needed
    if escalate_orders:
        rows_html = "".join(
            f"<tr><td style='padding:8px;border:1px solid #ddd;'>{so}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;'>{step}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;'>{age:.1f} days</td></tr>"
            for so, age, step in escalate_orders
        )

        html = f"""
        <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px;">
          <h2 style="margin:0 0 10px;">🚨 HOLD Escalation — Orders Held > 5 Days</h2>
          <p>The following orders have been on HOLD for more than <b>5 days</b>:</p>
          <table style="border-collapse:collapse;width:100%; font-size:14px;">
            <tr style="background:#fee2e2;">
              <th style="padding:8px;border:1px solid #ddd;">PolyTex SO</th>
              <th style="padding:8px;border:1px solid #ddd;">HOLD Step</th>
              <th style="padding:8px;border:1px solid #ddd;">Held For</th>
            </tr>
            {rows_html}
          </table>
          <p style="margin-top:12px;color:#555;">
            Please investigate and resolve these orders to prevent workflow backlog.
          </p>
        </div>
        """

        send_email(ADMIN_EMAILS, "🚨 LWS Workflow HOLD Escalation — Held > 5 Days", html)
        log.info(f"Sent HOLD escalation email for {len(escalate_orders)} orders.")

    return len(reminder_orders), len(escalate_orders)
