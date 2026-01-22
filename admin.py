#admin.py
from flask import Flask, render_template, redirect, url_for, request, flash
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import STATE_DB_PATH, get_readonly_conn
from db import state_conn, init_state_db, rquery, upsert_order_state


from config import get_readonly_conn
from db import rquery


app = Flask(__name__)
app.secret_key = "lws-admin-secret"  # can also use env var later

# IMPORTANT: waitress imports the module; it does NOT run __main__
# So we initialize schema + indexes at import time.
init_state_db()


def db():
    # Use shared state_conn() so admin + utility always behave the same
    return state_conn()


CT = ZoneInfo("America/Chicago")

VALID_LWS_SO_SQL = """
SELECT DISTINCT so."SOrderNum" AS SOrderNum
FROM "PUB"."PV_SOrder" so
JOIN "PUB"."PV_SOrderLine" sol
  ON so."CompNum" = sol."CompNum"
 AND so."PlantCode" = sol."PlantCode"
 AND so."SOrderNum" = sol."SOrderNum"
JOIN "PUB"."PM_Item" it
  ON it."CompNum" = so."CompNum"
 AND it."ItemCode" = sol."ItemCode"
WHERE so."CompNum" = 2
  AND so."PlantCode" = '4'
  AND so."SOSourceCode" = 'LWS'
  AND so."SOrderStat" IN (0,1,2)
  AND it."ProdGroupCode" = 'P4-LWS'
  AND so."SOrderNum" = ?
ORDER BY so."SOrderNum" DESC
"""



def _to_dt_utc(value):
    # value can be ISO string or datetime
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        # handles "2025-12-22T03:35:00+00:00" and "2025-12-22T03:35:00"
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def format_ct(value, fmt="%b %d, %Y · %I:%M %p"):
    dt_utc = _to_dt_utc(value)
    if not dt_utc:
        return ""
    return dt_utc.astimezone(CT).strftime(fmt)

app.jinja_env.filters["ct"] = format_ct

def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None




@app.template_filter("duration_s")
def duration_s(start_ts, end_ts):
    s = _parse_ts(start_ts)
    e = _parse_ts(end_ts)
    if not s or not e:
        return None
    try:
        return int((e - s).total_seconds())
    except Exception:
        return None


@app.route("/")
def dashboard():
    q = (request.args.get("q") or "").strip()
    mode = (request.args.get("mode") or "any").strip().lower()

    # ✅ Parse hide_empty correctly
    hide_vals = request.args.getlist("hide_empty")
    if not hide_vals:
        hide_empty = True
    else:
        hide_empty = "1" in hide_vals


    conn = db()
    # --------------------------------------------------------
    # ✅ Active Held Orders Widget (Phase2 HOLD + other HOLD)
    # --------------------------------------------------------
    held_orders = conn.execute("""
        SELECT
            sordernum,
            so_p2_num,
            po_p4_num,
            job_p4_code,
            job_p2_code,
            status,
            last_step,
            updated_ts,
            last_error_summary
        FROM lws_order_state
        WHERE status = 'HOLD'
        ORDER BY updated_ts DESC
        LIMIT 50
    """).fetchall()


    if not q:
        if hide_empty:
            runs = conn.execute("""
                SELECT *
                FROM workflow_runs
                WHERE COALESCE(eligible_count,0) > 0
                OR COALESCE(processed_count,0) > 0
                OR COALESCE(failed_count,0) > 0
                ORDER BY start_ts DESC
                LIMIT 25
            """).fetchall()
        else:
            runs = conn.execute(
                "SELECT * FROM workflow_runs ORDER BY start_ts DESC LIMIT 25"
            ).fetchall()
        match_count = None

    else:
        like = f"%{q}%"
        where_parts = []
        params = []

        def add_text(col):
            where_parts.append(f"{col} LIKE ?")
            params.append(like)

        def add_int(col):
            where_parts.append(f"CAST({col} AS TEXT) LIKE ?")
            params.append(like)

        # PolyTex SO = sordernum (INTEGER)
        if mode == "any":
            add_int("os.sordernum")
            add_int("os.so_p2_num")
            add_int("os.po_p4_num")
            add_text("os.job_p4_code")
            add_text("os.job_p2_code")
        elif mode == "polyso":
            add_int("os.sordernum")
        elif mode == "starso":
            add_int("os.so_p2_num")
        elif mode == "po":
            add_int("os.po_p4_num")
        elif mode == "job":
            add_text("os.job_p4_code")
            add_text("os.job_p2_code")
        else:
            add_int("os.sordernum")
            add_int("os.so_p2_num")
            add_int("os.po_p4_num")
            add_text("os.job_p4_code")
            add_text("os.job_p2_code")

        where_sql = " OR ".join(where_parts) if where_parts else "1=0"

        runs = conn.execute(f"""
            SELECT DISTINCT wr.*
            FROM workflow_runs wr
            JOIN run_orders ro ON ro.run_id = wr.run_id
            JOIN lws_order_state os ON os.sordernum = ro.sordernum
            WHERE ({where_sql})
            ORDER BY wr.start_ts DESC
            LIMIT 100
        """, params).fetchall()

        match_count = conn.execute(f"""
            SELECT COUNT(DISTINCT wr.run_id) AS cnt
            FROM workflow_runs wr
            JOIN run_orders ro ON ro.run_id = wr.run_id
            JOIN lws_order_state os ON os.sordernum = ro.sordernum
            WHERE ({where_sql})
        """, params).fetchone()["cnt"]

    # --------------------------------------------------------
    # ✅ Dashboard Insights Stats (Today + All-Time) — LWS
    # --------------------------------------------------------

    # Today totals (unique orders touched today)
    today_total_orders = conn.execute("""
        SELECT COUNT(DISTINCT sordernum) AS cnt
        FROM run_orders
        WHERE updated_ts >= datetime('now','start of day')
          AND updated_ts <  datetime('now','start of day','+1 day')
    """).fetchone()["cnt"] or 0

    today_processed_orders = conn.execute("""
        SELECT COUNT(DISTINCT sordernum) AS cnt
        FROM run_orders
        WHERE updated_ts >= datetime('now','start of day')
          AND updated_ts <  datetime('now','start of day','+1 day')
          AND status IN ('COMPLETE','SKIPPED','HOLD','FAILED')
    """).fetchone()["cnt"] or 0

    today_complete_orders = conn.execute("""
        SELECT COUNT(DISTINCT sordernum) AS cnt
        FROM run_orders
        WHERE updated_ts >= datetime('now','start of day')
          AND updated_ts <  datetime('now','start of day','+1 day')
          AND status = 'COMPLETE'
    """).fetchone()["cnt"] or 0

    # All-time totals (active + archived)
    total_orders_all_time = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM lws_order_state)
          + (SELECT COUNT(*) FROM lws_order_state_archive)
        AS cnt
    """).fetchone()["cnt"] or 0

    total_complete_all_time = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM lws_order_state WHERE UPPER(status)='COMPLETE')
          + (SELECT COUNT(*) FROM lws_order_state_archive WHERE UPPER(status)='COMPLETE')
        AS cnt
    """).fetchone()["cnt"] or 0

    total_hold_all_time = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM lws_order_state WHERE UPPER(status)='HOLD')
          + (SELECT COUNT(*) FROM lws_order_state_archive WHERE UPPER(status)='HOLD')
        AS cnt
    """).fetchone()["cnt"] or 0

    total_failed_all_time = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM lws_order_state WHERE UPPER(status)='FAILED')
          + (SELECT COUNT(*) FROM lws_order_state_archive WHERE UPPER(status)='FAILED')
        AS cnt
    """).fetchone()["cnt"] or 0

    conn.close()

    return render_template(
        "dashboard.html",
        runs=runs,
        held_orders=held_orders, 
        q=q,
        mode=mode,
        hide_empty=hide_empty,
        match_count=match_count,
        db_path=STATE_DB_PATH,

        # ✅ Insights (Today + All-Time)
        today_total_orders=today_total_orders,
        today_processed_orders=today_processed_orders,
        today_complete_orders=today_complete_orders,
        total_orders_all_time=total_orders_all_time,
        total_complete_all_time=total_complete_all_time,
        total_hold_all_time=total_hold_all_time,
        total_failed_all_time=total_failed_all_time,
    )




@app.route("/run/<run_id>")
def run_detail(run_id):
    conn = db()

    run = conn.execute(
        "SELECT * FROM workflow_runs WHERE run_id = ?",
        (run_id,)
    ).fetchone()

    orders = conn.execute("""
        SELECT
            ro.sordernum,
            os.so_p2_num,
            os.po_p4_num,
            os.shipreq_p2,
            os.job_p4_code,
            os.job_p2_code,
            COALESCE(ro.status, os.status) AS status,
            COALESCE(ro.last_step, os.last_step) AS last_step,
            COALESCE(ro.updated_ts, os.updated_ts) AS updated_ts,
            os.polytex_item_code
        FROM run_orders ro
        LEFT JOIN lws_order_state os
          ON os.sordernum = ro.sordernum
        WHERE ro.run_id = ?
        ORDER BY ro.sordernum DESC
    """, (run_id,)).fetchall()

    conn.close()
    return render_template("run_detail.html", run=run, orders=orders)


@app.route("/archived")
def archived_orders():
    conn = db()

    archived = conn.execute("""
        SELECT
            sordernum,
            so_p2_num,
            po_p4_num,
            job_p4_code,
            job_p2_code,
            status,
            last_step,
            updated_ts,
            archived_ts,
            last_error_summary
        FROM lws_order_state_archive
        ORDER BY archived_ts DESC
        LIMIT 500
    """).fetchall()

    conn.close()

    return render_template("archived.html", archived=archived)


@app.route("/order/<int:sordernum>")
def order_detail(sordernum):
    conn = db()
    order = conn.execute(
        "SELECT * FROM lws_order_state WHERE sordernum = ?",
        (sordernum,)
    ).fetchone()
    conn.close()
    return render_template("order_detail.html", order=order)





@app.route("/order/<int:sordernum>/retry")
def order_retry(sordernum):
    conn = db()
    conn.execute("""
        UPDATE lws_order_state
        SET status='NEW',
            last_step='ELIGIBLE',
            last_run_id=NULL,
            last_error_summary=NULL,

            -- API details
            last_api_entity=NULL,
            last_api_status=NULL,
            last_api_error_message=NULL,
            last_api_messages=NULL,
            last_api_raw=NULL,

            -- failure email dedupe
            last_failed_sig=NULL,
            last_failed_email_ts=NULL,

            -- hold aging
            hold_since_ts=NULL,
            last_hold_reminder_ts=NULL,
            hold_escalated_ts=NULL,

            updated_ts=datetime('now')
        WHERE sordernum=?
    """, (sordernum,))
    conn.commit()
    conn.close()
    return redirect(url_for("order_detail", sordernum=sordernum))



@app.route("/order/<int:sordernum>/remove", methods=["POST"])
def order_remove(sordernum):
    # 1) Always UPSERT into active state so REMOVED sticks
    upsert_order_state(
        sordernum=sordernum,
        status="REMOVED",
        last_step="REMOVED_BY_USER",
        last_error_summary="Removed by user",
        last_api_messages_json=None,
    )

    # 2) Also disable manual queue entry if your LWS schema has it
    # (this prevents "manual priority" from resurrecting it)
    conn = db()
    try:
        conn.execute("""
            UPDATE lws_manual_queue
            SET status='REMOVED'
            WHERE sordernum=?
        """, (sordernum,))
        conn.commit()
    except Exception:
        
        pass
    finally:
        conn.close()

    flash(
        "Removed from workflow. This order will not be auto-processed. Use Retry Next Run to resume.",
        "success"
    )
    return redirect(url_for("order_detail", sordernum=sordernum))



@app.route("/order/run_now", methods=["POST"])
def order_run_now():
    conn = db()

    so4 = request.form.get("so4", "").strip()
    if not so4.isdigit():
        conn.close()
        flash("Please enter a valid PolyTex SO number.", "warning")
        return redirect(url_for("dashboard"))

    so4 = int(so4)

    # ✅ Validate this is a real / valid LWS SO in Radius
    ro_conn = get_readonly_conn()
    try:
        rows = rquery(ro_conn, VALID_LWS_SO_SQL, (so4,))
    finally:
        ro_conn.close()

    if not rows:
        conn.close()
        flash(
            f"SO {so4} is not a valid LWS order (Plant 4 / Source=LWS / ProdGroup=P4-LWS).",
            "warning"
        )
        return redirect(url_for("dashboard"))

    # ✅ UPSERT into SQLite state so it works even if SO never ran before
    conn.close()

    upsert_order_state(
        sordernum=so4,
        status="NEW",
        last_step="ELIGIBLE",
        last_error_summary=None,
        last_api_messages_json=None,
    )

    flash(f"SO {so4} queued for next workflow run.", "success")
    return redirect(url_for("order_detail", sordernum=so4))




if __name__ == "__main__":
    # For local dev only. Waitress uses admin:app
    init_state_db()
    app.run(host="0.0.0.0", port=5050, debug=True)
