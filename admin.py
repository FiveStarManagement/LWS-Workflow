from flask import Flask, render_template, redirect, url_for, request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import STATE_DB_PATH
from db import state_conn, init_state_db  # <-- shared DB module

app = Flask(__name__)

# IMPORTANT: waitress imports the module; it does NOT run __main__
# So we initialize schema + indexes at import time.
init_state_db()


def db():
    # Use shared state_conn() so admin + utility always behave the same
    return state_conn()


CT = ZoneInfo("America/Chicago")

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


# @app.template_filter("pretty_ts")
# def pretty_ts(value):
#     dt = _parse_ts(value)
#     if not dt:
#         return "—"
#     if dt.tzinfo is None:
#         dt = dt.replace(tzinfo=timezone.utc)
#     return dt.astimezone(timezone.utc).strftime("%b %d, %Y • %I:%M %p (UTC)")


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

    conn = db()

    if not q:
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

    conn.close()

    return render_template(
        "dashboard.html",
        runs=runs,
        q=q,
        mode=mode,
        match_count=match_count,
        db_path=STATE_DB_PATH,
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
            last_error_summary=NULL,
            last_api_messages=NULL
        WHERE sordernum=?
    """, (sordernum,))
    conn.commit()
    conn.close()
    return redirect(url_for("order_detail", sordernum=sordernum))


if __name__ == "__main__":
    # For local dev only. Waitress uses admin:app
    init_state_db()
    app.run(host="0.0.0.0", port=5050, debug=True)
