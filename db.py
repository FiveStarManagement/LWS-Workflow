# db.py

import sqlite3
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional, Tuple
import pyodbc

from config import STATE_DB_PATH, ELIGIBLE_LOOKBACK_MINUTES
from logger import get_logger


log = get_logger("db")

# ---------- Radius Helpers ----------
def fetchall_dict(cur: pyodbc.Cursor) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    out = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))

        # Add lowercase aliases for all keys so code can use "JobCode" safely
        for k, v in list(d.items()):
            lk = str(k).lower()
            if lk not in d:
                d[lk] = v

        out.append(d)
    return out

def _table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, col_type: str) -> None:
    cols = _table_columns(cur, table)
    if column not in cols:
        log.info(f"DB MIGRATION: adding column {table}.{column} {col_type}")
        cur.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {col_type}')



def get_order_status(sordernum: int) -> Optional[str]:
    conn = state_conn()
    row = conn.execute(
        "SELECT status FROM lws_order_state WHERE sordernum=?",
        (sordernum,)
    ).fetchone()
    conn.close()
    return (row["status"] if row and row["status"] else None)

def is_order_complete(sordernum: int) -> bool:
    s = get_order_status(sordernum)
    return (s or "").strip().upper() == "COMPLETE"


def rquery(conn: pyodbc.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return fetchall_dict(cur)

def rexec(conn: pyodbc.Connection, sql: str, params: Tuple[Any, ...] = ()) -> int:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount

# ---------- Local State DB ----------
def state_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_state_db() -> None:
    conn = state_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS workflow_runs (
        run_id TEXT PRIMARY KEY,
        start_ts TEXT,
        end_ts TEXT,
        env TEXT,
        eligible_count INTEGER,
        processed_count INTEGER,
        failed_count INTEGER,
        log_file_path TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lws_order_state (
        sordernum INTEGER PRIMARY KEY,
        last_seen_ts TEXT,
        status TEXT,
        last_step TEXT,
        last_run_id TEXT,
        polytex_item_code TEXT,
        job_p4_code TEXT,
        po_p4_num INTEGER,
        so_p2_num INTEGER,
        shipreq_p2 TEXT,
        job_p2_code TEXT,
        last_error_summary TEXT,
        last_api_entity TEXT,
        last_api_status INTEGER,
        last_api_messages TEXT,
        updated_ts TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS run_orders (
        run_id TEXT,
        sordernum INTEGER,
        status TEXT,
        last_step TEXT,
        updated_ts TEXT,
        PRIMARY KEY (run_id, sordernum)
    )
    """)

        # ---- MIGRATIONS / SAFE UPGRADES ----
    _ensure_column(cur, "lws_order_state", "shipreq_p2", "TEXT")   # already in CREATE, but safe
    _ensure_column(cur, "workflow_runs", "held_count", "INTEGER")  # if you want to track holds
    
    conn.commit()
    conn.close()

    ensure_state_indexes()




def upsert_order_state(
    sordernum: int,
    status: str,
    last_step: str,
    last_run_id: Optional[str] = None,
    polytex_item_code: Optional[str] = None,
    job_p4: Optional[str] = None,
    po_p4: Optional[int] = None,
    so_p2: Optional[int] = None,
    shipreq_p2: Optional[str] = None, 
    job_p2: Optional[str] = None,
    last_error_summary: Optional[str] = None,
    last_api_entity: Optional[str] = None,
    last_api_status: Optional[int] = None,
    last_api_messages_json: Optional[str] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    conn = state_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO lws_order_state (
        sordernum, last_seen_ts, status, last_step,
        last_run_id, polytex_item_code,
        job_p4_code, po_p4_num, so_p2_num, shipreq_p2, job_p2_code,
        last_error_summary, last_api_entity, last_api_status, last_api_messages,
        updated_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sordernum) DO UPDATE SET
        last_seen_ts=excluded.last_seen_ts,
        status=excluded.status,
        last_step=excluded.last_step,
        last_run_id=COALESCE(excluded.last_run_id, lws_order_state.last_run_id),
        polytex_item_code=COALESCE(excluded.polytex_item_code, lws_order_state.polytex_item_code),
        job_p4_code=COALESCE(excluded.job_p4_code, lws_order_state.job_p4_code),
        po_p4_num=COALESCE(excluded.po_p4_num, lws_order_state.po_p4_num),
        so_p2_num=COALESCE(excluded.so_p2_num, lws_order_state.so_p2_num),
        shipreq_p2=COALESCE(excluded.shipreq_p2, lws_order_state.shipreq_p2),
        job_p2_code=COALESCE(excluded.job_p2_code, lws_order_state.job_p2_code),
        last_error_summary=excluded.last_error_summary,
        last_api_entity=excluded.last_api_entity,
        last_api_status=excluded.last_api_status,
        last_api_messages=excluded.last_api_messages,
        updated_ts=excluded.updated_ts
    """, (
        sordernum, now, status, last_step,
        last_run_id, polytex_item_code,
        job_p4, po_p4, so_p2, shipreq_p2, job_p2,
        last_error_summary, last_api_entity, last_api_status, last_api_messages_json,
        now
    ))



    conn.commit()
    conn.close()

def mark_run(run_id: str, start_ts: str, env: str, log_file_path: str) -> None:
    conn = state_conn()
    conn.execute("""
    INSERT INTO workflow_runs (run_id, start_ts, env, eligible_count, processed_count, failed_count, log_file_path)
    VALUES (?, ?, ?, 0, 0, 0, ?)
    """, (run_id, start_ts, env, log_file_path))
    conn.commit()
    conn.close()

def close_run(run_id: str, end_ts: str, eligible: int, processed: int, failed: int) -> None:
    conn = state_conn()
    conn.execute("""
    UPDATE workflow_runs
    SET end_ts=?, eligible_count=?, processed_count=?, failed_count=?
    WHERE run_id=?
    """, (end_ts, eligible, processed, failed, run_id))
    conn.commit()
    conn.close()

def mark_run_order(run_id: str, sordernum: int, status: str, last_step: str) -> None:
    now = datetime.utcnow().isoformat()
    conn = state_conn()
    conn.execute("""
    INSERT INTO run_orders (run_id, sordernum, status, last_step, updated_ts)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(run_id, sordernum) DO UPDATE SET
        status=excluded.status,
        last_step=excluded.last_step,
        updated_ts=excluded.updated_ts
    """, (run_id, sordernum, status, last_step, now))
    conn.commit()
    conn.close()

def last_run_start_ts() -> Optional[str]:
    conn = state_conn()
    row = conn.execute("SELECT start_ts FROM workflow_runs ORDER BY start_ts DESC LIMIT 1").fetchone()
    conn.close()
    return row["start_ts"] if row else None

def compute_eligibility_since() -> str:
    # ISO string used in SQL comparisons
    since = datetime.utcnow() - timedelta(minutes=ELIGIBLE_LOOKBACK_MINUTES)
    return since.strftime("%Y-%m-%d %H:%M:%S")

def execute(conn, sql: str, params: tuple = ()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount

def ensure_state_indexes() -> None:
    conn = state_conn()
    conn.executescript("""
    CREATE INDEX IF NOT EXISTS idx_run_orders_run_id ON run_orders(run_id);
    CREATE INDEX IF NOT EXISTS idx_run_orders_sordernum ON run_orders(sordernum);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_polytex_item_code
    ON lws_order_state(polytex_item_code);


    CREATE INDEX IF NOT EXISTS idx_lws_order_state_so_p2_num ON lws_order_state(so_p2_num);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_po_p4_num ON lws_order_state(po_p4_num);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_job_p4_code ON lws_order_state(job_p4_code);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_job_p2_code ON lws_order_state(job_p2_code);

    CREATE INDEX IF NOT EXISTS idx_workflow_runs_start_ts ON workflow_runs(start_ts);
    """)
    conn.commit()
    conn.close()

