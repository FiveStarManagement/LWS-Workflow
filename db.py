# db.py

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, List, Dict, Optional, Tuple
import json
import pyodbc

from config import STATE_DB_PATH, ELIGIBLE_LOOKBACK_MINUTES
from logger import get_logger

log = get_logger("db")


# ============================================================
# SQLite Row helper (works for sqlite3.Row, dict, tuple)
# ============================================================
def sget(row: Any, key: str, default: Any = None) -> Any:
    """
    Safe getter for sqlite3.Row / dict / tuple-like rows.
    - sqlite3.Row: supports row["col"] and row.keys()
    - dict: row.get(key)
    - tuple: fallback by index if key is numeric string
    """
    if row is None:
        return default

    # dict
    if isinstance(row, dict):
        return row.get(key, default)

    # sqlite3.Row
    try:
        if hasattr(row, "keys") and key in row.keys():
            v = row[key]
            return default if v is None else v
    except Exception:
        pass

    # attribute fallback
    try:
        v = getattr(row, key)
        return default if v is None else v
    except Exception:
        pass

    # tuple fallback (rare)
    try:
        if isinstance(key, str) and key.isdigit():
            idx = int(key)
            return row[idx]
    except Exception:
        pass

    return default


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

def get_order_state_row(sordernum: int) -> Optional[sqlite3.Row]:
    conn = state_conn()
    row = conn.execute(
        "SELECT * FROM lws_order_state WHERE sordernum=?",
        (int(sordernum),)
    ).fetchone()
    conn.close()
    return row


def mark_failure_email_sent(sordernum: int, sig: str) -> None:
    conn = state_conn()
    conn.execute("""
        UPDATE lws_order_state
           SET last_failed_sig = ?,
               last_failed_email_ts = datetime('now'),
               updated_ts = datetime('now')
         WHERE sordernum = ?
    """, (str(sig), int(sordernum)))
    conn.commit()
    conn.close()




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


def get_orders_to_monitor(limit: int = 200):
    conn = state_conn()
    rows = conn.execute("""
        SELECT sordernum
        FROM lws_order_state
        WHERE status IN ('COMPLETE','HOLD')
          AND UPPER(COALESCE(status,'')) != 'REMOVED'
          AND (
                last_step LIKE 'SO4_QTY_CHANGED_%'
             OR last_step LIKE 'P2_%'
             OR last_step='COMPLETE'
             OR last_step='SO4_CUSTREF_UPDATED_STARPAK'
          )
        ORDER BY updated_ts DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [r["sordernum"] for r in rows]


# Remove orders helpers
def is_order_removed(sordernum: int) -> bool:
    s = get_order_status(sordernum)
    return (s or "").strip().upper() == "REMOVED"


def get_removed_orders_set(limit: int = 50000) -> set[int]:
    """
    Returns a set of sordernum values marked REMOVED in active state table.
    """
    conn = state_conn()
    rows = conn.execute("""
        SELECT sordernum
        FROM lws_order_state
        WHERE UPPER(status)='REMOVED'
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return {int(r["sordernum"]) for r in rows}


def filter_out_removed(order_nums: list[int]) -> list[int]:
    """
    Given a list of SO numbers, remove any that are marked REMOVED in SQLite.
    """
    if not order_nums:
        return []
    removed = get_removed_orders_set()
    return [int(x) for x in order_nums if int(x) not in removed]



def archive_old_complete_orders(days: int = 14) -> int:
    """
    ✅ Archive COMPLETE orders older than N days into lws_order_state_archive,
    then remove them from lws_order_state to keep monitoring small.
    Does NOT touch HOLD / FAILED.
    """

    conn = state_conn()
    cur = conn.cursor()

    # 1) Insert into archive (ignore if already archived)
    cur.execute("""
        INSERT OR IGNORE INTO lws_order_state_archive (
            sordernum, last_seen_ts, status, last_step, last_run_id,
            polytex_item_code, job_p4_code, po_p4_num, so_p2_num,
            shipreq_p2, job_p2_code,
            last_error_summary, last_api_entity, last_api_status, last_api_error_message, last_api_messages,
            last_api_raw,
            updated_ts, archived_ts
        )
        SELECT
            sordernum, last_seen_ts, status, last_step, last_run_id,
            polytex_item_code, job_p4_code, po_p4_num, so_p2_num,
            shipreq_p2, job_p2_code,
            last_error_summary, last_api_entity, last_api_status, last_api_error_message, last_api_messages,
            last_api_raw,
            updated_ts, datetime('now')
        FROM lws_order_state
        WHERE status = 'COMPLETE'
          AND updated_ts < datetime('now', ?)
    """, (f"-{int(days)} days",))


    archived = cur.rowcount

    # 2) Remove from active table
    cur.execute("""
        DELETE FROM lws_order_state
        WHERE status = 'COMPLETE'
          AND updated_ts < datetime('now', ?)
    """, (f"-{int(days)} days",))

    conn.commit()
    conn.close()
    return archived


def purge_old_run_history(sqlite_conn: sqlite3.Connection, days_old: int = 90) -> dict:
    """
    ✅ Delete old run history rows to keep SQLite small and fast.
    Removes run_orders + workflow_runs older than N days.
    """
    cur = sqlite_conn.cursor()

    ro = cur.execute("""
        DELETE FROM run_orders
        WHERE updated_ts IS NOT NULL
          AND updated_ts != ''
          AND updated_ts <= datetime('now', ?)
    """, (f"-{int(days_old)} days",)).rowcount

    wr = cur.execute("""
        DELETE FROM workflow_runs
        WHERE start_ts IS NOT NULL
          AND start_ts != ''
          AND start_ts <= datetime('now', ?)
    """, (f"-{int(days_old)} days",)).rowcount

    sqlite_conn.commit()
    return {"run_orders_deleted": ro, "workflow_runs_deleted": wr}



# ---------- Local State DB ----------
def state_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ----------------------------
# Phase 2 helper: UTC timestamp
# ----------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        custref_p4 TEXT,
        last_error_summary TEXT,
        last_api_entity TEXT,
        last_api_status INTEGER,
        last_api_error_message TEXT,
        last_api_messages TEXT,
        last_api_raw TEXT,
        updated_ts TEXT
    )
    """)

    # ============================================================
    # ✅ ARCHIVE TABLE (keeps old COMPLETE orders out of active state)
    # ============================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lws_order_state_archive (
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
        last_api_error_message TEXT,
        last_api_messages TEXT,
        last_api_raw TEXT,
        updated_ts TEXT,
        archived_ts TEXT
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
    _ensure_column(cur, "lws_order_state", "shipreq_p2", "TEXT")
    _ensure_column(cur, "workflow_runs", "held_count", "INTEGER")
    _ensure_column(cur, "lws_order_state", "custref_p4", "TEXT")
    _ensure_column(cur, "lws_order_state", "last_failed_sig", "TEXT")
    _ensure_column(cur, "lws_order_state", "last_failed_email_ts", "TEXT")
    _ensure_column(cur, "lws_order_state", "last_api_error_message", "TEXT")
    _ensure_column(cur, "lws_order_state", "last_api_raw", "TEXT")
    _ensure_column(cur, "lws_order_state_archive", "last_api_error_message", "TEXT")
    _ensure_column(cur, "lws_order_state_archive", "last_api_raw", "TEXT")

    # ============================================================
    # ✅ Printed Film mismatch email de-dupe (once per signature)
    # ============================================================
    _ensure_column(cur, "lws_order_state", "printed_film_mismatch_sig", "TEXT")
    _ensure_column(cur, "lws_order_state", "printed_film_mismatch_sent_ts", "TEXT")

    # ============================================================
    # ✅ HOLD Aging columns (SQL Server friendly: plain TEXT timestamps)
    # ============================================================
    _ensure_column(cur, "lws_order_state", "hold_since_ts", "TEXT")
    _ensure_column(cur, "lws_order_state", "last_hold_reminder_ts", "TEXT")
    _ensure_column(cur, "lws_order_state", "hold_escalated_ts", "TEXT")

    # ============================================================
    # PHASE 2 TABLES (SAFE / ADDITIVE)
    # ============================================================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS so4_line_snapshot (
        so4_sordernum INTEGER NOT NULL,
        so4_linenum   INTEGER NOT NULL,
        itemcode      TEXT,
        orderedqty    REAL,
        reqdate       TEXT,
        updated_ts    TEXT NOT NULL,
        PRIMARY KEY (so4_sordernum, so4_linenum)
    )
    """)

    # -----------------------------
    # Phase2 Header Snapshot (CustRef tracking)
    # -----------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS so4_header_snapshot (
        so4_sordernum INTEGER PRIMARY KEY,
        custref       TEXT,
        updated_ts    TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS req_snapshot (
        requirement_id INTEGER PRIMARY KEY,
        jobcode         TEXT,
        requiredqty     REAL,
        requireddate    TEXT,
        updated_ts      TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS so4_to_po_map (
        so4_sordernum INTEGER NOT NULL,
        so4_linenum   INTEGER NOT NULL,
        po_num        INTEGER NOT NULL,
        po_linenum    INTEGER NOT NULL,
        created_ts    TEXT NOT NULL,
        PRIMARY KEY (so4_sordernum, so4_linenum)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_change_log (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id        TEXT,
        so4_sordernum INTEGER,
        so4_linenum   INTEGER,
        change_type   TEXT NOT NULL,
        old_value     TEXT,
        new_value     TEXT,
        details_json  TEXT,
        created_ts    TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS req_snapshot_keyed (
        jobcode      TEXT NOT NULL,
        reqgroupcode TEXT NOT NULL,
        itemcode     TEXT NOT NULL,
        requiredqty  REAL,
        requireddate TEXT,
        updated_ts   TEXT NOT NULL,
        PRIMARY KEY (jobcode, reqgroupcode, itemcode)
    )
    """)

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
    custref_p4: Optional[str] = None,
    last_error_summary: Optional[str] = None,
    last_api_entity: Optional[str] = None,
    last_api_status: Optional[int] = None,
    last_api_messages_json: Optional[str] = None,
    last_api_error_message: Optional[str] = None,
    last_api_raw: Optional[str] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    conn = state_conn()
    cur = conn.cursor()

    # ============================================================
    # ✅ HOLD Aging tracking (portable / SQL Server friendly)
    # - When status becomes HOLD: set hold_since_ts only once
    # - When leaving HOLD: clear hold_since_ts and reminder/escalation timestamps
    # ============================================================
    if str(status).upper() == "HOLD":
        cur.execute("""
            UPDATE lws_order_state
               SET hold_since_ts = COALESCE(hold_since_ts, ?)
             WHERE sordernum = ?
        """, (now, sordernum))
    else:
        cur.execute("""
            UPDATE lws_order_state
               SET hold_since_ts = NULL,
                   last_hold_reminder_ts = NULL,
                   hold_escalated_ts = NULL
             WHERE sordernum = ?
        """, (sordernum,))

    cur.execute("""
    INSERT INTO lws_order_state (
        sordernum, last_seen_ts, status, last_step,
        last_run_id, polytex_item_code,
        job_p4_code, po_p4_num, so_p2_num, shipreq_p2, job_p2_code, custref_p4,
        last_error_summary,
        last_api_entity, last_api_status, last_api_error_message, last_api_messages, last_api_raw,
        updated_ts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        custref_p4=COALESCE(excluded.custref_p4, lws_order_state.custref_p4),

        last_error_summary=excluded.last_error_summary,
        last_api_entity=excluded.last_api_entity,
        last_api_status=excluded.last_api_status,
        last_api_error_message=excluded.last_api_error_message,
        last_api_messages=excluded.last_api_messages,
        last_api_raw=excluded.last_api_raw,

        updated_ts=excluded.updated_ts
    """, (
        sordernum, now, status, last_step,
        last_run_id, polytex_item_code,
        job_p4, po_p4, so_p2, shipreq_p2, job_p2, custref_p4,
        last_error_summary,
        last_api_entity, last_api_status, last_api_error_message, last_api_messages_json, last_api_raw,
        now
    ))

    conn.commit()
    conn.close()



def get_printed_film_mismatch_sig(conn, sordernum: int):
    """
    Returns (sig, sent_ts) or (None, None)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT printed_film_mismatch_sig, printed_film_mismatch_sent_ts
          FROM lws_order_state
         WHERE sordernum = ?
        """,
        (int(sordernum),),
    )
    row = cur.fetchone()
    if not row:
        return None, None

    # sqlite3.Row supports dict-like access
    try:
        return row["printed_film_mismatch_sig"], row["printed_film_mismatch_sent_ts"]
    except Exception:
        # fallback tuple indexing
        return row[0], row[1]


def set_printed_film_mismatch_sig(conn, sordernum: int, sig: str, sent_ts: str):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE lws_order_state
           SET printed_film_mismatch_sig = ?,
               printed_film_mismatch_sent_ts = ?
         WHERE sordernum = ?
        """,
        (str(sig), str(sent_ts), int(sordernum)),
    )
    conn.commit()


# ============================================================
# ✅ NEW helper (used by reminder/escalation logic)
# ============================================================
def get_active_hold_orders_for_reminder(limit: int = 500):
    conn = state_conn()
    rows = conn.execute("""
        SELECT
            sordernum,
            so_p2_num,
            po_p4_num,
            job_p4_code,
            job_p2_code,
            last_step,
            hold_since_ts,
            last_hold_reminder_ts,
            hold_escalated_ts
        FROM lws_order_state
        WHERE status='HOLD'
          AND hold_since_ts IS NOT NULL
        ORDER BY hold_since_ts ASC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return rows


def get_phase2_held_orders(limit=1000):
    """
    Block ONLY P2-related holds from re-processing in Phase1.
    Do NOT block SO4_QTY_CHANGED_WAIT_RECONFIRM, because Phase2B needs to run
    after PolyTex reconfirm updates PV_Req.
    """
    conn = state_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT sordernum
        FROM lws_order_state
        WHERE status = 'HOLD'
          AND (last_step LIKE 'P2_%')
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return {int(r[0]) for r in rows}


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
    since = datetime.utcnow() - timedelta(minutes=ELIGIBLE_LOOKBACK_MINUTES)
    return since.strftime("%Y-%m-%d %H:%M:%S")


def execute(conn, sql: str, params: tuple = ()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount


def ensure_state_indexes() -> None:
    conn = state_conn()
    conn.executescript("""
    -- Core run history indexes
    CREATE INDEX IF NOT EXISTS idx_run_orders_run_id ON run_orders(run_id);
    CREATE INDEX IF NOT EXISTS idx_run_orders_sordernum ON run_orders(sordernum);
    CREATE INDEX IF NOT EXISTS idx_run_orders_updated_ts ON run_orders(updated_ts);

    CREATE INDEX IF NOT EXISTS idx_workflow_runs_start_ts ON workflow_runs(start_ts);

    -- Active state indexes
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_status ON lws_order_state(status);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_updated_ts ON lws_order_state(updated_ts);

    CREATE INDEX IF NOT EXISTS idx_lws_order_state_polytex_item_code ON lws_order_state(polytex_item_code);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_so_p2_num ON lws_order_state(so_p2_num);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_po_p4_num ON lws_order_state(po_p4_num);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_job_p4_code ON lws_order_state(job_p4_code);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_job_p2_code ON lws_order_state(job_p2_code);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_last_step ON lws_order_state(last_step);

    -- Archive indexes (important once archive grows)
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_archive_status ON lws_order_state_archive(status);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_archive_updated_ts ON lws_order_state_archive(updated_ts);

    -- Phase 2 indexes
    CREATE INDEX IF NOT EXISTS idx_so4_line_snapshot_so ON so4_line_snapshot(so4_sordernum);
    CREATE INDEX IF NOT EXISTS idx_so4_header_snapshot_so ON so4_header_snapshot(so4_sordernum);

    CREATE INDEX IF NOT EXISTS idx_req_snapshot_job ON req_snapshot(jobcode);
    CREATE INDEX IF NOT EXISTS idx_req_snapshot_keyed_job ON req_snapshot_keyed(jobcode);

    CREATE INDEX IF NOT EXISTS idx_so4_to_po_map_po ON so4_to_po_map(po_num);

    CREATE INDEX IF NOT EXISTS idx_order_change_log_so ON order_change_log(so4_sordernum, created_ts);
    CREATE INDEX IF NOT EXISTS idx_order_change_log_run_id ON order_change_log(run_id);
    CREATE INDEX IF NOT EXISTS idx_order_change_log_created_ts ON order_change_log(created_ts);
    CREATE INDEX IF NOT EXISTS idx_lws_order_state_last_failed_sig ON lws_order_state(last_failed_sig);

    """)
    conn.commit()
    conn.close()



# ============================================================
# PHASE 2 SQLITE HELPERS
# ============================================================

def get_so4_line_snapshot(sqlite_conn: sqlite3.Connection, so_num: int, line_num: int):
    return sqlite_conn.execute("""
        SELECT * FROM so4_line_snapshot
        WHERE so4_sordernum=? AND so4_linenum=?
    """, (so_num, line_num)).fetchone()


def upsert_so4_line_snapshot(sqlite_conn: sqlite3.Connection, so_num: int, line_num: int,
                            itemcode: str, orderedqty: float, reqdate: str):
    sqlite_conn.execute("""
        INSERT INTO so4_line_snapshot(so4_sordernum, so4_linenum, itemcode, orderedqty, reqdate, updated_ts)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(so4_sordernum, so4_linenum)
        DO UPDATE SET
          itemcode=excluded.itemcode,
          orderedqty=excluded.orderedqty,
          reqdate=excluded.reqdate,
          updated_ts=excluded.updated_ts
    """, (so_num, line_num, itemcode, float(orderedqty or 0), reqdate, _now_utc_iso()))
    sqlite_conn.commit()


def get_req_snapshot(sqlite_conn: sqlite3.Connection, requirement_id: int):
    return sqlite_conn.execute("""
        SELECT * FROM req_snapshot WHERE requirement_id=?
    """, (requirement_id,)).fetchone()


def upsert_req_snapshot(sqlite_conn: sqlite3.Connection, requirement_id: int,
                        jobcode: str, requiredqty: float, requireddate: str):
    sqlite_conn.execute("""
        INSERT INTO req_snapshot(requirement_id, jobcode, requiredqty, requireddate, updated_ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(requirement_id)
        DO UPDATE SET
          jobcode=excluded.jobcode,
          requiredqty=excluded.requiredqty,
          requireddate=excluded.requireddate,
          updated_ts=excluded.updated_ts
    """, (int(requirement_id), str(jobcode), float(requiredqty or 0), requireddate, _now_utc_iso()))
    sqlite_conn.commit()


def upsert_so4_to_po_map(sqlite_conn: sqlite3.Connection, so_num: int, line_num: int,
                         po_num: int, po_linenum: int):
    sqlite_conn.execute("""
        INSERT INTO so4_to_po_map(so4_sordernum, so4_linenum, po_num, po_linenum, created_ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(so4_sordernum, so4_linenum)
        DO UPDATE SET
          po_num=excluded.po_num,
          po_linenum=excluded.po_linenum
    """, (int(so_num), int(line_num), int(po_num), int(po_linenum), _now_utc_iso()))
    sqlite_conn.commit()


def get_po_map(sqlite_conn: sqlite3.Connection, so_num: int, line_num: int):
    return sqlite_conn.execute("""
        SELECT * FROM so4_to_po_map WHERE so4_sordernum=? AND so4_linenum=?
    """, (int(so_num), int(line_num))).fetchone()


def insert_change_log(sqlite_conn: sqlite3.Connection, run_id: str,
                      so_num: int, line_num: int, change_type: str,
                      old_value: Any = None, new_value: Any = None,
                      details: Optional[dict] = None):
    sqlite_conn.execute("""
        INSERT INTO order_change_log(
          run_id, so4_sordernum, so4_linenum, change_type, old_value, new_value, details_json, created_ts
        )
        VALUES(?,?,?,?,?,?,?,?)
    """, (
        run_id,
        int(so_num) if so_num is not None else None,
        int(line_num) if line_num is not None else None,
        str(change_type),
        None if old_value is None else str(old_value),
        None if new_value is None else str(new_value),
        json.dumps(details or {}),
        _now_utc_iso()
    ))
    sqlite_conn.commit()


def get_req_snapshot_keyed(sqlite_conn: sqlite3.Connection, jobcode: str, reqgroupcode: str, itemcode: str):
    return sqlite_conn.execute("""
        SELECT * FROM req_snapshot_keyed
        WHERE jobcode=? AND reqgroupcode=? AND itemcode=?
    """, (str(jobcode), str(reqgroupcode), str(itemcode))).fetchone()


def upsert_req_snapshot_keyed(sqlite_conn: sqlite3.Connection, jobcode: str, reqgroupcode: str, itemcode: str,
                              requiredqty: float, requireddate: str):
    sqlite_conn.execute("""
        INSERT INTO req_snapshot_keyed(jobcode, reqgroupcode, itemcode, requiredqty, requireddate, updated_ts)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(jobcode, reqgroupcode, itemcode)
        DO UPDATE SET
          requiredqty=excluded.requiredqty,
          requireddate=excluded.requireddate,
          updated_ts=excluded.updated_ts
    """, (str(jobcode), str(reqgroupcode), str(itemcode),
          float(requiredqty or 0), str(requireddate), _now_utc_iso()))
    sqlite_conn.commit()

def get_so4_header_snapshot(sqlite_conn: sqlite3.Connection, so_num: int):
    return sqlite_conn.execute("""
        SELECT * FROM so4_header_snapshot
        WHERE so4_sordernum=?
    """, (int(so_num),)).fetchone()


def upsert_so4_header_snapshot(sqlite_conn: sqlite3.Connection, so_num: int, custref: str):
    sqlite_conn.execute("""
        INSERT INTO so4_header_snapshot(so4_sordernum, custref, updated_ts)
        VALUES(?,?,?)
        ON CONFLICT(so4_sordernum)
        DO UPDATE SET
          custref=excluded.custref,
          updated_ts=excluded.updated_ts
    """, (int(so_num), str(custref or "").strip(), _now_utc_iso()))
    sqlite_conn.commit()
