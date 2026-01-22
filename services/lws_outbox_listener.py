import time
import pyodbc
from datetime import datetime

from app import run_once
from logger import get_logger

log = get_logger("outbox_listener")

SQL_SERVER = "PRO2SQL"
DB_NAME = "VisionIIProd"
USER = "odbcuser"
PWD = "odbcpass"

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};DATABASE={DB_NAME};UID={USER};PWD={PWD};"
)

POLL_SECONDS = 30


def has_lines(conn, compnum: int, plantcode: str, sordernum: int) -> bool:
    sql = """
    SELECT TOP 1 1
    FROM dbo.PV_SOrderLine
    WHERE CompNum=? AND PlantCode=? AND SOrderNum=?
    """
    cur = conn.cursor()
    cur.execute(sql, (compnum, plantcode, sordernum))
    return cur.fetchone() is not None


def fetch_pending(conn, batch_size=200):
    sql = """
    SELECT TOP (?)
        OutboxId, ChangeType, CompNum, PlantCode, SOrderNum, SOrderLineNum
    FROM dbo.LWS_Workflow_Outbox WITH (READPAST)
    WHERE Status = 'Pending'
    ORDER BY OutboxId
    """
    cur = conn.cursor()
    cur.execute(sql, (batch_size,))
    return cur.fetchall()


def mark_sent(conn, ids):
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    sql = f"""
    UPDATE dbo.LWS_Workflow_Outbox
    SET Status='Sent', ProcessedAt=SYSUTCDATETIME()
    WHERE OutboxId IN ({placeholders})
    """
    cur = conn.cursor()
    cur.execute(sql, ids)
    conn.commit()


def main():
    log.info("LWS Outbox Listener started (polling every 30 sec).")

    while True:
        try:
            conn = pyodbc.connect(CONN_STR)
            rows = fetch_pending(conn)

            if not rows:
                conn.close()
                time.sleep(POLL_SECONDS)
                continue

            log.info(f"[Outbox] Found {len(rows)} pending event(s).")

            # Group by order number
            orders_to_process = set()
            outbox_ids = []

            for r in rows:
                outbox_ids.append(r.OutboxId)

                # Only process if order has lines
                if has_lines(conn, r.CompNum, r.PlantCode, r.SOrderNum):
                    orders_to_process.add(r.SOrderNum)
                else:
                    log.info(f"[Outbox] SO {r.SOrderNum} has no lines yet → waiting.")

            # Mark events as sent (we don't want duplicates)
            mark_sent(conn, outbox_ids)

            conn.close()

            if orders_to_process:
                log.info(f"[Outbox] Triggering run_once() for orders: {sorted(orders_to_process)}")
                run_once()  # workflow will pick eligible orders itself
            else:
                log.info("[Outbox] No eligible orders with lines yet.")

        except Exception as e:
            log.warning(f"[Outbox Listener] Error (ignored): {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
