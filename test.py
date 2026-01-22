cur = sqlite_conn.cursor()
cur.execute("SELECT COUNT(*) FROM so4_header_snapshot")
log.info(f"[DEBUG] so4_header_snapshot rows = {cur.fetchone()[0]}")
