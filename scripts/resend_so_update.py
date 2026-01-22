# D:\Work\LWS\Phase2\lws_workflow\scripts\resend_so_update.py

import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from config import get_readonly_conn, get_db_conn
from services.polytex_po_update import update_so_line_qty_api
from services.starpak_so import get_so_status_p2


# ✅ local copy of the helper we added into Phase2
def force_starpak_so_authorized(rw_conn, so_num: int):
    sql = """
    UPDATE pub.pv_sorder
       SET sorderstat = 0
     WHERE compnum = 2
       AND plantcode = '2'
       AND sordernum = ?
    """
    cur = rw_conn.cursor()
    cur.execute(sql, (int(so_num),))
    rw_conn.commit()


def main():
    so_num = 247333

    ro_conn = get_readonly_conn()
    rw_conn = get_db_conn()

    try:
        # ✅ 1) Status BEFORE update
        before_status = get_so_status_p2(ro_conn, so_num)
        print(f"✅ BEFORE UPDATE: SO {so_num} status = {before_status}")

        # ✅ 2) XLink SO update
        resp = update_so_line_qty_api(
            so_num=so_num,
            itemcode="1600-2300-NPU01-0686T4",
            plantcode="2",
            so_linenum=1,
            new_qty=12631.0,
            reqdate="2025-12-31",
        )

        print("✅ XLink Response:")
        print(resp)

        # ✅ 3) Status AFTER update (likely Credit Held)
        after_update_status = get_so_status_p2(ro_conn, so_num)
        print(f"⚠️ AFTER UPDATE: SO {so_num} status = {after_update_status}")

        # ✅ 4) Force back to Authorized
        force_starpak_so_authorized(rw_conn, so_num)

        # ✅ 5) Status AFTER force authorize
        after_force_status = get_so_status_p2(ro_conn, so_num)
        print(f"✅ AFTER FORCE AUTH: SO {so_num} status = {after_force_status}")

    finally:
        ro_conn.close()
        rw_conn.close()


if __name__ == "__main__":
    main()
