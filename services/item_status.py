# lws_workflow/services/item_status.py
from typing import Optional
from db import rquery

def get_item_status(conn, compnum: int, itemcode: str) -> Optional[str]:
    sql = """
    SELECT "ItemStatusCode" AS ItemStatusCode
    FROM "PUB"."PM_Item"
    WHERE "CompNum" = ?
      AND "ItemCode" = ?
    """
    rows = rquery(conn, sql, (compnum, itemcode))
    if not rows:
        return None
    return (rows[0].get("ItemStatusCode")
            or rows[0].get("ITEMSTATUSCODE")
            or rows[0].get("itemstatuscode"))

def is_item_approved(conn, compnum: int, itemcode: str) -> bool:
    st = get_item_status(conn, compnum, itemcode)
    return (st or "").upper() == "APP"
