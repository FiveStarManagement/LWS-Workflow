#starpak_so.py
from typing import Optional
from datetime import datetime, timedelta

from db import rquery
from api import send_post_request, decode_sorder_response, b64_json
from logger import get_logger

log = get_logger("starpak_so")

PLANT_P2 = "2"
CUSTCODE_STP = "POL01"  # if you need different custcode, change here

def get_so_header(conn, sordernum: int) -> dict | None:
    sql = """
    SELECT so."SOrderNum", so."SOrderStat", so."CustCode", so."AddtCustRef"
    FROM "PUB"."PV_SOrder" so
    WHERE so."CompNum" = 2 AND so."PlantCode" = '2' AND so."SOrderNum" = ?
    """
    rows = rquery(conn, sql, (int(sordernum),))
    return rows[0] if rows else None

def get_so_status_p2(conn, sordernum: int) -> int | None:
    sql = """
    SELECT so."SOrderStat" AS SOrderStat
    FROM "PUB"."PV_SOrder" so
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '2'
      AND so."SOrderNum" = ?
    """
    rows = rquery(conn, sql, (int(sordernum),))
    if not rows:
        return None

    row0 = rows[0]

    # dict row
    if isinstance(row0, dict):
        # direct common keys
        for k in ("SOrderStat", "SORDERSTAT", "sorderstat"):
            if k in row0 and row0[k] is not None:
                return int(row0[k])

        # fallback: case-insensitive search
        for k, v in row0.items():
            if str(k).lower() == "sorderstat" and v is not None:
                return int(v)

        return None

    # tuple/pyodbc row
    try:
        return int(row0[0])
    except Exception:
        return None




def find_existing_so_by_po(conn, pordernum: int) -> Optional[int]:
    sql = """
    SELECT so."SOrderNum" AS SOrderNum
    FROM "PUB"."PV_SOrder" so
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '2'
      AND so."AddtCustRef" = ?
    ORDER BY so."LastUpdatedDateTime" DESC
    """
    rows = rquery(conn, sql, (str(pordernum),))
    if not rows:
        return None

    r0 = rows[0]
    so_num = (
        (r0.get("SOrderNum") if isinstance(r0, dict) else None)
        or r0.get("SORDERNUM")
        or r0.get("sordernum")
    )

    if so_num is None:
        raise RuntimeError(f"PV_SOrder returned row without SOrderNum key. Keys={list(r0.keys())}")

    return int(so_num)


def create_starpak_so(
    conn,
    pordernum: int,
    custref_value: str, 
    itemcode_1600: str,
    qty: float,
    required_date: str,
    logger
) -> int:

    # Build a simple SO payload compatible with XLinkAPISOrder.
    # The original script uses "Radius_RESTful_API_Sales_Order_Request_Body" template :contentReference[oaicite:8]{index=8}
    today = datetime.now()
    try:
        req_dt = datetime.fromisoformat(required_date[:10])
    except Exception:
        req_dt = today + timedelta(days=7)

    payload = {
        "XLSOrders": {
            "XLSOrder": [{
                "CompNum": 2,
                "PlantCode": PLANT_P2,
                "CustCode": CUSTCODE_STP,
                "CustRef": str(custref_value), 
                "AddtCustRef": str(pordernum),
                "SOrderDate": today.strftime("%Y-%m-%d"),
                "CustReqDate": req_dt.strftime("%Y-%m-%d"),
                "SOSourceCode": "LWS",
                "CurrCode": "USD",
                "TermsCode": "NET 30",
                "XLSOrderLine": [{
                    "SOrderLineNum": 1,
                    "PlantCode": "2",
                    "CompNum": 2,
                    "ItemCode": itemcode_1600,
                    "OrderedQty": float(qty),
                    "ReqDate": req_dt.strftime("%Y-%m-%d"),
                    "PriceUnitCode": "KFEET",
                    "UnitPrice": 0.01
                }]
            }]
        }
    }

    resp = send_post_request("XLinkAPISOrder", b64_json(payload), logger)
    decoded = decode_sorder_response(resp)

    if decoded.status_code == 9 and decoded.messages:
        raise RuntimeError(f"SO API error: {decoded.messages}")

    if decoded.status_code != 1:
        raise RuntimeError(f"SO failed: {decoded.messages}")

    dp = decoded.decoded_payload or {}

    orders = None
    if isinstance(dp, dict):
        if "XLSOrders" in dp:
            orders = (dp.get("XLSOrders") or {}).get("XLSOrder")
        elif "XLSOrder" in dp:
            orders = dp.get("XLSOrder")
    elif isinstance(dp, list):
        orders = dp

    if not orders:
        raise RuntimeError(f"SO succeeded but payload shape unexpected: {dp}")

    so_row = orders[0] if isinstance(orders, list) else orders

    so_num = (
        so_row.get("SOrderNum")
        or so_row.get("SORDERNUM")
        or so_row.get("sordernum")
    )

    if so_num is None:
        raise RuntimeError(f"SO succeeded but SOrderNum missing. Keys={list(so_row.keys())} row={so_row}")

    return int(so_num)

