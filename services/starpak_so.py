#starpak_so.py
from typing import Optional
from datetime import datetime, timedelta

from db import rquery
from api import send_post_request, decode_sorder_response, b64_json
from logger import get_logger
from exceptions import WorkflowApiError


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
      AND so."SOSourceCode" = 'LWS'
    ORDER BY so."LastUpdatedDateTime" DESC
    """
    rows = rquery(conn, sql, (str(pordernum),))
    if not rows:
        return None

    r0 = rows[0]

    if isinstance(r0, dict):
        so_num = r0.get("SOrderNum") or r0.get("SORDERNUM") or r0.get("sordernum")
    else:
        so_num = r0[0]  # first column

    if so_num is None:
        raise RuntimeError(f"PV_SOrder returned row without SOrderNum. row={r0}")

    return int(so_num)

def get_so_line_qty_p2(conn, sordernum: int, sorderlinenum: int = 1) -> float | None:
    sql = """
    SELECT sol."OrderedQty" AS OrderedQty
    FROM "PUB"."PV_SOrderLine" sol
    WHERE sol."CompNum" = 2
      AND sol."PlantCode" = '2'
      AND sol."SOrderNum" = ?
      AND sol."SOrderLineNum" = ?
    """
    rows = rquery(conn, sql, (int(sordernum), int(sorderlinenum)))
    if not rows:
        return None

    r0 = rows[0]
    if isinstance(r0, dict):
        v = r0.get("OrderedQty") or r0.get("ORDEREDQTY") or r0.get("orderedqty")
    else:
        v = r0[0]

    return float(v) if v is not None else None

def get_jobline_qty_p2(ro_conn, jobcode: str, fg_itemcode: str) -> float | None:
    """
    Returns Plant2 Job qty for the FG (1600-xxxx) line.
    DO NOT use JobLineNum=1 because Job lines may not be ordered consistently.
    """
    sql = """
    SELECT jl."OrderedQty" AS JobQty
    FROM "PUB"."PV_JobLine" jl
    WHERE jl."CompNum" = 2
      AND jl."PlantCode" = '2'
      AND jl."JobCode" = ?
      AND jl."ItemCode" = ?
    """
    rows = rquery(ro_conn, sql, (str(jobcode), str(fg_itemcode)))
    if not rows:
        return None

    r0 = rows[0]
    if isinstance(r0, dict):
        v = r0.get("JobQty") or r0.get("JOBQTY") or r0.get("jobqty")
    else:
        v = r0[0]

    return float(v) if v is not None else None


    r0 = rows[0]
    if isinstance(r0, dict):
        v = r0.get("OrderedQty") or r0.get("ORDEREDQTY") or r0.get("orderedqty")
    else:
        v = r0[0]

    return float(v) if v is not None else None


def create_starpak_so(
    conn,
    pordernum: int,
    custref_value: str,
    itemcode_1600: str,
    qty: float,
    required_date: str,
    so_item_type_code: str,
    logger
) -> int:

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
                "DaysPrior": 1,
                "TermsCode": "NET 30",
                "XLSOrderLine": [{
                    "SOrderLineNum": 1,
                    "PlantCode": "2",
                    "CompNum": 2,
                    "ItemCode": itemcode_1600,
                    "OrderedQty": float(qty),
                    "ReqDate": req_dt.strftime("%Y-%m-%d"),
                    "PriceUnitCode": "KFEET",
                    "SOItemTypeCode": so_item_type_code,
                    "UnitPrice": 0.01
                }]
            }]
        }
    }

    resp = send_post_request("XLinkAPISOrder", b64_json(payload), logger)
    decoded = decode_sorder_response(resp)

    # ------------------------------------------------------------
    # ✅ Raise structured API error so fail_order() can email real API details
    # ------------------------------------------------------------
    if decoded.status_code != 1:
        api_entity = getattr(decoded, "entity_name", None) or getattr(decoded, "entityName", None) or "XLinkAPISOrder"
        api_status = getattr(decoded, "status_code", None)

        api_error_message = (
            getattr(decoded, "error_message", None)
            or getattr(decoded, "errorMessage", None)
            or None
        )

        api_messages = list(getattr(decoded, "messages", None) or [])

        raise WorkflowApiError(
            f"SO API error: {api_messages}" if api_messages else "SO API error",
            api_entity=api_entity,
            api_status=api_status,
            api_error_message=api_error_message,
            api_messages=api_messages,
            raw_response_text=getattr(resp, "text", None),
        )

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

