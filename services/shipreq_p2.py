# lws_workflow/services/shipreq_p2.py

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from api import send_post_request, decode_generic, b64_json
from db import rquery
from logger import get_logger
import time

log = get_logger("shipreq_p2")

def _get_first(d: Dict[str, Any], *keys, default=None):
    # exact keys first
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]

    # case-insensitive fallback
    lower_map = {str(k).lower(): k for k in d.keys()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower_map:
            v = d[lower_map[lk]]
            if v not in (None, ""):
                return v

    return default

def _get_first(d: dict, *keys, default=None):
    """
    Case-insensitive safe getter for DB rows.
    """
    if not d:
        return default

    # direct hit
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]

    # case-insensitive fallback
    lower_map = {str(k).lower(): k for k in d.keys()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower_map:
            v = d[lower_map[lk]]
            if v not in (None, ""):
                return v

    return default


def _parse_date_yyyy_mm_dd(value: Any) -> Optional[str]:
    """
    Return YYYY-MM-DD or None.
    Handles datetime/date strings returned by Radius views.
    """
    if not value:
        return None
    s = str(value).strip()
    # often "2025-12-19 00:00:00" or ISO
    s10 = s[:10]
    try:
        datetime.strptime(s10, "%Y-%m-%d")
        return s10
    except Exception:
        return None


def _date_plus_one(date_yyyy_mm_dd: str) -> str:
    d = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")


def get_so_header_p2(conn, so_num: int) -> Dict[str, Any]:
    """
    Pull CustRef + a usable ReqDate from Plant 2 SO header.
    """
    sql = """
    SELECT
        so."SOrderNum" AS SOrderNum,
        so."CustRef" AS CustRef,
        so."CustReqDate" AS ReqDate,
        so."LastUpdatedDateTime" AS LastUpdatedDateTime
    FROM "PUB"."PV_SOrder" so
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '2'
      AND so."SOrderNum" = ?
    """
    rows = rquery(conn, sql, (so_num,))
    return rows[0] if rows else {}


def get_so_lines_p2(conn, so_num: int, retries: int = 6, delay_s: float = 1.0) -> List[Dict[str, Any]]:
    """
    Pull line item(s) from Plant 2 SO (with retries to handle view lag / RO snapshot).
    SQL query is unchanged.
    """
    sql = """
    SELECT
        sol."SOrderLineNum" AS SOrderLineNum,
        sol."ItemCode"      AS ItemCode,
        sol."OrderedQty"    AS OrderedQty,
        sol."ReqDate"       AS ReqDate
    FROM "PUB"."PV_SOrderLine" sol
    WHERE sol."CompNum" = 2
      AND sol."PlantCode" = '2'
      AND sol."SOrderNum" = ?
    ORDER BY sol."SOrderLineNum"
    """

    last: List[Dict[str, Any]] = []
    for i in range(retries):
        try:
            rows = rquery(conn, sql, (so_num,)) or []
            last = rows

            if rows:
                # validate first line has an itemcode (case-insensitive)
                l0 = rows[0]
                item = str(_get_first(l0, "ItemCode", "ITEMCODE", "itemcode", default="") or "").strip()
                if item:
                    return rows

            # IMPORTANT: reset RO transaction snapshot if any
            try:
                conn.rollback()
            except Exception:
                pass

        except Exception:
            # still retry
            try:
                conn.rollback()
            except Exception:
                pass

        time.sleep(delay_s)

    return last


def find_existing_shipreq_for_so_p2(conn, so_num: int) -> Optional[str]:
    """
    Try to locate an existing ShipReqNum for a Plant2 Sales Order.
    Uses multiple possible Radius view names (envs differ).
    Returns ShipReqNum if found, else None.
    """
    candidates = [
        # Most likely for XLink ship reqs
        """
        SELECT TOP 1 srl."ShipReqNum" AS ShipReqNum
        FROM "PUB"."PV_ShipReqLine" srl
        WHERE srl."CompNum" = 2
          AND srl."PlantCode" = '2'
          AND srl."SOrderNum" = ?
        ORDER BY srl."ShipReqNum" DESC
        """,
    ]

    for sql in candidates:
        try:
            rows = rquery(conn, sql, (so_num,))
            if rows:
                shipreq = rows[0].get("ShipReqNum") or rows[0].get("SHIPREQNUM") or rows[0].get("shipreqnum")
                if shipreq:
                    return str(shipreq)
        except Exception:
            # view not available in this DB/schema — ignore and try next
            continue

    return None

def create_shipreq_for_so_p2(
    ro_conn,
    so_num: int,
    logger=None,
) -> Optional[str]:
    """
    Create Shipping Request via entity 'XLinkAPIShipReq'
    after Plant 2 SO is successfully created.
    Returns ShipReqNum if API returns it, otherwise None (but still logs/validates).
    Raises RuntimeError on API/validation failure.
    """
    logger = logger or log
    existing = find_existing_shipreq_for_so_p2(ro_conn, so_num)
    if existing:
        logger.info(f"ShipReq already exists for Plant2 SO {so_num}: ShipReqNum={existing} (skip create)")
        return existing


    hdr = get_so_header_p2(ro_conn, so_num)
    lines = get_so_lines_p2(ro_conn, so_num)

    if not lines:
        raise RuntimeError(f"No Plant2 SO lines found for SO {so_num} (cannot create ShipReq)")

    # Use line 1 (or first line) per your payload example
    l0 = lines[0]

    itemcode = str(_get_first(l0, "ItemCode", "ITEMCODE", "itemcode", default="") or "").strip()
    if not itemcode:
        logger.error(f"Plant2 SO {so_num}: ItemCode missing. keys={list(l0.keys())} row={l0}")
        raise RuntimeError(f"Plant2 SO {so_num} line missing ItemCode (see debug log keys/row)")

    qty_raw = _get_first(l0, "OrderedQty", "ORDEREDQTY", "orderedqty", default=None)
    if qty_raw is None:
        logger.error(f"Plant2 SO {so_num}: OrderedQty missing. keys={list(l0.keys())} row={l0}")
        raise RuntimeError(f"Plant2 SO {so_num} line missing OrderedQty")

    ship_qty = float(qty_raw)

    line_num = int(_get_first(l0, "SOrderLineNum", "SORDERLINENUM", "sorderlinenum", default=1) or 1)

    ship_date = (
        _parse_date_yyyy_mm_dd(_get_first(l0, "ReqDate", "REQDATE", "reqdate"))
        or _parse_date_yyyy_mm_dd(_get_first(hdr, "ReqDate", "CUSTREQDATE", "custreqdate"))
    )
    if not ship_date:
        raise RuntimeError(f"Plant2 SO {so_num}: cannot determine ShipDate/ReqDate from header/line")

    est_arrival_date = _date_plus_one(ship_date)

    # ShippingRef = From SO order CustRef
    cust_ref = str(
        _get_first(hdr, "CustRef", "AddtCustRef", "custref", "addtcustref")
        or f"SO:{so_num}"
    ).strip()

    logger.info(
        f"ShipReq SO {so_num}: ShippingRef resolved to '{cust_ref}'"
)


    payload = {
        "XLShipReqs": {
            "XLShipReq": [
                {
                    "CustCode": "POL01",        # hardcode
                    "CompNum": 2,              # hardcode
                    "BillAddrNum": 107,        # hardcode
                    "ShipAddrNum": 2430,       # hardcode
                    "CustContactCode": "",
                    "DeliveryTerms": "FOB",
                    "EstArrivalDate": est_arrival_date,
                    "EstArrivalTime": "",
                    "ExternalRef": f"SO:{so_num}",
                    "PickStatus": 0,
                    "PlantCode": "2",
                    "ShipDate": ship_date,
                    "ShipReqNum": "",
                    "ShipReqStat": 1,
                    "XLShipReqLine": [
                        {
                            "CompNum": 2,
                            "ItemCode": itemcode,
                            "PlantCode": "2",
                            "SOPlantCode": "2",
                            "SOrderLineNum": line_num,
                            "SOrderNum": int(so_num),
                            "ShipQty": ship_qty,
                            "ShipReqLineNum": 1,
                            "ShipReqNum": "",
                            "ShippingRef": cust_ref,
                            "WhouseCode": "",
                        }
                    ],
                }
            ]
        }
    }

    logger.info(f"Creating ShipReq for Plant2 SO {so_num} (item={itemcode}, qty={ship_qty}, ship_date={ship_date})")

    resp = send_post_request("XLinkAPIShipReq", b64_json(payload), logger)
    decoded = decode_generic(resp)

    # Always log decoded payload for troubleshooting
    logger.debug("Decoded XLinkAPIShipReq response for SO %s: %s", so_num, decoded.decoded_payload)

    # Try to detect errors in the decoded payload (structure can vary)
    dp = decoded.decoded_payload or {}

    # Common patterns:
    # - dp may have a top-level "Errors"/"ErrorMessage"
    # - dp may echo the request and include status fields
    possible_errors = []

    # 1) generic
    for k in ("Errors", "Error", "error", "errorMessage", "ErrorMessage", "Message"):
        v = dp.get(k)
        if v:
            possible_errors.append(f"{k}: {v}")

    # 2) nested under XLShipReqs / XLShipReq / etc
    try:
        xls = dp.get("XLShipReqs") or {}
        xl = xls.get("XLShipReq") or []
        if isinstance(xl, dict):
            xl = [xl]
        if xl:
            x0 = xl[0] or {}
            for k in ("Errors", "Error", "Status", "Message", "ErrorMessage"):
                v = x0.get(k)
                if v:
                    possible_errors.append(f"XLShipReq.{k}: {v}")
            # sometimes response returns ShipReqNum here
            shipreq_num = x0.get("ShipReqNum") or x0.get("Ship Req Num") or x0.get("ShipReq")
        else:
            shipreq_num = None
    except Exception:
        shipreq_num = None

    # If API call itself failed at transport/protocol level, decode_generic usually carries error info;
    # but your current pattern is to throw based on decoded payload content.
    dup_text = " ".join(possible_errors).lower()
    if "already exists" in dup_text or "duplicate" in dup_text:
        existing = find_existing_shipreq_for_so_p2(ro_conn, so_num)
        if existing:
            logger.info(f"ShipReq duplicate reported by API, but existing found for SO {so_num}: {existing}")
            return existing
        logger.warning(f"ShipReq duplicate reported by API for SO {so_num}, but could not locate ShipReqNum. Continuing.")
        return None

    
    
    
    if possible_errors:
        # If it's clearly not an error (e.g. Status=0), you can relax this later,
        # but for now treat any explicit "Errors/ErrorMessage" as a failure.
        raise RuntimeError(" | ".join(possible_errors))

    # If we can’t find ShipReqNum, don’t hard-fail unless you require it.
    # If you DO require it, replace with raise RuntimeError(...)
    if shipreq_num:
        logger.info(f"ShipReq created for SO {so_num}: ShipReqNum={shipreq_num}")
        return str(shipreq_num)

    logger.info(f"ShipReq call completed for SO {so_num} (ShipReqNum not returned in payload)")
    return None
