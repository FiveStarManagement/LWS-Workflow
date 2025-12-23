from typing import Optional, Tuple
from datetime import datetime, timedelta
import base64, json


from db import rquery
from api import send_post_request, decode_porder_response, b64_json
from logger import get_logger
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class DecodedResponse:
    status_code: int
    messages: List[str]
    decoded_payload: Optional[Dict[str, Any]]
    error_message: str
    raw: Dict[str, Any]



log = get_logger("polytex_po")

SUPPCODE_POLYTEX_LWS = "P4-00684"  # from LWS PO script :contentReference[oaicite:3]{index=3}
ADDRNUM_POLYTEX_LWS = "3086"       # from LWS PO script :contentReference[oaicite:4]{index=4}
WHOUSE_POLYTEX_LWS = "9200"        # script sets whousecode 9200 :contentReference[oaicite:5]{index=5}
PLANT_P4 = "4"
UNIT_PRICE = 0.01
PRICE_UNIT_CODE = "KFEET"
PRICE_GROUP_NO = 1

def b64_json_decode(payload_b64: str):
    return json.loads(base64.b64decode(payload_b64).decode("utf-8"))

def find_existing_po_by_job(conn, jobcode: str) -> Optional[int]:
    sql = """
    SELECT po."POrderNum" AS POrderNum
    FROM "PUB"."PV_POrder" po
    WHERE po."CompNum" = 2
      AND po."SuppRef" = ?
    ORDER BY po."LastUpdatedDateTime" DESC
    """
    rows = rquery(conn, sql, (jobcode,))
    if not rows:
        return None

    r0 = rows[0]

    # robust key lookup (Progress/driver may return different casing)
    po_num = None
    if isinstance(r0, dict):
        po_num = (
            r0.get("POrderNum")
            or r0.get("PORDERNUM")
            or r0.get("pordernum")
        )
        if po_num is None:
            # last resort: case-insensitive search
            for k, v in r0.items():
                if str(k).lower() == "pordernum":
                    po_num = v
                    break
    else:
        # tuple/Row fallback
        try:
            po_num = r0[0]
        except Exception:
            po_num = getattr(r0, "POrderNum", None) or getattr(r0, "PORDERNUM", None)

    return int(po_num) if po_num is not None else None


def decode_porder_response(resp):
    body = resp.json()
    efi = body.get("efiRadiusResponse", {})

    status_code = efi.get("statusCode")
    error_message = efi.get("errorMessage", "")
    payload_b64 = efi.get("payload")

    messages = []
    decoded_payload = None

    if error_message:
        messages.append(error_message)

    if payload_b64:
        try:
            decoded_payload = b64_json_decode(payload_b64)
        except Exception as e:
            messages.append(str(e))

    return DecodedResponse(
        status_code=status_code,
        messages=messages,
        decoded_payload=decoded_payload,
        error_message=error_message,
        raw=body,
    )



def create_polytex_po(conn, jobcode: str, itemcode: str, qty: float, required_date: str, dim_a: float, logger) -> int:


    today = datetime.now()
    price_date = datetime.now().strftime("%Y-%m-%d")


    try:
        req_dt = datetime.fromisoformat(required_date[:10])
    except Exception:
        req_dt = today + timedelta(days=1)

    new_req = req_dt - timedelta(days=14)
    if new_req <= today:
        new_req = today + timedelta(days=1)

    
    suppcode = SUPPCODE_POLYTEX_LWS
    termscode = "NET 30"

    UNIT_PRICE = 0.01
    PRICE_UNIT_CODE = "KFEET"
    PRICE_GROUP_NO = 1

    payload = {
        "XLPOrders": {
            "XLPOrder": [{
                # --------------------
                # HEADER
                # --------------------
                "CompNum": 2,
                "PlantCode": PLANT_P4,                  # "4"
                "SuppCode": SUPPCODE_POLYTEX_LWS,        # "P4-00684"
                "SuppRef": str(jobcode),
                "POAddrNum": ADDRNUM_POLYTEX_LWS,        # "3086"
                "TermsCode": "NET 30",
                "WHouseCode": WHOUSE_POLYTEX_LWS,        # "9200"
                "POStatus": 2,
                "RequiredDate": req_dt.strftime("%Y-%m-%d"),
                "ReqDate": new_req.strftime("%Y-%m-%d"),
                "POrderNum": "",                        # force create

                # --------------------
                # LINES
                # --------------------
                "XLPOrderLine": [{
                    "CompNum": 2,
                    "PlantCode": PLANT_P4,              # "4"
                    "SuppCode": SUPPCODE_POLYTEX_LWS,
                    "WhouseCode": WHOUSE_POLYTEX_LWS,
                    "POrderLineNum": 1,
                    "ItemCode": itemcode,
                    "DimA": float(dim_a),               # ✅ from Req table
                    "DimB": 0.0,
                    "DimC": 0.0,
                    "OrderedQty": float(qty),           # ✅ from Req table
                    "ReqDate": req_dt.strftime("%Y-%m-%d"),
                    "PriceGroupNo": PRICE_GROUP_NO,
                    "PriceUnitCode": PRICE_UNIT_CODE,
                    "FCUnitPrice": UNIT_PRICE,          # ✅ hard code
                    "UnitPrice": UNIT_PRICE,            # ✅ hard code
                    "POLineStatus": 10,
                    "LastUserCode": "radius",
                    "MaxRollWeight": 0.0,
                    "NumberOfRolls": 0,
                }],

                # --------------------
                # PRICE
                # --------------------
                "XLPOrderPrice": [{
                    "CompNum": 2,
                    "ItemCode": itemcode,
                    "LastUserCode": "radius",
                    "PriceDate": price_date,
                    "PriceGroupNo": PRICE_GROUP_NO,
                    "PriceUnitCode": PRICE_UNIT_CODE,
                    "FCUnitPrice": UNIT_PRICE,          # ✅ hard code
                    "PriceStatus": 0,
                }],
            }]
        }
    }





    resp = send_post_request("XLinkAPIPOrder", b64_json(payload), logger)
    decoded = decode_porder_response(resp)

    if decoded.status_code == 9:
        raise RuntimeError(f"PO API error: {decoded.messages or decoded.error_message or 'Unknown Radius error'}")


    if decoded.status_code != 1:
        raise RuntimeError(f"PO failed: {decoded.messages}")

    # Extract PO number from decoded payload (robust + case-insensitive)
    decoded_payload = decoded.decoded_payload or {}

    orders = None
    if isinstance(decoded_payload, dict):
        if "XLPOrders" in decoded_payload:
            orders = decoded_payload.get("XLPOrders", {}).get("XLPOrder")
        elif "XLPOrder" in decoded_payload:
            orders = decoded_payload.get("XLPOrder")
    elif isinstance(decoded_payload, list):
        orders = decoded_payload

    if not orders:
        raise RuntimeError(
            f"PO succeeded but payload shape unexpected: {decoded_payload}"
        )

    po_row = orders[0] if isinstance(orders, list) else orders

    po_num = (
        po_row.get("POrderNum")
        or po_row.get("PORDERNUM")
        or po_row.get("pordernum")
    )

    if po_num is None:
        raise RuntimeError(
            f"PO succeeded but POrderNum missing. Keys={list(po_row.keys())} row={po_row}"
        )

    return int(po_num)


