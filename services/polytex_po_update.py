# services/polytex_po_update.py

import base64
import json
from typing import Any, Dict
from logger import get_logger
from config import API_URL, SESSION


log = get_logger("xlink_api")


# --------------------------------------------------------------------
# Core POST Helper (used for BOTH PO + SO update)
# --------------------------------------------------------------------
def _post_xlink(entity_name: str, payload_dict: dict) -> dict:
    """
    Posts payload_dict as Base64 JSON to Radius adapter.
    Handles XML responses safely.
    Always logs payload + raw response.
    """

    # ✅ pretty JSON (log-friendly)
    payload_json = json.dumps(payload_dict, indent=2)
    payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("utf-8")

    # ✅ log what we send
    log.info(f"[XLink POST] entity={entity_name}")
    log.info(f"[XLink POST] payload_decoded=\n{payload_json}")
    log.info(f"[XLink POST] payload_b64={payload_b64[:500]}...(truncated)")

    # ✅ correct adapter wrapper
    body = {"efiRadiusRequest": {"entityName": entity_name, "payload": payload_b64}}

    # ✅ send request
    resp = SESSION.post(API_URL, json=body, timeout=60)

    raw_text = ""
    try:
        raw_text = resp.text.strip()
    except Exception:
        pass

    log.info(f"[XLink POST] http_status={resp.status_code}")
    log.info(f"[XLink POST] raw_response={raw_text[:2000]}")

    # HTTP errors
    try:
        resp.raise_for_status()
    except Exception as e:
        return {
            "ok": False,
            "entityName": entity_name,
            "http_status": resp.status_code,
            "statusCode": None,
            "errorMessage": str(e),
            "raw_response": raw_text[:2000],
            "payload_decoded": None,
        }

    # Try JSON decode (adapter normally returns JSON)
# Try JSON decode
    try:
        data = resp.json()
    except Exception:
        # ✅ XML fallback (adapter sometimes returns XML even on success)
        if "<efiRadiusResponse>" in raw_text:
            import xml.etree.ElementTree as ET
            try:
                root = ET.fromstring(raw_text)
                status = root.find(".//statusCode")
                err = root.find(".//errorMessage")
                payload = root.find(".//payload")
                ent = root.find(".//entityName")

                status_code = int(status.text) if status is not None and status.text else 9
                error_message = err.text if err is not None and err.text else ""
                payload_b64 = payload.text if payload is not None and payload.text else ""
                entity_val = ent.text if ent is not None and ent.text else entity_name

                payload_decoded = None
                if payload_b64:
                    try:
                        decoded_txt = base64.b64decode(payload_b64).decode("utf-8")
                        payload_decoded = json.loads(decoded_txt)
                    except Exception:
                        payload_decoded = None

                return {
                    "ok": status_code == 1,
                    "entityName": entity_val,
                    "http_status": resp.status_code,
                    "statusCode": status_code,
                    "errorMessage": error_message,
                    "raw_response": raw_text[:2000],
                    "payload_decoded": payload_decoded,
                }

            except Exception as ex:
                return {
                    "ok": False,
                    "entityName": entity_name,
                    "http_status": resp.status_code,
                    "statusCode": 9,
                    "errorMessage": f"XML parse failed: {ex}",
                    "raw_response": raw_text[:2000],
                    "payload_decoded": None,
                }

        # If not XML either
        return {
            "ok": False,
            "entityName": entity_name,
            "http_status": resp.status_code,
            "statusCode": 9,
            "errorMessage": "API did not return JSON (not XML either).",
            "raw_response": raw_text[:2000],
            "payload_decoded": None,
        }


    # Unwrap adapter JSON
    if isinstance(data, dict) and "efiRadiusResponse" in data:
        data = data["efiRadiusResponse"]

    # decode payload if present
    payload_decoded = None
    payload_b64_resp = None

    if isinstance(data, dict):
        payload_b64_resp = data.get("payload")

    if payload_b64_resp:
        try:
            decoded_txt = base64.b64decode(payload_b64_resp).decode("utf-8")
            payload_decoded = json.loads(decoded_txt)
        except Exception:
            payload_decoded = None

    # attach final info
    if isinstance(data, dict):
        data["payload_decoded"] = payload_decoded
        data["entityName"] = entity_name
        data["http_status"] = resp.status_code
        data["ok"] = (data.get("statusCode") == 1)

    return data


# --------------------------------------------------------------------
# ✅ PO UPDATE
# --------------------------------------------------------------------
def update_po_line_qty_api(
    
    po_num: int,
    itemcode: str,
    plantcode: str,
    po_linenum: int,
    new_qty: float
) -> dict:

    payload = {
        "XLPOrders": {
            "XLPOrder": [
                {
                    "CompNum": 2,
                    "CurrCode": "USD",
                    "POrderNum": int(po_num),
                    "XLPOrderLine": [
                        {
                            "CompNum": 2,
                            "ItemCode": str(itemcode),
                            "OrderedQty": float(new_qty),
                            "POrderLineNum": int(po_linenum),
                            "POrderNum": int(po_num),
                            "PlantCode": str(plantcode),
                        }
                    ],
                }
            ]
        }
    }

    return _post_xlink("XLinkAPIPOrder", payload)


# --------------------------------------------------------------------
# ✅ SO UPDATE
# --------------------------------------------------------------------
def update_so_line_qty_api(
    so_num: int,
    itemcode: str,
    plantcode: str,
    so_linenum: int,
    new_qty: float,
    reqdate: str,
) -> dict:

    payload = {
        "XLSOrders": {
            "XLSOrder": [
                {
                    "CompNum": 2,
                    "PlantCode": str(plantcode),
                    "SOSourceCode": "LWS",
                    "CurrCode": "USD",
                    "SOrderStat": 0,
                    "SOrderNum": int(so_num),
                    "XLSOrderLine": [
                        {
                            "SOrderLineNum": int(so_linenum),
                            "PlantCode": str(plantcode),
                            "CompNum": 2,
                            "SOrderNum": int(so_num),
                            "ItemCode": str(itemcode),
                            "OrderedQty": float(new_qty),
                            "ReqDate": str(reqdate),
                        }
                    ],
                }
            ]
        }
    }

    return _post_xlink("XLinkAPISOrder", payload)

def update_shipreq_line_qty_api(
    shipreq_num: int,
    so_num: int,
    itemcode: str,
    new_qty: float,
    shipreq_linenum: int = 1,
    so_linenum: int = 1,
    plantcode: str = "2",
    complanum: int = 2,
):
    """
    Update StarPak (Plant 2) Ship Request line quantity using minimal payload.

    Returns dict with:
      ok (bool), entityName, http_status, errorMessage, raw_response, payload_decoded, payload_b64, statusCode
    """
    payload = {
        "XLShipReqs": {
            "XLShipReq": [
                {
                    "CompNum": complanum,
                    "PlantCode": str(plantcode),
                    "ShipReqNum": int(shipreq_num),
                    "ShipReqStat": 1,
                    "XLShipReqLine": [
                        {
                            "CompNum": complanum,
                            "PlantCode": str(plantcode),
                            "ShipReqNum": int(shipreq_num),
                            "ShipReqLineNum": int(shipreq_linenum),
                            "ItemCode": str(itemcode),
                            "SOPlantCode": str(plantcode),
                            "SOrderNum": int(so_num),
                            "SOrderLineNum": int(so_linenum),
                            "ShipQty": float(new_qty),
                            
                        }
                    ],
                }
            ]
        }
    }

    
    return _post_xlink("XLinkAPIShipReq", payload)
