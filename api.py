#api.py
import base64
import json
from typing import Optional, Tuple, List, Dict, Any

import requests

from config import API_URL, SESSION
from models import ApiDecodeResult
from logger import get_logger

log = get_logger("api")

def b64_json(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload)
    return base64.b64encode(raw.encode()).decode()

def send_post_request(entity_name: str, b64_payload: str, logger) -> requests.Response:
    body = {"efiRadiusRequest": {"entityName": entity_name, "payload": b64_payload}}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        decoded = base64.b64decode(b64_payload).decode()
        logger.debug(f"Sending '{entity_name}' payload: {decoded}")
    except Exception as e:
        logger.debug(f"Could not decode payload before send: {e}")

    resp = SESSION.post(API_URL, json=body, headers=headers, timeout=60)
    logger.debug(f"API Response: {resp.status_code} {resp.text}")
    return resp

def decode_generic(resp: requests.Response) -> ApiDecodeResult:
    status = None
    entity = ""
    raw_err = ""
    messages: List[str] = []
    decoded_payload = None

    try:
        api_json = resp.json()
        radius = api_json.get("efiRadiusResponse", {})
        status = radius.get("statusCode")
        entity = radius.get("entityName", "") or ""
        raw_err = radius.get("errorMessage", "") or ""
        b64 = radius.get("payload")

        if b64:
            try:
                decoded = base64.b64decode(b64).decode()
                decoded_payload = json.loads(decoded)
            except Exception as e:
                messages.append(f"Failed to decode payload: {e}")

    except Exception as e:
        raw_err = f"Exception parsing API response JSON: {e}"

    if raw_err and not messages:
        messages.append(raw_err)

    return ApiDecodeResult(status, entity, raw_err, messages, decoded_payload)

def decode_porder_response(resp):
    """
    Decode XLinkAPIPOrder response.
    Handles non-JSON payloads safely and preserves Radius errorMessage.
    """
    import json
    import base64

    body = resp.json()
    r = body.get("efiRadiusResponse", {})

    status_code = int(r.get("statusCode", 0) or 0)
    error_message = r.get("errorMessage") or ""

    messages = []
    if error_message:
        messages.append(error_message)

    decoded_payload = None
    raw_payload = r.get("payload")

    if raw_payload:
        try:
            txt = base64.b64decode(raw_payload).decode("utf-8", errors="replace").strip()

            # Only attempt JSON decode if it actually looks like JSON
            if txt.startswith("{") or txt.startswith("["):
                decoded_payload = json.loads(txt)
            else:
                decoded_payload = {"_payload_text": txt}

        except Exception as e:
            messages.append(f"Failed to decode payload: {e}")

    return type("DecodedResponse", (), {
        "status_code": status_code,
        "messages": messages,
        "decoded_payload": decoded_payload,
        "error_message": error_message,
        "raw": body,
    })()

def decode_sorder_response(resp: requests.Response) -> ApiDecodeResult:
    out = decode_generic(resp)
    if not out.decoded_payload:
        return out

    msgs: List[str] = []
    data = out.decoded_payload
    try:
        for so in data.get("XLSOrders", {}).get("XLSOrder", []):
            sn = so.get("SOrderNum", "")
            hdr = so.get("ErrorMessage")
            if hdr:
                msgs.append(f"SO {sn}: {hdr.strip()}")
            for line in so.get("XLSOrderPrice", []):
                lm = line.get("ErrorMessage")
                item = line.get("ItemCode", "")
                if lm:
                    msgs.append(f"SO {sn} Item {item}: {lm.strip()}")
    except Exception as e:
        msgs.append(f"Failed to parse SO messages: {e}")

    if msgs:
        out.messages = msgs
    return out

def decode_item_response(resp: requests.Response) -> ApiDecodeResult:
    # Item responses often place errors on XLItems.XLItem[].ErrorMessage
    out = decode_generic(resp)
    if not out.decoded_payload:
        return out

    msgs: List[str] = []
    data = out.decoded_payload
    try:
        for it in data.get("XLItems", {}).get("XLItem", []):
            code = it.get("ItemCode", "")
            em = it.get("ErrorMessage")
            if em:
                msgs.append(f"Item {code}: {em.strip()}")
    except Exception as e:
        msgs.append(f"Failed to parse Item messages: {e}")

    if msgs:
        out.messages = msgs
    return out
