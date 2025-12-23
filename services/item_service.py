from typing import Tuple
import json

from db import rquery
from api import send_post_request, decode_item_response, b64_json
from emailer import send_email
from config import CSR_EMAILS
from logger import get_logger

log = get_logger("item_service")

def normalize_1600(itemcode: str) -> str:
    if itemcode.startswith("1600"):
        return itemcode
    # 1600 + substring(4) as in original LWS logic :contentReference[oaicite:1]{index=1}
    return "1600" + itemcode[4:]

def item_exists(conn, compnum: int, itemcode: str) -> bool:
    sql = """
    SELECT 1
    FROM "PUB"."PM_Item"
    WHERE "CompNum" = ?
      AND "ItemCode" = ?
    """
    return bool(rquery(conn, sql, (compnum, itemcode)))

def create_starpak_item_wait(itemcode_1600: str, logger) -> None:
    # Create StarPak Item script posts XLinkAPIItem and sets ItemStatusCode WAIT 
    payload = {
        "XLItems": {
            "XLItem": [{
                "CompNum": 2,
                "ItemCode": itemcode_1600,
                "TaxCode": "E",
                "ProdGroupCode": "Rollstock",
                "ReqGroupCode": "FIN ROLL",
                "CustCode": "",
                "ItemStatusCode": "WAIT",
                "AutoReserve": 0,
                "FixedCost": 1,
                "AutoActual": 2,
                "Produced": "true",
                "Sold": "true",
                "MasterEstimateCode": "",
                "ItemGroupCode": "FG-L",
                "ItemTypeCode": "FGL-PRT",
                "GTIN": "0",
                "OuterLabelCode": "",
                "MinorProductGroup": "",
                "SuccessorItemCode": "",
                "PreviousItemCode": "",
                "Class": 6,
                "PurchasePriceCode": "",
                "InvRefLabelCode": "",
                "XLItemAnalysis": []
            }]
        }
    }

    resp = send_post_request("XLinkAPIItem", b64_json(payload), logger)
    decoded = decode_item_response(resp)
    if decoded.status_code != 1:
        raise RuntimeError(f"Item create failed: {decoded.messages}")

def ensure_1600_item(conn, base_item: str, sordernum: int, logger) -> Tuple[str, bool]:
    fg = normalize_1600(base_item)
    if item_exists(conn, 2, fg):
        return fg, False

    create_starpak_item_wait(fg, logger)

    # Notify CSR: new item created WAIT
    html = f"""
    <div style="font-family:Segoe UI,Arial,sans-serif; max-width:800px;">
      <h2 style="margin:0 0 10px;">LWS Workflow — 1600 Item Created (WAIT)</h2>
      <p>The StarPak FG item did not exist and was created automatically.</p>
      <table style="border-collapse:collapse; width:100%;">
        <tr><td style="padding:8px; border:1px solid #ddd;"><b>Sales Order (Plant 4)</b></td>
            <td style="padding:8px; border:1px solid #ddd;">{sordernum}</td></tr>
        <tr><td style="padding:8px; border:1px solid #ddd;"><b>New Item</b></td>
            <td style="padding:8px; border:1px solid #ddd;">{fg}</td></tr>
        <tr><td style="padding:8px; border:1px solid #ddd;"><b>Status</b></td>
            <td style="padding:8px; border:1px solid #ddd;">WAIT</td></tr>
      </table>
      <p style="margin-top:12px; color:#555;">Please review item setup as needed.</p>
    </div>
    """
    send_email(CSR_EMAILS, f"LWS: 1600 Item Created (WAIT) - SO {sordernum}", html)
    return fg, True
