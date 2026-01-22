#Services\item_service.py
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

    # Notify CSR: new item created WAIT (House Style)
    html = f"""
    <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px; margin:0 auto;">
      
      <div style="padding:18px 20px; border:1px solid #e5e7eb; border-radius:12px; background:#ffffff;">
        
        <h2 style="margin:0; font-size:22px; color:#111827;">
          📌 LWS Workflow — New StarPak Item Created (WAIT)
        </h2>

        <p style="margin:10px 0 0; font-size:14px; color:#374151;">
          The required StarPak Finished Good item did not exist and was automatically created in <b>WAIT</b> status.
          The workflow is paused until the item is reviewed and approved.
        </p>

        <div style="margin-top:14px; padding:14px; border:1px solid #e5e7eb; background:#f9fafb; border-radius:10px;">
          <table style="width:100%; border-collapse:collapse; font-size:14px;">
            <tr>
              <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb; width:200px;"><b>PolyTex SO (Plant 4)</b></td>
              <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;">{sordernum}</td>
            </tr>
            <tr>
              <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;"><b>New StarPak Item</b></td>
              <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;">{fg}</td>
            </tr>
            <tr>
              <td style="padding:8px 10px;"><b>Status</b></td>
              <td style="padding:8px 10px;">
                <span style="display:inline-block; padding:4px 10px; border-radius:999px; background:#fff7ed; color:#9a3412; font-weight:600;">
                  WAIT
                </span>
              </td>
            </tr>
          </table>
        </div>

        <div style="margin-top:14px; padding:14px; border-left:4px solid #f59e0b; background:#fff7ed; border-radius:8px;">
          <b style="color:#92400e;">Next Step:</b>
          <div style="margin-top:6px; color:#111827; font-size:14px;">
            Please review the item setup in Radius and approve it (change status to <b>APP</b>).
            Once approved, the workflow will resume automatically on the next scheduled run.
          </div>
        </div>

        <div style="margin-top:18px; font-size:12px; color:#6b7280; border-top:1px solid #e5e7eb; padding-top:12px;">
          This email was generated automatically by the LWS Workflow Monitor.
        </div>
      </div>

    </div>
    """

    send_email(CSR_EMAILS, f"Action Required – Approve New StarPak Item (WAIT) – SO {sordernum}", html)

    return fg, True
