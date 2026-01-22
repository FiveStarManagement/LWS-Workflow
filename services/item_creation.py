#services\item_creation/py

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api import send_post_request, b64_json, decode_generic
from db import rquery, execute
from emailer import send_email
from config import CSR_EMAILS
from logger import get_logger

log = get_logger("item_creation")


REQUIRED_ANALYSIS_TYPES = [
    ".030 Plates",
    ".045 Plates",
    ".067 Plates",
    "Area",
    "Art Vendor #",
    "ART/DES#",
    "Bag Style",
    "BottleSize",
    "Brand",
    "Broker Code",
    "Broker Rep",
    "Category",
    "CO-OP Discount",
    "Color Standard",
    "Commission",
    "Contract",
    "Contract Expire",
    "CSR REP",
    "CSR REP2",
    "CSR Supervisor",
    "CustSpec#",
    "CustSpecDate",
    "DieLine",
    "Dusting",
    "EG Print",
    "Flavor",
    "Freight Terms",
    "GP ID#",
    "HD Plates",
    "Header Size",
    "High OP White",
    "InkJet",
    "Len Files",
    "Lip",
    "Location Type",
    "Mandrel",
    "MCR File",
    "MSR date",
    "NBM",
    "NFM",
    "Number Across",
    "Number Around",
    "Number UP",
    "Order Type",
    "Over/Under Terms",
    "Pack",
    "Pack Type",
    "Plant",
    "Plate Type",
    "PLT Life FEET",
    "Pocket Length",
    "Price EFF Date",
    "Price QTR",
    "Printer",
    "Prod Line #",
    "Product Segment",
    "Rebate",
    "Resin",
    "Sales Person",
    "Shipping Request",
    "Sleeve",
    "Sub Customer",
    "Vender/Broker",
    "VMI",
    "Whse Terms",
    "Yield",
]


# -----------------------------
# Result reporting (Automator-like)
# -----------------------------
@dataclass
class ItemActionResult:
    itemCode: str
    compNum: int
    action: str
    success: bool
    message: str
    timestamp: str


@dataclass
class ItemCreationReport:
    items: List[ItemActionResult]

    def add(self, itemCode: str, compNum: int, action: str, success: bool, message: str) -> None:
        self.items.append(
            ItemActionResult(
                itemCode=itemCode,
                compNum=compNum,
                action=action,
                success=success,
                message=message,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        )


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_date(d: datetime) -> str:
    """
    IMPORTANT:
    PV_XLPRICE date fields are commonly stored as 8-char strings in Progress/RADIUS.
    Use YYYYMMDD to avoid ODBC 'right truncated' errors.
    """
    return d.strftime("%Y%m%d")



def _safe_get(d: Dict[str, Any], *keys: str, default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default


def build_getitem_filter_payload(template_itemcode: str) -> Dict[str, Any]:
    """
    GetItem should use a REAL existing template itemcode.
    We will now pass the FULL base itemcode (example: 2300-NPU01-0686T).
    """
    return {
        "Filter": [
            {
                "page": 1,
                "PageSize": 1,
                "includechild": False,
                "Criteria": [
                    {"column": "compnum", "op": "eq", "value1": "2"},
                    {"column": "itemcode", "op": "eq", "value1": str(template_itemcode)},
                ],
            }
        ]
    }



# -----------------------------
# Radius API - GetItem
# -----------------------------
def get_item_template_from_radius(template_itemcode: str, logger) -> Dict[str, Any]:
    """
    Calls entityName=GetItem and returns decoded payload JSON as dict.
    Automator treats success when efiRadiusResponse.statusCode == 0 for GetItem.
    """
    payload = build_getitem_filter_payload(template_itemcode)
    resp = send_post_request("GetItem", b64_json(payload), logger)
    dec = decode_generic(resp)

    # Automator: statusCode must be 0 on GetItem
    if dec.status_code != 0:
        raise RuntimeError(f"GetItem failed: status={dec.status_code}, messages={dec.messages}")

    dp = dec.decoded_payload
    if not isinstance(dp, dict):
        raise RuntimeError("GetItem decoded payload is not JSON object")

    return dp


# -----------------------------
# PV_XLPRICE insert (Automator does eDB.upsert with INSERT)
# -----------------------------
def upsert_xlprice(
    conn,
    *,
    itemcode: str,
    plantcode: str,
    pricetype: int,
    breakunitcode: str,
    priceunitcode: str,
    currcode: str,
    direction: int = 1,
    lastusercode: str = "Fusion",
    recordtype: int = 0,
    recordstatus: int = 0,
    effdate_days_back: int = 5,
    expiry_days_forward: Optional[int] = None,
) -> None:
    """
    Mimic the Automator INSERT into pub.pv_xlprice.
    We use a guarded insert (only insert if not exists by linkingref+itemcode+plantcode+pricetype).
    """
    now = datetime.now(timezone.utc)

    # IMPORTANT: bind as DATE objects (prevents ODBC truncation issues)
    creationdate = now.date()
    effdate = (now - timedelta(days=effdate_days_back)).date()
    expirydate = (now + timedelta(days=expiry_days_forward)).date() if expiry_days_forward else None


    # This "smart insert" prevents duplicates on reruns.
    exists_sql = """
    SELECT 1
    FROM "PUB"."PV_XLPRICE"
    WHERE "CompNum" = 2
      AND "ItemCode" = ?
      AND "PlantCode" = ?
      AND "PriceType" = ?
      AND "Direction" = ?
      AND "LinkingRef" = ?
    """
    linkingref = itemcode
    rows = rquery(conn, exists_sql, (itemcode, plantcode, pricetype, direction, linkingref))
    if rows:
        log.info(f"PV_XLPRICE already exists for {itemcode} plant={plantcode} pricetype={pricetype}")
        return

    if expirydate is None:
        sql = """
        INSERT INTO "PUB"."PV_XLPRICE"
        ("Direction","LinkingRef","CreationDate","LastUserCode","RecordType","RecordStatus",
         "CompNum","ItemCode","EffDate","BreakUnitCode","PriceUnitCode","CurrCode","PriceType","PlantCode")
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        params = (
            direction, linkingref, creationdate, lastusercode, recordtype, recordstatus,
            2, itemcode, effdate, breakunitcode, priceunitcode, currcode, pricetype, plantcode
        )
    else:
        sql = """
        INSERT INTO "PUB"."PV_XLPRICE"
        ("Direction","LinkingRef","CreationDate","ExpiryDate","LastUserCode","RecordType","RecordStatus",
         "CompNum","ItemCode","EffDate","BreakUnitCode","PriceUnitCode","CurrCode","PriceType","PlantCode")
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        params = (
            direction, linkingref, creationdate, expirydate, lastusercode, recordtype, recordstatus,
            2, itemcode, effdate, breakunitcode, priceunitcode, currcode, pricetype, plantcode
        )

    log.debug(f"PV_XLPRICE insert params (len): {[len(str(p)) if isinstance(p, str) else None for p in params]}")
    log.debug(f"PV_XLPRICE insert params (vals): {params}")

    execute(conn, sql, params)
    log.info(f"Inserted PV_XLPRICE for {itemcode} plant={plantcode} pricetype={pricetype}")
    log.debug(f"PV_XLPRICE insert params: {params}")



# -----------------------------
# Payload mutations (match Automator)
# -----------------------------
def _ensure_array(obj: Dict[str, Any], key: str) -> List[Any]:
    if key not in obj or not isinstance(obj[key], list):
        obj[key] = []
    return obj[key]


def mutate_for_polytex_rm(parsed: Dict[str, Any], pt_itemcode: str) -> Dict[str, Any]:
    """
    Matches postPTRM() from Automator.
    Takes GetItem decoded payload, sets XLItems.XLItem[0] fields, rebuilds XLUDEElements and XLItemAnalysis.
    """
    xl_item = parsed["XLItems"]["XLItem"][0]

    xl_item["CompNum"] = 2
    xl_item["ItemCode"] = pt_itemcode
    xl_item["ItemShortDesc"] = pt_itemcode
    xl_item["EstimateUse"] = 101
    xl_item["AutoRequisition"] = 1
    xl_item["ClassId"] = 1
    xl_item["RollType"] = 1
    xl_item["ItemStatusCode"] = "WAIT"
    xl_item["AutoReserve"] = 0
    xl_item["FixedCost"] = 1
    xl_item["AutoActual"] = 0
    xl_item["UnitCode"] = "FEET"
    xl_item["BWCode"] = "1000 Sq. Feet"
    xl_item["BW"] = 25
    xl_item["CustCode"] = "POL01"
    xl_item["Caliper"] = "0.01"
    xl_item["Produced"] = "false"
    xl_item["Sold"] = "false"
    xl_item["Issued"] = "true"
    xl_item["Purchased"] = "true"
    xl_item["MasterEstimateCode"] = ""
    xl_item["ItemGroupCode"] = "P4-F29"
    xl_item["ItemTypeCode"] = "PrintOPP"
    xl_item["ReqGroupCode"] = "P4-PF"
    xl_item["GTIN"] = "0"
    xl_item["OuterLabelCode"] = ""
    xl_item["MinorProductGroup"] = ""
    xl_item["SuccessorItemCode"] = ""
    xl_item["PreviousItemCode"] = ""
    xl_item["PurchasePriceCode"] = ""
    xl_item["SalesPriceCode"] = ""
    xl_item["EndUseCode"] = "10"
    xl_item["InvRefLabelCode"] = "InvASN01"
    xl_item["CustCode"] = ""
    xl_item["CustItemRef"] = ""
    xl_item["CustItemRef2"] = ""
    xl_item["ProdGroupCode"] = ""

    xl_item["Weight"] = 0
    xl_item["WeightPer"] = 0
    xl_item["WeightUnitCode"] = ""
    xl_item["MRPItem"] = "false"

    # Reset arrays exactly like script
    xl_item["XLUDEElements"] = [
        {"Compnum": 2, "UDGroup": "Item Attributes", "UDElement": "Market Product", "UDValue": "Other", "Linkpoint": 3},
        {"Compnum": 2, "UDGroup": "Item Attributes", "UDElement": "Market Segment", "UDValue": "Other", "Linkpoint": 3},
    ]

    analysis_types = [
        ".030 Plates", ".045 Plates", ".067 Plates", "Area", "Art Vendor #", "ART/DES#", "Bag Style",
        "BottleSize", "Brand", "Broker Code", "Broker Rep", "Category", "CO-OP Discount", "Color Standard",
        "Commission", "Contract", "Contract Expire", "CSR REP", "CSR REP2", "CSR Supervisor", "CustSpec#",
        "CustSpecDate", "DieLine", "Dusting", "EG Print", "Flavor", "Freight Terms", "GP ID#", "HD Plates",
        "Header Size", "High OP White", "InkJet", "Len Files", "Lip", "Location Type", "Mandrel", "MCR File",
        "MSR date", "NBM", "NFM", "Number Across", "Number Around", "Number UP", "Order Type",
        "Over/Under Terms", "Pack", "Pack Type", "Plant", "Plate Type", "PLT Life FEET", "Pocket Length",
        "Price EFF Date", "Price QTR", "Printer", "Prod Line #", "Product Segment", "Rebate", "Resin",
        "Sales Person", "Shipping Request", "Sleeve", "Sub Customer", "Vender/Broker", "VMI", "Whse Terms",
        "Yield",
    ]
    xl_item["XLItemAnalysis"] = [
        {
            "CompNum": 2,
            "ItemCode": pt_itemcode,
            "AnalysisType": at,
            "AnalysisCode": "None",
        }
        for at in REQUIRED_ANALYSIS_TYPES
    ]

    return parsed


def mutate_for_starpak_fg(parsed: Dict[str, Any], sp_itemcode: str) -> Dict[str, Any]:
    """
    Matches postItem() from Automator.
    Removes XLItemAnalysis then recreates with specific values, sets many fields.
    """
    xl_item = parsed["XLItems"]["XLItem"][0]

    xl_item["ItemCode"] = sp_itemcode
    xl_item["TaxCode"] = "E"
    xl_item["UnitCode"] = "FEET"

    # Recreate arrays exactly like script
    xl_item["XLUDEElements"] = [
        {"Compnum": 2, "UDGroup": "Item Attributes", "UDElement": "Market Product", "UDValue": "Other", "Linkpoint": 3},
        {"Compnum": 2, "UDGroup": "Item Attributes", "UDElement": "Market Segment", "UDValue": "Other", "Linkpoint": 3},
    ]

    xl_item["XLItemAnalysis"] = [
    {
        "CompNum": 2,
        "ItemCode": sp_itemcode,
        "AnalysisType": at,
        "AnalysisCode": "None",
    }
    for at in REQUIRED_ANALYSIS_TYPES
]


    # Field mutations
    xl_item["CompNum"] = 2
    xl_item["AutoRequisition"] = 0
    xl_item["ProdGroupCode"] = "Rollstock"
    xl_item["ReqGroupCode"] = "FIN ROLL"
    xl_item["CustCode"] = "POL01"
    xl_item["ItemStatusCode"] = "WAIT"
    xl_item["EstimateUse"] = 601
    xl_item["AutoReserve"] = 0
    xl_item["FixedCost"] = 0
    xl_item["AutoActual"] = 0
    xl_item["Produced"] = "true"
    xl_item["Purchased"] = "false"
    xl_item["Issued"] = "false"
    xl_item["Sold"] = "true"
    xl_item["MasterEstimateCode"] = ""
    xl_item["ItemGroupCode"] = "FG_L_TOL"
    xl_item["ItemTypeCode"] = "PRT ROLL"
    # xl_item["GTIN"] = "0"
    xl_item["OuterLabelCode"] = "RLPOL02"
    xl_item["MinorProductGroup"] = "PRTFILM"
    xl_item["SuccessorItemCode"] = ""
    xl_item["PreviousItemCode"] = ""
    xl_item["ClassId"] = 6
    xl_item["PurchasePriceCode"] = ""
    xl_item["SalesPriceCode"] = ""
    xl_item["InvRefLabelCode"] = "InvASN01"
    xl_item["EndUseCode"] = "10"

    xl_item["Weight"] = 0
    xl_item["WeightPer"] = 0
    xl_item["WeightUnitCode"] = ""
    xl_item["MRPItem"] = "false"
        

    return parsed


def post_xlink_api_item(payload_obj: Dict[str, Any], logger) -> None:
    """
    Automator: XLinkAPIItem expects efiRadiusResponse.statusCode == 1 for success.
    """
    resp = send_post_request("XLinkAPIItem", b64_json(payload_obj), logger)
    dec = decode_generic(resp)
    if dec.status_code != 1:
        raise RuntimeError(f"XLinkAPIItem failed: status={dec.status_code}, messages={dec.messages}")


# -----------------------------
# Main entry point: create both items + prices
# -----------------------------
def create_pt_and_sp_items_from_so_itemcode(
    conn,
    polytex_so_line_itemcode: str,
    sordernum: int,
    logger,
) -> ItemCreationReport:
    """
    Mimics the Automator script:
    - template GetItem uses itemCode.substring(5)
    - PT item = original itemcode (ptSubstrate1)
    - SP item = "1600" + itemcode.substring(4)
    - Creates PV_XLPRICE rows for each
    - Posts both items with XLinkAPIItem
    """
    report = ItemCreationReport(items=[])

    itemCode = polytex_so_line_itemcode.strip()

    # NEW required codes
    ptSubstrate1 = f"16P4-{itemCode}"
    spSubstrate1 = f"1600-{itemCode}"

    # NEW: GetItem template uses the FULL base itemcode
    template_key = itemCode

    log.info(
        f"Item create flow: SO={sordernum} baseItem={itemCode} "
        f"pt={ptSubstrate1} sp={spSubstrate1} templateLookup={template_key}"
    )



    # 1) Get template from Radius
    try:
        template_payload = get_item_template_from_radius(template_key, logger)
    except Exception as e:
        msg = f"GetItem template failed for key '{template_key}': {e}"
        report.add(itemCode, 2, "GetItem template", False, msg)
        raise

    # The decoded GetItem payload should already contain XLItems.XLItem[0]
    if "XLItems" not in template_payload or "XLItem" not in template_payload["XLItems"]:
        msg = f"GetItem template missing XLItems.XLItem for key '{template_key}'"
        report.add(itemCode, 2, "GetItem template", False, msg)
        raise RuntimeError(msg)

    # 2) Create PT RM item
    try:
        pt_obj = json.loads(json.dumps(template_payload))  # deep copy
        pt_obj = mutate_for_polytex_rm(pt_obj, ptSubstrate1)

        # Insert purchase price header (pricetype 0, plant 4)
        upsert_xlprice(
            conn,
            itemcode=ptSubstrate1,
            plantcode="4",
            pricetype=0,
            breakunitcode=pt_obj["XLItems"]["XLItem"][0].get("UnitCode", "FEET"),
            priceunitcode="KFEET",
            currcode="USD",
            expiry_days_forward=None,
        )

        post_xlink_api_item(pt_obj, logger)
        report.add(ptSubstrate1, 2, "Create PTRM Item (PT)", True, "PTRM Item created successfully")
    except Exception as e:
        report.add(ptSubstrate1, 2, "Create PTRM Item (PT)", False, str(e))
        raise

    # 3) Create SP FG item
    try:
        sp_obj = json.loads(json.dumps(template_payload))  # deep copy
        sp_obj = mutate_for_starpak_fg(sp_obj, spSubstrate1)

        # Insert sales price header (pricetype 1, plant 2, expiry +90)
        upsert_xlprice(
            conn,
            itemcode=spSubstrate1,
            plantcode="2",
            pricetype=1,
            breakunitcode=sp_obj["XLItems"]["XLItem"][0].get("UnitCode", "FEET"),
            priceunitcode="KFEET",
            currcode="USD",
            expiry_days_forward=90,
        )

        post_xlink_api_item(sp_obj, logger)
        report.add(spSubstrate1, 2, "Create SP FG Item (1600)", True, "FG Item created successfully")
    except Exception as e:
        report.add(spSubstrate1, 2, "Create SP FG Item (1600)", False, str(e))
        raise

    # Notify CSR that items were created in WAIT (House Style)
    try:
        html = f"""
        <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px; margin:0 auto;">
          
          <div style="padding:18px 20px; border:1px solid #e5e7eb; border-radius:12px; background:#ffffff;">
            
            <h2 style="margin:0; font-size:22px; color:#111827;">
              📌 LWS Workflow — New Items Created (WAIT Approval Required)
            </h2>

            <p style="margin:10px 0 0; font-size:14px; color:#374151;">
              The workflow detected missing required items and created them automatically in <b>WAIT</b> status.
              The workflow will remain paused until both items are reviewed and approved.
            </p>

            <div style="margin-top:14px; padding:14px; border:1px solid #e5e7eb; background:#f9fafb; border-radius:10px;">
              <table style="width:100%; border-collapse:collapse; font-size:14px;">
                <tr>
                  <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb; width:220px;"><b>PolyTex SO (Plant 4)</b></td>
                  <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;">{sordernum}</td>
                </tr>
                <tr>
                  <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;"><b>PolyTex RM Item (Plant 4)</b></td>
                  <td style="padding:8px 10px; border-bottom:1px solid #e5e7eb;">
                    {ptSubstrate1}
                    <span style="margin-left:10px; display:inline-block; padding:3px 10px; border-radius:999px; background:#fff7ed; color:#9a3412; font-weight:600;">
                      WAIT
                    </span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:8px 10px;"><b>StarPak FG Item (Plant 2)</b></td>
                  <td style="padding:8px 10px;">
                    {spSubstrate1}
                    <span style="margin-left:10px; display:inline-block; padding:3px 10px; border-radius:999px; background:#fff7ed; color:#9a3412; font-weight:600;">
                      WAIT
                    </span>
                  </td>
                </tr>
              </table>
            </div>

            <div style="margin-top:14px; padding:14px; border-left:4px solid #f59e0b; background:#fff7ed; border-radius:8px;">
              <b style="color:#92400e;">Next Step:</b>
              <div style="margin-top:6px; color:#111827; font-size:14px;">
                Please review the items in Radius and approve them (change status to <b>APP</b>).
                Once approved, the workflow will resume automatically on the next scheduled run.
              </div>
            </div>

            <div style="margin-top:18px; font-size:12px; color:#6b7280; border-top:1px solid #e5e7eb; padding-top:12px;">
              This email was generated automatically by the LWS Workflow Monitor.
            </div>
          </div>

        </div>
        """
        send_email(CSR_EMAILS, f"Action Required – Approve New Items (WAIT) – SO {sordernum}", html)

    except Exception as e:
        log.warning(f"Failed to send CSR email for item create SO {sordernum}: {e}")


    return report
