# lws_workflow/app.py

import json
import uuid
from datetime import datetime, timezone
import time

from typing import Optional, Tuple, List, Dict, Any
import requests 

# ---------------- CONFIG / CORE ----------------
from config import (
    ENV,
    MAX_ORDERS_PER_RUN,
    get_readonly_conn,
    get_db_conn,
    ADMIN_EMAILS,
)

from logger import get_logger
from emailer import send_email

# ---------------- STATE DB ----------------
from db import (
    init_state_db,
    compute_eligibility_since,
    mark_run,
    close_run,
    mark_run_order,
    upsert_order_state,
    rquery,
    is_order_complete,
)

# ---------------- ELIGIBILITY ----------------
from services.eligibility import find_eligible_sorders

# ---------------- PLANT 4 (POLYTEX) ----------------
from services.job_p4 import (
    find_existing_job_p4,
    create_job_p4,
    JobHold as JobHoldP4,
)

from services.job_requirements import get_job_requirements
from services.polytex_po import (
    find_existing_po_by_job,
    create_polytex_po,
)

# ---------------- PLANT 2 (STARPAK) ----------------
from services.starpak_so import (
    find_existing_so_by_po,
    create_starpak_so,
    get_so_status_p2,
)

from services.job_p2 import (
    find_existing_job_p2,
    create_job_p2,
    JobHold as JobHoldP2,
)

from services.shipreq_p2 import create_shipreq_for_so_p2

# ---------------- ITEM CREATION (AUTOMATOR) ----------------
from services.item_creation import create_pt_and_sp_items_from_so_itemcode



log = get_logger("app")



class WorkflowHold(Exception):
    """Stop processing intentionally (not a failure). Can carry created item codes."""
    def __init__(self, message: str, created_items: list[str] | None = None):
        super().__init__(message)
        self.created_items = created_items or []


def force_starpak_so_authorized(rw_conn, so_num: int, logger):
    """
    Due to API bug creating SO as Credit Held, force Plant2 SO to Authorized (sorderstat=0).
    """
    sql = """
    UPDATE pub.pv_sorder
       SET sorderstat = 0
     WHERE compnum = 2
       AND plantcode = '2'
       AND sordernum = ?
    """
    cur = rw_conn.cursor()
    cur.execute(sql, (so_num,))
    rw_conn.commit()
    logger.info(f"Forced Plant2 SO {so_num} to AUTHORIZED (sorderstat=0).")


def set_polytex_po_confirmed(rw_conn, po_num: int, logger):
    """
    After StarPak SO is successfully created/exists, set PolyTex PO status to Confirmed (porderstat=2).
    """
    sql = """
    UPDATE pub.pv_porder
       SET porderstat = 2
     WHERE compnum = 2
       AND pordernum = ?
    """
    cur = rw_conn.cursor()
    cur.execute(sql, (po_num,))
    rw_conn.commit()
    logger.info(f"Set PolyTex PO {po_num} to CONFIRMED (porderstat=2).")

def get_so_header_p4(ro_conn, sordernum: int) -> dict:
    sql = """
    SELECT so."CustRef"
    FROM "PUB"."PV_SOrder" so
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '4'
      AND so."SOrderNum" = ?
    """
    rows = rquery(ro_conn, sql, (sordernum,))
    if not rows:
        raise RuntimeError(f"No PV_SOrder header found for Plant4 SO {sordernum}")
    return rows[0]


# ------------------------------------------------------------
# Helpers: item status + itemcode transforms
# ------------------------------------------------------------
def _item_status(conn, compnum: int, itemcode: str, logger=None):
    sql = """
        SELECT ITEMSTATUSCODE AS STATUS
        FROM PUB.PM_ITEM
        WHERE COMPNUM = ?
          AND ITEMCODE = ?
    """
    rows = rquery(conn, sql, (compnum, itemcode))
    if not rows:
        return None

    row = rows[0]

    status = None

    # Case 1: dict row
    if isinstance(row, dict):
        status = row.get("STATUS") or row.get("status")
        if status is None:
            # fallback: find key case-insensitively
            for k, v in row.items():
                if str(k).lower() == "status":
                    status = v
                    break

    # Case 2: pyodbc.Row or tuple-like
    else:
        try:
            status = row[0]
        except Exception:
            status = getattr(row, "STATUS", None) or getattr(row, "status", None)

    if logger and status is None:
        logger.warning(
            f"_item_status DEBUG compnum={compnum} itemcode={itemcode} row_type={type(row)} row={row}"
        )

    return str(status).strip().upper() if status else None


def _pt_16p4_itemcode(base_itemcode: str) -> str:
    # NEW: 16P4-<original item>
    return f"16P4-{base_itemcode}"

def _sp_1600_itemcode(base_itemcode: str) -> str:
    # NEW: 1600-<original item>
    return f"1600-{base_itemcode}"


def get_so_line_items_p4(conn, sordernum: int) -> list[dict]:
    sql = """
    SELECT sol."SOrderLineNum", sol."ItemCode", sol."OrderedQty", sol."ReqDate"
    FROM "PUB"."PV_SOrderLine" sol
    WHERE sol."CompNum" = 2
      AND sol."PlantCode" = '4'
      AND sol."SOrderNum" = ?
    ORDER BY sol."SOrderLineNum"
    """
    return rquery(conn, sql, (sordernum,))



# ------------------------------------------------------------
# Failure handling (stop ONLY this order, email admin)
# ------------------------------------------------------------
def fail_order(
    sordernum: int,
    run_id: str,
    step: str,
    err: Exception,
    api_entity: Optional[str] = None,
    api_status: Optional[int] = None,
    api_messages: Optional[list] = None,
):
    msg = str(err)
    log.error(f"Order {sordernum} FAILED at {step}: {msg}")

    upsert_order_state(
        sordernum=sordernum,
        status="FAILED",
        last_step=step,
        last_run_id=run_id,
        last_error_summary=msg,
        last_api_entity=api_entity,
        last_api_status=api_status,
        last_api_messages_json=json.dumps(api_messages or []),
    )
    mark_run_order(run_id, sordernum, "FAILED", step)

    html = f"""
    <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px;">
      <h2 style="color:#b00020;">LWS Workflow Failure</h2>
      <p><b>Sales Order (Plant 4):</b> {sordernum}</p>
      <p><b>Failed Step:</b> {step}</p>
      <p><b>Error:</b> {msg}</p>
      {"<h3>API Messages</h3><ul>" + "".join(f"<li>{m}</li>" for m in api_messages) + "</ul>" if api_messages else ""}
      <p style="color:#666;">This order was stopped. Other orders continue.</p>
    </div>
    """

    send_email(
        ADMIN_EMAILS,
        f"LWS Workflow FAILED – SO {sordernum} ({step})",
        html,
    )


def _item_exists(conn, compnum: int, itemcode: str) -> bool:
    """
    True if the item exists in Radius for this compnum, regardless of status.
    Uses the same table/source that _item_status reads from.
    """
    sql = """
        SELECT 1
        FROM PUB.PM_ITEM
        WHERE COMPNUM = ?
          AND ITEMCODE = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (compnum, itemcode))
    return cur.fetchone() is not None


def _core_itemcode(itemcode: str) -> str:
    s = (itemcode or "").strip()
    # remove known prefixes if already present
    for prefix in ("16P4-", "1600-"):
        if s.upper().startswith(prefix):
            s = s[len(prefix):]
    return s


# ------------------------------------------------------------
# Item gate logic (Automator mimic)
# ------------------------------------------------------------
def ensure_items_ready_or_create_wait(
    ro_conn,
    rw_conn,
    run_id: str,
    sordernum: int,
    base_itemcode: str,
    require_sp_app: bool = True,   # ✅ NEW
) -> None:
    
    # 🚫 NEW: Do not start workflow if original PolyTex SO item is not APP
    base_status = _item_status(ro_conn, 2, base_itemcode)

    if str(base_status).upper() != "APP":
        upsert_order_state(
            sordernum=sordernum,
            status="HOLD",
            last_step="BASE_ITEM_WAIT",
            last_run_id=run_id,
            last_error_summary=f"Base item {base_itemcode} not APP (status={base_status}).",
        )
        mark_run_order(run_id, sordernum, "HOLD", "BASE_ITEM_WAIT")
        raise WorkflowHold(
            f"Base item {base_itemcode} not APP (status={base_status})."
        )

    
    
    core = _core_itemcode(base_itemcode)

    pt_item = _pt_16p4_itemcode(core)
    sp_item = _sp_1600_itemcode(core)

    pt_exists = _item_exists(ro_conn, 2, pt_item)
    sp_exists = _item_exists(ro_conn, 2, sp_item)

    pt_status = _item_status(ro_conn, 2, pt_item) if pt_exists else None
    sp_status = _item_status(ro_conn, 2, sp_item) if sp_exists else None

    log.info(
        f"Item gate SO {sordernum}: "
        f"PT={pt_item} exists={pt_exists} status={pt_status} | "
        f"SP={sp_item} exists={sp_exists} status={sp_status} "
        f"(require_sp_app={require_sp_app})"
    )

    # Missing -> create both and STOP (unchanged behavior)
    if (not pt_exists) or (not sp_exists):
        log.info(f"SO {sordernum}: item missing -> running Automator item creation (WAIT)")
        create_pt_and_sp_items_from_so_itemcode(
            conn=rw_conn,
            polytex_so_line_itemcode=core,
            sordernum=sordernum,
            logger=log,
        )
        rw_conn.commit()

        upsert_order_state(
            sordernum=sordernum,
            status="HOLD",
            last_step="ITEM_CREATE_WAIT",
            last_run_id=run_id,
            last_error_summary="Items created in WAIT. Stop until CSR approves (APP).",
        )
        mark_run_order(run_id, sordernum, "HOLD", "ITEM_CREATE_WAIT")
        raise WorkflowHold(
            "Items created in WAIT. Stop until APP.",
            created_items=[pt_item, sp_item],  
        )


    # ✅ ALWAYS require PT to be APP (for PO)
    if str(pt_status).upper() != "APP":
        upsert_order_state(
            sordernum=sordernum,
            status="HOLD",
            last_step="ITEM_WAIT_GATE_PT",
            last_run_id=run_id,
            last_error_summary=f"PT item not APP (PT={pt_status}). Stop until APP.",
        )
        mark_run_order(run_id, sordernum, "HOLD", "ITEM_WAIT_GATE_PT")
        raise WorkflowHold(f"PT item not APP (PT={pt_status}). Stop until APP.")

    # ✅ Only require SP to be APP when creating StarPak SO (or later steps)
    if require_sp_app and str(sp_status).upper() != "APP":
        upsert_order_state(
            sordernum=sordernum,
            status="HOLD",
            last_run_id=run_id,
            last_step="ITEM_WAIT_GATE_SP",
            last_error_summary=f"SP item not APP (SP={sp_status}). Stop until APP.",
        )
        mark_run_order(run_id, sordernum, "HOLD", "ITEM_WAIT_GATE_SP")
        raise WorkflowHold(f"SP item not APP (SP={sp_status}). Stop until APP.")

def apply_price_code_updates(rw_conn, itemcodes: set[str], logger):
    """
    After XLink update success, set:
      - 16P4-* PurchasePriceCode = ItemCode
      - 1600-* SalesPriceCode = ItemCode
    Only for itemcodes passed in (newly created items).
    """
    cur = rw_conn.cursor()

    for item in sorted(itemcodes):
        u = item.upper()

        if u.startswith("16P4-"):
            cur.execute("""
                UPDATE PUB."PM_Item"
                   SET "PurchasePriceCode" = "ItemCode"
                 WHERE "CompNum" = 2
                   AND "ItemCode" = ?
            """, (item,))
            logger.info(f"Updated PurchasePriceCode for {item}")

        elif u.startswith("1600-"):
            cur.execute("""
                UPDATE PUB."PM_Item"
                   SET "SalesPriceCode" = "ItemCode"
                 WHERE "CompNum" = 2
                   AND "ItemCode" = ?
            """, (item,))
            logger.info(f"Updated SalesPriceCode for {item}")

    rw_conn.commit()

def trigger_xlink_price_update(logger):
    """
    Runs XLink batch update for prices at end of run.
    Does NOT hold/fail the workflow if it errors.
    """
    try:
        resp = requests.post("http://RDSPOL-EFI02:4545/run_xlink", timeout=900)
        resp.raise_for_status()
        result = resp.json()

        ok = bool(result.get("success"))
        if ok:
            logger.info("XLink price update ran successfully (success=true).")
        else:
            logger.warning(
                "XLink price update returned success=false. "
                f"stderr={result.get('stderr')!r} error={result.get('error')!r}"
            )
        return ok

    except Exception as e:
        logger.warning(f"XLink price update call failed (ignored): {e}")
        return False



# ------------------------------------------------------------
# Process ONE LWS Sales Order
# ------------------------------------------------------------
def process_one_order(ro_conn, rw_conn, run_id: str, sordernum: int):

    if is_order_complete(sordernum):
        log.info(f"SO {sordernum} already COMPLETE in state DB - skipping")
        mark_run_order(run_id, sordernum, "SKIPPED", "ALREADY_COMPLETE")
        return  
    mark_run_order(run_id, sordernum, "IN_PROGRESS", "START")
    upsert_order_state(
        sordernum,
        "IN_PROGRESS",
        "ELIGIBLE",
        last_run_id=run_id
    )

    mark_run_order(run_id, sordernum, "IN_PROGRESS", "ELIGIBLE")

    customer = "LWS"  # placeholder; not used by Radius logic

    try:
        # ----------------------------------------------------
        # STEP 0: ITEM GATE (BEFORE JOB_P4)
        # Pull base item from Plant4 SO line(s)
        # ----------------------------------------------------
        step = "ITEM_GATE_PRE_JOB"

        upsert_order_state(
            sordernum=sordernum,
            status="IN_PROGRESS",
            last_step=step,
            last_run_id=run_id
        )

        lines = get_so_line_items_p4(ro_conn, sordernum)
        if not lines:
            raise RuntimeError(f"No PV_SOrderLine found for SO {sordernum} Plant 4")

        base_itemcode = str(lines[0]["ItemCode"])

        upsert_order_state(
            sordernum=sordernum,
            status="IN_PROGRESS",
            last_step=step,
            last_run_id=run_id,
            polytex_item_code=base_itemcode
        )


        # ✅ NEW: Get Plant4 SO header CustRef to use on StarPak SO
        p4_hdr = get_so_header_p4(ro_conn, sordernum)
        p4_custref = str(p4_hdr.get("CustRef") or p4_hdr.get("custref") or "").strip()


        if not p4_custref:
            log.warning(f"Plant4 SO {sordernum} CustRef is blank; StarPak SO CustRef will be blank.")

        ensure_items_ready_or_create_wait(
            ro_conn=ro_conn,
            rw_conn=rw_conn,
            run_id=run_id,
            sordernum=sordernum,
            base_itemcode=base_itemcode,
            require_sp_app=False,
        )




        # ----------------------------------------------------
        # STEP 1: JOB CREATION – PLANT 4
        # ----------------------------------------------------
        step = "JOB_P4"
        job_p4 = find_existing_job_p4(ro_conn, sordernum)

        if not job_p4:
            try:
                job_p4 = create_job_p4(sordernum, customer, log)
            except JobHoldP4 as e:
                msg = str(e).strip()
                upsert_order_state(
                    sordernum=sordernum,
                    status="HOLD",
                    last_step="JOB_P4_HOLD",
                    last_error_summary=msg,
                    last_run_id=run_id,
                )
                mark_run_order(run_id, sordernum, "HOLD", "JOB_P4_HOLD")
                raise WorkflowHold(msg)   


        upsert_order_state(
            sordernum,
            "IN_PROGRESS",
            step,
            last_run_id=run_id,
            job_p4=job_p4,
        )
        mark_run_order(run_id, sordernum, "IN_PROGRESS", step)

        # ----------------------------------------------------
        # STEP 2: FETCH REQUIREMENTS FROM PV_Req
        # ----------------------------------------------------
        step = "REQS_P4"
        reqs = get_job_requirements(ro_conn, job_p4)
        if not reqs:
            raise RuntimeError(f"No eligible PV_Req rows found for Job {job_p4}")
        log.info(f"PV_Req keys sample: {list(reqs[0].keys())}")


        
        if not reqs:
            raise RuntimeError(f"No eligible PV_Req rows found for Job {job_p4}")

        # ----------------------------------------------------
        # STEP 2A: ITEM GATE (PT substrate + 1600 FG)
        # Use first requirement ItemCode as the base itemcode (matches how your flow derives it)
        # ----------------------------------------------------
        step = "ITEM_GATE"

        req0 = reqs[0]

        base_itemcode = (
            req0.get("ItemCode")
            or req0.get("Item Code")
            or req0.get("ITEMCODE")
            or req0.get("itemcode")
        )

        if not base_itemcode:
            raise KeyError(
                f"Requirement missing ItemCode. Keys={list(req0.keys())}"
            )

        base_itemcode = str(base_itemcode).strip()

        ensure_items_ready_or_create_wait(
            ro_conn=ro_conn,
            rw_conn=rw_conn,
            run_id=run_id,
            sordernum=sordernum,
            base_itemcode=str(base_itemcode),
            require_sp_app=False, 
        )
        # ----------------------------------------------------
        # STEP 3+: PROCESS EACH REQUIREMENT
        # (PO -> SO -> JOB P2)
        # ----------------------------------------------------
        last_po = None
        last_so = None
        last_job_p2 = None

        def _get_first(d: dict, *keys):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            # case-insensitive fallback
            lower_map = {str(k).lower(): k for k in d.keys()}
            for k in keys:
                lk = str(k).lower()
                if lk in lower_map:
                    v = d[lower_map[lk]]
                    if v is not None:
                        return v
            return None


        for req in reqs:
            base_item = _get_first(req, "ItemCode", "ITEMCODE", "Item Code", "itemcode")
            if not base_item:
                raise KeyError(f"Req row missing ItemCode. Keys={list(req.keys())}")

            qty_val = _get_first(req, "RequiredQty", "REQUIREDQTY", "Required Qty", "requiredqty")
            if qty_val is None:
                raise KeyError(f"Req row missing RequiredQty. Keys={list(req.keys())}")

            date_val = _get_first(req, "RequiredDate", "REQUIREDDATE", "Required Date", "requireddate")
            if not date_val:
                raise KeyError(f"Req row missing RequiredDate. Keys={list(req.keys())}")

            base_item = str(base_item).strip()
            qty = float(qty_val)
            required_date = str(date_val)[:10]
            dim_a = (
                req.get("DimA")
                or req.get("DIMA")
                or req.get("dima")
                or 0
            )
            dim_a = float(dim_a or 0)


            # ----------------------------------------------
            # 3a: POLYTEX PO (PLANT 4) – by JobCode
            # ----------------------------------------------
            step = "PO_P4"
            po_existing = find_existing_po_by_job(ro_conn, job_p4)
            if po_existing:
                po_num = po_existing
            else:
                po_num = create_polytex_po(
                    conn=rw_conn,   # use rw_conn since you're creating the PO
                    jobcode=job_p4,
                    itemcode=base_item,
                    qty=qty,
                    required_date=required_date,
                    dim_a=dim_a,
                    logger=log,
                )


            last_po = po_num

            upsert_order_state(
                sordernum,
                "IN_PROGRESS",
                step,
                job_p4=job_p4,
                last_run_id=run_id,
                po_p4=po_num,
            )
            mark_run_order(run_id, sordernum, "IN_PROGRESS", step)

            # ----------------------------------------------
            # 3b: STARPAK SO (PLANT 2) – AddtCustRef = PO
            # ItemCode = 1600 + base_item[4:]  (Automator logic)
            # ----------------------------------------------
            step = "SO_P2"
            core = _core_itemcode(base_item)
            fg_item = _sp_1600_itemcode(core)

            so_existing = find_existing_so_by_po(ro_conn, po_num)

            # 1) Decide/create the SO number first (ONLY ONCE)
            if so_existing:
                so_num = int(so_existing)
                log.info(f"Existing Plant2 SO found for PO {po_num}: SO={so_num} (skip create)")
            else:
                so_num = create_starpak_so(
                    conn=rw_conn,
                    pordernum=po_num,
                    custref_value=p4_custref,
                    itemcode_1600=fg_item,
                    qty=qty,
                    required_date=required_date,
                    logger=log,
                )

            # 2) Force authorize once
            force_starpak_so_authorized(rw_conn, so_num, log)

            # 3) Poll status after force authorize
            so_status = None
            for attempt in range(10):
                so_status = get_so_status_p2(ro_conn, so_num)  # ✅ use ro_conn consistently
                if so_status is not None:
                    break
                time.sleep(1)

            log.info(f"StarPak SO {so_num} status after force authorize: {so_status}")

            # 4) Confirm PO only if SO is authorized/complete
            if so_status in (0, 9):
                set_polytex_po_confirmed(rw_conn, po_num, log)
                log.info(f"PolyTex PO {po_num} confirmed after StarPak SO {so_num} status={so_status}.")
            else:
                log.warning(f"PolyTex PO {po_num} NOT confirmed because StarPak SO {so_num} status={so_status}")

            last_so = so_num




            

            # SO status meanings:
            # 0 = Authorised
            # 1 = Held
            # 2 = Credit Held
            # 9 = Complete

            if so_status in (1, 2):
                upsert_order_state(
                    sordernum=sordernum,
                    status="HOLD",
                    last_step="SO_P2_STATUS_HOLD",
                    last_run_id=run_id,
                    last_error_summary=f"Plant2 SO {so_num} is not Authorised (status={so_status}).",
                )
                mark_run_order(run_id, sordernum, "HOLD", "SO_P2_STATUS_HOLD")
                raise WorkflowHold(
                    f"Plant2 SO {so_num} held or credit-held (status={so_status}).  Stop until released."
                )

            if so_status == 9:
                # SO already completed — do NOT create Job P2
                upsert_order_state(
                    sordernum,
                    "COMPLETE",
                    "SO_P2_ALREADY_COMPLETE",
                    so_p2=so_num,
                    last_run_id=run_id,
                )
                mark_run_order(run_id, sordernum, "COMPLETE", "SO_P2_ALREADY_COMPLETE")
                return


            upsert_order_state(
                sordernum,
                "IN_PROGRESS",
                step,
                job_p4=job_p4,
                po_p4=po_num,
                so_p2=so_num,
                last_run_id=run_id,
            )
            mark_run_order(run_id, sordernum, "IN_PROGRESS", step)

            # ----------------------------------------------
            # 3b-1: SHIPPING REQUEST (PLANT 2) – immediately after SO_P2
            # Create even if JOB_P2 later fails/holds.
            # ----------------------------------------------
            step = "SHIPREQ_P2"

            upsert_order_state(
                sordernum=sordernum,
                status="IN_PROGRESS",
                last_step=step,
                last_run_id=run_id,
                job_p4=job_p4,
                po_p4=po_num,
                so_p2=so_num,
            )
            mark_run_order(run_id, sordernum, "IN_PROGRESS", step)

            try:
                shipreq_num = create_shipreq_for_so_p2(ro_conn, so_num, logger=log)

                # Persist ShipReqNum so Admin page can show it
                if shipreq_num:
                    upsert_order_state(
                        sordernum=sordernum,
                        status="IN_PROGRESS",
                        last_step=step,
                        last_run_id=run_id,
                        so_p2=so_num,
                        shipreq_p2=str(shipreq_num),
                    )

            except Exception as e:
                msg = str(e).strip()
                low = msg.lower()

                # View lag: lines not visible yet -> HOLD and retry next run
                if "no plant2 so lines found" in low or "cannot create shipreq" in low:
                    upsert_order_state(
                        sordernum=sordernum,
                        status="HOLD",
                        last_step="SHIPREQ_P2_WAIT_LINES",
                        last_error_summary=msg,
                        last_run_id=run_id,
                        so_p2=so_num,
                    )
                    mark_run_order(run_id, sordernum, "HOLD", "SHIPREQ_P2_WAIT_LINES")
                    raise WorkflowHold(msg)

                # Otherwise: real ShipReq failure -> FAIL the order
                raise




            # ----------------------------------------------
            # 3c: JOB CREATION – PLANT 2 (only if items are APP)
            # ----------------------------------------------
            step = "JOB_P2"
            job2_existing = find_existing_job_p2(ro_conn, so_num)
            if job2_existing:
                job_p2 = job2_existing
            else:
                try:
                    job_p2 = create_job_p2(so_num, customer, log)
                except Exception as e:
                    msg = str(e).strip()
                    low = msg.lower()

                    is_hold_reason = (
                        "on hold" in low
                        or "did not produce a job code" in low
                        or "valid estimate cannot be determined" in low
                        or "job code missing" in low
                    )

                    if is_hold_reason:
                        upsert_order_state(
                            sordernum=sordernum,
                            status="HOLD",
                            last_step="JOB_P2_SO_ON_HOLD",
                            last_error_summary=msg,
                            last_run_id=run_id,
                        )
                        mark_run_order(run_id, sordernum, "HOLD", "JOB_P2_SO_ON_HOLD")

                        # ✅ IMPORTANT: do NOT add another prefix.
                        # msg already contains the best available reason from job_p2.py
                        raise WorkflowHold(msg)

                    raise


            last_job_p2 = job_p2

            upsert_order_state(
                sordernum,
                "IN_PROGRESS",
                step,
                job_p4=job_p4,
                po_p4=po_num,
                so_p2=so_num,
                job_p2=job_p2,
                last_run_id=run_id,
            )
            mark_run_order(run_id, sordernum, "IN_PROGRESS", step)

        # # ----------------------------------------------
        # # STEP 3d: SHIPPING REQUEST – PLANT 2
        # # ----------------------------------------------
        # step = "SHIPREQ_P2"

        # try:
        #     shipreq_num = create_shipreq_for_so_p2(
        #         ro_conn=ro_conn,
        #         so_num=so_num,
        #         logger=log,
        #     )
        # except Exception as e:
        #     msg = str(e).strip()
        #     low = msg.lower()

        #     # ✅ View lag / lines not visible yet -> HOLD and retry next run
        #     if (
        #         "no plant2 so lines found" in low
        #         or "cannot create shipreq" in low
        #         or "retry" in low
        #         or "view lag" in low
        #     ):
        #         upsert_order_state(
        #             sordernum=sordernum,
        #             status="HOLD",
        #             last_step="SHIPREQ_P2_WAIT_LINES",
        #             last_error_summary=msg,
        #             last_run_id=run_id,
        #             so_p2=so_num,
        #             job_p2=last_job_p2,
        #         )
        #         mark_run_order(run_id, sordernum, "HOLD", "SHIPREQ_P2_WAIT_LINES")
        #         raise WorkflowHold(msg)

        #     # Otherwise treat as real failure (will go to fail_order)
        #     raise

        # upsert_order_state(
        #     sordernum=sordernum,
        #     status="IN_PROGRESS",
        #     last_step=step,
        #     last_run_id=run_id,
        #     shipreq_p2=shipreq_num,
        #     so_p2=so_num,
        #     job_p2=last_job_p2,
        # )

        # mark_run_order(run_id, sordernum, "IN_PROGRESS", step)


        # ----------------------------------------------------
        # FINAL: MARK ORDER COMPLETE
        # ----------------------------------------------------
        step = "COMPLETE"
        upsert_order_state(
            sordernum,
            "COMPLETE",
            step,
            job_p4=job_p4,
            po_p4=last_po,
            so_p2=last_so,
            job_p2=last_job_p2,
            last_run_id=run_id,
        )
        mark_run_order(run_id, sordernum, "COMPLETE", step)

        log.info(
            f"Order {sordernum} COMPLETE | "
            f"JobP4={job_p4}, PO={last_po}, SO={last_so}, JobP2={last_job_p2}"
        )

    except WorkflowHold as e:
        # Intentional stop (WAIT / not APP). Do NOT call fail_order().
        msg = str(e)
        log.info(f"Order {sordernum} put on HOLD at {step}: {msg}")
        # Re-raise so run_once() can count holds separately (we'll update run_once next)
        raise

    except Exception as e:
        # Real failure
        try:
            fail_order(sordernum, run_id, step if "step" in locals() else "FAILED", e)
        finally:
            raise



# ------------------------------------------------------------
# RUN ONCE (called every 30 mins by scheduler)
# ------------------------------------------------------------
def run_once():
    init_state_db()

    run_id = str(uuid.uuid4())
    start_ts = datetime.now(timezone.utc).isoformat()

    mark_run(
        run_id=run_id,
        start_ts=start_ts,
        env=ENV,
        log_file_path="logs/lws_workflow.log",
    )

    eligible = 0
    processed = 0
    failed = 0
    held = 0
    new_items_created = set()   # ✅ NEW: only items created this run



    since = compute_eligibility_since()

    ro_conn = get_readonly_conn()
    rw_conn = get_db_conn()

    try:
        # ---- TEST ONE ORDER ----
        sorders = [247320]

        # ---- PRODUCTION ----
        # sorders = find_eligible_sorders(ro_conn, since, MAX_ORDERS_PER_RUN)


        sorders = [so for so in sorders if not is_order_complete(so)]

        eligible = len(sorders)

        for sordernum in sorders:
            processed += 1
            try:
                process_one_order(ro_conn, rw_conn, run_id, sordernum)
            except WorkflowHold as e:
                held += 1
                for it in getattr(e, "created_items", []):
                    new_items_created.add(it)

            except Exception:
                failed += 1

        try:
            xlink_ok = trigger_xlink_price_update(log)

            # ✅ Only after successful XLink run, apply price code updates
            if xlink_ok and new_items_created:
                apply_price_code_updates(rw_conn, new_items_created, log)
            else:
                if not xlink_ok:
                    log.warning("Skipping PM_Item price code updates because XLink success=false.")

        except Exception:
            pass


    finally:
        try:
            ro_conn.close()
        except Exception:
            pass
        try:
            rw_conn.close()
        except Exception:
            pass

    end_ts = datetime.now(timezone.utc).isoformat()
    close_run(run_id, end_ts, eligible, processed, failed)

    log.info(
        f"Run {run_id} finished | "
        f"eligible={eligible}, processed={processed}, held={held}, failed={failed}"
    )



if __name__ == "__main__":
    run_once()
