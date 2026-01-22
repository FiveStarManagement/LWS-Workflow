# lws_workflow/app.py
import os
import json
import uuid
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import requests
import base64
import html


# ---------------- CONFIG / CORE ----------------
from config import (
    ENV,
    MAX_ORDERS_PER_RUN,
    get_readonly_conn,
    get_db_conn,
    ADMIN_EMAILS,
    STATE_DB_PATH,
    REQUIRED_DATE_LEAD_DAYS,
    FULFILLMENT_EMAILS,
)

from logger import get_logger
from emailer import send_email
from services.hold_reminder import send_hold_reminders_if_needed

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
    state_conn,  # ✅ SQLite connection (phase 2 snapshots / mapping)
    # Phase2 mapping (SQLite)
    upsert_so4_to_po_map,
    is_order_removed,
    get_order_state_row,
    mark_failure_email_sent,
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
)

from services.shipreq_p2 import create_shipreq_for_so_p2

# ---------------- ITEM CREATION (AUTOMATOR) ----------------
from services.item_creation import create_pt_and_sp_items_from_so_itemcode

# ---------------- PHASE 2 ----------------
from services.phase2_qty_changes import (
    detect_so4_qty_changes_or_hold,
    detect_starpak_reconfirm_or_complete,
    apply_req_changes_to_po,
     _get_state_fields
)

from services.phase2_custref_changes import detect_so4_custref_changes_and_update_starpak
from services.film_validation_lws import validate_printed_film_base_or_fail



from exceptions import WorkflowHold



log = get_logger("app")

import config
log.debug(f"[DEBUG] SQLite DB path in use: {config.STATE_DB_PATH}")

from config import LOG_LEVEL
log.debug(f"Logger running in {LOG_LEVEL} mode")
log.info(f"[ENV] Workflow running in ENV={ENV}")




def _house_email_wrap(title: str, intro_html: str, body_html: str, footer_html: str = "") -> str:
    return f"""
    <div style="font-family:Segoe UI,Arial,sans-serif; max-width:950px; margin:auto; border:1px solid #e5e7eb;
                border-radius:10px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.04);">
      <div style="background:#0f172a; padding:16px 20px;">
        <h2 style="margin:0; color:white; font-size:20px;">🚨 {title}</h2>
      </div>
      <div style="padding:18px 20px; background:white;">
        <p style="margin:0 0 14px; font-size:14px; color:#111827;">
          {intro_html}
        </p>
        {body_html}
        <div style="margin-top:22px; font-size:12px; color:#6b7280; border-top:1px solid #e5e7eb; padding-top:12px;">
          {footer_html or "This alert was generated automatically by the LWS Workflow Monitor."}
        </div>
      </div>
    </div>
    """

def _summarize_radius_error_html(raw_text: str) -> str:
    """
    Build a readable HTML block from Radius API raw response.
    Shows entity/status/errorMessage + decoded payload ErrorMessage + line-level errors.
    """
    if not raw_text:
        return ""

    try:
        raw_obj = json.loads(raw_text)
    except Exception:
        # Not JSON
        return (
            "<h3 style='margin:16px 0 6px; font-size:15px; color:#111827;'>Raw API Response</h3>"
            "<pre style='white-space:pre-wrap; word-break:break-word; font-size:12px; "
            "padding:10px; border:1px solid #e5e7eb; border-radius:10px; background:#f9fafb;'>"
            f"{html.escape(raw_text[:4000])}"
            "</pre>"
        )

    resp = (raw_obj.get("efiRadiusResponse") or {}) if isinstance(raw_obj, dict) else {}
    entity = resp.get("entityName")
    status = resp.get("statusCode")
    err_msg = resp.get("errorMessage")
    payload_b64 = resp.get("payload")

    payload_obj = None
    if payload_b64:
        try:
            payload_json = base64.b64decode(payload_b64).decode("utf-8", errors="replace")
            payload_obj = json.loads(payload_json)
        except Exception:
            payload_obj = None

    # Pull common Radius payload fields
    header_error = None
    line_errors = []

    try:
        # payload typically: {"XLSOrders":{"XLSOrder":[{...,"ErrorMessage":"Problem with Line #1","XLSOrderLine":[{...,"ErrorMessage":"Item is inactive"}]}]}}
        order = None
        if isinstance(payload_obj, dict):
            if "XLSOrders" in payload_obj:
                x = payload_obj.get("XLSOrders") or {}
                xs = x.get("XLSOrder")
                if isinstance(xs, list) and xs:
                    order = xs[0]
                elif isinstance(xs, dict):
                    order = xs
            elif "XLSOrder" in payload_obj:
                xs = payload_obj.get("XLSOrder")
                if isinstance(xs, list) and xs:
                    order = xs[0]
                elif isinstance(xs, dict):
                    order = xs

        if isinstance(order, dict):
            header_error = order.get("ErrorMessage") or order.get("errorMessage")

            lines = order.get("XLSOrderLine")
            if isinstance(lines, dict):
                lines = [lines]
            if isinstance(lines, list):
                for ln in lines:
                    if not isinstance(ln, dict):
                        continue
                    line_errors.append({
                        "SOrderLineNum": ln.get("SOrderLineNum") or ln.get("SOrderLineNum".lower()),
                        "ItemCode": ln.get("ItemCode") or ln.get("itemcode"),
                        "ErrorMessage": ln.get("ErrorMessage") or ln.get("errorMessage"),
                        "Action": ln.get("Action") or ln.get("action"),
                    })
    except Exception:
        pass

    # Build HTML
    parts = []
    parts.append(
        "<h3 style='margin:16px 0 6px; font-size:15px; color:#111827;'>API Details</h3>"
        "<table style='width:100%; border-collapse:collapse; font-size:14px;'>"
        f"<tr><td style='padding:4px 0; width:160px;'><b>Entity</b></td><td style='padding:4px 0;'>{html.escape(str(entity or '—'))}</td></tr>"
        f"<tr><td style='padding:4px 0;'><b>Status</b></td><td style='padding:4px 0;'>{html.escape(str(status or '—'))}</td></tr>"
        f"<tr><td style='padding:4px 0;'><b>Error Message</b></td><td style='padding:4px 0;'>{html.escape(str(err_msg or '—'))}</td></tr>"
        f"<tr><td style='padding:4px 0;'><b>Payload Error</b></td><td style='padding:4px 0;'>{html.escape(str(header_error or '—'))}</td></tr>"
        "</table>"
    )

    if line_errors:
        parts.append(
            "<h4 style='margin:12px 0 6px; font-size:14px; color:#111827;'>Line Errors</h4>"
            "<table style='width:100%; border-collapse:collapse; font-size:13px;'>"
            "<thead>"
            "<tr>"
            "<th style='text-align:left; padding:6px 8px; border-bottom:1px solid #e5e7eb;'>Line</th>"
            "<th style='text-align:left; padding:6px 8px; border-bottom:1px solid #e5e7eb;'>Item</th>"
            "<th style='text-align:left; padding:6px 8px; border-bottom:1px solid #e5e7eb;'>Message</th>"
            "<th style='text-align:left; padding:6px 8px; border-bottom:1px solid #e5e7eb;'>Action</th>"
            "</tr>"
            "</thead><tbody>"
            + "".join(
                "<tr>"
                f"<td style='padding:6px 8px; border-bottom:1px solid #f3f4f6;'>{html.escape(str(le.get('SOrderLineNum') or '—'))}</td>"
                f"<td style='padding:6px 8px; border-bottom:1px solid #f3f4f6;'>{html.escape(str(le.get('ItemCode') or '—'))}</td>"
                f"<td style='padding:6px 8px; border-bottom:1px solid #f3f4f6;'>{html.escape(str(le.get('ErrorMessage') or '—'))}</td>"
                f"<td style='padding:6px 8px; border-bottom:1px solid #f3f4f6;'>{html.escape(str(le.get('Action') or '—'))}</td>"
                "</tr>"
                for le in line_errors
            )
            + "</tbody></table>"
        )

    # Optional: include a short raw snippet at bottom (not required, but useful)
    raw_snip = raw_text[:1500]
    parts.append(
        "<div class='muted small' style='margin-top:10px;'>Raw API Response (snippet)</div>"
        "<pre style='white-space:pre-wrap; word-break:break-word; font-size:12px; "
        "padding:10px; border:1px solid #e5e7eb; border-radius:10px; background:#f9fafb;'>"
        f"{html.escape(raw_snip)}"
        "</pre>"
    )

    return "".join(parts)


# ------------------------------------------------------------
# Small local helpers for Phase 2 gating (avoid db.py dependency)
# ------------------------------------------------------------
def get_phase2_held_orders(limit: int = 5000) -> set[int]:
    """
    Returns SOs that are currently in a Phase2 HOLD step, so Phase1 does not re-complete them.
    """
    conn = state_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sordernum
              FROM lws_order_state
             WHERE status = 'HOLD'
               AND (last_step LIKE 'SO4_QTY_CHANGED_%' OR last_step LIKE 'P2_%')
             LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        return {int(r[0]) for r in rows} if rows else set()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_orders_in_step(step: str, limit: int = 500) -> list[int]:
    """
    Returns SOs whose lws_order_state.last_step matches exactly the given step.
    """
    conn = state_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sordernum
              FROM lws_order_state
             WHERE last_step = ?
             ORDER BY sordernum DESC
             LIMIT ?
            """,
            (str(step), int(limit)),
        )
        rows = cur.fetchall()
        return [int(r[0]) for r in rows] if rows else []
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    log.info(f"Forced Plant2 SO {so_num} to AUTHORIZED (sorderstat=0).")


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
    log.info(f"Set PolyTex PO {po_num} to CONFIRMED (porderstat=2).")


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

    if isinstance(row, dict):
        status = row.get("STATUS") or row.get("status")
        if status is None:
            for k, v in row.items():
                if str(k).lower() == "status":
                    status = v
                    break
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
    return f"16P4-{base_itemcode}"


def _sp_1600_itemcode(base_itemcode: str) -> str:
    return f"1600-{base_itemcode}"


def get_so_line_items_p4(conn, sordernum: int) -> list[dict]:
    sql = """
    SELECT sol."SOrderLineNum", sol."ItemCode", sol."OrderedQty", sol."ReqDate", sol."SOItemTypeCode"
    FROM "PUB"."PV_SOrderLine" sol
    WHERE sol."CompNum" = 2
      AND sol."PlantCode" = '4'
      AND sol."SOrderNum" = ?
    ORDER BY sol."SOrderLineNum"
    """
    return rquery(conn, sql, (sordernum,))



def adjust_required_date(req_date_yyyy_mm_dd: str, lead_days: int) -> str:
    """
    Take PolyTex required date, subtract lead_days (default 15),
    and clamp so it never becomes a past date (min = today).
    """
    d = datetime.strptime(req_date_yyyy_mm_dd[:10], "%Y-%m-%d").date()
    adjusted = d - timedelta(days=int(lead_days))
    today = datetime.now().date()

    if adjusted < today:
        adjusted = today

    return adjusted.strftime("%Y-%m-%d")


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
    import html
    import hashlib

    # --------------------------------------------
    # 1) Build a "better" message (prefer API detail)
    # --------------------------------------------
    raw_text = getattr(err, "raw_response_text", None)  # raw API response text (string)
    ex_api_error_message = getattr(err, "api_error_message", None)  # Radius envelope errorMessage

    # Base message (always there)
    msg = str(err).strip()

    # Prefer Radius envelope error message if present
    if ex_api_error_message and str(ex_api_error_message).strip():
        msg = str(ex_api_error_message).strip()

    # If api_messages exists and msg is generic, include first message
    if api_messages and isinstance(api_messages, list):
        first = str(api_messages[0]) if api_messages else ""
        if first and (msg == str(err).strip() or msg.lower().startswith("so api error")):
            msg = first.strip()

    log.error(f"Order {sordernum} FAILED at {step}: {msg}")

    # --------------------------------------------
    # 2) Persist failure state (✅ include API details)
    # --------------------------------------------
    upsert_order_state(
        sordernum=sordernum,
        status="FAILED",
        last_step=step,
        last_run_id=run_id,
        last_error_summary=msg,
        last_api_entity=api_entity,
        last_api_status=api_status,
        last_api_messages_json=json.dumps(api_messages or []),

        # ✅ NEW: store envelope + raw response for admin page + email
        last_api_error_message=str(ex_api_error_message).strip() if ex_api_error_message else None,
        last_api_raw=str(raw_text) if raw_text else None,
    )
    mark_run_order(run_id, sordernum, "FAILED", step)

    # --------------------------------------------
    # 3) Compute failure signature (dedupe key)
    #    Same SO + same step + same "meaningful error" = same signature
    # --------------------------------------------
    sig_parts = {
        "step": str(step or ""),
        "msg": str(msg or ""),
        "api_entity": str(api_entity or ""),
        "api_status": str(api_status or ""),
        "api_err": str(ex_api_error_message or ""),
    }

    # Include first api message only (stable + small)
    if api_messages:
        try:
            sig_parts["api_msg0"] = str(api_messages[0])
        except Exception:
            sig_parts["api_msg0"] = ""

    sig_raw = json.dumps(sig_parts, sort_keys=True)
    fail_sig = hashlib.sha1(sig_raw.encode("utf-8", errors="ignore")).hexdigest()

    # --------------------------------------------
    # 4) Check if we've already emailed this same failure signature
    # --------------------------------------------
    try:
        state_row = get_order_state_row(sordernum)
        last_sig = None
        if state_row is not None:
            # sqlite3.Row supports keys(); dict supports in; handle both
            try:
                if hasattr(state_row, "keys") and "last_failed_sig" in state_row.keys():
                    last_sig = state_row["last_failed_sig"]
                elif isinstance(state_row, dict) and "last_failed_sig" in state_row:
                    last_sig = state_row["last_failed_sig"]
            except Exception:
                last_sig = None
    except Exception:
        last_sig = None

    if last_sig and str(last_sig) == str(fail_sig):
        log.info(f"[FAIL EMAIL] Skipping duplicate failure email for SO {sordernum} (same signature).")
        return  # ✅ do not resend

    # --------------------------------------------
    # 5) Build API HTML (best effort)
    # --------------------------------------------
    api_html = ""

    if raw_text:
        # Best output: show Radius envelope + decoded payload errors (your helper)
        api_html = _summarize_radius_error_html(str(raw_text))
    else:
        # Fallback detail table + messages list if any
        rows = ""

        if api_entity:
            rows += f"""
            <tr>
              <td style="padding:6px 0; width:180px;"><b>API Entity:</b></td>
              <td style="padding:6px 0;">{html.escape(str(api_entity))}</td>
            </tr>
            """

        if api_status is not None:
            rows += f"""
            <tr>
              <td style="padding:6px 0;"><b>API Status:</b></td>
              <td style="padding:6px 0;">{html.escape(str(api_status))}</td>
            </tr>
            """

        if ex_api_error_message:
            rows += f"""
            <tr>
              <td style="padding:6px 0;"><b>API Error:</b></td>
              <td style="padding:6px 0;">{html.escape(str(ex_api_error_message))}</td>
            </tr>
            """

        if rows:
            api_html = f"""
            <h3 style='margin:16px 0 6px; font-size:15px; color:#111827;'>API Details</h3>
            <table style="width:100%; border-collapse:collapse; font-size:14px;">
              {rows}
            </table>
            """

        if api_messages:
            api_html += (
                "<h3 style='margin:16px 0 6px; font-size:15px; color:#111827;'>API Messages</h3>"
                "<ul style='margin:6px 0 0; padding-left:20px; font-size:14px; color:#111827;'>"
                + "".join(f"<li>{html.escape(str(m))}</li>" for m in api_messages)
                + "</ul>"
            )

    # --------------------------------------------
    # 6) Email HTML
    # --------------------------------------------
    body_html = f"""
    <div style="margin:16px 0; padding:14px; border:1px solid #fee2e2; background:#fef2f2; border-radius:10px;">
      <table style="width:100%; border-collapse:collapse; font-size:14px;">
        <tr>
          <td style="padding:6px 0; width:180px;"><b>PolyTex SO:</b></td>
          <td style="padding:6px 0;">{sordernum}</td>
        </tr>
        <tr>
          <td style="padding:6px 0;"><b>Failed Step:</b></td>
          <td style="padding:6px 0;">{html.escape(str(step))}</td>
        </tr>
        <tr>
          <td style="padding:6px 0;"><b>Error:</b></td>
          <td style="padding:6px 0;">{html.escape(str(msg))}</td>
        </tr>
      </table>
    </div>

    {api_html}

    <p style="margin:0; font-size:14px; color:#111827;">
      ✅ This order was stopped. Other orders will continue processing normally.
    </p>
    """

    html_doc = _house_email_wrap(
        title="LWS Workflow Failure",
        intro_html="The workflow encountered an error while processing an order.",
        body_html=body_html,
        footer_html="You may retry this order from the Admin Dashboard after resolving the issue."
    )

    # --------------------------------------------
    # 7) Send + mark signature as emailed
    # --------------------------------------------
    send_email(
        ADMIN_EMAILS,
        f"LWS Workflow FAILED – SO {sordernum} ({step})",
        html_doc,
    )

    # ✅ store signature so we don't resend same failure again
    try:
        mark_failure_email_sent(sordernum, fail_sig)
    except Exception as e:
        log.warning(f"[FAIL EMAIL] Could not persist failure email signature for SO {sordernum}: {e}")




def _item_exists(conn, compnum: int, itemcode: str) -> bool:
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
    require_sp_app: bool = True,
) -> None:

    base_status = _item_status(ro_conn, 2, base_itemcode)
    base_status_u = str(base_status or "").strip().upper()

    # ✅ NEW: If base item is not APP, we still want to create missing 16P4/1600 items
    # (in WAIT) so CSR can approve everything together.
    base_item_not_app = (base_status_u != "APP")


    core = _core_itemcode(base_itemcode)

    pt_item = _pt_16p4_itemcode(core)
    sp_item = _sp_1600_itemcode(core)

    pt_exists = _item_exists(ro_conn, 2, pt_item)
    sp_exists = _item_exists(ro_conn, 2, sp_item)

    pt_status = _item_status(ro_conn, 2, pt_item) if pt_exists else None
    sp_status = _item_status(ro_conn, 2, sp_item) if sp_exists else None

    log.debug(
        f"Item gate SO {sordernum}: "
        f"PT={pt_item} exists={pt_exists} status={pt_status} | "
        f"SP={sp_item} exists={sp_exists} status={sp_status} "
        f"(require_sp_app={require_sp_app})"
    )

    if (not pt_exists) or (not sp_exists):
        log.debug(f"SO {sordernum}: item missing -> running Automator item creation (WAIT)")

        create_pt_and_sp_items_from_so_itemcode(
            conn=rw_conn,
            polytex_so_line_itemcode=core,
            sordernum=sordernum,
            logger=log,
        )
        rw_conn.commit()

        # -------------------------
        # XLink price update (ENV driven) - RUN ALL COMPANIES
        # -------------------------
        xlink_env = "live" if ENV == "LIVE" else "test"

        try:
            result = trigger_xlink_price_update(
                logger=log,
                env=xlink_env,
                run_all=True,
                timeout=900
            )

            xlink_ok = bool(result.get("success", True)) if isinstance(result, dict) else True
            log.info(f"[XLINK] Triggered XLink price update run_all=True env={xlink_env} ok={xlink_ok} result={result}")

        except Exception as e:
            xlink_ok = False
            log.warning(f"[XLINK] Trigger failed (run_all=True env={xlink_env}) but workflow continues: {e}")

        # ✅ Step 4: Apply PM_Item PriceCode updates immediately
        if xlink_ok:
            apply_price_code_updates(rw_conn, {pt_item, sp_item}, log)
        else:
            log.warning("Skipping PM_Item price code updates because XLink success=false.")

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
            created_items=[pt_item, sp_item]
        )



    

    # ✅ NEW: If base item itself is not APP, we stop here (even if derived items exist),
    # because CSR must approve base item before workflow continues.
    if base_item_not_app:
        upsert_order_state(
            sordernum=sordernum,
            status="HOLD",
            last_step="BASE_ITEM_WAIT",
            last_run_id=run_id,
            last_error_summary=f"Base item {base_itemcode} not APP (status={base_status_u}).",
        )
        mark_run_order(run_id, sordernum, "HOLD", "BASE_ITEM_WAIT")
        raise WorkflowHold(f"Base item {base_itemcode} not APP (status={base_status_u}).")


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
    cur = rw_conn.cursor()

    for item in sorted(itemcodes):
        u = item.upper()

        if u.startswith("16P4-"):
            cur.execute(
                """
                UPDATE PUB."PM_Item"
                   SET "PurchasePriceCode" = "ItemCode"
                 WHERE "CompNum" = 2
                   AND "ItemCode" = ?
                """,
                (item,),
            )
            logger.info(f"Updated PurchasePriceCode for {item}")

        elif u.startswith("1600-"):
            cur.execute(
                """
                UPDATE PUB."PM_Item"
                   SET "SalesPriceCode" = "ItemCode"
                 WHERE "CompNum" = 2
                   AND "ItemCode" = ?
                """,
                (item,),
            )
            logger.info(f"Updated SalesPriceCode for {item}")

    rw_conn.commit()


# def trigger_xlink_price_update(logger):
#     try:
#         resp = requests.post("http://RDSPOL-EFI02:4545/run_xlink", timeout=900)
#         # resp = requests.post("http://10.8.10.145:4545/run_xlink", timeout=900)
#         resp.raise_for_status()
#         result = resp.json()

#         ok = bool(result.get("success"))
#         if ok:
#             log.info("XLink price update ran successfully (success=true).")
#         else:
#             log.warning(
#                 "XLink price update returned success=false. "
#                 f"stderr={result.get('stderr')!r} error={result.get('error')!r}"
#             )
#         return ok

#     except Exception as e:
#         logger.warning(f"XLink price update call failed (ignored): {e}")
#         return False

def trigger_xlink_price_update(logger, env="LIVE", company_id=None, run_all=False, timeout=900):
    """
    Trigger XLink batch update service.

    Args:
        env: "test" or "live"
        company_id: int or str (ex: 2 or 9) - run only one company
        run_all: bool - run all configured companies
        timeout: request timeout in seconds

    Examples:
        trigger_xlink_price_update(logger, env="live", company_id=9)      # UltraPak LIVE
        trigger_xlink_price_update(logger, env="test", company_id=2)      # PolyStar TEST
        trigger_xlink_price_update(logger, env="live", run_all=True)      # ALL LIVE
        trigger_xlink_price_update(logger, env="test", run_all=True)      # ALL TEST
    """

    env = env.lower().strip()
    if env not in ("test", "live"):
        raise ValueError("env must be 'test' or 'live'")

    # URLs (can override using environment variables)
    live_url = os.getenv("XLINK_RUNNER_LIVE_URL", "http://RDSPOL-EFI02:4545/run_xlink")
    test_url = os.getenv("XLINK_RUNNER_TEST_URL", "http://10.8.10.145:4545/run_xlink")

    url = live_url if env == "live" else test_url

    # Build payload
    if run_all:
        payload = {"run_all": True}
    else:
        if company_id is None:
            raise ValueError("company_id is required unless run_all=True")
        payload = {"company_id": int(company_id)}

    # Optional API key support (only if you enable it later in JSON)
    api_key = os.getenv("XLINK_RUNNER_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-KEY"] = api_key

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        r.raise_for_status()

        logger.info(
            f"Triggered XLink update | env={env} | payload={payload} | status={r.status_code}"
        )

        return r.json()

    except Exception as e:
        logger.error(
            f"Failed to trigger XLink update | env={env} | url={url} | payload={payload} | error={e}",
            exc_info=True
        )
        raise



# ------------------------------------------------------------
# Process ONE LWS Sales Order
# ------------------------------------------------------------
def process_one_order(ro_conn, rw_conn, run_id: str, sordernum: int):
    if is_order_complete(sordernum):
        log.debug(f"SO {sordernum} already COMPLETE in state DB - skipping")
        mark_run_order(run_id, sordernum, "SKIPPED", "ALREADY_COMPLETE")
        return

    mark_run_order(run_id, sordernum, "IN_PROGRESS", "START")
    upsert_order_state(sordernum, "IN_PROGRESS", "ELIGIBLE", last_run_id=run_id)
    mark_run_order(run_id, sordernum, "IN_PROGRESS", "ELIGIBLE")

    customer = "LWS"

    sqlite_conn = state_conn()  # ✅ SQLite for phase 2 snapshots/mapping
    log.debug(f"[DEBUG] Using SQLite DB file: {STATE_DB_PATH}")

    try:
        # ----------------------------------------------------
        # STEP 0: ITEM GATE (BEFORE JOB_P4)
        # ----------------------------------------------------
        step = "ITEM_GATE_PRE_JOB"

        upsert_order_state(
            sordernum=sordernum,
            status="IN_PROGRESS",
            last_step=step,
            last_run_id=run_id,
        )

        lines = get_so_line_items_p4(ro_conn, sordernum)
        p4_so_item_type_code = str(lines[0].get("SOItemTypeCode") or "").strip()
        if not lines:
            raise RuntimeError(f"No PV_SOrderLine found for SO {sordernum} Plant 4")

        # Phase 2A: detect SO4 OrderedQty changes -> email + HOLD
        detect_so4_qty_changes_or_hold(sqlite_conn, run_id, sordernum, lines, log)

        base_itemcode = str(lines[0]["ItemCode"])

        upsert_order_state(
            sordernum=sordernum,
            status="IN_PROGRESS",
            last_step=step,
            last_run_id=run_id,
            polytex_item_code=base_itemcode,
        )

        # Plant4 SO header CustRef -> StarPak SO CustRef
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
        

        # ✅ Phase1 Film validation BEFORE creating PO/SO
        # Validate ALL printed film PV_Req items (16P4-...) against Plant4 SO base itemcode
        printed_16p4_items = []
        for rr in reqs:
            it = str(rr.get("ItemCode") or rr.get("ITEMCODE") or rr.get("itemcode") or "").strip()
            if it.upper().startswith("16P4-"):
                printed_16p4_items.append(it)

        validate_printed_film_base_or_fail(
            sqlite_conn=sqlite_conn,
            run_id=run_id,
            sordernum=int(sordernum),
            so_base_itemcode=str(base_itemcode),
            pvreq_itemcode=printed_16p4_items,  # pass the list
            house_email_wrap_func=_house_email_wrap,
            order_fulfillment_emails=FULFILLMENT_EMAILS,
            upsert_order_state_func=upsert_order_state,
            mark_run_order_func=mark_run_order,
            core_itemcode_func=_core_itemcode,
            logger=log,
        )



        # Phase 2B: apply PV_Req changes -> PO+SO updates + HOLD for StarPak reconfirm
        try:
            apply_req_changes_to_po(
                sqlite_conn=sqlite_conn,
                ro_conn=ro_conn,
                rw_conn=rw_conn,
                run_id=run_id,
                so4_sordernum=sordernum,
                job_p4=str(job_p4),
                reqs=reqs,
                logger=log,
            )
        except WorkflowHold:
            raise
        except Exception as e:
            msg = f"Phase2 apply_req_changes_to_po error: {e}"
            upsert_order_state(
                sordernum=sordernum,
                status="HOLD",
                last_step="PHASE2_REQ_APPLY_HOLD",
                last_error_summary=msg,
                last_run_id=run_id,
            )
            mark_run_order(run_id, sordernum, "HOLD", "PHASE2_REQ_APPLY_HOLD")
            raise WorkflowHold(msg)

        log.debug(f"PV_Req keys sample: {list(reqs[0].keys())}")

        # ----------------------------------------------------
        # STEP 2A: ITEM GATE (PT substrate + 1600 FG)
        # ----------------------------------------------------
        step = "ITEM_GATE"

        req0 = reqs[0]
        base_itemcode_req = (
            req0.get("ItemCode")
            or req0.get("Item Code")
            or req0.get("ITEMCODE")
            or req0.get("itemcode")
        )
        if not base_itemcode_req:
            raise KeyError(f"Requirement missing ItemCode. Keys={list(req0.keys())}")

        base_itemcode_req = str(base_itemcode_req).strip()

        ensure_items_ready_or_create_wait(
            ro_conn=ro_conn,
            rw_conn=rw_conn,
            run_id=run_id,
            sordernum=sordernum,
            base_itemcode=base_itemcode_req,
            require_sp_app=False,
        )

        # ----------------------------------------------------
        # STEP 3+: PROCESS EACH REQUIREMENT (PO -> SO -> JOB P2)
        # ----------------------------------------------------
        last_po = None
        last_so = None
        last_job_p2 = None

        def _get_first(d: dict, *keys):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
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
            required_date_raw = str(date_val)[:10]
            required_date = adjust_required_date(required_date_raw, REQUIRED_DATE_LEAD_DAYS)

            dim_a = req.get("DimA") or req.get("DIMA") or req.get("dima") or 0
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
                    conn=rw_conn,
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
            # ----------------------------------------------
            step = "SO_P2"
            core = _core_itemcode(base_item)
            fg_item = _sp_1600_itemcode(core)
            # ✅ Phase1 Gate: STOP StarPak SO creation if 1600 item is WAIT (not APP)
            fg_status = _item_status(ro_conn, 2, fg_item)

            if str(fg_status or "").upper() != "APP":
                upsert_order_state(
                    sordernum=sordernum,
                    status="HOLD",
                    last_step="ITEM_WAIT_GATE_SP",
                    last_run_id=run_id,
                    last_error_summary=f"StarPak FG item {fg_item} not APP (status={fg_status}). Stop before StarPak SO creation.",
                )
                mark_run_order(run_id, sordernum, "HOLD", "ITEM_WAIT_GATE_SP")
                raise WorkflowHold(f"StarPak item {fg_item} not APP (status={fg_status}). Waiting for approval.")


            so_existing = find_existing_so_by_po(ro_conn, po_num)

            if so_existing:
                so_num = int(so_existing)
                log.debug(f"Existing Plant2 SO found for PO {po_num}: SO={so_num} (skip create)")
            else:
                so_num = create_starpak_so(
                    conn=rw_conn,
                    pordernum=po_num,
                    custref_value=p4_custref,
                    itemcode_1600=fg_item,
                    qty=qty,
                    required_date=required_date,
                    so_item_type_code=p4_so_item_type_code,
                    logger=log,
                )

            force_starpak_so_authorized(rw_conn, so_num, log)

            so_status = None
            for _ in range(10):
                so_status = get_so_status_p2(ro_conn, so_num)
                if so_status is not None:
                    break
                time.sleep(1)

            log.debug(f"StarPak SO {so_num} status after force authorize: {so_status}")

            if so_status in (0, 9):
                set_polytex_po_confirmed(rw_conn, po_num, log)
                log.debug(f"PolyTex PO {po_num} confirmed after StarPak SO {so_num} status={so_status}.")
            else:
                log.warning(f"PolyTex PO {po_num} NOT confirmed because StarPak SO {so_num} status={so_status}")

            last_so = so_num

            # Phase2 mapping (initial): single line mapping
            try:
                so4_line = int(req.get("SOrderLineNum") or req.get("SORDERLINENUM") or req.get("sorderlinenum") or 1)
                upsert_so4_to_po_map(sqlite_conn, sordernum, so4_line, int(po_num), 1)
            except Exception as e:
                log.warning(f"Phase2: could not upsert so4->po map (ignored for phase1 behavior): {e}")

            # Hold if SO held/credit-held
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
                    f"Plant2 SO {so_num} held or credit-held (status={so_status}). Stop until released."
                )

            if so_status == 9:
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
            # 3b-1: SHIPPING REQUEST (PLANT 2)
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

                raise

            # ----------------------------------------------
            # 3c: JOB CREATION – PLANT 2
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

        log.debug(
            f"Order {sordernum} COMPLETE | "
            f"JobP4={job_p4}, PO={last_po}, SO={last_so}, JobP2={last_job_p2}"
        )

    except WorkflowHold as e:
        msg = str(e)
        log.debug(f"Order {sordernum} put on HOLD at {step}: {msg}")
        raise

    except Exception as e:
        try:
            fail_order(sordernum, run_id, step if "step" in locals() else "FAILED", e)
        finally:
            raise

    finally:
        try:
            sqlite_conn.close()
        except Exception:
            pass



def get_manual_queue_orders(limit: int = 25) -> list[int]:
    """
    Orders manually queued via Admin Run Now.
    These must be processed on the next scheduler run regardless of normal eligibility.
    """
    conn = state_conn()
    try:
        rows = conn.execute("""
            SELECT sordernum
            FROM lws_order_state
            WHERE status='NEW'
              AND last_step='ELIGIBLE'
            ORDER BY updated_ts DESC
            LIMIT ?
        """, (int(limit),)).fetchall()

        return [int(r["sordernum"]) for r in rows] if rows else []
    finally:
        try:
            conn.close()
        except Exception:
            pass



# ------------------------------------------------------------
# RUN ONCE (called every 10 mins by scheduler)
# ------------------------------------------------------------
def run_once():
    init_state_db()

    # ✅ Cleanup: archive old COMPLETE orders so monitoring list stays small
    from db import archive_old_complete_orders

    archived = archive_old_complete_orders(days=30)
    if int(archived or 0) > 0:
        log.info(f"[ARCHIVE] Archived {archived} COMPLETE orders older than 30 days.")
    else:
        log.info("[ARCHIVE] No COMPLETE orders eligible for archiving.")

    # ✅ Cleanup: purge run history older than 90 days (run_orders + workflow_runs)
    try:
        from db import purge_old_run_history

        sqlite_cleanup_conn = state_conn()
        try:
            stats = purge_old_run_history(sqlite_cleanup_conn, days_old=90)
        finally:
            sqlite_cleanup_conn.close()

        ro = int((stats or {}).get("run_orders_deleted") or 0)
        wr = int((stats or {}).get("workflow_runs_deleted") or 0)

        if (ro + wr) > 0:
            log.info(f"[MAINT] Purged run history older than 90 days. run_orders={ro}, workflow_runs={wr}.")
        else:
            log.info("[MAINT] No run history eligible for purging.")

    except Exception as e:
        log.warning(f"[MAINT] Purging run history failed (ignored): {e}")



    # =====================================================
    # ✅ STEP 5: HOLD Aging reminders + escalation
    # Runs every time scheduler calls run_once()
    # =====================================================
    try:
        reminder_count, escalated_count = send_hold_reminders_if_needed()
        if reminder_count or escalated_count:
            log.debug(
                f"[HOLD Reminder] Sent reminders={reminder_count}, escalations={escalated_count}"
            )
    except Exception as e:
        log.warning(f"[HOLD Reminder] Failed (ignored): {e}")


    run_id = str(uuid.uuid4())
    start_ts = datetime.now(timezone.utc).isoformat()

    mark_run(
        run_id=run_id,
        start_ts=start_ts,
        env=ENV,
        log_file_path="logs/lws_workflow.log",
    )

    eligible = processed = failed = held = 0


    ro_conn = get_readonly_conn()
    rw_conn = get_db_conn()
    sqlite_conn = state_conn()
    

    try:
        # =====================================================
        # 🔴 PHASE 2A – MONITOR COMPLETED ORDERS FOR SO4 QTY CHANGES
        # =====================================================
        held_sos = set()

        from db import get_orders_to_monitor

        monitor_sos = set(get_orders_to_monitor(200))
        log.debug(f"[Phase2A Monitor] checking {len(monitor_sos)} orders: {list(monitor_sos)[:10]}")


        for so4 in monitor_sos:
            try:
                # ✅ GUARD: Phase2A must NOT overwrite StarPak HOLD states
                state = _get_state_fields(sqlite_conn, int(so4))
                if state and state.get("last_step") in (
                    "P2_SO_QTY_UPDATED_WAIT_RECONFIRM",
                    "P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED",
                    "P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM",
                ):
                    log.debug(
                        f"[Phase2A] SO {so4} already in StarPak HOLD ({state.get('last_step')}), skipping Phase2A."
                    )
                    continue

                lines = get_so_line_items_p4(ro_conn, so4)
                if not lines:
                    continue

                qty_hold_triggered = False

                # ✅ Qty Monitor (may HOLD)
                try:
                    detect_so4_qty_changes_or_hold(
                        sqlite_conn=sqlite_conn,
                        run_id=run_id,
                        so4_sordernum=so4,
                        so_lines=lines,
                        logger=log,
                    )
                except WorkflowHold:
                    qty_hold_triggered = True
                    raise   # keep behavior unchanged (still HOLD)

                finally:
                    # ✅ CustRef monitor runs regardless (even when qty HOLD happens)
                    try:
                        state = _get_state_fields(sqlite_conn, int(so4))
                        log.debug(f"[Phase2 CustRef DEBUG] checking so4={so4} state={dict(state) if state else None}")

                        so_p2 = (
                            state.get("so_p2_num")
                            or state.get("so_p2")
                            if state else None
                        )

                        detect_so4_custref_changes_and_update_starpak(
                            sqlite_conn=sqlite_conn,
                            ro_conn=ro_conn,
                            rw_conn=rw_conn,
                            run_id=run_id,
                            so4_sordernum=so4,
                            so_p2=so_p2,
                            force_authorize_func=force_starpak_so_authorized,
                            logger=log
                        )
                    except Exception as e:
                        log.warning(f"[Phase2 CustRef] Failed (ignored): {e}")




            except WorkflowHold as e:
                held_sos.add(so4)
                held += 1

                # ✅ NEW GUARD: do not overwrite COMPLETE
                state = _get_state_fields(sqlite_conn, int(so4))
                if state and state.get("status") == "COMPLETE":
                    log.debug(f"[Phase2A] SO {so4} already COMPLETE, skipping HOLD overwrite.")
                    continue

                # ✅ existing guard (keep this)
                if state and state.get("last_step") in (
                    "P2_SO_QTY_UPDATED_WAIT_RECONFIRM",
                    "P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED",
                ):
                    log.debug(f"[Phase2A] SO {so4} already in StarPak HOLD state ({state.get('last_step')}), skipping overwrite.")
                    continue

                # ✅ DO NOT overwrite if already in manual complete HOLD state
                state = _get_state_fields(sqlite_conn, int(so4))
                if state and state.get("last_step") in (
                    "P2_SO_QTY_UPDATED_WAIT_RECONFIRM",
                    "P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED",
                ):
                    log.debug(f"[Phase2A] SO {so4} already in StarPak HOLD state ({state.get('last_step')}), skipping overwrite.")
                    continue

                # IMPORTANT: Phase2B needs job_p4_code to detect PV_Req changes after reconfirm
                from services.job_p4 import find_existing_job_p4
                job_p4 = find_existing_job_p4(ro_conn, so4)

                upsert_order_state(
                    sordernum=so4,
                    status="HOLD",
                    last_step="SO4_QTY_CHANGED_WAIT_RECONFIRM",
                    last_run_id=run_id,
                    last_error_summary=str(e),
                    job_p4=str(job_p4) if job_p4 else None,   # ✅ store job code
                )

                mark_run_order(run_id, so4, "HOLD", "SO4_QTY_CHANGED_WAIT_RECONFIRM")



        # =====================================================
        # 🟠 PHASE 2B – MONITOR HOLD ORDERS WAITING FOR POLYTEX RECONFIRM
        # If SO4 was changed and job was reconfirmed, PV_Req qty will change.
        # We detect that and then update PO + StarPak SO.
        # =====================================================
        cur = sqlite_conn.cursor()
        cur.execute("""
            SELECT sordernum, job_p4_code
            FROM lws_order_state
            WHERE status = 'HOLD'
              AND last_step = 'SO4_QTY_CHANGED_WAIT_RECONFIRM'
            ORDER BY updated_ts DESC
            LIMIT 200
        """)
        hold_polytex = cur.fetchall()

        from services.job_requirements import get_job_requirements
        from services.phase2_qty_changes import apply_req_changes_to_po
        


        for row in hold_polytex:
            so4 = int(row["sordernum"])
            job_p4 = row["job_p4_code"]

            if not job_p4:
                from services.job_p4 import find_existing_job_p4
                job_p4 = find_existing_job_p4(ro_conn, so4)
                if not job_p4:
                    log.warning(f"Phase2B: SO {so4} HOLD but job_p4_code missing and could not be found.")
                    continue

                # Save it so next run doesn't need to re-find
                upsert_order_state(
                    sordernum=so4,
                    status="HOLD",
                    last_step="SO4_QTY_CHANGED_WAIT_RECONFIRM",
                    last_run_id=run_id,
                    job_p4=str(job_p4),
                    last_error_summary="Recovered missing job_p4_code for Phase2B processing.",
                )


            try:
                # Pull current PV_Req rows for this PolyTex job (your JOB_REQ_SQL already filters P4-FILM)
                from services.phase2_qty_changes import get_p4_film_requirements

                reqs = get_p4_film_requirements(ro_conn, str(job_p4))
                log.info(
                    f"[Phase2B FETCH] so4={so4} job={job_p4} rows={len(reqs)} "
                    f"groups={sorted({(r.get('ReqGroupCode') or r.get('reqgroupcode')) for r in (reqs or [])})}"
                )

                if not reqs:
                    log.debug(f"Phase2B: job={job_p4} req rows={len(reqs)} groups={sorted({(r.get('ReqGroupCode') or r.get('reqgroupcode')) for r in reqs})}")

                    continue

                # If PV_Req changed, this will:
                #  - update PO via API
                #  - update StarPak SO via API
                #  - set HOLD = P2_SO_QTY_UPDATED_WAIT_RECONFIRM
                #  - raise WorkflowHold intentionally
                apply_req_changes_to_po(
                    sqlite_conn=sqlite_conn,
                    ro_conn=ro_conn,
                    rw_conn=rw_conn,
                    run_id=run_id,
                    so4_sordernum=so4,
                    job_p4=str(job_p4),
                    reqs=reqs,
                    logger=log,
                )

            except WorkflowHold as e:
                held += 1
                log.debug(f"Phase2B: SO {so4} moved to next HOLD state: {e}")

            except Exception as e:
                failed += 1
                log.error(f"Phase2B: SO {so4} error while checking PV_Req changes: {e}")

        
        
        
        # =====================================================
        # PHASE 2C – RELEASE ORDERS AFTER STARPAK RECONFIRM
        # Condition: StarPak SO qty == StarPak JOB qty
        # =====================================================

        log.debug("Phase2C block started...")

        pending_release = (
            get_orders_in_step("P2_SO_QTY_UPDATED_WAIT_RECONFIRM", limit=200)
            + get_orders_in_step("P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED", limit=200)
            + get_orders_in_step("P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM", limit=200)
        )


        log.debug(f"Phase2C pending_release orders: {pending_release}")

        for so4 in pending_release:
            try:
                # ✅ Always record that Phase2C evaluated this order in THIS run
                mark_run_order(run_id, so4, "IN_PROGRESS", "PHASE2C_CHECK")

                released = detect_starpak_reconfirm_or_complete(
                    sqlite_conn=sqlite_conn,
                    ro_conn=ro_conn,
                    rw_conn=rw_conn, 
                    run_id=run_id,
                    so4_sordernum=so4,
                    logger=log,
                )

                # ✅ If COMPLETE, record it
                if released:
                    mark_run_order(run_id, so4, "COMPLETE", "PHASE2C_COMPLETE")
                    log.debug(f"Phase2C: SO {so4} released back to COMPLETE.")
                else:
                    # ✅ Not released yet = still HOLD, record current hold step
                    state = _get_state_fields(sqlite_conn, int(so4))
                    step = (state.get("last_step") if state else "P2_SO_QTY_UPDATED_WAIT_RECONFIRM") or "P2_SO_QTY_UPDATED_WAIT_RECONFIRM"
                    mark_run_order(run_id, so4, "HOLD", step)

            except Exception as e:
                # ✅ record failure in run_orders too
                mark_run_order(run_id, so4, "FAILED", "PHASE2C_ERROR")
                log.warning(f"Phase2C: error while checking reconfirm for SO {so4}: {e}")




        # =====================================================
        # 🔵 EXISTING LOGIC – DO NOT CHANGE (Phase 1 core)
        # =====================================================
        since = compute_eligibility_since()

        # ---- TEST ----
        # sorders = [250001]

        # ---- PROD ----
        # ✅ Manual queued orders (Admin Run Now) ALWAYS go first
        manual_sos = get_manual_queue_orders(limit=25)
        if manual_sos:
            log.debug(f"[Manual Queue] {len(manual_sos)} manual order(s) queued: {manual_sos}")

        # ---- PROD ----
        sorders = find_eligible_sorders(ro_conn, MAX_ORDERS_PER_RUN)

        # ✅ Force manual orders into the run list (front of list)
        # Remove duplicates and preserve priority
        sorders = manual_sos + [so for so in sorders if so not in manual_sos]

        # ✅ Always include manual orders even if eligible list is full
        sorders = sorders[:MAX_ORDERS_PER_RUN]



        # Block Phase2 holds (from previous runs) + holds found this run
        phase2_blocked = get_phase2_held_orders()
        sorders = [so for so in sorders if so not in phase2_blocked]
        sorders = [so for so in sorders if so not in held_sos]

        # Keep Phase1 behavior: never reprocess COMPLETE
        sorders = [so for so in sorders if not is_order_complete(so)]

        # Never process REMOVED (even if manual queued or eligible from Radius)
        before = list(sorders)
        sorders = [so for so in sorders if not is_order_removed(so)]
        skipped = [so for so in before if so not in sorders]
        if skipped:
            log.info(f"[REMOVED] Skipping removed orders: {skipped}")

        eligible = len(sorders)

        for sordernum in sorders:
            processed += 1
            try:
                process_one_order(ro_conn, rw_conn, run_id, sordernum)
            except WorkflowHold:
                held += 1
            except Exception:
                failed += 1



    finally:
        try:
            ro_conn.close()
        except Exception:
            pass
        try:
            rw_conn.close()
        except Exception:
            pass
        try:
            sqlite_conn.close()
        except Exception:
            pass

    end_ts = datetime.now(timezone.utc).isoformat()
    close_run(run_id, end_ts, eligible, processed, failed)

    log.debug(
        f"Run {run_id} finished | "
        f"eligible={eligible}, processed={processed}, held={held}, failed={failed}"
    )


if __name__ == "__main__":
    run_once()
