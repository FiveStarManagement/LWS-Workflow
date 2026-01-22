# lws_workflow/services/film_validation_lws.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import List, Optional, Sequence, Union

from emailer import send_email
from db import get_printed_film_mismatch_sig, set_printed_film_mismatch_sig


@dataclass
class FilmValidationResult:
    expected_printed: List[str]
    printed_found: List[str]
    invalid_printed: List[str]
    expected_1600: str


def validate_printed_film_base_or_fail(
    *,
    sqlite_conn,
    run_id: str,
    sordernum: int,
    so_base_itemcode: str,                 # Plant4 SO base itemcode (e.g., "2300-...T1")
    pvreq_itemcode: Union[str, Sequence[str]],  # printed film PV_Req item(s) (16P4-...)
    house_email_wrap_func,                 # pass _house_email_wrap from app.py
    order_fulfillment_emails=None,
    fulfillment_emails=None,               # backward compat for earlier calls
    upsert_order_state_func=None,
    mark_run_order_func=None,
    core_itemcode_func=None,               # pass _core_itemcode from app.py
    logger=None,
) -> FilmValidationResult:
    """
    LWS Phase1 validation (printed film base check):
      - BASE = core_itemcode_func(so_base_itemcode)
      - PV_Req printed film items must be exactly "16P4-{BASE}" (or optionally "16P4-{BASE}..." if you decide later)
      - StarPak sell item is "1600-{BASE}" (derived later in app)
    LWS has no gusset/side panel; ignore 1600-XA entirely (not part of validation).
    """

    # ----------------------------
    # Backward-compatible email param
    # ----------------------------
    if fulfillment_emails and not order_fulfillment_emails:
        order_fulfillment_emails = fulfillment_emails

    if not core_itemcode_func:
        raise RuntimeError("validate_printed_film_base_or_fail: missing core_itemcode_func")

    # Normalize pvreq item(s) to a list
    if pvreq_itemcode is None:
        pvreq_items: List[str] = []
    elif isinstance(pvreq_itemcode, (list, tuple, set)):
        pvreq_items = [str(x or "").strip() for x in pvreq_itemcode]
    else:
        pvreq_items = [str(pvreq_itemcode or "").strip()]

    # Only consider printed film buy rows (16P4-...)
    printed_found = [x.upper() for x in pvreq_items if str(x or "").strip().upper().startswith("16P4-")]

    # If nothing to validate, allow pass (your choice)
    if not printed_found:
        return FilmValidationResult(
            expected_printed=[],
            printed_found=[],
            invalid_printed=[],
            expected_1600=f"1600-{core_itemcode_func(so_base_itemcode)}".upper(),
        )

    core = str(core_itemcode_func(so_base_itemcode) or "").strip()
    expected_16p4_prefix = f"16P4-{core}".upper()
    expected_16p4_exact = expected_16p4_prefix  # LWS: strict exact match
    expected_1600 = f"1600-{core}".upper()

    expected_printed = [expected_16p4_exact]
    invalid_printed = [x for x in printed_found if x != expected_16p4_exact]

    # ✅ PASS
    if not invalid_printed:
        if logger:
            logger.info(
                f"[FILM VALIDATION ✅] so={sordernum} base={so_base_itemcode} "
                f"expected={expected_16p4_exact} printed_found={printed_found}"
            )
        return FilmValidationResult(
            expected_printed=expected_printed,
            printed_found=printed_found,
            invalid_printed=[],
            expected_1600=expected_1600,
        )

    # ---------------- FAIL PATH ----------------
    summary = (
        f"PRINTED_FILM_MISMATCH: SO={sordernum} BASE={so_base_itemcode} "
        f"Expected={expected_16p4_exact} InvalidPrinted={invalid_printed} AllPrinted={printed_found}"
    )
    if logger:
        logger.error(summary)

    # ✅ Persist FAILED state (so admin page shows it)
    if upsert_order_state_func:
        upsert_order_state_func(
            sordernum=int(sordernum),
            status="FAILED",
            last_step="PHASE1_FILM_VALIDATION",
            last_run_id=run_id,
            last_error_summary=summary,
        )
    if mark_run_order_func:
        mark_run_order_func(run_id, int(sordernum), "FAILED", "PHASE1_FILM_VALIDATION")

    # ------------------------------------------------------------
    # ✅ DEDUPE: store mismatch signature in film-specific columns
    # ------------------------------------------------------------
    sig_payload = {
        "type": "LWS_FILM_BASE_MISMATCH",
        "so": int(sordernum),
        "so_base": str(so_base_itemcode or ""),
        "expected_16p4": expected_16p4_exact,
        "invalid_printed": invalid_printed,
        "printed_found": printed_found,
        "expected_1600": expected_1600,
    }
    sig_raw = json.dumps(sig_payload, sort_keys=True)
    new_sig = hashlib.sha1(sig_raw.encode("utf-8", errors="ignore")).hexdigest()

    existing_sig, sent_ts = get_printed_film_mismatch_sig(sqlite_conn, int(sordernum))

    # Build QSB-style HTML (nice format)
    html_doc = _build_film_mismatch_email_html_qsb_style(
        house_email_wrap_func=house_email_wrap_func,
        sordernum=int(sordernum),
        job_code="—",  # LWS doesn’t need job code in this check; you can pass job_p4 later if desired
        base_itemcode=str(core),
        expected_printed=expected_printed,
        invalid_printed=invalid_printed,
        all_printed=printed_found,
        expected_1600=expected_1600,
    )

    if existing_sig == new_sig:
        if logger:
            logger.info(
                f"[FILM VALIDATION EMAIL] Duplicate suppressed for SO={sordernum} "
                f"(sent_ts={sent_ts}) sig={existing_sig}"
            )
    else:
        # ✅ send once per signature
        if order_fulfillment_emails:
            send_email(
                order_fulfillment_emails,
                f"🚨 [LWS] Printed Film Mismatch (SO {sordernum})",
                html_doc,
            )
            set_printed_film_mismatch_sig(
                sqlite_conn,
                int(sordernum),
                new_sig,
                datetime.now().isoformat(timespec="seconds"),
            )
            if logger:
                logger.info(f"[FILM VALIDATION EMAIL] Sent mismatch email for SO={sordernum}")
        else:
            if logger:
                logger.warning("[FILM VALIDATION EMAIL] No recipients configured; email not sent.")

    raise RuntimeError(summary)


def _build_film_mismatch_email_html_qsb_style(
    *,
    house_email_wrap_func,
    sordernum: int,
    job_code: str,
    base_itemcode: str,
    expected_printed: List[str],
    invalid_printed: List[str],
    all_printed: List[str],
    expected_1600: str,
) -> str:
    """
    QSB-style: Expected / Invalid / Full list, plus LWS-specific expected 1600 sell item.
    """

    def _li_bold(items: List[str]) -> str:
        return "".join(f"<li style='margin-bottom:4px;'><b>{x}</b></li>" for x in items) or "<li>—</li>"

    def _li(items: List[str]) -> str:
        return "".join(f"<li style='margin-bottom:4px;'>{x}</li>" for x in items) or "<li>—</li>"

    body_html = f"""
    <div style="font-family:Segoe UI,Arial,sans-serif; max-width:900px;">
      <p style="font-size:14px; margin:0 0 10px;">
        The LWS workflow detected a <b>Printed Film mismatch</b> during <b>Phase 1</b>.
        The workflow stopped <b>before creating the PolyTex PO / StarPak SO</b>.
      </p>

      <table style="border-collapse:collapse;width:100%;font-size:14px;margin-top:10px;">
        <tr style="background:#fef2f2;">
          <td style="padding:8px;border:1px solid #ddd;"><b>PolyTex SO</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{sordernum}</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #ddd;"><b>Job</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{job_code}</td>
        </tr>
        <tr style="background:#f9fafb;">
          <td style="padding:8px;border:1px solid #ddd;"><b>Base Item (CORE)</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{base_itemcode}</td>
        </tr>
        <tr>
          <td style="padding:8px;border:1px solid #ddd;"><b>Expected StarPak SO Item (Sell)</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{expected_1600}</td>
        </tr>
      </table>

      <h3 style="margin-top:16px;">✅ Expected Printed Film Items (Buy from StarPak)</h3>
      <ul>{_li_bold(expected_printed)}</ul>

      <h3 style="margin-top:16px;color:#b91c1c;">❌ Invalid Printed Film Items Found in PV_Req</h3>
      <ul>{_li_bold(invalid_printed)}</ul>

      <h3 style="margin-top:16px;">PV_Req Printed Film Items (Full List)</h3>
      <ul style="color:#555;">{_li(all_printed)}</ul>

      <p style="margin-top:16px;color:#555;">
        Please correct the Production Estimate / PV_Req requirements so the printed film item is exactly
        <b>16P4-{base_itemcode}</b>, then re-run the order.
      </p>
    </div>
    """

    return house_email_wrap_func(
        title="LWS Workflow ERROR — Printed Film Mismatch (Phase 1)",
        intro_html="Printed film must match the PolyTex SO base item. LWS does not use 1600-XA gusset/side panel items.",
        body_html=body_html,
        footer_html="This alert will only send once per unique mismatch condition for this SO.",
    )
