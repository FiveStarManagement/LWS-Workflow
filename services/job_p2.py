# lws_workflow/services/job_p2.py

from typing import Optional

from api import send_post_request, decode_generic, b64_json
from db import rquery
from logger import get_logger

log = get_logger("job_p2")


class JobHold(Exception):
    """Raised when AOP indicates the sales order is on hold (or similar hold condition)."""
    pass


def find_existing_job_p2(conn, sordernum: int) -> Optional[str]:
    sql = """
    SELECT "SOrderNum" AS SOrderNum, "JobCode" AS JobCode
    FROM "PUB"."PV_JobSOLink"
    WHERE "SOrderNum" = ?
      AND "CompNum" = 2
      AND "PlantCode" = '2'
      AND "SOPlantCode" = '2'
      ORDER BY "TableRecId" DESC
    """
    rows = rquery(conn, sql, (sordernum,))
    if rows:
        jc = rows[0].get("JobCode") or rows[0].get("JOBCODE") or rows[0].get("jobcode")
        log.info(f"Existing Plant2 job found for SO {sordernum}: {jc}")
        return str(jc) if jc else None
    return None


def _first_key(d: dict, *keys, default=None):
    """Try keys in order, then case-insensitive."""
    if not isinstance(d, dict):
        return default

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

    return default


def _aop_best_reason(dp: dict, sordernum: int) -> str:
    """
    Best possible reason when AOP doesn't produce a Job Code.
    Priority:
      1) Results[0].Errors (if present)
      2) Build fallback summary from Results/Requirements details
      3) Generic fallback
    """
    out = dp.get("Output", {}) or {}
    results = out.get("Results", []) or []

    # 1) Prefer explicit Errors
    if results and isinstance(results[0], dict):
        r0 = results[0]
        errors = _first_key(r0, "Errors", "Error", "errors", default="")
        errors = str(errors).strip() if errors is not None else ""
        if errors:
            return errors

    # 2) Build fallback summary
    parts = [f"Job Code missing and AOP returned no Errors. SO={sordernum}"]

    grp = out.get("Groups", {}) or {}
    reqs_hdr = out.get("Requirements", {}) or {}
    aop_status = (dp.get("AdvancedOrderProcessing", {}) or {}).get("Status")

    if aop_status is not None:
        parts.append(f"AOPStatus={aop_status}")

    if reqs_hdr.get("Total") is not None:
        parts.append(f"RequirementsTotal={reqs_hdr.get('Total')}")

    if grp.get("Total") is not None:
        parts.append(f"GroupsTotal={grp.get('Total')}")
    if grp.get("Successful") is not None:
        parts.append(f"GroupsSuccessful={grp.get('Successful')}")
    if grp.get("Failed") is not None:
        parts.append(f"GroupsFailed={grp.get('Failed')}")

    if results and isinstance(results[0], dict):
        r0 = results[0]
        if _first_key(r0, "Group") is not None:
            parts.append(f"Group={_first_key(r0, 'Group')}")
        if _first_key(r0, "Failed") is not None:
            parts.append(f"Failed={_first_key(r0, 'Failed')}")
        if _first_key(r0, "Total") is not None:
            parts.append(f"Total={_first_key(r0, 'Total')}")

        req_list = _first_key(r0, "Requirements", default=[])
        if isinstance(req_list, list) and req_list:
            req0 = req_list[0] if isinstance(req_list[0], dict) else None
            if req0:
                src = _first_key(req0, "Source")
                item = _first_key(req0, "Item Code", "ItemCode", "ITEMCODE")
                qty = _first_key(req0, "Quantity")

                if src:
                    parts.append(f"Requirement={src}")
                if item:
                    parts.append(f"Item={item}")
                if qty is not None:
                    parts.append(f"Qty={qty}")

    return ", ".join(parts)


def create_job_p2(sordernum: int, customer: str, logger) -> str:
    payload = {
        "AdvancedGroupingParameters": {
            "UserCode": "Radius",
            "GroupingMode": 2,
            "ShowLoadingMessages": False,
        },
        "OrderProcessingLoadCriteria": [
            {"CompNum": 2, "SOPlantCode": "2", "SOrderNum": int(sordernum)}
        ],
    }

    log.info(f"Creating Plant2 job for SO {sordernum}")

    resp = send_post_request("AdvancedOrderProcessing", b64_json(payload), logger)
    decoded = decode_generic(resp)

    dp = decoded.decoded_payload or {}
    log.debug("Decoded SO payload: %s", dp)
    log.debug("Decoded AdvancedOrderProcessing response for Plant2 SO %s: %s", sordernum, dp)

    # Success = Output.Results[0]["Job Code"] exists (AOP statusCode can be 0 even if no job)
    out = dp.get("Output", {}) or {}
    results = out.get("Results", []) or []

    if not results:
        # No results at all -> meaningful summary
        reason = _aop_best_reason(dp, sordernum)
        raise RuntimeError(f"JOB_P2 did not produce a Job Code: {reason}")

    r0 = results[0] if isinstance(results[0], dict) else {}

    # If Errors exist, prefer them
    errors = _first_key(r0, "Errors", "Error", "errors", default="")
    errors = str(errors).strip() if errors is not None else ""

    # If hold-like error, raise JobHold so app.py can treat as HOLD
    if errors and "hold" in errors.lower():
        # ✅ Make sure this message is exactly what admin should show
        raise JobHold(errors)

    job_code = _first_key(r0, "Job Code", "JobCode", "jobcode")

    if not job_code:
        # If no job code, show either Errors or fallback details
        reason = errors if errors else _aop_best_reason(dp, sordernum)
        raise RuntimeError(f"JOB_P2 did not produce a Job Code: {reason}")

    log.info(f"Plant2 job created for SO {sordernum}: JobCode={job_code}")
    return str(job_code)
