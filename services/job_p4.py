# lws_workflow/services/job_p4.py

from typing import Optional
import html

from api import send_post_request, decode_generic, b64_json
from db import rquery
from logger import get_logger


log = get_logger("job_p4")


class JobHold(Exception):
    """Use when AOP returns a meaningful reason but no job is created."""
    pass

def _raise_aop_error(
    *,
    exc_cls,
    message: str,
    resp,
    decoded,
    decoded_payload=None,
):
    """
    Raise an exception that carries raw API response + decoded details so fail_order()
    can always include them in the admin email.
    """
    ex = exc_cls(message)

    # Consistent fields that fail_order() knows how to use
    ex.api_entity = "AdvancedOrderProcessing"
    ex.api_status = getattr(decoded, "status_code", None)
    ex.api_messages = getattr(decoded, "messages", None)

    # If decode_generic exposes envelope error message, capture it too
    env_err = getattr(decoded, "error_message", None)
    if env_err:
        ex.api_error_message = str(env_err)

    # Raw response text (best for email)
    try:
        ex.raw_response_text = resp.text
    except Exception:
        ex.raw_response_text = str(resp)

    # Optional: attach decoded payload if caller passes it
    if decoded_payload is not None:
        ex.decoded_payload = decoded_payload

    raise ex



def find_existing_job_p4(conn, sordernum: int) -> Optional[str]:
    sql = """
    SELECT "SOrderNum" AS SOrderNum, "JobCode" AS JobCode
    FROM "PUB"."PV_JobSOLink"
    WHERE "SOrderNum" = ?
      AND "CompNum" = 2
      AND "PlantCode" = '4'
      AND "SOPlantCode" = '4'
    ORDER BY "TableRecId" DESC
    """
    rows = rquery(conn, sql, (sordernum,))
    if rows:
        jc = rows[0].get("JobCode") or rows[0].get("JOBCODE") or rows[0].get("jobcode")
        log.info(f"Existing Plant4 job found for SO {sordernum}: {jc}")
        return str(jc) if jc else None
    return None


def create_job_p4(sordernum: int, customer: str, logger) -> str:
    payload = {
        "AdvancedGroupingParameters": {
            "UserCode": "Radius",
            "GroupingMode": 2,
            "ShowLoadingMessages": False
        },
        "OrderProcessingLoadCriteria": [
            {"CompNum": 2, "SOPlantCode": "4", "SOrderNum": int(sordernum)}
        ]
    }

    log.info(f"Creating Plant4 job for SO {sordernum}")

    resp = send_post_request("AdvancedOrderProcessing", b64_json(payload), logger)
    decoded = decode_generic(resp)

    # Always log decoded payload for troubleshooting
    log.debug(
        "Decoded AdvancedOrderProcessing response for Plant4 SO %s: %s",
        sordernum,
        decoded.decoded_payload
    )

    dp = decoded.decoded_payload or {}
    out = dp.get("Output", {}) or {}
    results = out.get("Results", []) or []

    # No Results at all -> build a meaningful fallback
    if not results:
        req_total = (out.get("Requirements", {}) or {}).get("Total")
        grp = out.get("Groups", {}) or {}
        grp_total = grp.get("Total")
        grp_succ = grp.get("Successful")
        grp_fail = grp.get("Failed")
        aop_status = (dp.get("AdvancedOrderProcessing", {}) or {}).get("Status")

        _raise_aop_error(
            exc_cls=RuntimeError,
            message=(
                "JOB_P4 did not produce a Job Code. "
                "AdvancedOrderProcessing returned no Results. "
                f"(efiStatusCode={decoded.status_code}, AOPStatus={aop_status}, "
                f"RequirementsTotal={req_total}, GroupsTotal={grp_total}, "
                f"GroupsSuccessful={grp_succ}, GroupsFailed={grp_fail})"
            ),
            resp=resp,
            decoded=decoded,
            decoded_payload=dp,
        )

    r0 = results[0]

    # If AOP gives Errors, use it (this is the meaningful admin message you want)
    errors = (
        r0.get("Errors")
        or r0.get("Error")
        or r0.get("errors")
        or ""
    )
    if errors:
        # Preserve the nice "meaningful reason" behavior (HOLD), but include API response for email/debug
        _raise_aop_error(
            exc_cls=JobHold,
            message=str(errors).strip(),
            resp=resp,
            decoded=decoded,
            decoded_payload=dp,
        )


    # Success if Job Code present
    job_code = (
        r0.get("Job Code")
        or r0.get("JobCode")
        or r0.get("jobcode")
    )
    if not job_code:
        _raise_aop_error(
            exc_cls=RuntimeError,
            message=(
                "JOB_P4 did not produce a Job Code. "
                f"Results[0] present but Job Code missing. Keys={list(r0.keys())}"
            ),
            resp=resp,
            decoded=decoded,
            decoded_payload=dp,
        )

    log.info(f"Plant4 job created for SO {sordernum}: JobCode={job_code}")
    return str(job_code)
