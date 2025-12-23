import os
from datetime import datetime, timezone
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ENV = os.getenv("ENV", "TEST").upper()

DEFAULTS = {
    "TEST": {
        "API_URL": "http://FSMRATEST2:8081/radadapter/radius/api",
        "DSN": "Radius_Test64",
        "DB_USER": "odbcuser",
        "DB_PASS": "odbcpass",
    },
    "LIVE": {
        "API_URL": "http://fsmradius:8081/radadapter/radius/api",
        "DSN": "Radius_Live64",
        "DB_USER": "vision",
        "DB_PASS": "vision",
    }
}

cfg = DEFAULTS["LIVE"] if ENV == "LIVE" else DEFAULTS["TEST"]

API_URL  = os.getenv("RADIUS_API_URL", cfg["API_URL"])
DSN      = os.getenv("DSN", cfg["DSN"])
DB_USER  = os.getenv("DB_USER", cfg["DB_USER"])
DB_PASS  = os.getenv("DB_PASS", cfg["DB_PASS"])

# Workflow behavior
RUN_EVERY_MINUTES = int(os.getenv("RUN_EVERY_MINUTES", "30"))
ELIGIBLE_LOOKBACK_MINUTES = int(os.getenv("ELIGIBLE_LOOKBACK_MINUTES", "120"))  # buffer window
MAX_ORDERS_PER_RUN = int(os.getenv("MAX_ORDERS_PER_RUN", "200"))

# --- JOB REQUIREMENT SQL (Option B) ---
# Change table/columns here if your Radius schema differs.
JOB_REQ_SQL = os.getenv("JOB_REQ_SQL", """
SELECT
    a."RequirementId" AS RequirementId,
    a."JobCode"       AS JobCode,
    a."ItemCode"      AS ItemCode,
    a."RequiredQty"   AS RequiredQty,
    a."RequiredDate"  AS RequiredDate,
    a."ReqStatus"     AS ReqStatus,
    a."ReqGroupCode"  AS ReqGroupCode,
    a."POResQty"      AS POResQty,
    a."InProdResQty"  AS InProdResQty,
    a."SOrderNum"     AS SOrderNum,
    a."SOrderLineNum" AS SOrderLineNum,
    a.DimA
FROM "PUB"."PV_Req" a
WHERE a."CompNum" = 2
  AND a."PlantCode" = '4'
  AND a."JobCode" = ?
  AND a."ReqGroupCode" = 'P4-FILM'
  AND a."ReqStatus" IN (10, 11, 20, 21)
  AND COALESCE(a."POResQty",0) < 1
  AND COALESCE(a."InProdResQty",0) < 1
  AND a."RequiredQty" > 0
ORDER BY a."RequiredDate" ASC
""").strip()


# Email recipients
CSR_EMAILS = [e.strip() for e in os.getenv(
    "CSR_EMAILS",
    "ukalidas@fivestarmanagement.com, mbravo@fivestarmanagement.com"
).split(",") if e.strip()]

ADMIN_EMAILS = [e.strip() for e in os.getenv(
    "ADMIN_EMAILS",
    "ukalidas@fivestarmanagement.com, mbravo@fivestarmanagement.com"
).split(",") if e.strip()]

EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.office365.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", 587)),
    "smtp_username": os.getenv("SMTP_USERNAME", "smtpo365@fivestarmanagement.com"),
    "smtp_password": os.getenv("SMTP_PASSWORD", "00FSM@email00"),  # set via ENV
    "from_addr": os.getenv("FROM_EMAIL", "smtpo365@fivestarmanagement.com"),
}

# ---------------- Paths (stable, absolute) ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Logging
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.getenv("LOG_FILE", "lws_workflow.log")

# Local state DB (SQLite) - ALWAYS under project folder
STATE_DB_PATH = os.path.join(BASE_DIR, "state.db")


# -------------- DB Helpers --------------
def get_db_conn() -> pyodbc.Connection:
    return pyodbc.connect(
        f"DSN={DSN};UID={DB_USER};PWD={DB_PASS}",
        autocommit=False,
        timeout=30,
    )

def get_readonly_conn() -> pyodbc.Connection:
    return pyodbc.connect(
        f"DSN={DSN};UID={DB_USER};PWD={DB_PASS}",
        autocommit=True,
        timeout=30,
    )

# -------------- HTTP Session --------------
SESSION = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=2.0,
    status_forcelist=[502, 503, 504],
    allowed_methods=["POST"],
)
SESSION.mount("http://", HTTPAdapter(max_retries=retries))
SESSION.mount("https://", HTTPAdapter(max_retries=retries))

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
