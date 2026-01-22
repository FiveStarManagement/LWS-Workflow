# LWS Workflow – Technical Documentation

**Version:** 2.0  
**Last Updated:** January 21, 2026  
**Environment:** TEST / LIVE (configurable)

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Phase 1: Initial Order Processing](#phase-1-initial-order-processing)
5. [Phase 2: Change Management](#phase-2-change-management)
6. [Configuration](#configuration)
7. [Database Schema](#database-schema)
8. [API Integration](#api-integration)
9. [Error Handling](#error-handling)
10. [Monitoring & Administration](#monitoring--administration)
11. [Deployment](#deployment)
12. [Troubleshooting](#troubleshooting)
13. [Appendix](#appendix)

---

## System Overview

### Purpose

The LWS Workflow automates the end-to-end order fulfillment process for Liquid Water Solutions (LWS) orders across two manufacturing plants:

- **Plant 4 (PolyTex)**: Receives customer orders, creates jobs for printed film production
- **Plant 2 (StarPak)**: Manufactures Printed Films goods from StarPak

### Key Capabilities

- **Automated Order Processing**: Converts Plant 4 sales orders into Plant 2 purchase orders and sales orders
- **Item Creation**: Automatically creates derivative items (16P4-* and 1600-* variants) with proper pricing
- **Change Management**: Detects and handles quantity, date, and reference changes across the order lifecycle
- **Hold Management**: Implements multi-stage approval gates with automated reminders and escalation
- **Error Recovery**: Captures detailed error information and enables targeted retries

### Business Value

- **Reduced Manual Work**: Eliminates repetitive data entry across two ERP systems
- **Faster Turnaround**: Automated processing reduces order fulfillment time from hours to minutes
- **Error Reduction**: Enforces business rules and validates data consistency automatically
- **Audit Trail**: Complete history of all order changes and workflow decisions

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    LWS WORKFLOW SYSTEM                      │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼───────┐   ┌────────▼────────┐   ┌───────▼────────┐
│   Scheduler   │   │   Core Engine   │   │  Admin Portal  │
│  (APScheduler)│   │    (app.py)     │   │   (Flask)      │
└───────────────┘   └─────────────────┘   └────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐   ┌────────▼────────┐   ┌──────▼────────┐
│ State Database │   │  Radius ERP DB  │   │  Radius API   │
│   (SQLite)     │   │  (Progress DB)  │   │   (REST)      │
└────────────────┘   └─────────────────┘   └───────────────┘
```

### Technology Stack

- **Python 3.10+**: Core application runtime
- **Flask**: Admin web interface
- **APScheduler**: Workflow execution scheduler
- **SQLite**: Local state and change tracking
- **PyODBC**: Radius database connectivity
- **Requests**: HTTP API client for Radius API

### File Structure

```
lws_workflow/
├── app.py                    # Main workflow engine
├── admin.py                  # Flask admin interface
├── config.py                 # Configuration management
├── db.py                     # Database helpers (SQLite + Radius)
├── logger.py                 # Logging configuration
├── emailer.py                # Email notification system
├── exceptions.py             # Custom exception classes
├── scheduler.py              # APScheduler configuration
├── services/
│   ├── eligibility.py        # Order eligibility logic
│   ├── job_p4.py             # Plant 4 job creation
│   ├── job_p2.py             # Plant 2 job creation
│   ├── polytex_po.py         # PolyTex PO creation
│   ├── polytex_po_update.py  # PolyTex PO/SO update APIs
│   ├── starpak_so.py         # StarPak SO creation
│   ├── shipreq_p2.py         # Shipping request creation
│   ├── item_creation.py      # Automated item creation
│   ├── job_requirements.py   # PV_Req queries
│   ├── phase2_qty_changes.py # Change detection logic
│   ├── phase2_custref_changes.py # Reference tracking
│   ├── film_validation_lws.py    # Film item validation
│   └── hold_reminder.py      # Hold aging & escalation
├── templates/                # Flask HTML templates
├── logs/                     # Application logs
└── state.db                  # SQLite state database
```

---

## Data Flow

### High-Level Process Flow

```
1. ELIGIBILITY CHECK
   └─> Find Plant 4 LWS orders (Source=LWS, Status=0/1/2, ProdGroup=P4-LWS)

2. ITEM GATE (Pre-Job)
   └─> Validate base item exists and is approved
   └─> Create 16P4-* and 1600-* items if missing (status=WAIT)
   └─> HOLD until CSR approves all items

3. JOB CREATION (Plant 4)
   └─> Create PolyTex job for printed film production
   └─> Pull requirements from PV_Req (P4-FILM, P4-PF groups)

4. PRINTED FILM VALIDATION
   └─> Verify 16P4-* item matches Plant 4 SO base item
   └─> HOLD and email if mismatch detected

5. POLYTEX PO CREATION
   └─> Create Plant 4 PO for printed film substrate
   └─> Link to PolyTex job

6. STARPAK SO CREATION (Plant 2)
   └─> Create Plant 2 SO for finished goods
   └─> Use 1600-* item variant
   └─> Copy CustRef from Plant 4 SO

7. SHIPPING REQUEST
   └─> Create Plant 2 shipping request
   └─> HOLD if SO lines not ready

8. JOB CREATION (Plant 2)
   └─> Create StarPak job for finished goods production
   └─> HOLD if SO is held or estimate issues

9. COMPLETION
   └─> Mark order COMPLETE in state database
   └─> Continue monitoring for changes (Phase 2)
```

### Cross-Plant Relationships

```
Plant 4 (PolyTex)              Plant 2 (StarPak)
─────────────────              ─────────────────
Sales Order 250001   ───┐
  │                     │
  ├─> Job: P4-12345    │
  │     │               │
  │     └─> PV_Req     │      Purchase Order 350001
  │           │         └───>    │
  │           │                  ├─> Sales Order 450001
  │           │                  │     │
  │           │                  │     └─> Job: P2-67890
  │           │                  │
  └─────────────────────────────┘
       (AddtCustRef link)
```

---

## Phase 1: Initial Order Processing

### Eligibility Criteria

Orders must meet ALL of the following conditions:

```sql
-- Plant 4 Sales Orders
CompNum = 2
PlantCode = '4'
SOSourceCode = 'LWS'
SOrderStat IN (0, 1, 2)  -- Authorized, Held, Credit Held
ProdGroupCode = 'P4-LWS'
EnteredDate >= LWS_ELIGIBILITY_START_DATE
```

### Item Gate Logic

The workflow enforces a multi-stage item approval gate:

#### Stage 1: Base Item Check
- Validates Plant 4 SO item exists and is approved (status=APP)
- If not approved, sets HOLD state `BASE_ITEM_WAIT`

#### Stage 2: Derivative Item Creation
- Derives item codes:
  - PolyTex substrate: `16P4-{base_itemcode}`
  - StarPak finished goods: `1600-{base_itemcode}`
- If items don't exist:
  - Creates both items via Automator service
  - Sets initial status to WAIT (requires CSR approval)
  - Triggers XLink price update (run_all=true)
  - Applies price codes immediately:
    - `16P4-*`: Sets PurchasePriceCode = ItemCode
    - `1600-*`: Sets SalesPriceCode = ItemCode
  - Sets HOLD state `ITEM_CREATE_WAIT`

#### Stage 3: Approval Verification
- Before PolyTex PO creation: Checks `16P4-*` item is APP
- Before StarPak SO creation: Checks `1600-*` item is APP
- HOLDs at respective gates if not approved

### Job Creation (Plant 4)

**API Endpoint:** `POST {API_URL}/CompNum/2/PolytexJob/Job`

**Payload Structure:**
```json
{
  "XLSJob": {
    "CompNum": 2,
    "PlantCode": "4",
    "Customer": "LWS",
    "SOrderNum": 250001,
    "SOrderLine": [
      {"SOrderLineNum": 1},
      {"SOrderLineNum": 2}
    ]
  }
}
```

**Hold Conditions:**
- If Plant 4 SO is on hold (status 1 or 2)
- If API returns job hold status
- Sets state `JOB_P4_HOLD`

### Requirement Fetching

**Query:** `JOB_REQ_SQL` (configurable in config.py)

**Default Filters:**
```sql
ReqGroupCode IN ('P4-PF', 'P4-FILM')
ReqStatus IN (10, 11, 20, 21)  -- Open requirements only
POResQty < 1  -- No existing PO reservation
InProdResQty < 1  -- No in-production reservation
RequiredQty > 0
```

**Key Fields:**
- `RequirementId`: Unique identifier (used in Phase 2 snapshots)
- `ItemCode`: Substrate or film item (16P4-* prefix expected)
- `RequiredQty`: Quantity needed
- `RequiredDate`: Date required at Plant 4
- `ReqGroupCode`: P4-FILM or P4-PF
- `DimA`: Width dimension (inches)

### Printed Film Validation

**Purpose:** Prevent incorrect film from being ordered for a job

**Logic:**
```python
# Extract base item from Plant 4 SO line
so_base = "WIDGET-123"

# Extract base from requirement item (16P4-WIDGET-456)
req_base = "WIDGET-456"

# Validate they match
if so_base != req_base:
    # Email order fulfillment team
    # HOLD order at PRINTED_FILM_BASE_MISMATCH
    # Deduplicate emails by signature hash
```

**Email Recipients:** `FULFILLMENT_EMAILS` (config.py)

### PolyTex PO Creation

**API Endpoint:** `POST {API_URL}/CompNum/2/POrder/POrder`

**Payload Structure:**
```json
{
  "XLSOrder": {
    "CompNum": 2,
    "PlantCode": "4",
    "Vendor": "STARPAK",
    "AddtCustRef": "P4-12345",  // Job code
    "XLSOrderLine": [
      {
        "Action": "Add",
        "POLineNum": 1,
        "ItemCode": "16P4-WIDGET-123",
        "OrderedQty": 5000,
        "RequiredDate": "2026-02-05",
        "DimA": 12.5
      }
    ]
  }
}
```

**Status Updates:**
- Initial status: Open (0)
- After StarPak SO creation: Confirmed (2)

### StarPak SO Creation

**API Endpoint:** `POST {API_URL}/CompNum/2/SOrder/SOrder`

**Payload Structure:**
```json
{
  "XLSOrder": {
    "CompNum": 2,
    "PlantCode": "2",
    "Customer": "POLYTEX",
    "AddtCustRef": "350001",  // PO number
    "CustRef": "Customer PO#",  // From Plant 4 SO
    "XLSOrderLine": [
      {
        "Action": "Add",
        "SOrderLineNum": 1,
        "ItemCode": "1600-WIDGET-123",
        "OrderedQty": 5000,
        "RequiredDate": "2026-02-05",
        "SOItemTypeCode": "FG"  // From Plant 4 SO
      }
    ]
  }
}
```

**Special Handling:**
- API bug creates SO as credit held (status=2)
- Workflow forces authorization (status=0) via direct SQL:
  ```sql
  UPDATE pub.pv_sorder 
  SET sorderstat = 0 
  WHERE compnum = 2 AND plantcode = '2' AND sordernum = ?
  ```

**Hold Conditions:**
- If StarPak SO remains held/credit-held after force authorize
- Sets state `SO_P2_STATUS_HOLD`

### Shipping Request Creation

**Query:** Finds open SO lines for Plant 2 SO

**API Endpoint:** `POST {API_URL}/CompNum/2/ShipRequest/ShipRequest`

**Payload:**
```json
{
  "XLSShipRequest": {
    "CompNum": 2,
    "PlantCode": "2",
    "SOrderNum": 450001,
    "ShipRequestLine": [
      {"SOrderLineNum": 1, "RequestedQty": 5000}
    ]
  }
}
```

**Hold Conditions:**
- If no SO lines exist yet (timing issue)
- Sets state `SHIPREQ_P2_WAIT_LINES`

### Job Creation (Plant 2)

**API Endpoint:** `POST {API_URL}/CompNum/2/StarpakJob/Job`

**Payload:**
```json
{
  "XLSJob": {
    "CompNum": 2,
    "PlantCode": "2",
    "Customer": "LWS",
    "SOrderNum": 450001,
    "SOrderLine": [{"SOrderLineNum": 1}]
  }
}
```

**Hold Conditions:**
- If StarPak SO is on hold
- If API cannot determine valid estimate
- Sets state `JOB_P2_SO_ON_HOLD`

---

## Phase 2: Change Management

### Overview

Phase 2 monitors completed orders for changes and propagates updates across systems. It runs continuously alongside Phase 1 processing.

### Phase 2A: Plant 4 Quantity Changes

**Trigger:** Change to `OrderedQty` on Plant 4 SO line

**Detection Logic:**
```python
# Compare current SO line against snapshot
current_qty = so_line["OrderedQty"]
snapshot_qty = snapshot["orderedqty"]

if current_qty != snapshot_qty:
    # Email CSR team
    # Update snapshot
    # HOLD order at SO4_QTY_CHANGED_WAIT_RECONFIRM
```

**Email Recipients:** `CSR_EMAILS` (config.py)

**Hold State:** `SO4_QTY_CHANGED_WAIT_RECONFIRM`

**Resolution:** CSR must reconfirm PolyTex job, which updates PV_Req quantities

### Phase 2B: Requirement Changes After Reconfirm

**Trigger:** PV_Req quantity/date changes after job reconfirm

**Detection Logic:**
```python
# Compare current PV_Req against snapshot (keyed by job + reqgroup + item)
current_qty = req["RequiredQty"]
snapshot_qty = snapshot["requiredqty"]

if current_qty != snapshot_qty:
    # Find linked PO using so4_to_po_map
    # Update PO via API
    # Update StarPak SO via API
    # Update snapshots
    # HOLD at P2_SO_QTY_UPDATED_WAIT_RECONFIRM
```

**API Calls:**
1. **Update PolyTex PO:**
   ```json
   {
     "XLPOrders": {
       "XLPOrder": [{
         "CompNum": 2,
         "POrderNum": 350001,
         "XLPOrderLine": [{
           "POrderLineNum": 1,
           "OrderedQty": 6000
         }]
       }]
     }
   }
   ```

2. **Update StarPak SO:**
   ```json
   {
     "XLSOrders": {
       "XLSOrder": [{
         "CompNum": 2,
         "PlantCode": "2",
         "SOrderNum": 450001,
         "XLSOrderLine": [{
           "SOrderLineNum": 1,
           "OrderedQty": 6000
         }]
       }]
     }
   }
   ```

**Hold States:**
- `P2_SO_QTY_UPDATED_WAIT_RECONFIRM`: Waiting for StarPak job reconfirm (increase)
- `P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM`: Quantity decreased (special handling)
- `P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED`: Manual intervention needed

**Decrease Flow Special Handling:**

When PV_Req quantity decreases:

1. PolyTex PO is updated immediately via API
2. StarPak SO and ShipReq are NOT updated (due to Radius reserved qty rules)
3. Email sent to fulfillment team with instructions
4. Order enters HOLD: `P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM`
5. Fulfillment must:
   - Update StarPak production estimate
   - Reconfirm StarPak job
6. Phase 2C detects reconfirm and updates SO/ShipReq quantities
7. Order returns to COMPLETE

### Phase 2C: StarPak Reconfirm Detection

**Trigger:** StarPak job quantity matches updated SO quantity

**Detection Logic:**
```python
# Query StarPak job requirements
job_qty = sum([req["RequiredQty"] for req in job_reqs])

# Query StarPak SO lines
so_qty = sum([line["OrderedQty"] for line in so_lines])

if job_qty == so_qty:
    # Release order back to COMPLETE
    # Update state and run_orders
```

**For Increase Flow:**
- Waits until StarPak job qty matches target qty
- Returns order to COMPLETE automatically

**For Decrease Flow:**
- Waits until StarPak job qty matches target qty
- Updates StarPak SO line qty via API
- Updates ShipReq qty via API
- Forces SO authorization
- Returns order to COMPLETE

**Completion:** Order returns to `COMPLETE` status and Phase 2A monitoring resumes

### Phase 2D: CustRef Changes

**Trigger:** Change to `CustRef` on Plant 4 SO header

**Detection Logic:**
```python
# Compare current header against snapshot
current_custref = so_header["CustRef"]
snapshot_custref = snapshot["custref"]

if current_custref != snapshot_custref:
    # Find linked StarPak SO
    # Update StarPak SO CustRef via API
    # Force authorize if needed
    # Update snapshot
    # Email fulfillment team
```

**API Call:**
```json
{
  "XLSOrders": {
    "XLSOrder": [{
      "CompNum": 2,
      "PlantCode": "2",
      "SOrderNum": 450001,
      "SOrderStat": 0,
      "CustRef": "NEW-CUST-REF-123"
    }]
  }
}
```

**Email Notification:**
- Recipients: `FULFILLMENT_EMAILS`
- Content: Notifies that CustRef changed, jobs should be reconfirmed for AutoCount labels
- Includes: PolyTex SO, StarPak SO, PO, both job numbers

**No Hold:** CustRef changes are transparent updates (no manual intervention required)

---

## Configuration

### Environment Variables

**Core Settings:**
```bash
ENV=LIVE                           # TEST or LIVE
RADIUS_API_URL=http://fsmradius:8081/radadapter/radius/api
DSN=Radius_Live64
DB_USER=vision
DB_PASS=vision
```

**Workflow Behavior:**
```bash
RUN_EVERY_MINUTES=30              # Scheduler interval
ELIGIBLE_LOOKBACK_MINUTES=120     # Eligibility window buffer
MAX_ORDERS_PER_RUN=200            # Batch size limit
REQUIRED_DATE_LEAD_DAYS=15        # Film shipping lead time
```

**Email Configuration:**
```bash
CSR_EMAILS=csr@company.com,csr2@company.com
ADMIN_EMAILS=admin@company.com
STARPAK_EMAILS=starpak@company.com
FULFILLMENT_EMAILS=fulfillment@company.com

SMTP_SERVER=smtp.office365.com
SMTP_PORT=587
SMTP_USERNAME=smtp@company.com
SMTP_PASSWORD=secret
FROM_EMAIL=noreply@company.com
```

**Logging:**
```bash
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR
LOG_FILE=lws_workflow.log
```

### config.py Key Settings

**Eligibility Start Date:**
```python
LWS_ELIGIBILITY_START_DATE = "12/26/2025"
```

**Job Requirement SQL:**
```python
JOB_REQ_SQL = """
SELECT a."RequirementId", a."JobCode", a."ItemCode", ...
FROM "PUB"."PV_Req" a
WHERE a."CompNum" = 2
  AND a."PlantCode" = '4'
  AND a."JobCode" = ?
  AND a."ReqGroupCode" IN ('P4-PF','P4-FILM')
  ...
"""
```

### Email Recipients

**CSR_EMAILS:** Receive quantity change notifications  
**ADMIN_EMAILS:** Receive failure alerts and system issues  
**STARPAK_EMAILS:** Currently unused (reserved for future)  
**FULFILLMENT_EMAILS:** Receive printed film mismatch alerts and CustRef change notifications

---

## Database Schema

### SQLite State Database

**Location:** `state.db` (project root directory)

#### Table: lws_order_state

Primary state tracking for all orders.

```sql
CREATE TABLE lws_order_state (
    sordernum INTEGER PRIMARY KEY,          -- Plant 4 SO number
    last_seen_ts TEXT,                      -- Last workflow run time
    status TEXT,                            -- NEW, IN_PROGRESS, COMPLETE, HOLD, FAILED, REMOVED
    last_step TEXT,                         -- Last workflow step completed
    last_run_id TEXT,                       -- UUID of last workflow run
    
    -- Order artifacts
    polytex_item_code TEXT,                 -- Base item from Plant 4 SO
    job_p4_code TEXT,                       -- PolyTex job code
    po_p4_num INTEGER,                      -- PolyTex PO number
    so_p2_num INTEGER,                      -- StarPak SO number
    shipreq_p2 TEXT,                        -- StarPak shipping request
    job_p2_code TEXT,                       -- StarPak job code
    custref_p4 TEXT,                        -- Customer reference from Plant 4
    
    -- Error tracking
    last_error_summary TEXT,                -- Human-readable error message
    last_api_entity TEXT,                   -- Radius API entity name
    last_api_status INTEGER,                -- HTTP status code
    last_api_error_message TEXT,            -- Radius envelope errorMessage
    last_api_messages TEXT,                 -- JSON array of error details
    last_api_raw TEXT,                      -- Full raw API response
    
    -- Failure email deduplication
    last_failed_sig TEXT,                   -- SHA1 hash of failure signature
    last_failed_email_ts TEXT,              -- Last failure email sent time
    
    -- Printed film mismatch deduplication
    printed_film_mismatch_sig TEXT,         -- SHA1 hash of mismatch signature
    printed_film_mismatch_sent_ts TEXT,     -- Last mismatch email sent time
    
    -- Hold aging & escalation
    hold_since_ts TEXT,                     -- When order entered HOLD state
    last_hold_reminder_ts TEXT,             -- Last reminder email sent
    hold_escalated_ts TEXT,                 -- When escalation email sent
    
    updated_ts TEXT                         -- Last update timestamp
);
```

**Status Values:**
- `NEW`: Newly queued (manual Run Now)
- `IN_PROGRESS`: Currently being processed
- `COMPLETE`: Successfully completed all steps
- `HOLD`: Waiting for manual intervention
- `FAILED`: Error occurred, needs admin attention
- `REMOVED`: Manually removed from workflow

**Key Step Values:**
- `ELIGIBLE`: Order passed eligibility check
- `ITEM_CREATE_WAIT`: Items created, waiting for approval
- `BASE_ITEM_WAIT`: Base item not approved
- `ITEM_WAIT_GATE_PT`: PolyTex item not approved
- `ITEM_WAIT_GATE_SP`: StarPak item not approved
- `JOB_P4_HOLD`: PolyTex job on hold
- `PRINTED_FILM_BASE_MISMATCH`: Film item mismatch detected
- `SO_P2_STATUS_HOLD`: StarPak SO held or credit held
- `SHIPREQ_P2_WAIT_LINES`: Shipping request waiting for SO lines
- `JOB_P2_SO_ON_HOLD`: StarPak job creation hold
- `SO4_QTY_CHANGED_WAIT_RECONFIRM`: Phase 2A hold (Plant 4 qty change)
- `P2_SO_QTY_UPDATED_WAIT_RECONFIRM`: Phase 2B hold (awaiting StarPak reconfirm - increase)
- `P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM`: Phase 2B qty decrease hold
- `P2_SO_QTY_UPDATED_MANUAL_COMPLETE_REQUIRED`: Phase 2B manual intervention
- `SO4_CUSTREF_UPDATED_STARPAK`: Phase 2D CustRef update
- `COMPLETE`: All steps completed successfully

#### Table: lws_order_state_archive

Archived completed orders (auto-archived after 30 days).

```sql
CREATE TABLE lws_order_state_archive (
    -- Same columns as lws_order_state
    ...,
    archived_ts TEXT                        -- Archive timestamp
);
```

#### Table: workflow_runs

Tracks each scheduler execution.

```sql
CREATE TABLE workflow_runs (
    run_id TEXT PRIMARY KEY,                -- UUID
    start_ts TEXT,                          -- Run start time
    end_ts TEXT,                            -- Run end time
    env TEXT,                               -- TEST or LIVE
    eligible_count INTEGER,                 -- Orders eligible
    processed_count INTEGER,                -- Orders processed
    failed_count INTEGER,                   -- Orders failed
    held_count INTEGER,                     -- Orders put on hold
    log_file_path TEXT                      -- Log file location
);
```

#### Table: run_orders

Tracks individual orders within each run.

```sql
CREATE TABLE run_orders (
    run_id TEXT,                            -- Links to workflow_runs
    sordernum INTEGER,                      -- Plant 4 SO number
    status TEXT,                            -- Order status at end of run
    last_step TEXT,                         -- Last step completed in this run
    updated_ts TEXT,                        -- Update timestamp
    PRIMARY KEY (run_id, sordernum)
);
```

#### Table: so4_line_snapshot

Phase 2 quantity change detection (Plant 4 SO lines).

```sql
CREATE TABLE so4_line_snapshot (
    so4_sordernum INTEGER NOT NULL,         -- Plant 4 SO number
    so4_linenum INTEGER NOT NULL,           -- SO line number
    itemcode TEXT,                          -- Item code
    orderedqty REAL,                        -- Ordered quantity
    reqdate TEXT,                           -- Required date
    updated_ts TEXT NOT NULL,               -- Snapshot timestamp
    PRIMARY KEY (so4_sordernum, so4_linenum)
);
```

#### Table: so4_header_snapshot

Phase 2 CustRef change detection (Plant 4 SO headers).

```sql
CREATE TABLE so4_header_snapshot (
    so4_sordernum INTEGER PRIMARY KEY,      -- Plant 4 SO number
    custref TEXT,                           -- Customer reference
    updated_ts TEXT NOT NULL                -- Snapshot timestamp
);
```

#### Table: req_snapshot_keyed

Phase 2 requirement change detection (PV_Req snapshots).

```sql
CREATE TABLE req_snapshot_keyed (
    jobcode TEXT NOT NULL,                  -- PolyTex job code
    reqgroupcode TEXT NOT NULL,             -- P4-FILM or P4-PF
    itemcode TEXT NOT NULL,                 -- Requirement item code
    requiredqty REAL,                       -- Required quantity
    requireddate TEXT,                      -- Required date
    updated_ts TEXT NOT NULL,               -- Snapshot timestamp
    PRIMARY KEY (jobcode, reqgroupcode, itemcode)
);
```

#### Table: so4_to_po_map

Phase 2 mapping (Plant 4 SO line → PolyTex PO line).

```sql
CREATE TABLE so4_to_po_map (
    so4_sordernum INTEGER NOT NULL,         -- Plant 4 SO number
    so4_linenum INTEGER NOT NULL,           -- SO line number
    po_num INTEGER NOT NULL,                -- PolyTex PO number
    po_linenum INTEGER NOT NULL,            -- PO line number
    created_ts TEXT NOT NULL,               -- Mapping creation time
    PRIMARY KEY (so4_sordernum, so4_linenum)
);
```

#### Table: order_change_log

Phase 2 audit log (all detected changes).

```sql
CREATE TABLE order_change_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,                            -- Workflow run ID
    so4_sordernum INTEGER,                  -- Plant 4 SO number
    so4_linenum INTEGER,                    -- SO line number (if applicable)
    change_type TEXT NOT NULL,              -- SO4_QTY_CHANGE, REQ_QTY_CHANGE, etc.
    old_value TEXT,                         -- Previous value
    new_value TEXT,                         -- New value
    details_json TEXT,                      -- Additional context (JSON)
    created_ts TEXT NOT NULL                -- Change detection timestamp
);
```

### Radius Database (Progress)

The workflow reads from and writes to several Radius ERP tables via ODBC and REST API.

**Key Tables (Read-Only Access):**
- `PUB.PV_SOrder`: Sales order headers
- `PUB.PV_SOrderLine`: Sales order lines
- `PUB.PV_POrder`: Purchase order headers
- `PUB.PV_POrderLine`: Purchase order lines
- `PUB.PV_Req`: Job requirements
- `PUB.PM_Item`: Item master
- `PUB.JM_Job`: Job master
- `PUB.JM_JobReq`: Job requirement details

**Write Operations (via API only):**
- Job creation (PolyTex and StarPak)
- PO creation/update (PolyTex)
- SO creation/update (StarPak)
- Shipping request creation/update

---

## API Integration

### Authentication

No authentication required (internal network only). API endpoints are environment-specific:

- **TEST:** `http://FSMRATEST2:8081/radadapter/radius/api`
- **LIVE:** `http://fsmradius:8081/radadapter/radius/api`

### Request Format

All API requests use POST method with JSON payloads.

**Headers:**
```
Content-Type: application/json
```

**Standard Wrapper:**
```json
{
  "efiRadiusRequest": {
    "entityName": "XLinkAPIPOrder",
    "payload": "base64_encoded_json"
  }
}
```

**Standard Response:**
```json
{
  "efiRadiusResponse": {
    "entityName": "POrder",
    "statusCode": 1,
    "errorMessage": null,
    "payload": "base64_encoded_xml_or_json"
  }
}
```

### Error Response Format

**HTTP 400/500:**
```json
{
  "efiRadiusResponse": {
    "entityName": "SOrder",
    "statusCode": 400,
    "errorMessage": "Invalid quantity",
    "payload": "base64_encoded_error_details"
  }
}
```

**Decoded Payload (typical error):**
```json
{
  "XLSOrders": {
    "XLSOrder": [{
      "ErrorMessage": "Problem with Line #1",
      "XLSOrderLine": [{
        "SOrderLineNum": 1,
        "ItemCode": "1600-WIDGET-123",
        "ErrorMessage": "Item is inactive",
        "Action": "Add"
      }]
    }]
  }
}
```

### Retry Logic

API calls use exponential backoff for transient failures:

```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retries = Retry(
    total=3,
    backoff_factor=2.0,
    status_forcelist=[502, 503, 504],
    allowed_methods=["POST"]
)
```

**Retry Schedule:**
- Attempt 1: Immediate
- Attempt 2: 2 second delay
- Attempt 3: 4 second delay
- Attempt 4: Fails permanently

### XLink API Update Endpoints

**Phase 2 quantity updates use XLink-prefixed entities:**

#### Update PolyTex PO (XLinkAPIPOrder)

```json
{
  "XLPOrders": {
    "XLPOrder": [{
      "CompNum": 2,
      "CurrCode": "USD",
      "POrderNum": 350001,
      "XLPOrderLine": [{
        "CompNum": 2,
        "ItemCode": "16P4-WIDGET-123",
        "OrderedQty": 6000,
        "POrderLineNum": 1,
        "POrderNum": 350001,
        "PlantCode": "4"
      }]
    }]
  }
}
```

#### Update StarPak SO (XLinkAPISOrder)

```json
{
  "XLSOrders": {
    "XLSOrder": [{
      "CompNum": 2,
      "PlantCode": "2",
      "SOSourceCode": "LWS",
      "CurrCode": "USD",
      "SOrderStat": 0,
      "SOrderNum": 450001,
      "XLSOrderLine": [{
        "SOrderLineNum": 1,
        "PlantCode": "2",
        "CompNum": 2,
        "SOrderNum": 450001,
        "ItemCode": "1600-WIDGET-123",
        "OrderedQty": 6000,
        "ReqDate": "2026-02-05"
      }]
    }]
  }
}
```

#### Update Shipping Request (XLinkAPIShipReq)

```json
{
  "XLShipReqs": {
    "XLShipReq": [{
      "CompNum": 2,
      "PlantCode": "2",
      "ShipReqNum": 12345,
      "ShipReqStat": 1,
      "XLShipReqLine": [{
        "CompNum": 2,
        "PlantCode": "2",
        "ShipReqNum": 12345,
        "ShipReqLineNum": 1,
        "ItemCode": "1600-WIDGET-123",
        "SOPlantCode": "2",
        "SOrderNum": 450001,
        "SOrderLineNum": 1,
        "ShipQty": 6000
      }]
    }]
  }
}
```

#### Update CustRef Only (Header Update)

```json
{
  "XLSOrders": {
    "XLSOrder": [{
      "CompNum": 2,
      "PlantCode": "2",
      "SOrderNum": 450001,
      "SOrderStat": 0,
      "CustRef": "NEW-CUSTOMER-REF-123"
    }]
  }
}
```

---

## Error Handling

### Error Classification

**1. Transient Errors (Retry Automatically)**
- Network timeouts
- HTTP 502/503/504 (server unavailable)
- Database deadlocks
- API rate limits

**2. Business Logic Errors (HOLD)**
- Item not approved (status ≠ APP)
- SO on hold (status 1 or 2)
- PO already received/closed
- Job creation validation failures
- Printed film mismatch

**3. Data Errors (FAIL + Email Admin)**
- Missing required fields
- Invalid item codes
- API payload validation failures
- Constraint violations

**4. System Errors (FAIL + Email Admin)**
- Database connection failures
- Configuration errors
- Missing environment variables
- File permission issues

### Error Handling Strategy

```python
try:
    process_one_order(ro_conn, rw_conn, run_id, sordernum)
    
except WorkflowHold as e:
    # Expected hold condition - no admin notification
    held += 1
    logger.info(f"Order {sordernum} HOLD: {e}")
    
except Exception as e:
    # Unexpected failure - notify admins
    fail_order(
        sordernum=sordernum,
        run_id=run_id,
        step=current_step,
        err=e,
        api_entity=getattr(e, 'api_entity', None),
        api_status=getattr(e, 'api_status', None),
        api_messages=getattr(e, 'api_messages', None)
    )
    failed += 1
    raise
```

### Failure Email Deduplication

**Problem:** Same error condition should not spam admins with repeated emails.

**Solution:** Failure signature hashing

```python
sig_parts = {
    "step": "JOB_P4",
    "msg": "SO is on hold",
    "api_entity": "PolytexJob",
    "api_status": 400,
    "api_err": "Sales order must be authorized"
}

sig_raw = json.dumps(sig_parts, sort_keys=True)
fail_sig = hashlib.sha1(sig_raw.encode("utf-8")).hexdigest()

# Only send email if signature changed
if last_sig != fail_sig:
    send_email(ADMIN_EMAILS, subject, html)
    mark_failure_email_sent(sordernum, fail_sig)
```

### Hold Aging & Escalation

**Purpose:** Prevent orders from sitting in HOLD indefinitely without visibility.

**Configuration:**
```python
# Reminder thresholds (days)
HOLD_REMINDER_DAYS = 3
HOLD_ESCALATION_DAYS = 7
```

**Process:**
1. **Day 0:** Order enters HOLD, `hold_since_ts` recorded
2. **Day 3:** First reminder email sent to CSR team
3. **Day 7:** Escalation email sent to management
4. **Daily:** Reminder continues until HOLD resolved

**Reminder Email Recipients:**
- Days 0-6: `CSR_EMAILS`
- Day 7+: `ADMIN_EMAILS` (escalated)

---

## Monitoring & Administration

### Admin Dashboard

**URL:** `http://server:5050/`

**Features:**
- **Run History:** Last 25 workflow executions with statistics
- **Active Holds:** Real-time view of orders awaiting manual intervention
- **Search:** Find orders by PolyTex SO, StarPak SO, PO, Job codes
- **Insights Dashboard:**
  - Today's processing stats
  - All-time completion metrics
  - Hold/Failed counts

### Order Detail View

**URL:** `http://server:5050/order/{sordernum}`

**Information Displayed:**
- Current status and last step
- Complete order chain (SO4 → PO → SO2 → Jobs)
- Error details (if failed)
- API response details (decoded payload)
- Hold aging information
- Change history

**Actions Available:**
- **Retry Next Run:** Resets order to NEW status
- **Remove:** Permanently excludes order from workflow

### Run Detail View

**URL:** `http://server:5050/run/{run_id}`

**Shows:**
- Run timestamp and duration
- Environment (TEST/LIVE)
- Order-by-order processing results
- Success/Hold/Fail breakdown

### Archived Orders

**URL:** `http://server:5050/archived`

**Purpose:** View COMPLETE orders older than 30 days (auto-archived)

**Retention:** Archives kept indefinitely for audit trail

### Manual Queue (Run Now)

**Purpose:** Force specific orders to run on next scheduler cycle

**Usage:**
1. Navigate to dashboard
2. Enter PolyTex SO number in "Run Now" box
3. Click "Queue for Next Run"

**Validation:**
- Must be valid LWS order (Plant 4, Source=LWS, ProdGroup=P4-LWS)
- Creates/updates state record with status=NEW
- Runs at front of queue on next cycle

**Use Cases:**
- Testing new orders
- Retry after fixing data issues
- Priority processing

### Database Maintenance

**Auto-Archiving:**
```python
# Runs on every scheduler cycle
archived = archive_old_complete_orders(days=30)
```

**Auto-Purging:**
```python
# Removes old run history
stats = purge_old_run_history(conn, days_old=90)
```

**Manual Cleanup (if needed):**
```sql
-- Remove test orders from state
DELETE FROM lws_order_state 
WHERE sordernum IN (250001, 250002);

-- Clear change log for specific order
DELETE FROM order_change_log 
WHERE so4_sordernum = 250001;

-- Reset all snapshots (dangerous!)
DELETE FROM so4_line_snapshot;
DELETE FROM req_snapshot_keyed;
```

### Monitoring Queries

**Active holds by category:**
```sql
SELECT 
    last_step,
    COUNT(*) as count,
    MIN(hold_since_ts) as oldest_hold
FROM lws_order_state
WHERE status = 'HOLD'
GROUP BY last_step
ORDER BY count DESC;
```

**Recent failures:**
```sql
SELECT 
    sordernum,
    last_step,
    last_error_summary,
    updated_ts
FROM lws_order_state
WHERE status = 'FAILED'
ORDER BY updated_ts DESC
LIMIT 25;
```

**Phase 2 monitoring orders:**
```sql
SELECT COUNT(*) as monitoring_count
FROM lws_order_state
WHERE status = 'COMPLETE'
  AND (last_step LIKE 'SO4_%' OR last_step LIKE 'P2_%');
```

**Today's processing stats:**
```sql
SELECT 
    status,
    COUNT(*) as count
FROM run_orders
WHERE updated_ts >= datetime('now', 'start of day')
GROUP BY status;
```

---

## Deployment

### System Requirements

**Operating System:** Windows Server 2019+ or Linux (RHEL/Ubuntu)

**Python:** 3.10 or higher

**Dependencies:**
- PyODBC with Progress ODBC driver
- SQLite 3.35+
- Access to Radius database (ODBC DSN configured)
- Network access to Radius API endpoints

**Hardware:**
- 2+ CPU cores
- 4GB+ RAM
- 10GB+ disk space (for logs and SQLite database)

### Installation Steps

**1. Install Python Dependencies**

```bash
cd lws_workflow
pip install -r requirements.txt
```

**requirements.txt:**
```
Flask==3.0.0
APScheduler==3.10.4
pyodbc==5.0.1
requests==2.31.0
python-dateutil==2.8.2
```

**2. Configure ODBC DSN**

**Windows:**
- Open ODBC Data Sources (64-bit)
- Add System DSN for Progress database
- Name: `Radius_Live64` (or `Radius_Test64`)
- Configure connection parameters

**Linux:**
```ini
# /etc/odbc.ini
[Radius_Live64]
Description = Radius Live Database
Driver = Progress OpenEdge Wire Protocol Driver
Host = fsmradius
Port = 20931
Database = radius
```

**3. Set Environment Variables**

```bash
# Windows (PowerShell)
$env:ENV = "LIVE"
$env:SMTP_PASSWORD = "your_smtp_password"

# Linux
export ENV=LIVE
export SMTP_PASSWORD=your_smtp_password
```

**4. Initialize Database**

```bash
python -c "from db import init_state_db; init_state_db()"
```

**5. Test Configuration**

```bash
# Test database connection
python -c "from config import get_readonly_conn; conn = get_readonly_conn(); print('Connection OK'); conn.close()"

# Test API connection
python -c "from config import API_URL; import requests; r = requests.get(API_URL.replace('/api', '/health')); print(r.status_code)"
```

**6. Start Services**

**Scheduler (background service):**
```bash
python scheduler.py
```

**Admin Interface:**
```bash
# Development
python admin.py

# Production (Waitress)
waitress-serve --host=0.0.0.0 --port=5050 admin:app
```

### Windows Service Installation

**Using NSSM (Non-Sucking Service Manager):**

```powershell
# Install scheduler service
nssm install LWSWorkflowScheduler "C:\Python310\python.exe" "C:\lws_workflow\scheduler.py"
nssm set LWSWorkflowScheduler AppDirectory "C:\lws_workflow"
nssm set LWSWorkflowScheduler DisplayName "LWS Workflow Scheduler"
nssm set LWSWorkflowScheduler Description "Automated LWS order processing"
nssm set LWSWorkflowScheduler Start SERVICE_AUTO_START

# Install admin service
nssm install LWSWorkflowAdmin "C:\Python310\python.exe" "-m waitress --host=0.0.0.0 --port=5050 admin:app"
nssm set LWSWorkflowAdmin AppDirectory "C:\lws_workflow"
nssm set LWSWorkflowAdmin DisplayName "LWS Workflow Admin"

# Start services
nssm start LWSWorkflowScheduler
nssm start LWSWorkflowAdmin
```

### Linux Systemd Service

**scheduler.service:**
```ini
[Unit]
Description=LWS Workflow Scheduler
After=network.target

[Service]
Type=simple
User=lws
WorkingDirectory=/opt/lws_workflow
Environment="ENV=LIVE"
Environment="SMTP_PASSWORD=secret"
ExecStart=/usr/bin/python3 /opt/lws_workflow/scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**admin.service:**
```ini
[Unit]
Description=LWS Workflow Admin Interface
After=network.target

[Service]
Type=simple
User=lws
WorkingDirectory=/opt/lws_workflow
Environment="ENV=LIVE"
ExecStart=/usr/bin/python3 -m waitress --host=0.0.0.0 --port=5050 admin:app
Restart=always

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl enable lws-scheduler
sudo systemctl enable lws-admin
sudo systemctl start lws-scheduler
sudo systemctl start lws-admin
```

### Deployment Checklist

- [ ] Python 3.10+ installed
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] ODBC DSN configured and tested
- [ ] Environment variables set correctly
- [ ] SQLite database initialized
- [ ] Email SMTP credentials validated
- [ ] Test order processed successfully
- [ ] Services installed and running
- [ ] Admin dashboard accessible
- [ ] Logs directory writable
- [ ] Firewall rules configured (port 5050 for admin)

---

## Troubleshooting

### Common Issues

#### Issue: "No module named 'pyodbc'"

**Cause:** Missing Python dependency

**Solution:**
```bash
pip install pyodbc
```

#### Issue: "Data source name not found"

**Cause:** ODBC DSN not configured

**Solution:**
```bash
# Windows: Configure via ODBC Administrator
# Linux: Check /etc/odbc.ini and /etc/odbcinst.ini

# Test connection
python -c "import pyodbc; pyodbc.connect('DSN=Radius_Live64;UID=vision;PWD=vision')"
```

#### Issue: Orders stuck in "ITEM_CREATE_WAIT"

**Cause:** Items created but CSR hasn't approved them in Radius

**Solution:**
1. Check item status in Radius:
   ```sql
   SELECT ItemCode, ItemStatusCode 
   FROM PM_Item 
   WHERE CompNum=2 AND ItemCode IN ('16P4-XXX', '1600-XXX')
   ```
2. CSR must change status from WAIT to APP
3. Workflow will auto-retry on next run

#### Issue: Orders stuck in "SO4_QTY_CHANGED_WAIT_RECONFIRM"

**Cause:** PolyTex SO quantity changed but job not reconfirmed

**Solution:**
1. CSR must update production estimate in Radius
2. Reconfirm PolyTex job
3. PV_Req will update automatically
4. Workflow detects change and proceeds to Phase 2B

#### Issue: "Printed Film Base Mismatch"

**Cause:** Production estimate uses wrong printed film item

**Solution:**
1. Check Plant 4 SO base item: `2300-WIDGET-T1`
2. Expected printed film: `16P4-2300-WIDGET-T1`
3. Update production estimate to use correct item
4. Delete incorrect PV_Req rows
5. Retry order from admin dashboard

#### Issue: Phase 2B updates fail with "PO cannot be updated"

**Cause:** PolyTex PO already received or closed

**Solution:**
- If received: Manual coordination required
- If closed: Re-open PO or create new workflow chain
- Workflow will email CSR team automatically

#### Issue: StarPak SO created as "Credit Held"

**Cause:** Known Radius API bug

**Solution:** Workflow automatically forces authorization via SQL:
```sql
UPDATE pub.pv_sorder 
SET sorderstat = 0 
WHERE sordernum = ?
```

**If still held after workflow runs:**
1. Check customer credit limit
2. Verify SO doesn't exceed limits
3. Manually authorize in Radius

#### Issue: XLink price update fails

**Cause:** XLink runner service not available

**Solution:**
```bash
# Check service status
curl http://RDSPOL-EFI02:4545/health

# Restart service if needed
# (Workflow continues even if XLink fails)
```

#### Issue: Logs growing too large

**Cause:** DEBUG logging enabled in production

**Solution:**
```python
# config.py
LOG_LEVEL = "INFO"  # Change from DEBUG

# Rotate logs manually if needed
mv logs/lws_workflow.log logs/lws_workflow.log.old
```

#### Issue: SQLite database locked

**Cause:** Multiple processes accessing database simultaneously

**Solution:**
```bash
# Check for stale locks
lsof state.db  # Linux
handle.exe state.db  # Windows

# Kill stale processes if found
# Restart scheduler service
```

#### Issue: Phase 2C not detecting reconfirm

**Cause:** StarPak job qty doesn't match target qty yet

**Solution:**
1. Verify fulfillment updated StarPak production estimate
2. Confirm StarPak job was reconfirmed in Radius
3. Check that job qty now equals target qty:
   ```sql
   SELECT RequiredQty FROM PV_Req 
   WHERE JobCode = 'P2-12345' AND ItemCode = '1600-XXX'
   ```
4. Workflow will auto-detect on next run (every 30 minutes)

#### Issue: CustRef change not syncing to StarPak

**Cause:** Phase 2D monitoring may not be running for this order

**Solution:**
1. Verify order is in COMPLETE status
2. Check that so_p2_num is populated in state:
   ```sql
   SELECT sordernum, so_p2_num, last_step 
   FROM lws_order_state WHERE sordernum = 250001
   ```
3. If so_p2_num is NULL, order never completed Phase 1
4. Manually trigger via admin dashboard if needed

### Debug Mode

**Enable verbose logging:**
```python
# config.py
LOG_LEVEL = "DEBUG"
```

**Test single order with full logging:**
```bash
python -c "
from app import process_one_order, get_readonly_conn, get_db_conn
from db import state_conn
import uuid

ro = get_readonly_conn()
rw = get_db_conn()

try:
    process_one_order(ro, rw, str(uuid.uuid4()), 250001)
finally:
    ro.close()
    rw.close()
"
```

**Check order state directly:**
```bash
sqlite3 state.db "SELECT * FROM lws_order_state WHERE sordernum=250001;"
```

### Log Analysis

**Find recent errors:**
```bash
grep "ERROR" logs/lws_workflow.log | tail -50
```

**Track specific order:**
```bash
grep "SO 250001" logs/lws_workflow.log
```

**API payload debugging:**
```bash
grep "XLink POST" logs/lws_workflow.log | grep "payload_decoded"
```

**Phase 2 debugging:**
```bash
# Find all Phase 2 activities for an order
grep "Phase2" logs/lws_workflow.log | grep "250001"

# Check snapshot updates
grep "snapshot" logs/lws_workflow.log | grep "250001"
```

### Performance Tuning

**Increase batch size (if system can handle):**
```python
# config.py
MAX_ORDERS_PER_RUN = 500  # Default: 200
```

**Reduce scheduler frequency (if load is high):**
```python
# config.py
RUN_EVERY_MINUTES = 60  # Default: 30
```

**Database optimization:**
```sql
-- Rebuild indexes
REINDEX;

-- Analyze query patterns
EXPLAIN QUERY PLAN SELECT ...;

-- Vacuum database
VACUUM;
```

---

## Appendix

### Glossary

**Term** | **Definition**
---------|---------------
**Plant 4** | PolyTex manufacturing plant (printed film production)
**Plant 2** | StarPak manufacturing plant (finished goods production)
**SO4** | PolyTex Sales Order (Plant 4)
**SO2 / SO_P2** | StarPak Sales Order (Plant 2)
**PO / PO_P4** | PolyTex Purchase Order (buying from StarPak)
**PV_Req** | Radius job requirements table
**16P4-*** | PolyTex substrate item prefix (printed film)
**1600-*** | StarPak finished goods item prefix
**Phase 1** | Initial order creation workflow
**Phase 2** | Change detection and update workflow
**Phase 2A** | Plant 4 SO quantity change detection
**Phase 2B** | Requirement change propagation (PO/SO updates)
**Phase 2C** | StarPak job reconfirm detection and completion
**Phase 2D** | CustRef change detection and sync
**HOLD** | Order waiting for manual intervention
**COMPLETE** | Order successfully processed through all steps
**WorkflowHold** | Python exception indicating expected hold condition
**XLink** | API prefix for update operations (vs initial creation)
**Snapshot** | Baseline data stored for change detection
**Reconfirm** | Updating production estimate and regenerating job requirements

### Phase 2 Flow Summary

```
Phase 2A: PT SO qty changes
  ↓ (CSR reconfirms PT job)
  ↓ (PV_Req updates)
  ↓
Phase 2B: Detect PV_Req change
  ↓ (Update PT PO via API)
  ↓ (Update SP SO via API)
  ↓ (Update ShipReq if increase)
  ↓ (Email fulfillment if decrease)
  ↓
Phase 2C: Wait for SP job reconfirm
  ↓ (Fulfillment reconfirms SP job)
  ↓ (Update SP SO/ShipReq if decrease)
  ↓ (Return to COMPLETE)
  ↓
Phase 2A: Resume monitoring
```

### Quick Reference Commands

```bash
# Check scheduler status
ps aux | grep scheduler.py

# Restart scheduler
sudo systemctl restart lws-scheduler

# View live logs
tail -f logs/lws_workflow.log

# Check database size
du -h state.db

# Count active orders
sqlite3 state.db "SELECT status, COUNT(*) FROM lws_order_state GROUP BY status;"

# Manual archive trigger
python -c "from db import archive_old_complete_orders; print(archive_old_complete_orders(30))"

# Queue order manually (command line)
python -c "
from db import upsert_order_state
upsert_order_state(250001, 'NEW', 'ELIGIBLE', last_error_summary=None, last_api_messages_json=None)
print('Order 250001 queued')
"

# Check Phase 2 monitoring count
sqlite3 state.db "SELECT COUNT(*) FROM lws_order_state WHERE status='COMPLETE' AND (last_step LIKE 'SO4_%' OR last_step LIKE 'P2_%');"

# Find orders in specific hold state
sqlite3 state.db "SELECT sordernum, updated_ts FROM lws_order_state WHERE last_step='P2_QTY_DECREASE_WAIT_SP_JOB_RECONFIRM';"
```

### Email Template Reference

**CSR Quantity Change Email (Phase 2A):**
- Subject: "PolyTex SO {so_num} qty changed – reconfirm job required"
- Recipients: CSR_EMAILS
- Action: Update estimate and reconfirm PolyTex job

**Fulfillment Decrease Email (Phase 2B):**
- Subject: "Action Required – Qty Decrease (SO {so_num} / Job {job_p2})"
- Recipients: FULFILLMENT_EMAILS
- Action: Update StarPak estimate and reconfirm job

**Fulfillment Increase Email (Phase 2B):**
- Subject: "Action Required – Complete StarPak SO {so_num}"
- Recipients: FULFILLMENT_EMAILS
- Action: Reconfirm and complete StarPak order

**Fulfillment CustRef Email (Phase 2D):**
- Subject: "[LWS] Line Number (CustRef) Updated – Reconfirm Jobs (SO4 {so_num})"
- Recipients: FULFILLMENT_EMAILS
- Action: Reconfirm both jobs for AutoCount label update

**Admin Failure Email:**
- Subject: "LWS Workflow FAILED – SO {so_num} ({step})"
- Recipients: ADMIN_EMAILS
- Action: Review error details and retry from admin dashboard

**Printed Film Mismatch Email:**
- Subject: "🚨 [LWS] Printed Film Mismatch (SO {so_num})"
- Recipients: FULFILLMENT_EMAILS
- Action: Correct production estimate item

### Support Contacts

**Development Team:**
- Email: ukalidas@fivestarmanagement.com
- Email: mbravo@fivestarmanagement.com

**Escalation Path:**
1. Check admin dashboard for error details
2. Review logs for API responses
3. Contact development team with SO number and error details
4. Provide screenshots if issue is UI-related

**After-Hours Support:**
- Critical production issues only
- Email both contacts with "URGENT" in subject line

---

**Document Version:** 2.0  
**Last Updated:** January 21, 2026  
**Maintained By:** Five Star Management Development Team

---

## Document Change Log

**Version 2.0 (January 21, 2026)**
- Complete documentation rewrite for Phase 2 implementation
- Added Phase 2A, 2B, 2C, 2D detailed workflows
- Added XLink API update endpoints
- Added decrease flow special handling
- Added CustRef change management
- Added troubleshooting for Phase 2 scenarios
- Added comprehensive database schema
- Added hold aging and escalation documentation

**Version 1.0 (December 26, 2024)**
- Initial documentation for Phase 1 workflow
- Basic setup and deployment instructions
- Core error handling procedures