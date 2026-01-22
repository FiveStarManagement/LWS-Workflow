#models.py
from dataclasses import dataclass, field
from typing import Optional, List, Dict

@dataclass
class ApiDecodeResult:
    status_code: Optional[int]
    entity: str
    raw_error: str
    messages: List[str] = field(default_factory=list)
    decoded_payload: Optional[dict] = None

@dataclass
class OrderState:
    sordernum: int
    status: str               # NEW/IN_PROGRESS/FAILED/COMPLETE
    last_step: str            # ELIGIBLE/JOB_P4/PO_P4/SO_P2/JOB_P2
    job_p4: Optional[str] = None
    po_p4: Optional[int] = None
    so_p2: Optional[int] = None
    job_p2: Optional[str] = None
    last_error_summary: Optional[str] = None
    last_api_entity: Optional[str] = None
    last_api_status: Optional[int] = None
    last_api_messages: Optional[List[str]] = None
