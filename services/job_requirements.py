from typing import List, Dict, Any
from db import rquery
from config import JOB_REQ_SQL
from logger import get_logger

log = get_logger("job_requirements")

def get_job_requirements(conn, jobcode: str) -> List[Dict[str, Any]]:
    return rquery(conn, JOB_REQ_SQL, (jobcode,))
    # expected columns: JobCode, ItemCode, ReqQty
    return rows
