from typing import List
from datetime import datetime, timezone, timedelta
from db import rquery
from logger import get_logger

from config import LWS_ELIGIBILITY_START_DATE


log = get_logger("eligibility")

def format_dt_tz(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00:00"

def find_eligible_sorders(conn, limit: int) -> List[int]:
    sql = """
    SELECT DISTINCT so."SOrderNum" AS SOrderNum
    FROM "PUB"."PV_SOrder" so
    JOIN "PUB"."PV_SOrderLine" sol
      ON so."CompNum" = sol."CompNum"
     AND so."PlantCode" = sol."PlantCode"
     AND so."SOrderNum" = sol."SOrderNum"
     AND sol."SOItemTypeCode" <> 'P4ART' 
    JOIN "PUB"."PM_Item" it
      ON it."CompNum" = so."CompNum"
     AND it."ItemCode" = sol."ItemCode"
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '4'
      AND so."SOSourceCode" = 'LWS'
      AND so."SOrderStat" IN (0,1,2)
      AND it."ProdGroupCode" = 'P4-LWS'
      --AND it."ItemStatusCode" = 'APP'
      AND so."SOrderDate" >= ?
    ORDER BY so."SOrderNum" DESC
    """

    # ✅ convert config string to Python date object
    start_date = datetime.strptime(LWS_ELIGIBILITY_START_DATE, "%m/%d/%Y").date()

    rows = rquery(conn, sql, (start_date,))
    sorders = [int(r.get("SOrderNum") or r.get("sordernum")) for r in rows[:limit]]


    log.info(f"Eligible LWS SOs since {LWS_ELIGIBILITY_START_DATE}: {len(sorders)}")
    return sorders
