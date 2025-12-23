from typing import List
from db import rquery
from logger import get_logger

log = get_logger("eligibility")

def find_eligible_sorders(conn, since_ts: str, limit: int) -> List[int]:
    sql = """
    SELECT DISTINCT so."SOrderNum" AS SOrderNum
    FROM "PUB"."PV_SOrder" so
    JOIN "PUB"."PV_SOrderLine" sol
      ON so."CompNum" = sol."CompNum"
     AND so."PlantCode" = sol."PlantCode"
     AND so."SOrderNum" = sol."SOrderNum"
    JOIN "PUB"."PM_Item" it
      ON it."CompNum" = so."CompNum"
     AND it."ItemCode" = sol."ItemCode"
    WHERE so."CompNum" = 2
      AND so."PlantCode" = '4'
      AND so."SOSourceCode" = 'LWS'
      AND it."ProdGroupCode" = 'P4-LWS'
      AND it."ItemStatusCode" = 'APP'
      AND so."LastUpdatedDateTime" >= ?
    ORDER BY so."SOrderNum" DESC
    """
    rows = rquery(conn, sql, (since_ts,))
    sorders = [int(r["SOrderNum"]) for r in rows[:limit]]
    log.info(f"Eligible LWS SOs since {since_ts}: {len(sorders)}")
    return sorders
