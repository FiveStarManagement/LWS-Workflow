USE VisionIIProd;
GO

CREATE OR ALTER TRIGGER dbo.trg_LWS_NewOrderHeader
ON dbo.PV_SOrder
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.LWS_Workflow_Outbox (ChangeType, CompNum, PlantCode, SOrderNum, SOrderLineNum)
    SELECT
        'NEW_ORDER',
        i.CompNum,
        i.PlantCode,
        i.SOrderNum,
        NULL
    FROM inserted i
    WHERE i.CompNum = 2
      AND i.PlantCode = '4'
      AND i.SOSourceCode = 'LWS'
      AND i.SOrderDate >= '2025-12-27';   -- ✅ hardcoded for now (you can change later)
END
GO
