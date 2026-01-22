USE VisionIIProd;
GO

CREATE OR ALTER TRIGGER dbo.trg_LWS_OrderLineQtyChange
ON dbo.PV_SOrderLine
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.LWS_Workflow_Outbox (ChangeType, CompNum, PlantCode, SOrderNum, SOrderLineNum)
    SELECT
        'QTY_CHANGE',
        i.CompNum,
        i.PlantCode,
        i.SOrderNum,
        i.SOrderLineNum
    FROM inserted i
    JOIN deleted d
      ON i.CompNum = d.CompNum
     AND i.PlantCode = d.PlantCode
     AND i.SOrderNum = d.SOrderNum
     AND i.SOrderLineNum = d.SOrderLineNum
    WHERE i.CompNum = 2
      AND i.PlantCode = '4'
      AND ISNULL(i.OrderedQty,0) <> ISNULL(d.OrderedQty,0)
      AND EXISTS (
          SELECT 1
          FROM dbo.PV_SOrder so
          WHERE so.CompNum = i.CompNum
            AND so.PlantCode = i.PlantCode
            AND so.SOrderNum = i.SOrderNum
            AND so.SOSourceCode = 'LWS'
      );
END
GO
