USE VisionIIProd;
GO

CREATE TABLE dbo.LWS_Workflow_Outbox (
    OutboxId BIGINT IDENTITY(1,1) PRIMARY KEY,
    ChangeType VARCHAR(30) NOT NULL,  -- NEW_ORDER / QTY_CHANGE
    CompNum INT NOT NULL,
    PlantCode VARCHAR(5) NOT NULL,
    SOrderNum INT NOT NULL,
    SOrderLineNum INT NULL,           -- null for header event
    OccurredAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    Status VARCHAR(20) NOT NULL DEFAULT 'Pending',  -- Pending / Sent
    ProcessedAt DATETIME2 NULL
);

CREATE INDEX IX_LWS_Outbox_Status ON dbo.LWS_Workflow_Outbox(Status, OutboxId);
GO
