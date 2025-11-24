-- 9. Fact_RoomUsage
CREATE TABLE dbo.Fact_RoomUsage (
    RoomUsageKey BIGINT IDENTITY(1,1) PRIMARY KEY,

    DateKey INT NOT NULL,
    RoomKey INT NOT NULL,
    UnitKey INT NOT NULL,

    DurationMinutes INT NOT NULL,
    SessionType VARCHAR(50) NOT NULL,

    CONSTRAINT FK_RoomUsage_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey),

    CONSTRAINT FK_RoomUsage_Room
        FOREIGN KEY (RoomKey) REFERENCES dbo.Dim_Room(RoomKey),

    CONSTRAINT FK_RoomUsage_Unit
        FOREIGN KEY (UnitKey) REFERENCES dbo.Dim_Unit(UnitKey)
);
GO

-- 10. Fact_Repair
CREATE TABLE dbo.Fact_Repair (
    RepairKey BIGINT IDENTITY(1,1) PRIMARY KEY,

    ItemKey INT NOT NULL,
    Status VARCHAR(50) NOT NULL,
    DaysToComplete INT NULL,
    DateKey INT NOT NULL,

    CONSTRAINT FK_Repair_Item
        FOREIGN KEY (ItemKey) REFERENCES dbo.Dim_Item(ItemKey),

    CONSTRAINT FK_Repair_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey)
);
GO

-- 11. Fact_FacilityRequest
CREATE TABLE dbo.Fact_FacilityRequest (
    FacilityReqKey BIGINT IDENTITY(1,1) PRIMARY KEY,

    DateKey INT NOT NULL,
    UnitKey INT NOT NULL,

    JenisPermintaan VARCHAR(200) NOT NULL,
    Prioritas VARCHAR(20) NOT NULL,
    Status VARCHAR(50) NOT NULL,

    CONSTRAINT FK_FacReq_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey),

    CONSTRAINT FK_FacReq_Unit
        FOREIGN KEY (UnitKey) REFERENCES dbo.Dim_Unit(UnitKey)
);
GO

CREATE TABLE dbo.Fact_ItemMovement (
    MovementID BIGINT IDENTITY(1,1) PRIMARY KEY,

    ItemKey INT NOT NULL,
    DariRuangan INT NULL,
    KeRuangan INT NULL,
    TglMutasi DATE NOT NULL,

    CONSTRAINT FK_Movement_Item
        FOREIGN KEY (ItemKey) REFERENCES dbo.Dim_Item(ItemKey),

    CONSTRAINT FK_Movement_Dari
        FOREIGN KEY (DariRuangan) REFERENCES dbo.Dim_Room(RoomKey),

    CONSTRAINT FK_Movement_Ke
        FOREIGN KEY (KeRuangan) REFERENCES dbo.Dim_Room(RoomKey)
);
GO
