
IF OBJECT_ID('dbo.Fact_RoomUsage_Partitioned', 'U') IS NOT NULL DROP TABLE dbo.Fact_RoomUsage_Partitioned;
IF OBJECT_ID('dbo.Fact_FacilityRequest_Partitioned', 'U') IS NOT NULL DROP TABLE dbo.Fact_FacilityRequest_Partitioned;
IF OBJECT_ID('dbo.Fact_ItemMovement_Partitioned', 'U') IS NOT NULL DROP TABLE dbo.Fact_ItemMovement_Partitioned;
GO

IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_DateKey_Year') 
    DROP PARTITION SCHEME PS_DateKey_Year;
GO

IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_DateKey_Year') 
    DROP PARTITION FUNCTION PF_DateKey_Year;
GO

CREATE PARTITION FUNCTION PF_DateKey_Year (INT)
AS RANGE RIGHT FOR VALUES (2020, 2021, 2022, 2023, 2024, 2025, 2026);
GO


CREATE PARTITION SCHEME PS_DateKey_Year
AS PARTITION PF_DateKey_Year
ALL TO ([PRIMARY]);
GO

CREATE TABLE dbo.Fact_RoomUsage_Partitioned
(
    RoomUsageKey BIGINT NOT NULL,
    DateKey INT NOT NULL,
    RoomKey INT NOT NULL,
    UnitKey INT NOT NULL,
    DurationMinutes INT NOT NULL,
    SessionType VARCHAR(50) NOT NULL,

    CONSTRAINT FK_RoomUsageP_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_RoomUsageP_Room
        FOREIGN KEY (RoomKey) REFERENCES dbo.Dim_Room(RoomKey),
    CONSTRAINT FK_RoomUsageP_Unit
        FOREIGN KEY (UnitKey) REFERENCES dbo.Dim_Unit(UnitKey)
)
ON PS_DateKey_Year(DateKey);
GO

ALTER TABLE dbo.Fact_RoomUsage_Partitioned
ADD CONSTRAINT PK_Fact_RoomUsage_Partitioned
PRIMARY KEY CLUSTERED (RoomUsageKey, DateKey)
ON PS_DateKey_Year(DateKey);
GO

CREATE TABLE dbo.Fact_FacilityRequest_Partitioned
(
    FacilityReqKey BIGINT NOT NULL,
    DateKey INT NOT NULL,
    UnitKey INT NOT NULL,
    JenisPermintaan VARCHAR(200) NOT NULL,
    Prioritas VARCHAR(20) NOT NULL,
    Status VARCHAR(50) NOT NULL,

    CONSTRAINT FK_FacReqP_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_FacReqP_Unit
        FOREIGN KEY (UnitKey) REFERENCES dbo.Dim_Unit(UnitKey)
)
ON PS_DateKey_Year(DateKey);
GO

ALTER TABLE dbo.Fact_FacilityRequest_Partitioned
ADD CONSTRAINT PK_Fact_FacilityRequest_Partitioned
PRIMARY KEY CLUSTERED (FacilityReqKey, DateKey)
ON PS_DateKey_Year(DateKey);
GO

CREATE TABLE dbo.Fact_ItemMovement_Partitioned
(
    MovementID BIGINT NOT NULL,
    ItemKey INT NOT NULL,
    DariRuangan INT NULL,
    KeRuangan INT NULL,
    DateKey INT NOT NULL,
    TglMutasi DATE NOT NULL,

    CONSTRAINT FK_MovementP_Item
        FOREIGN KEY (ItemKey) REFERENCES dbo.Dim_Item(ItemKey),
    CONSTRAINT FK_MovementP_Dari
        FOREIGN KEY (DariRuangan) REFERENCES dbo.Dim_Room(RoomKey),
    CONSTRAINT FK_MovementP_Ke
        FOREIGN KEY (KeRuangan) REFERENCES dbo.Dim_Room(RoomKey),
    CONSTRAINT FK_MovementP_Date
        FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date(DateKey)
)
ON PS_DateKey_Year(DateKey);
GO

ALTER TABLE dbo.Fact_ItemMovement_Partitioned
ADD CONSTRAINT PK_ItemMovement_Partitioned
PRIMARY KEY CLUSTERED (MovementID, DateKey)
ON PS_DateKey_Year(DateKey);
GO
