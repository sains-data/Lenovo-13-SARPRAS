-- Staging Schema
CREATE SCHEMA stg;
GO

CREATE TABLE stg.Dim_Date (
    DateKey INT,
    FullDate DATE,

    DayNumberOfWeek TINYINT,
    DayName VARCHAR(10),

    DayNumberOfMonth TINYINT,
    DayNumberOfYear SMALLINT,
    WeekNumberOfYear TINYINT,

    MonthName VARCHAR(20),
    MonthNumber TINYINT,

    Quarter TINYINT,
    QuarterName VARCHAR(10),
    Year SMALLINT,

    IsWeekend BIT,
    IsHoliday BIT,
    HolidayName VARCHAR(100),

    AcademicYear VARCHAR(9),
    Semester TINYINT,

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_Unit (
    NamaUnit VARCHAR(200),
    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO


GO

CREATE TABLE stg.Dim_Gedung (
    GedungID INT,
    NamaGedung VARCHAR(200),
    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_RoomType (
    RoomTypeID INT,
    NamaTipeRuang VARCHAR(200),
    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_Room (
    RoomKey INT,
    RoomTypeID INT,
    RoomCode VARCHAR(50),
    NamaRuang VARCHAR(200),
    Kapasitas INT,
    GedungID INT,

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_ItemType (
    ItemTypeID INT,
    NamaJenisItem VARCHAR(200),
    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_KondisiItem (
    KondisiID INT,
    Kondisi VARCHAR(50),

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Dim_Item (
    ItemKey INT,
    ItemTypeID INT,
    NamaItem VARCHAR(200),
    KondisiID INT,
    CurrentRoomKey INT,

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Fact_RoomUsage (
    DateKey INT,
    RoomKey INT,
    UnitKey INT,

    DurationMinutes INT,
    SessionType VARCHAR(50),

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Fact_Repair (
    ItemKey INT,
    Status VARCHAR(50),
    DaysToComplete INT,
    DateKey INT,

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Fact_FacilityRequest (
    DateKey INT,
    UnitKey INT,

    JenisPermintaan VARCHAR(200),
    Prioritas VARCHAR(20),
    Status VARCHAR(50),

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE stg.Fact_ItemMovement (
    ItemKey INT,
    DariRuangan INT,
    KeRuangan INT,
    TglMutasi DATE,

    SourceSystem VARCHAR(50),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO
