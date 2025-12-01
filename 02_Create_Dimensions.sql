-- 1. Dim_Date
CREATE TABLE dbo.Dim_Date (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,

    DayNumberOfWeek TINYINT NOT NULL,
    DayName VARCHAR(10) NOT NULL,

    DayNumberOfMonth TINYINT NOT NULL,
    DayNumberOfYear SMALLINT NOT NULL,
    WeekNumberOfYear TINYINT NOT NULL,

    MonthName VARCHAR(20) NOT NULL,
    MonthNumber TINYINT NOT NULL,

    Quarter TINYINT NOT NULL,
    QuarterName VARCHAR(10) NOT NULL,
    Year SMALLINT NOT NULL,

    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL,
    HolidayName VARCHAR(100) NULL,

    AcademicYear VARCHAR(9) NULL,
    Semester TINYINT NULL
);
GO

-- 2. Dim_Unit
CREATE TABLE dbo.Dim_Unit (
    UnitKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaUnit VARCHAR(100) NOT NULL
);
GO

-- 3. Dim_Gedung
CREATE TABLE dbo.Dim_Gedung (
    GedungID INT IDENTITY(1,1) PRIMARY KEY,
    NamaGedung VARCHAR(100) NOT NULL
);
GO

-- 4. Dim_RoomType
CREATE TABLE dbo.Dim_RoomType (
    RoomTypeID INT IDENTITY(1,1) PRIMARY KEY,
    NamaTipeRuang VARCHAR(100) NOT NULL
);
GO

-- 5. Dim_Room
CREATE TABLE dbo.Dim_Room (
    RoomKey INT IDENTITY(1,1) PRIMARY KEY,
    RoomTypeID INT NOT NULL,
    RoomCode VARCHAR(30) NOT NULL,
    NamaRuang VARCHAR(100) NOT NULL,
    Kapasitas INT NOT NULL,
    GedungID INT NOT NULL,

    FOREIGN KEY (RoomTypeID) REFERENCES dbo.Dim_RoomType(RoomTypeID),
    FOREIGN KEY (GedungID) REFERENCES dbo.Dim_Gedung(GedungID)
);
GO

-- 6. Dim_ItemType
CREATE TABLE dbo.Dim_ItemType (
    ItemTypeID INT IDENTITY(1,1) PRIMARY KEY,
    NamaJenisItem VARCHAR(100) NOT NULL
);
GO


-- 7. Dim_KondisiItem
CREATE TABLE dbo.Dim_KondisiItem (
    KondisiID INT IDENTITY(1,1) PRIMARY KEY,
    Kondisi VARCHAR(50) NOT NULL
);
GO

-- 8. Dim_Item
CREATE TABLE dbo.Dim_Item (
    ItemKey INT IDENTITY(1,1) PRIMARY KEY,
    ItemTypeID INT NOT NULL,
    NamaItem VARCHAR(100) NOT NULL,
    KondisiID INT NOT NULL,
    CurrentRoomKey INT NULL,

    FOREIGN KEY (ItemTypeID) REFERENCES dbo.Dim_ItemType(ItemTypeID),
    FOREIGN KEY (KondisiID) REFERENCES dbo.Dim_KondisiItem(KondisiID),
    FOREIGN KEY (CurrentRoomKey) REFERENCES dbo.Dim_Room(RoomKey)
);
GO

