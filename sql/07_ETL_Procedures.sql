-- Buat schema ETL 
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl');
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_Unit
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_Unit AS tgt
    USING (
        SELECT DISTINCT NamaUnit, SourceSystem
        FROM stg.Dim_Unit
    ) AS src
        ON tgt.NamaUnit = src.NamaUnit
           AND tgt.IsCurrent = 1
    WHEN MATCHED AND (
        tgt.NamaUnit <> src.NamaUnit
    )
        THEN UPDATE
             SET tgt.IsCurrent = 0,
                 tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (NamaUnit, SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.NamaUnit, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_Gedung
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_Gedung AS tgt
    USING (
        SELECT GedungID, NamaGedung, SourceSystem
        FROM stg.Dim_Gedung
    ) AS src
        ON tgt.GedungID = src.GedungID AND tgt.IsCurrent = 1
    WHEN MATCHED AND (tgt.NamaGedung <> src.NamaGedung)
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (GedungID, NamaGedung, SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.GedungID, src.NamaGedung, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_RoomType
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_RoomType AS tgt
    USING (
        SELECT RoomTypeID, NamaTipeRuang, SourceSystem
        FROM stg.Dim_RoomType
    ) AS src
        ON tgt.RoomTypeID = src.RoomTypeID AND tgt.IsCurrent = 1
    WHEN MATCHED AND (tgt.NamaTipeRuang <> src.NamaTipeRuang)
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (RoomTypeID, NamaTipeRuang, SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.RoomTypeID, src.NamaTipeRuang, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_ItemType
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_ItemType AS tgt
    USING (
        SELECT ItemTypeID, NamaJenisItem, SourceSystem
        FROM stg.Dim_ItemType
    ) AS src
        ON tgt.ItemTypeID = src.ItemTypeID AND tgt.IsCurrent = 1
    WHEN MATCHED AND (tgt.NamaJenisItem <> src.NamaJenisItem)
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (ItemTypeID, NamaJenisItem, SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.ItemTypeID, src.NamaJenisItem, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_KondisiItem
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_KondisiItem AS tgt
    USING (
        SELECT KondisiID, Kondisi, SourceSystem
        FROM stg.Dim_KondisiItem
    ) AS src
        ON tgt.KondisiID = src.KondisiID AND tgt.IsCurrent = 1
    WHEN MATCHED AND (tgt.Kondisi <> src.Kondisi)
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (KondisiID, Kondisi, SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.KondisiID, src.Kondisi, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_Room
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_Room AS tgt
    USING (
        SELECT RoomKey, RoomTypeID, RoomCode, NamaRuang, Kapasitas, GedungID, SourceSystem
        FROM stg.Dim_Room
    ) AS src
        ON tgt.RoomKey = src.RoomKey AND tgt.IsCurrent = 1
    WHEN MATCHED AND (
        tgt.RoomTypeID <> src.RoomTypeID OR
        tgt.RoomCode <> src.RoomCode OR
        tgt.NamaRuang <> src.NamaRuang OR
        tgt.Kapasitas <> src.Kapasitas OR
        tgt.GedungID <> src.GedungID
    )
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (RoomKey, RoomTypeID, RoomCode, NamaRuang, Kapasitas, GedungID,
                     SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.RoomKey, src.RoomTypeID, src.RoomCode, src.NamaRuang,
                     src.Kapasitas, src.GedungID, src.SourceSystem,
                     GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_Item
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_Item AS tgt
    USING (
        SELECT ItemKey, ItemTypeID, NamaItem, KondisiID, CurrentRoomKey, SourceSystem
        FROM stg.Dim_Item
    ) AS src
        ON tgt.ItemKey = src.ItemKey AND tgt.IsCurrent = 1
    WHEN MATCHED AND (
        tgt.ItemTypeID <> src.ItemTypeID OR
        tgt.NamaItem <> src.NamaItem OR
        tgt.KondisiID <> src.KondisiID OR
        tgt.CurrentRoomKey <> src.CurrentRoomKey
    )
        THEN UPDATE SET tgt.IsCurrent = 0, tgt.ExpiryDate = GETDATE()

    WHEN NOT MATCHED BY TARGET
        THEN INSERT (ItemKey, ItemTypeID, NamaItem, KondisiID, CurrentRoomKey,
                     SourceSystem, EffectiveDate, ExpiryDate, IsCurrent)
             VALUES (src.ItemKey, src.ItemTypeID, src.NamaItem, src.KondisiID,
                     src.CurrentRoomKey, src.SourceSystem, GETDATE(), NULL, 1);
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Dim_Date
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.Dim_Date AS tgt
    USING stg.Dim_Date AS src
        ON tgt.DateKey = src.DateKey
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (
            DateKey, FullDate, DayNumberOfWeek, DayName, DayNumberOfMonth,
            DayNumberOfYear, WeekNumberOfYear, MonthName, MonthNumber,
            Quarter, QuarterName, Year, IsWeekend, IsHoliday,
            HolidayName, AcademicYear, Semester, SourceSystem
        )
        VALUES (
            src.DateKey, src.FullDate, src.DayNumberOfWeek, src.DayName,
            src.DayNumberOfMonth, src.DayNumberOfYear, src.WeekNumberOfYear,
            src.MonthName, src.MonthNumber, src.Quarter, src.QuarterName,
            src.Year, src.IsWeekend, src.IsHoliday, src.HolidayName,
            src.AcademicYear, src.Semester, src.SourceSystem
        );
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Fact_RoomUsage
AS
BEGIN
    INSERT INTO dw.Fact_RoomUsage (
        DateKey, RoomKey, UnitKey, DurationMinutes, SessionType, SourceSystem
    )
    SELECT DateKey, RoomKey, UnitKey, DurationMinutes, SessionType, SourceSystem
    FROM stg.Fact_RoomUsage;
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Fact_Repair
AS
BEGIN
    INSERT INTO dw.Fact_Repair (
        ItemKey, Status, DaysToComplete, DateKey, SourceSystem
    )
    SELECT ItemKey, Status, DaysToComplete, DateKey, SourceSystem
    FROM stg.Fact_Repair;
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Fact_FacilityRequest
AS
BEGIN
    INSERT INTO dw.Fact_FacilityRequest (
        DateKey, UnitKey, JenisPermintaan, Prioritas, Status, SourceSystem
    )
    SELECT DateKey, UnitKey, JenisPermintaan, Prioritas, Status, SourceSystem
    FROM stg.Fact_FacilityRequest;
END;
GO

CREATE OR ALTER PROCEDURE etl.Load_Fact_ItemMovement
AS
BEGIN
    INSERT INTO dw.Fact_ItemMovement (
        ItemKey, DariRuangan, KeRuangan, TglMutasi, SourceSystem
    )
    SELECT ItemKey, DariRuangan, KeRuangan, TglMutasi, SourceSystem
    FROM stg.Fact_ItemMovement;
END;
GO

CREATE OR ALTER PROCEDURE etl.Master_ETL_Sarpras
AS
BEGIN
    PRINT 'START ETL...';

    EXEC etl.Load_Dim_Unit;
    EXEC etl.Load_Dim_Gedung;
    EXEC etl.Load_Dim_RoomType;
    EXEC etl.Load_Dim_ItemType;
    EXEC etl.Load_Dim_KondisiItem;
    EXEC etl.Load_Dim_Room;
    EXEC etl.Load_Dim_Item;
    EXEC etl.Load_Dim_Date;

    EXEC etl.Load_Fact_RoomUsage;
    EXEC etl.Load_Fact_Repair;
    EXEC etl.Load_Fact_FacilityRequest;
    EXEC etl.Load_Fact_ItemMovement;

    PRINT 'ETL COMPLETED.';
END;
GO
