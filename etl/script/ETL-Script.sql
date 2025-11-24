/* ============================================================
   ETL SCRIPT     : ETL_Sarpras_All.sql
   PURPOSE        : ETL Stored Script untuk seluruh Dimensi
   AUTHOR         : Sistem Sarpras DWH
   ============================================================ */


/* ============================================================
   0. CREATE TABLE LOG (JIKA BELUM ADA)
   ============================================================ */
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETL_Log')
BEGIN
    CREATE TABLE ETL_Log (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        ETL_Procedure VARCHAR(200),
        Status VARCHAR(20),
        ErrorMessage VARCHAR(MAX) NULL,
        LogDate DATETIME DEFAULT GETDATE()
    );
END;
GO



/* ============================================================
   1. ETL DIM_UNIT
   ============================================================ */
BEGIN TRY
    BEGIN TRANSACTION;

    -- Extract
    SELECT 
        UnitID,
        NamaUnit
    INTO #Temp_Unit
    FROM Stg_Dim_Unit
    WHERE NamaUnit IS NOT NULL;

    -- Transform
    UPDATE #Temp_Unit
    SET NamaUnit = UPPER(LTRIM(RTRIM(NamaUnit)));

    -- Load
    MERGE INTO Dim_Unit AS Target
    USING #Temp_Unit AS Source
        ON Target.UnitID = Source.UnitID
    WHEN MATCHED THEN
        UPDATE SET
            Target.NamaUnit = Source.NamaUnit,
            Target.LastUpdate = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (UnitID, NamaUnit, CreatedDate)
        VALUES (Source.UnitID, Source.NamaUnit, GETDATE());

    DROP TABLE #Temp_Unit;

    INSERT INTO ETL_Log (ETL_Procedure, Status, LogDate)
    VALUES ('Load_DimUnit', 'SUCCESS', GETDATE());

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;

    INSERT INTO ETL_Log (ETL_Procedure, Status, ErrorMessage, LogDate)
    VALUES ('Load_DimUnit', 'FAILED', ERROR_MESSAGE(), GETDATE());
END CATCH;
GO




/* ============================================================
   2. ETL DIM_GEDUNG
   ============================================================ */
BEGIN TRY
    BEGIN TRANSACTION;

    -- Extract
    SELECT 
        GedungID,
        NamaGedung,
        JumlahLantai
    INTO #Temp_Gedung
    FROM Stg_Dim_Gedung
    WHERE NamaGedung IS NOT NULL;

    -- Transform
    UPDATE #Temp_Gedung
    SET NamaGedung = UPPER(LTRIM(RTRIM(NamaGedung)));

    -- Load
    MERGE INTO Dim_Gedung AS Target
    USING #Temp_Gedung AS Source
        ON Target.GedungID = Source.GedungID
    WHEN MATCHED THEN
        UPDATE SET
            Target.NamaGedung = Source.NamaGedung,
            Target.JumlahLantai = Source.JumlahLantai,
            Target.LastUpdate = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (GedungID, NamaGedung, JumlahLantai, CreatedDate)
        VALUES (Source.GedungID, Source.NamaGedung, Source.JumlahLantai, GETDATE());

    DROP TABLE #Temp_Gedung;

    INSERT INTO ETL_Log (ETL_Procedure, Status, LogDate)
    VALUES ('Load_DimGedung', 'SUCCESS', GETDATE());

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;

    INSERT INTO ETL_Log (ETL_Procedure, Status, ErrorMessage, LogDate)
    VALUES ('Load_DimGedung', 'FAILED', ERROR_MESSAGE(), GETDATE());
END CATCH;
GO




/* ============================================================
   FUTURE EXTENSION – FACT TABLES
   Copy–paste block di bawah untuk tiap faktanya.
   ============================================================

BEGIN TRY
    BEGIN TRANSACTION;

    -- Extract


    -- Transform


    -- Load


    INSERT INTO ETL_Log (...)
    VALUES (...);

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;

    INSERT INTO ETL_Log (...)
    VALUES (...);
END CATCH;
GO

============================================================
*/


/* ============================================================
   ■ ETL SELESAI
   ============================================================ */
PRINT 'ETL Selesai dengan sukses.';
GO
