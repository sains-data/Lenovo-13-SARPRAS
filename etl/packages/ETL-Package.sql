/* ============================================================
   FILE       : ETL_Sarpras_Package.sql
   PURPOSE    : Mendefinisikan Package ETL dalam schema ETL
   CONTENTS   : 
                - Schema etl
                - ETL Logging Table
                - Procedure ETL Dimensi
                - Master Procedure
   ============================================================ */



/* ============================================================
   0. CREATE SCHEMA ETL (PACKAGE ROOT)
   ============================================================ */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl');
GO



/* ============================================================
   1. CREATE ETL LOG TABLE
   ============================================================ */
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ETL_Log')
BEGIN
    CREATE TABLE etl.ETL_Log (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        ETL_Procedure VARCHAR(200),
        Status VARCHAR(20),
        ErrorMessage VARCHAR(MAX) NULL,
        LogDate DATETIME DEFAULT GETDATE()
    );
END;
GO



/* ============================================================
   2. PROCEDURE – LOAD DIM UNIT
   ============================================================ */
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'Load_DimUnit')
    DROP PROCEDURE etl.Load_DimUnit;
GO

CREATE PROCEDURE etl.Load_DimUnit
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Extract
        SELECT UnitID, NamaUnit
        INTO #Temp_Unit
        FROM Stg_Dim_Unit
        WHERE NamaUnit IS NOT NULL;

        -- Transform
        UPDATE #Temp_Unit
        SET NamaUnit = UPPER(LTRIM(RTRIM(NamaUnit)));

        -- Load
        MERGE INTO Dim_Unit AS T
        USING #Temp_Unit AS S
            ON T.UnitID = S.UnitID
        WHEN MATCHED THEN
            UPDATE SET 
                T.NamaUnit = S.NamaUnit,
                T.LastUpdate = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (UnitID, NamaUnit, CreatedDate)
            VALUES (S.UnitID, S.NamaUnit, GETDATE());

        DROP TABLE #Temp_Unit;

        INSERT INTO etl.ETL_Log (ETL_Procedure, Status)
        VALUES ('Load_DimUnit', 'SUCCESS');

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        INSERT INTO etl.ETL_Log (ETL_Procedure, Status, ErrorMessage)
        VALUES ('Load_DimUnit', 'FAILED', ERROR_MESSAGE());
    END CATCH;
END;
GO



/* ============================================================
   3. PROCEDURE – LOAD DIM GEDUNG
   ============================================================ */
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'Load_DimGedung')
    DROP PROCEDURE etl.Load_DimGedung;
GO

CREATE PROCEDURE etl.Load_DimGedung
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT 
            GedungID,
            NamaGedung,
            JumlahLantai
        INTO #Temp_Gedung
        FROM Stg_Dim_Gedung
        WHERE NamaGedung IS NOT NULL;

        UPDATE #Temp_Gedung
        SET NamaGedung = UPPER(LTRIM(RTRIM(NamaGedung)));

        MERGE INTO Dim_Gedung AS T
        USING #Temp_Gedung AS S
            ON T.GedungID = S.GedungID
        WHEN MATCHED THEN
            UPDATE SET
                T.NamaGedung = S.NamaGedung,
                T.JumlahLantai = S.JumlahLantai,
                T.LastUpdate = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (GedungID, NamaGedung, JumlahLantai, CreatedDate)
            VALUES (S.GedungID, S.NamaGedung, S.JumlahLantai, GETDATE());

        DROP TABLE #Temp_Gedung;

        INSERT INTO etl.ETL_Log (ETL_Procedure, Status)
        VALUES ('Load_DimGedung', 'SUCCESS');

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        INSERT INTO etl.ETL_Log (ETL_Procedure, Status, ErrorMessage)
        VALUES ('Load_DimGedung', 'FAILED', ERROR_MESSAGE());
    END CATCH;
END;
GO



/* ============================================================
   4. MASTER PROCEDURE – MENJALANKAN SEMUA
   ============================================================ */
IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'Master_ETL_Sarpras')
    DROP PROCEDURE etl.Master_ETL_Sarpras;
GO

CREATE PROCEDURE etl.Master_ETL_Sarpras
AS
BEGIN
    PRINT 'Memulai ETL SARPRAS...';

    EXEC etl.Load_DimUnit;
    EXEC etl.Load_DimGedung;

    PRINT 'ETL SARPRAS selesai.';
END;
GO



/* ============================================================
   ■ END OF PACKAGE
   ============================================================ */

