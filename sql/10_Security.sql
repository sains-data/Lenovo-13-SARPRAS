Create User Roles
-- Create Database Roles
CREATE ROLE db_executive;
CREATE ROLE db_analyst;
CREATE ROLE db_viewer;
CREATE ROLE db_etl_operator;
GO

-- Executive Full Access for SELECT and ETL Procedure
GRANT SELECT ON SCHEMA::dbo TO db_executive;
GRANT EXECUTE ON SCHEMA::dbo TO db_executive;
GO

-- Analyst: Can analyze and edit staging before loading
GRANT SELECT ON SCHEMA::dbo TO db_analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO db_analyst;
GO

-- Viewer: Read-only access to all dimensional & fact tables
GRANT SELECT ON SCHEMA::dbo TO db_viewer;
GO

-- ETL Operator: Full access to staging + loading to DWH
GRANT EXECUTE ON SCHEMA::dbo TO db_etl_operator;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO db_etl_operator;
GRANT INSERT, UPDATE ON SCHEMA::dbo TO db_etl_operator;
GO


-- Create SQL Logins
CREATE LOGIN executive_user WITH PASSWORD = 'Barcelona123';
CREATE LOGIN analyst_user WITH PASSWORD = 'Barcelona123';
CREATE LOGIN viewer_user WITH PASSWORD = 'Barcelona123';
CREATE LOGIN etl_service WITH PASSWORD = 'Barcelona123';
GO

-- Create Database Users
USE DM_SARPRAS_DW;
GO

CREATE USER executive_user FOR LOGIN executive_user;
CREATE USER analyst_user FOR LOGIN analyst_user;
CREATE USER viewer_user FOR LOGIN viewer_user;
CREATE USER etl_service FOR LOGIN etl_service;
GO

-- Assign Users to Roles
ALTER ROLE db_executive ADD MEMBER executive_user;
ALTER ROLE db_analyst ADD MEMBER analyst_user;
ALTER ROLE db_viewer ADD MEMBER viewer_user;
ALTER ROLE db_etl_operator ADD MEMBER etl_service;
GO


-- Masking otomatis hanya jika kolom ditemukan

-- Nama Item (contoh jika NamaItem yang dipakai)
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'Dim_Item' AND COLUMN_NAME = 'NamaItem'
)
BEGIN
    ALTER TABLE dbo.Dim_Item
    ALTER COLUMN NamaItem 
    ADD MASKED WITH (FUNCTION = 'partial(1,"xxx",1)');
END

-- Deskripsi Item (alternative)
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'Dim_Item' AND COLUMN_NAME = 'ItemDescription'
)
BEGIN
    ALTER TABLE dbo.Dim_Item
    ALTER COLUMN ItemDescription 
    ADD MASKED WITH (FUNCTION = 'partial(1,"xxxxxxx",1)');
END

-- Serial Number / Code (default masking)
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'Dim_Item' AND COLUMN_NAME = 'KodeItem'
)
BEGIN
    ALTER TABLE dbo.Dim_Item
    ALTER COLUMN KodeItem 
    ADD MASKED WITH (FUNCTION = 'default()');
END
GO

-- Permission UNMASK
GRANT UNMASK TO db_executive;
GRANT UNMASK TO db_etl_operator;
GO

Create Audit Table
-- Create Audit Table
CREATE TABLE dbo.AuditLog (
    AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
    EventTime DATETIME2 DEFAULT SYSDATETIME(),
    UserName NVARCHAR(128) DEFAULT SUSER_SNAME(),
    EventType NVARCHAR(50),
    SchemaName NVARCHAR(128),
    ObjectName NVARCHAR(128),
    RowsAffected INT,
    SQLStatement NVARCHAR(MAX) NULL,
    HostName VARCHAR(128) NULL
);
GO


CREATE TRIGGER trg_Audit_Fact_Repair
ON dbo.Fact_Repair
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventType NVARCHAR(50);

    IF EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted)
        SET @EventType = 'UPDATE';
    ELSE IF EXISTS(SELECT * FROM inserted)
        SET @EventType = 'INSERT';
    ELSE
        SET @EventType = 'DELETE';

    INSERT INTO dbo.AuditLog (EventType, SchemaName, ObjectName, RowsAffected)
    VALUES (@EventType, 'dbo', 'Fact_Repair', @@ROWCOUNT);
END;
GO

USE master;
GO

IF EXISTS (SELECT * FROM sys.server_audits WHERE name = 'Sarpras_Audit')
    DROP SERVER AUDIT Sarpras_Audit;
GO

-- Buat audit target ke Application Log
CREATE SERVER AUDIT Sarpras_Audit
TO APPLICATION_LOG
WITH (ON_FAILURE = CONTINUE);
GO

ALTER SERVER AUDIT Sarpras_Audit
WITH (STATE = ON);
GO

CREATE DATABASE AUDIT SPECIFICATION Sarpras_DB_Audit
FOR SERVER AUDIT Sarpras_Audit
ADD (SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo BY public);
GO

ALTER DATABASE AUDIT SPECIFICATION Sarpras_DB_Audit 
WITH (STATE = ON);
GO
