-- Full Backup
BACKUP DATABASE DM_Sarpras_DW
TO DISK = N'/var/opt/mssql/backup/DM_Sarpras_DW_Full.bak'
WITH
    COMPRESSION,
    INIT,
    NAME = 'Full Backup',
    STATS = 10;
GO

-- Differential Backup
BACKUP DATABASE DM_Sarpras_DW
TO DISK = N'/var/opt/mssql/backup/DM_Sarpras_DW_Diff.bak'
WITH DIFFERENTIAL, COMPRESSION, INIT, STATS = 10;
GO

-- Transaction Log Backup
SELECT name, recovery_model_desc 
FROM sys.databases WHERE name='DM_Sarpras_DW';

ALTER DATABASE DM_Sarpras_DW SET RECOVERY FULL;
GO

BACKUP LOG DM_Sarpras_DW
TO DISK = N'/var/opt/mssql/backup/DM_Sarpras_DW_Log.trn'
WITH COMPRESSION, INIT, STATS = 10;
GO
