-- Dim_Unit
SELECT 'Dim_Unit' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN NamaUnit IS NULL THEN 1 ELSE 0 END) AS Null_NamaUnit
FROM dbo.Dim_Unit;

-- Dim_Gedung
SELECT 'Dim_Gedung' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN GedungID IS NULL THEN 1 ELSE 0 END) AS Null_GedungID,
       SUM(CASE WHEN NamaGedung IS NULL THEN 1 ELSE 0 END) AS Null_NamaGedung
FROM dbo.Dim_Gedung;

-- Dim_RoomType
SELECT 'Dim_RoomType' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN RoomTypeID IS NULL THEN 1 ELSE 0 END) AS Null_RoomTypeID,
       SUM(CASE WHEN NamaTipeRuang IS NULL THEN 1 ELSE 0 END) AS Null_NamaTipeRuang
FROM dbo.Dim_RoomType;

-- Dim_Room
SELECT 'Dim_Room' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN RoomKey IS NULL THEN 1 ELSE 0 END) AS Null_RoomKey,
       SUM(CASE WHEN RoomCode IS NULL THEN 1 ELSE 0 END) AS Null_RoomCode,
       SUM(CASE WHEN NamaRuang IS NULL THEN 1 ELSE 0 END) AS Null_NamaRuang,
       SUM(CASE WHEN GedungID IS NULL THEN 1 ELSE 0 END) AS Null_GedungID
FROM dbo.Dim_Room;

-- Dim_ItemType
SELECT 'Dim_ItemType' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN ItemTypeID IS NULL THEN 1 ELSE 0 END) AS Null_ItemTypeID,
       SUM(CASE WHEN NamaJenisItem IS NULL THEN 1 ELSE 0 END) AS Null_NamaJenisItem
FROM dbo.Dim_ItemType;

-- Dim_KondisiItem
SELECT 'Dim_KondisiItem' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN KondisiID IS NULL THEN 1 ELSE 0 END) AS Null_KondisiID,
       SUM(CASE WHEN Kondisi IS NULL THEN 1 ELSE 0 END) AS Null_Kondisi
FROM dbo.Dim_KondisiItem;

-- Dim_Item
SELECT 'Dim_Item' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN ItemKey IS NULL THEN 1 ELSE 0 END) AS Null_ItemKey,
       SUM(CASE WHEN ItemTypeID IS NULL THEN 1 ELSE 0 END) AS Null_ItemTypeID,
       SUM(CASE WHEN KondisiID IS NULL THEN 1 ELSE 0 END) AS Null_KondisiID,
       SUM(CASE WHEN CurrentRoomKey IS NULL THEN 1 ELSE 0 END) AS Null_CurrentRoomKey
FROM dbo.Dim_Item;

-- Dim_Date
SELECT 'Dim_Date' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS Null_DateKey,
       SUM(CASE WHEN FullDate IS NULL THEN 1 ELSE 0 END) AS Null_FullDate
FROM dbo.Dim_Date;

-- Fact_RoomUsage
SELECT 'Fact_RoomUsage' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS Null_DateKey,
       SUM(CASE WHEN RoomKey IS NULL THEN 1 ELSE 0 END) AS Null_RoomKey,
       SUM(CASE WHEN UnitKey IS NULL THEN 1 ELSE 0 END) AS Null_UnitKey
FROM dbo.Fact_RoomUsage;

-- Fact_Repair
SELECT 'Fact_Repair' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN ItemKey IS NULL THEN 1 ELSE 0 END) AS Null_ItemKey,
       SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS Null_DateKey
FROM dbo.Fact_Repair;

-- Fact_FacilityRequest
SELECT 'Fact_FacilityRequest' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) AS Null_DateKey,
       SUM(CASE WHEN UnitKey IS NULL THEN 1 ELSE 0 END) AS Null_UnitKey
FROM dbo.Fact_FacilityRequest;

-- Fact_ItemMovement
SELECT 'Fact_ItemMovement' AS TableName,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN ItemKey IS NULL THEN 1 ELSE 0 END) AS Null_ItemKey,
       SUM(CASE WHEN DariRuangan IS NULL THEN 1 ELSE 0 END) AS Null_DariRuangan,
       SUM(CASE WHEN KeRuangan IS NULL THEN 1 ELSE 0 END) AS Null_KeRuangan
FROM dbo.Fact_ItemMovement;

-- Orphan RoomKey
SELECT COUNT(*) AS Orphan_RoomKey
FROM dbo.Fact_RoomUsage f
LEFT JOIN dbo.Dim_Room d ON f.RoomKey = d.RoomKey
WHERE d.RoomKey IS NULL;

-- Orphan UnitKey
SELECT COUNT(*) AS Orphan_UnitKey
FROM dbo.Fact_RoomUsage f
LEFT JOIN dbo.Dim_Unit u ON f.UnitKey = u.UnitKey
WHERE u.UnitKey IS NULL;

-- Orphan DateKey
SELECT COUNT(*) AS Orphan_DateKey
FROM dbo.Fact_RoomUsage f
LEFT JOIN dbo.Dim_Date dt ON f.DateKey = dt.DateKey
WHERE dt.DateKey IS NULL;

SELECT COUNT(*) AS Orphan_ItemKey
FROM dbo.Fact_Repair f
LEFT JOIN dbo.Dim_Item i ON f.ItemKey = i.ItemKey
WHERE i.ItemKey IS NULL;

SELECT COUNT(*) AS Orphan_DateKey
FROM dbo.Fact_Repair f
LEFT JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

SELECT COUNT(*) AS Orphan_UnitKey
FROM dbo.Fact_FacilityRequest f
LEFT JOIN dbo.Dim_Unit u ON f.UnitKey = u.UnitKey
WHERE u.UnitKey IS NULL;

SELECT COUNT(*) AS Orphan_DateKey
FROM dbo.Fact_FacilityRequest f
LEFT JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

SELECT COUNT(*) AS Orphan_ItemKey
FROM dbo.Fact_ItemMovement f
LEFT JOIN dbo.Dim_Item i ON f.ItemKey = i.ItemKey
WHERE i.ItemKey IS NULL;

-- RoomUsage duration must be > 0
SELECT COUNT(*) AS Invalid_Duration
FROM dbo.Fact_RoomUsage
WHERE DurationMinutes <= 0;

-- FacilityRequest Priority must be valid
SELECT COUNT(*) AS Invalid_Prioritas
FROM dbo.Fact_FacilityRequest
WHERE Prioritas NOT IN ('Low','Medium','High');

-- Repair DaysToComplete must be >= 0
SELECT COUNT(*) AS Invalid_RepairDays
FROM dbo.Fact_Repair
WHERE DaysToComplete < 0;

-- Duplicate RoomUsage (DateKey, RoomKey, UnitKey)
SELECT DateKey, RoomKey, UnitKey,
       COUNT(*) AS DuplicateCount
FROM dbo.Fact_RoomUsage
GROUP BY DateKey, RoomKey, UnitKey
HAVING COUNT(*) > 1;

-- Duplicate ItemMovement for same item-date
SELECT ItemKey, TglMutasi,
       COUNT(*) AS DuplicateCount
FROM dbo.Fact_ItemMovement
GROUP BY ItemKey, TglMutasi
HAVING COUNT(*) > 1;

-- RoomUsage
SELECT 'stg.Fact_RoomUsage' AS SourceTable, COUNT(*) AS RecordCount
FROM stg.Fact_RoomUsage
UNION ALL
SELECT 'dbo.Fact_RoomUsage', COUNT(*) 
FROM dbo.Fact_RoomUsage;

-- Repair
SELECT 'stg.Fact_Repair', COUNT(*) FROM stg.Fact_Repair
UNION ALL
SELECT 'dbo.Fact_Repair', COUNT(*) FROM dbo.Fact_Repair;

-- FacilityRequest
SELECT 'stg.Fact_FacilityRequest', COUNT(*) FROM stg.Fact_FacilityRequest
UNION ALL
SELECT 'dbo.Fact_FacilityRequest', COUNT(*) FROM dbo.Fact_FacilityRequest;

-- ItemMovement
SELECT 'stg.Fact_ItemMovement', COUNT(*) FROM stg.Fact_ItemMovement
UNION ALL
SELECT 'dbo.Fact_ItemMovement', COUNT(*) FROM dbo.Fact_ItemMovement;
