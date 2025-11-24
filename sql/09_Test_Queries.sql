SET STATISTICS TIME ON;
SET STATISTICS IO ON;

-- 1. Total Usage & Duration per Item Category (2025)
SELECT
    it.NamaJenisItem AS ItemCategory,
    COUNT(fr.RoomKey) AS TotalUsage,
    SUM(fr.DurationMinutes) AS TotalDuration
FROM dbo.Fact_RoomUsage fr
INNER JOIN dbo.Dim_Item i
    ON fr.RoomKey = i.CurrentRoomKey
INNER JOIN dbo.Dim_ItemType it
    ON i.ItemTypeID = it.ItemTypeID
INNER JOIN dbo.Dim_Date d
    ON fr.DateKey = d.DateKey
WHERE d.Year = 2025
GROUP BY it.NamaJenisItem
ORDER BY TotalUsage DESC;

-- 2. Total Usage & Duration per Month
SELECT
    d.Year,
    d.MonthNumber,
    d.MonthName,
    COUNT(fr.RoomKey) AS TotalUsage,
    SUM(fr.DurationMinutes) AS TotalDuration
FROM dbo.Fact_RoomUsage fr
INNER JOIN dbo.Dim_Date d
    ON fr.DateKey = d.DateKey
GROUP BY 
    d.Year, d.MonthNumber, d.MonthName
ORDER BY 
    d.Year, d.MonthNumber;

-- 3. Total Duration per Room & Building
SELECT
    g.NamaGedung AS BuildingName,
    r.NamaRuang AS RoomName,
    SUM(fr.DurationMinutes) AS TotalDuration
FROM dbo.Fact_RoomUsage fr
INNER JOIN dbo.Dim_Room r
    ON fr.RoomKey = r.RoomKey
INNER JOIN dbo.Dim_Gedung g
    ON r.GedungID = g.GedungID
GROUP BY 
    g.NamaGedung, r.NamaRuang
ORDER BY 
    TotalDuration DESC;

-- 4. Top 10 Items by Total Duration
SELECT TOP 10
    i.NamaItem,
    it.NamaJenisItem AS ItemCategory,
    SUM(fr.DurationMinutes) AS TotalDuration
FROM dbo.Fact_RoomUsage fr
INNER JOIN dbo.Dim_Item i
    ON fr.RoomKey = i.CurrentRoomKey
INNER JOIN dbo.Dim_ItemType it
    ON i.ItemTypeID = it.ItemTypeID
GROUP BY 
    i.NamaItem, it.NamaJenisItem
ORDER BY 
    TotalDuration DESC;
