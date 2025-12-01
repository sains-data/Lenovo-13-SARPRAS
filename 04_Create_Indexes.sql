CREATE NONCLUSTERED INDEX IX_Fact_Repair_Covering
ON dbo.Fact_Repair(ItemKey, DateKey)
INCLUDE (DaysToComplete, Status);
GO

CREATE NONCLUSTERED INDEX IX_Fact_RoomUsage_Covering
ON dbo.Fact_RoomUsage(DateKey, RoomKey)
INCLUDE (UnitKey, DurationMinutes, SessionType);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Repair_Item
ON dbo.Fact_Repair(ItemKey)
INCLUDE (DaysToComplete, Status);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Repair_Date
ON dbo.Fact_Repair(DateKey)
INCLUDE (DaysToComplete, Status);
GO

CREATE NONCLUSTERED INDEX IX_Fact_FacilityRequest_Covering
ON dbo.Fact_FacilityRequest(DateKey, UnitKey)
INCLUDE (JenisPermintaan, Prioritas, Status);
GO

CREATE NONCLUSTERED INDEX IX_Fact_ItemMovement_Item
ON dbo.Fact_ItemMovement(ItemKey)
INCLUDE (DariRuangan, KeRuangan, TglMutasi);
GO

CREATE NONCLUSTERED INDEX IX_Fact_ItemMovement_Rooms
ON dbo.Fact_ItemMovement(DariRuangan, KeRuangan)
INCLUDE (TglMutasi, ItemKey);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_RoomUsage
ON dbo.Fact_RoomUsage
(
    DateKey, RoomKey, UnitKey,
    DurationMinutes, SessionType
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_Repair
ON dbo.Fact_Repair
(
    ItemKey, DateKey,
    DaysToComplete, Status
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_FacilityRequest
ON dbo.Fact_FacilityRequest
(
    DateKey, UnitKey,
    JenisPermintaan, Prioritas, Status
);
GO
