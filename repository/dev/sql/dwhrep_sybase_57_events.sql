create table Thresholds (
ID unsigned int,
Name varchar(128),
Techpack varchar(128),
Threshold varchar(128),
StartTime timestamp null,
EndTime	timestamp null
)

insert into Thresholds(ID, Name, Techpack, Threshold, StartTime, EndTime)
values (1, 'ECNO', 'EVENT_E_RAN_SESSION', '-14', '2012-02-15 00:00', null)

insert into Thresholds(ID, Name, Techpack, Threshold, StartTime, EndTime)
values (2, 'RSCP', 'EVENT_E_RAN_SESSION', '-100', '2012-02-15 00:00', null)