alter table UniverseParameters drop primary key
alter table UniverseParameters add primary key (VERSIONID, CLASSNAME, OBJECTNAME,UNIVERSEEXTENSION,ORDERNRO); 