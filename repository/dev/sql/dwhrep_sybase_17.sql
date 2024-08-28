alter table UniverseFormulas add OBJECTTYPE varchar(16) null;
alter table UniverseFormulas add QUALIFICATION varchar(16) null;
alter table UniverseFormulas add AGGREGATION varchar(16) null;

create table UniverseParameters (
   VERSIONID        varchar(128) not null,
   CLASSNAME        varchar(35) not null,
   OBJECTNAME       varchar(35) not null,
   UNIVERSEEXTENSION       varchar(4) not null,
   ORDERNRO         int not null,
   NAME             varchar(255) null
      
) @system@;

alter table UniverseParameters
   add primary key (VERSIONID, CLASSNAME, OBJECTNAME,UNIVERSEEXTENSION);
