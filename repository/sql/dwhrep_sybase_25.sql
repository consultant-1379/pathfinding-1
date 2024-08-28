
alter table UniverseFormulas drop primary key;

create table UniverseFormulas_new (
   VERSIONID           varchar(128) not null,
   TECHPACK_TYPE       varchar(32) null,
   NAME                varchar(255) not null,
   FORMULA             varchar(32000) null,
   OBJECTTYPE 		   varchar(16) null,
   QUALIFICATION       varchar(16) null,
   AGGREGATION         varchar(16) null,
   ORDERNRO 		   numeric null
      
) @system@;


insert into UniverseFormulas_new select * from UniverseFormulas;


drop table UniverseFormulas;


ALTER table UniverseFormulas_new RENAME UniverseFormulas;


alter table UniverseFormulas
   add primary key (VERSIONID, NAME);