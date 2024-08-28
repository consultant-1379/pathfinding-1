alter table UniverseParameters drop primary key;

create table UniverseParameters_new (

   VERSIONID        	varchar(128) not null,
   CLASSNAME        	varchar(128) not null,
   OBJECTNAME       	varchar(128) not null,
   UNIVERSEEXTENSION    varchar(4) not null,
   ORDERNRO         	int not null,
   NAME             	varchar(255) null,
   TYPENAME				varchar(255) null
      
) @system@;


insert into UniverseParameters_new select * from UniverseParameters;


drop table UniverseParameters;


ALTER table UniverseParameters_new RENAME UniverseParameters;


alter table UniverseParameters
   add primary key (VERSIONID, CLASSNAME, OBJECTNAME,UNIVERSEEXTENSION,ORDERNRO); 
