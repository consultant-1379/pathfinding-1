/*==============================================================*/
/* Table: ReferenceTable and ReferenceColumn                    */
/* BASEDEF added and included in the primary key.               */
/*==============================================================*/
alter table dwhrep.ReferenceColumn drop primary key;
alter table dwhrep.ReferenceTable drop primary key;


create table dwhrep.ReferenceColumn_new (
   TYPEID               varchar(255)   not null,
   DATANAME             varchar(128)   not null,
   COLNUMBER            numeric(9)     null,
   DATATYPE             varchar(50)    null,
   DATASIZE             integer        null,
   DATASCALE            integer        null,
   UNIQUEVALUE          numeric(9)     null,
   NULLABLE             integer        null,
   INDEXES              varchar(20)    null,
   UNIQUEKEY            integer        null,
   INCLUDESQL           integer        null,
   INCLUDEUPD           integer        null,
   COLTYPE              varchar(16)    null,
   DESCRIPTION          varchar(32000) null,
   UNIVERSECLASS        varchar(35)    null,
   UNIVERSEOBJECT       varchar(128)   null,
   UNIVERSECONDITION    varchar(255)   null,
   DATAID               varchar(255)   null,
   BASEDEF              integer        not null default 0
) @system@;


create table dwhrep.ReferenceTable_new (
   TYPEID               varchar(255)   not null,
   VERSIONID            varchar(128)   not null,
   TYPENAME             varchar(255)   null,
   OBJECTID             varchar(255)   null,
   OBJECTNAME           varchar(255)   null,
   OBJECTVERSION        varchar(50)    null,
   OBJECTTYPE           varchar(255)   null,
   DESCRIPTION          varchar(32000) null,
   STATUS               numeric(9)     null,
   UPDATE_POLICY	      numeric(9)     null,
   TABLE_TYPE           varchar(12)    null,
   DATAFORMATSUPPORT    int            null,
   BASEDEF              integer        not null default 0
) @system@;


insert into dwhrep.ReferenceColumn_new (TYPEID, DATANAME, COLNUMBER, DATATYPE, DATASIZE, DATASCALE, UNIQUEVALUE, NULLABLE, INDEXES, UNIQUEKEY, INCLUDESQL, INCLUDEUPD, COLTYPE, DESCRIPTION, UNIVERSECLASS, UNIVERSEOBJECT, UNIVERSECONDITION, DATAID) 
   select TYPEID, DATANAME, COLNUMBER, DATATYPE, DATASIZE, DATASCALE, UNIQUEVALUE, NULLABLE, INDEXES, UNIQUEKEY, INCLUDESQL, INCLUDEUPD, COLTYPE, DESCRIPTION, UNIVERSECLASS, UNIVERSEOBJECT, UNIVERSECONDITION, DATAID from dwhrep.ReferenceColumn;

insert into dwhrep.ReferenceTable_new (TYPEID, VERSIONID, TYPENAME, OBJECTID, OBJECTNAME, OBJECTVERSION, OBJECTTYPE, DESCRIPTION, STATUS, UPDATE_POLICY, TABLE_TYPE, DATAFORMATSUPPORT)
   select TYPEID, VERSIONID, TYPENAME, OBJECTID, OBJECTNAME, OBJECTVERSION, OBJECTTYPE, DESCRIPTION, STATUS, UPDATE_POLICY, TABLE_TYPE, DATAFORMATSUPPORT from dwhrep.ReferenceTable;


drop table dwhrep.ReferenceTable;

drop table dwhrep.ReferenceColumn;


alter table dwhrep.ReferenceTable_new
     rename ReferenceTable;

alter table dwhrep.ReferenceColumn_new
   rename ReferenceColumn;   

alter table dwhrep.ReferenceColumn
   add primary key (TYPEID, DATANAME, BASEDEF);
   
alter table dwhrep.ReferenceTable
     add primary key (TYPEID, BASEDEF);
