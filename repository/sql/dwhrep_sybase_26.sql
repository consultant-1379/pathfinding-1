
/*==============================================================*/
/* Changes due to modifications of the universe extension size. */
/*                                                              */
/* Impacted universe tables are marked with + sign              */
/* + UniverseClass                                              */
/* + UniverseComputedObject                                     */
/* + UniverseCondition                                          */
/* - UniverseFromulas                                           */
/* - UniverseJoin                                               */
/* + UniverseName                                               */
/* + UniverseObject                                             */
/* + UniverseParameters                                         */
/* + UniverseTable                                              */
/*==============================================================*/
          
/*==============================================================*/
/* Drop primary keys                                            */
/*==============================================================*/

alter table UniverseClass drop primary key;
alter table UniverseComputedObject drop primary key;
alter table UniverseCondition drop primary key;
alter table UniverseName drop primary key;
alter table UniverseObject drop primary key;
alter table UniverseParameters drop primary key;
alter table UniverseTable drop primary key;


/*==============================================================*/
/* Create temporary tables                                      */
/*==============================================================*/

create table UniverseClass_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  DESCRIPTION           varchar(32000)  null,
  PARENT                varchar(128)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null,
  ORDERNRO              numeric         null

) @system@;

create table UniverseComputedObject_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  OBJECTNAME            varchar(128)    not null,
  DESCRIPTION           varchar(32000)  null,
  OBJECTTYPE            varchar(16)     null,
  QUALIFICATION         varchar(16)     null,
  AGGREGATION           varchar(16)     null,
  OBJSELECT             varchar(32000)  null,
  OBJWHERE              varchar(32000)  null,
  PROMPTHIERARCHY       varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null,
  ORDERNRO              numeric         null
) @system@;

create table UniverseCondition_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  UNIVERSECONDITION     varchar(128)    not null,
  DESCRIPTION           varchar(32000)  null,
  CONDWHERE             varchar(32000)  null,
  AUTOGENERATE          int             null,
  CONDOBJCLASS          varchar(128)    null,
  CONDOBJECT            varchar(128)    null,
  PROMPTTEXT            varchar(255)    null,
  MULTISELECTION        int             null,
  FREETEXT              int             null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null,
  ORDERNRO              numeric         null
) @system@;

create table UniverseName_new (
  VERSIONID             varchar(128)    not null,
  UNIVERSENAME          varchar(30)     not null,
  UNIVERSEEXTENSION     varchar(16)     not null,
  ORDERNRO              numeric         null
) @system@;

create table UniverseObject_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  OBJECTNAME            varchar(128)    not null,
  DESCRIPTION           varchar(32000)  null,
  OBJECTTYPE            varchar(16)     null,
  QUALIFICATION         varchar(16)     null,
  AGGREGATION           varchar(16)     null,
  OBJSELECT             varchar(32000)  null,
  OBJWHERE              varchar(32000)  null,
  PROMPTHIERARCHY       varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null,
  ORDERNRO              numeric         null
) @system@;

create table UniverseParameters_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  OBJECTNAME            varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  ORDERNRO              int             not null,
  NAME                  varchar(255)    null,
  TYPENAME              varchar(255)    null
) @system@;

create table UniverseTable_new (
  VERSIONID             varchar(128)    not null,
  TABLENAME             varchar(255)    not null,
  UNIVERSEEXTENSION     varchar(12)     not null,
  OWNER                 varchar(255)    not null,
  ALIAS                 varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null,
  ORDERNRO              numeric         null
) @system@;


/*==============================================================*/
/* Insert data from old tables to new.                          */
/*==============================================================*/

insert into UniverseClass_new select * from UniverseClass;
insert into UniverseComputedObject_new select * from UniverseComputedObject;
insert into UniverseCondition_new select * from UniverseCondition;
insert into UniverseName_new select * from UniverseName;
insert into UniverseObject_new select * from UniverseObject;
insert into UniverseParameters_new select * from UniverseParameters;
insert into UniverseTable_new select * from UniverseTable;


/*==============================================================*/
/* Drop old tables.                                             */
/*==============================================================*/

drop table UniverseClass;
drop table UniverseComputedObject;
drop table UniverseCondition;
drop table UniverseName;
drop table UniverseObject;
drop table UniverseParameters;
drop table UniverseTable;


/*==============================================================*/
/* Rename new tables.                                           */
/*==============================================================*/

ALTER table UniverseClass_new RENAME UniverseClass;
ALTER table UniverseComputedObject_new RENAME UniverseComputedObject;
ALTER table UniverseCondition_new RENAME UniverseCondition;
ALTER table UniverseName_new RENAME UniverseName;
ALTER table UniverseObject_new RENAME UniverseObject;
ALTER table UniverseParameters_new RENAME UniverseParameters;
ALTER table UniverseTable_new RENAME UniverseTable;


/*==============================================================*/
/* Add primary keys.                                            */
/*==============================================================*/

alter table UniverseClass
  add primary key (VERSIONID, CLASSNAME, UNIVERSEEXTENSION);        

alter table UniverseComputedObject
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,OBJECTNAME);

alter table UniverseCondition
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,UNIVERSECONDITION);

alter table UniverseName
  add primary key (VERSIONID,UNIVERSEEXTENSION);

alter table UniverseObject
  add primary key (VERSIONID,CLASSNAME,UNIVERSEEXTENSION, OBJECTNAME);

alter table UniverseParameters
   add primary key (VERSIONID, CLASSNAME, OBJECTNAME,UNIVERSEEXTENSION,ORDERNRO);  

alter table UniverseTable
  add primary key (VERSIONID, TABLENAME, UNIVERSEEXTENSION);
