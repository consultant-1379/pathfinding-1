alter table UniverseComputedObject drop primary key;
alter table UniverseCondition drop primary key;
alter table UniverseClass drop primary key;
alter table UniverseObject drop primary key;

create table UniverseClass_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)    not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  DESCRIPTION           varchar(32000)  null,
  PARENT                varchar(128)     null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

create table UniverseObject_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  OBJECTNAME            varchar(128)     not null,
  DESCRIPTION           varchar(32000)  null,
  OBJECTTYPE            varchar(16)     null,
  QUALIFICATION         varchar(16)     null,
  AGGREGATION           varchar(16)     null,
  OBJSELECT             varchar(32000)  null,
  OBJWHERE              varchar(32000)  null,
  PROMPTHIERARCHY       varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

create table UniverseCondition_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  UNIVERSECONDITION     varchar(128)     not null,
  DESCRIPTION           varchar(32000)  null,
  CONDWHERE             varchar(32000)  null,
  AUTOGENERATE          int             null,
  CONDOBJCLASS          varchar(128)     null,
  CONDOBJECT            varchar(128)     null,
  PROMPTTEXT            varchar(255)    null,
  MULTISELECTION        int             null,
  FREETEXT              int             null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

create table UniverseComputedObject_new (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(128)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  OBJECTNAME            varchar(128)     not null,
  DESCRIPTION           varchar(32000)  null,
  OBJECTTYPE            varchar(16)     null,
  QUALIFICATION         varchar(16)     null,
  AGGREGATION           varchar(16)     null,
  OBJSELECT             varchar(32000)  null,
  OBJWHERE              varchar(32000)  null,
  PROMPTHIERARCHY       varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;


insert into UniverseClass_new select * from UniverseClass;
insert into UniverseObject_new select * from UniverseObject;
insert into UniverseCondition_new select * from UniverseCondition;
insert into UniverseComputedObject_new select * from UniverseComputedObject;

drop table UniverseClass;
drop table UniverseObject;
drop table UniverseCondition;
drop table UniverseComputedObject;

ALTER table UniverseClass_new RENAME UniverseClass;
ALTER table UniverseObject_new RENAME UniverseObject;
ALTER table UniverseCondition_new RENAME UniverseCondition;
ALTER table UniverseComputedObject_new RENAME UniverseComputedObject;

alter table UniverseClass
  add primary key (VERSIONID, CLASSNAME, UNIVERSEEXTENSION);
        
alter table UniverseObject
  add primary key (VERSIONID,CLASSNAME,UNIVERSEEXTENSION, OBJECTNAME);
       
alter table UniverseCondition
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,UNIVERSECONDITION);
   
alter table UniverseComputedObject
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,OBJECTNAME);    
    