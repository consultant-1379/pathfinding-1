/*==============================================================*/
/* Table: MeasurementType                                       */
/*==============================================================*/
alter table MeasurementType add VECTORSUPPORT integer null;
alter table MeasurementType add DATAFORMATSUPPORT integer null;
alter table MeasurementType drop OBJECTBHSUPPORT;

/*==============================================================*/
/* Table: MeasurementCounter                                    */
/*==============================================================*/
alter table MeasurementCounter add COUNTERPROCESS varchar(16) null;
alter table Measurementcounter modify UNIVOBJECT varchar(128);
alter table MeasurementCounter drop VENDORRELEASE;
alter table MeasurementCounter drop VINDEX;
alter table MeasurementCounter drop VFROM;
alter table MeasurementCounter drop VTO;
alter table MeasurementCounter drop MEASURE;

/*==============================================================*/
/* Table: UniverseName                                          */
/*==============================================================*/
alter table UniverseName drop primary key;

alter table UniverseName drop UNIVERSEEXTENSION;
alter table UniverseName add UNIVERSEEXTENSION varchar(16) not null;

alter table UniverseName
  add primary key (VERSIONID,UNIVERSEEXTENSION);

/*==============================================================*/
/* Table: Versioning                                          */
/*==============================================================*/
alter table Versioning add ENIQ_LEVEL varchar(12) not null default '1.0'; 

/*==============================================================*/
/* Table: MeasurementObjBHSupport                               */
/*==============================================================*/
create table MeasurementObjBHSupport (
  TYPEID		varchar(255) not null,
  OBJBHSUPPORT		varchar(32)  not null,
) @system@;

alter table MeasurementObjBHSupport
  add primary key (TYPEID, OBJBHSUPPORT);
  
/*==============================================================*/
/* Table: InterfaceDependency                               */
/*==============================================================*/

alter table dwhrep.InterfaceDependency
  drop primary key
alter table dwhrep.InterfaceDependency
  add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION);

/*==============================================================*/
/* Table: Versioning DataInterface                              */
/*==============================================================*/
alter table DataInterface add ENIQ_LEVEL varchar(12) null;

/*==============================================================*/
/* Table: MeasurementDeltaCalcSupport                           */
/*==============================================================*/
alter table MeasurementDeltaCalcSupport add VERSIONID varchar(128) not null;

alter table MeasurementDeltaCalcSupport drop primary key;

alter table MeasurementDeltaCalcSupport add primary key (TYPEID, VERSIONID, VENDORRELEASE);

/*==============================================================*/
/* Table: MeasurementVector                                     */
/*==============================================================*/
alter table MeasurementVector drop primary key;

alter table MeasurementVector add primary key (TYPEID, DATANAME, VENDORRELEASE, VINDEX);

---

/*==============================================================*/
/* Table: Transformer                                           */
/*==============================================================*/
alter table Transformer add TYPE varchar(50) not null default 'SPECIFIC';

/*==============================================================*/
/* Table: MeasurementKey                                        */
/*==============================================================*/
alter table Measurementkey modify UNIVOBJECT varchar(128);

/*==============================================================*/
/* Table: ReferenceColumn                                       */
/*==============================================================*/
alter table Referencecolumn modify UNIVERSEOBJECT varchar(128);

/*==============================================================*/
/* Table: Busyhour                                              */
/*==============================================================*/
create table Busyhour (
  VERSIONID       varchar(128) not null,
  BHLEVEL         varchar(32)  not null,
  BHTYPE          varchar(32)  not null,
  BHCRITERIA      varchar(32000)  null,
  WHERECLAUSE     varchar(32000)  null,
  DESCRIPTION     varchar(32000)  null
) @system@;

alter table Busyhour
  add primary key (VERSIONID,BHLEVEL,BHTYPE);
    
/*==============================================================*/
/* Table: BusyhourSource                                        */
/*==============================================================*/
create table BusyhourSource (
  VERSIONID       varchar(128) not null,
  BHLEVEL         varchar(32)  not null,
  BHTYPE          varchar(32)  not null,
  TYPENAME        varchar(255) not null
  ) @system@;

alter table BusyhourSource
  add primary key (VERSIONID,BHLEVEL,BHTYPE,TYPENAME);
    
/*==============================================================*/
/* Table: BusyhourJoincolumn                                    */
/*==============================================================*/
create table BusyhourJoincolumn (
  VERSIONID       varchar(128) not null,
  BHLEVEL         varchar(32)  not null,
  BHTYPE          varchar(32)  not null,
  JOINCOLUMN      varchar(128) not null
  ) @system@;

alter table BusyhourJoincolumn
  add primary key (VERSIONID,BHLEVEL,BHTYPE,JOINCOLUMN);
    
/*==============================================================*/
/* Table: BusyhourRankkeys                                      */
/*==============================================================*/
create table BusyhourRankkeys (
  VERSIONID       varchar(128) not null,
  BHLEVEL         varchar(32)  not null,
  BHTYPE          varchar(32)  not null,
  KEYNAME         varchar(128) not null,
  KEYVALUE        varchar(128) not null,
  ORDERNBR		  numeric(9)   not null
  ) @system@;

alter table BusyhourRankkeys
  add primary key (VERSIONID,BHLEVEL,BHTYPE,KEYNAME);

/*==============================================================*/
/* Table: UniverseTable                                         */
/*==============================================================*/

drop table UniverseTable;

create table UniverseTable (
  VERSIONID             varchar(128)    not null,
  TABLENAME             varchar(255)    not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  OWNER                 varchar(255)    not null,
  ALIAS                 varchar(255)    null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

alter table UniverseTable
  add primary key (VERSIONID, TABLENAME, UNIVERSEEXTENSION);

/*==============================================================*/
/* Table: UniverseClass                                         */
/*==============================================================*/

drop table UniverseClass;

create table UniverseClass (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(35)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  DESCRIPTION           varchar(32000)  null,
  PARENT                varchar(35)     null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

alter table UniverseClass
  add primary key (VERSIONID, CLASSNAME, UNIVERSEEXTENSION);

/*==============================================================*/
/* Table: UniverseObject                                        */
/*==============================================================*/

drop table UniverseObject;

create table UniverseObject (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(35)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  OBJECTNAME            varchar(35)     not null,
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

alter table UniverseObject
  add primary key (VERSIONID,CLASSNAME,UNIVERSEEXTENSION, OBJECTNAME);

/*==============================================================*/
/* Table: UniverseCondition                                     */
/*==============================================================*/

drop table UniverseCondition;

create table UniverseCondition (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(35)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  UNIVERSECONDITION     varchar(35)     not null,
  DESCRIPTION           varchar(32000)  null,
  CONDWHERE             varchar(32000)  null,
  AUTOGENERATE          int             null,
  CONDOBJCLASS          varchar(35)     null,
  CONDOBJECT            varchar(35)     null,
  PROMPTTEXT            varchar(255)    null,
  MULTISELECTION        int             null,
  FREETEXT              int             null,
  OBJ_BH_REL            int             null,
  ELEM_BH_REL           int             null,
  INHERITANCE           int             null
) @system@;

alter table UniverseCondition
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,UNIVERSECONDITION);

/*==============================================================*/
/* Table: UniverseComputedObject                                */
/*==============================================================*/
create table UniverseComputedObject (
  VERSIONID             varchar(128)    not null,
  CLASSNAME             varchar(35)     not null,
  UNIVERSEEXTENSION     varchar(4)      not null,
  OBJECTNAME            varchar(35)     not null,
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

alter table UniverseComputedObject
  add primary key (VERSIONID, CLASSNAME,UNIVERSEEXTENSION,OBJECTNAME);

/*==============================================================*/
/* Table: UniverseJoin                                          */
/*==============================================================*/

alter table UniverseJoin drop primary key;

alter table UniverseJoin modify SOURCETABLE varchar(32000);
alter table UniverseJoin modify TARGETTABLE varchar(32000);

alter table UniverseJoin
  add primary key (VERSIONID, SOURCETABLE, SOURCELEVEL, SOURCECOLUMN, TARGETTABLE, TARGETLEVEL, TARGETCOLUMN);
