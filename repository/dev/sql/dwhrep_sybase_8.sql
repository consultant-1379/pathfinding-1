/*==============================================================*/
/* Table: MeasurementColumn                                     */
/*==============================================================*/
alter table MeasurementColumn add COLTYPE varchar(16) null;

/*==============================================================*/
/* Table: ReferenceColumn                                       */
/*==============================================================*/
alter table ReferenceColumn add COLTYPE varchar(16) null;
alter table ReferenceColumn add DESCRIPTION varchar(32000) null;

/*==============================================================*/
/* Table: Versioning                                            */
/*==============================================================*/
alter table Versioning add LOCKEDBY           varchar(255)   null;
alter table Versioning add LOCKDATE           datetime       null;
alter table Versioning add BASEDEFINITION     varchar(128)   null;
alter table Versioning add BASEVERSION        varchar(16)    null;
alter table Versioning add INSTALLDESCRIPTION varchar(32000) null;
alter table Versioning add UNIVERSENAME       varchar(30)    null;
alter table Versioning add UNIVERSEEXTENSION  varchar(16)    null;

/*==============================================================*/
/* Table: InterfaceTechpacks                                    */
/*==============================================================*/
alter table InterfaceTechpacks drop primary key;

alter table InterfaceTechpacks add TECHPACKVERSION varchar(32) not null default 'N/A';
alter table InterfaceTechpacks add INTERFACEVERSION varchar(32) not null default 'N/A';

alter table InterfaceTechpacks add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION);

/*==============================================================*/
/* Table: InterfaceMeasurement                                  */
/*==============================================================*/
alter table InterfaceMeasurement drop primary key;

alter table InterfaceMeasurement add TECHPACKVERSION varchar(32) not null default 'N/A';
alter table InterfaceMeasurement add INTERFACEVERSION varchar(32) not null default 'N/A';

alter table InterfaceMeasurement add primary key (TAGID,INTERFACENAME,INTERFACEVERSION);

/*==============================================================*/
/* Table: DataInterface                                         */
/*==============================================================*/
alter table DataInterface drop primary key;

alter table DataInterface add INTERFACEVERSION varchar(32) not null default 'N/A';
alter table DataInterface add LOCKEDBY         varchar(255) null;
alter table DataInterface add LOCKDATE         datetime     null;
alter table DataInterface add PRODUCTNUMBER varchar(255) null;

alter table DataInterface add primary key (INTERFACENAME,INTERFACEVERSION);

/*==============================================================*/
/* Table: UserAccount                                           */
/*==============================================================*/
create table UserAccount (
  NAME       varchar(255) not null,
  PASSWORD   varchar(16)  not null,
  ROLE       varchar(16)  not null,
  LASTLOGIN  datetime     null
) @system@;

alter table UserAccount
  add primary key (NAME);

/*==============================================================*/
/* Table: SupportedVendorRelease                                */
/*==============================================================*/
create table SupportedVendorRelease (
  VERSIONID     varchar(128) not null,
  VENDORRELEASE varchar(16)  not null
) @system@;

alter table SupportedVendorRelease
  add primary key (VERSIONID, VENDORRELEASE);

/*==============================================================*/
/* Table: TechPackDependency                                    */
/*==============================================================*/
create table TechPackDependency (
  VERSIONID    varchar(128) not null,
  TECHPACKNAME varchar(30)  not null,
  VERSION      varchar(32)  not null
) @system@;

alter table TechPackDependency
  add primary key (VERSIONID, TECHPACKNAME);

/*==============================================================*/
/* Table: InterfaceDependency                                    */
/*==============================================================*/
create table InterfaceDependency (
  INTERFACEVERSION    varchar(32) not null,
  INTERFACENAME varchar(50)  not null,
  TECHPACKNAME varchar(255)  not null,
  TECHPACKVERSION      varchar(128)  not null
) @system@;

alter table InterfaceDependency                                    
  add primary key (INTERFACEVERSION, INTERFACENAME);

/*==============================================================*/
/* Table: VectorCounter                                         */
/*==============================================================*/
create table VectorCounter (
  MTABLEID       varchar(255) not null,
  DATANAME       varchar(128) not null,
  VENDORRELEASE  varchar(16)  not null,
  VINDEX         numeric      not null,
  VFROM          varchar(255) not null,
  VTO            varchar(255) not null,
  MEASURE       varchar(255)  not null
) @system@;

alter table VectorCounter
  add primary key (MTABLEID, DATANAME, VENDORRELEASE, VINDEX);

/*==============================================================*/
/* Table: UniverseClass                                         */
/*==============================================================*/
create table UniverseClass (
  VERSIONID   varchar(128)   not null,
  CLASSNAME   varchar(255)   not null,
  DESCRIPTION varchar(32000) null,
  PARENT      varchar(255)   null,
  EXTENSION   varchar(255)   null
) @system@;

alter table UniverseClass
  add primary key (VERSIONID, CLASSNAME);

/*==============================================================*/
/* Table: VerificationObject                                    */
/*==============================================================*/
create table VerificationObject (
  VERSIONID     varchar(128)   not null,
  OBJNAME       varchar(255)   not null,
  TYPE          varchar(255)   not null,
  QUALIFICATION varchar(255)   not null,
  CLASSNAME     varchar(255)   not null,
  AGGREGATION   varchar(255)   null,
  OBJSELECT     varchar(32000) null,
  OBJWHERE      varchar(32000) null,
  DESCRIPTION   varchar(32000) null
) @system@;

alter table VerificationObject
  add primary key (VERSIONID, OBJNAME);

/*==============================================================*/
/* Table: VerificationCondition                                 */
/*==============================================================*/
create table VerificationCondition (
  VERSIONID   varchar(128)   not null,
  CLASSNAME   varchar(255)   not null,
  CONDITION   varchar(255)   not null,
  CONDWHERE   varchar(32000) null,
  DESCRIPTION varchar(32000) null,
) @system@;

alter table VerificationCondition
  add primary key (VERSIONID, CLASSNAME, CONDITION);

/*==============================================================*/
/* Table: UniverseTable                                         */
/*==============================================================*/
create table UniverseTable (
  VERSIONID varchar(128) not null,
  TABLENAME varchar(255) not null,
  OWNER     varchar(255) not null,
  ALIAS     varchar(255) null
) @system@;

alter table UniverseTable
  add primary key (VERSIONID, TABLENAME);

/*==============================================================*/
/* Table: UniverseJoin                                          */
/*==============================================================*/
create table UniverseJoin (
  VERSIONID        varchar(128)   not null,
  SOURCETABLE      varchar(255)   not null,
  SOURCELEVEL      varchar(255)   not null,
  SOURCECOLUMN     varchar(255)   not null,
  TARGETTABLE      varchar(255)   not null,
  TARGETLEVEL      varchar(255)   not null,
  TARGETCOLUMN     varchar(255)   not null,
  EXPRESSION       varchar(255)   not null,
  CARDINALITY      varchar(255)   null,
  CONTEXT          varchar(32000) null,
  EXCLUDEDCONTEXTS varchar(32000) null
) @system@;

alter table UniverseJoin
  add primary key (VERSIONID, SOURCETABLE, SOURCELEVEL, SOURCECOLUMN, TARGETTABLE, TARGETLEVEL, TARGETCOLUMN);

/*==============================================================*/
/* Table: UniverseObject                                        */
/*==============================================================*/
create table UniverseObject (
  VERSIONID       varchar(128) not null,
  MEASUREMENTTYPE varchar(30)  not null,
  AGGRLEVEL       varchar(30)  null,
  CLASSNAME       varchar(30)  null,
  OBJECT          varchar(30)  null
) @system@;

alter table UniverseObject
  add primary key (MEASUREMENTTYPE);

/*==============================================================*/
/* Table: UniverseCondition                                     */
/*==============================================================*/
create table UniverseCondition (
  VERSIONID       varchar(128) not null,
  MEASUREMENTTYPE varchar(30)  not null,
  AGGRLEVEL       varchar(30)  null,
  CONDITION       varchar(30)  null,
  PROMPTTEXT      varchar(30)  null,
  PROPMTVALUE     varchar(30)  null
) @system@;

alter table UniverseCondition
  add primary key (MEASUREMENTTYPE);
    
/*==============================================================*/
/* Table: MeasurementType */
/*==============================================================*/
alter table MeasurementType add SIZING varchar(16) null;
alter table MeasurementType add TOTALAGG integer null;
alter table MeasurementType add ELEMENTBHSUPPORT integer null;
alter table MeasurementType add RANKINGTABLE integer null;
alter table MeasurementType add DELTACALCSUPPORT integer null;
alter table MeasurementType add PLAINTABLE integer null;
alter table MeasurementType add UNIVERSEEXTENSION varchar(4) null;
alter table MeasurementType add OBJECTBHSUPPORT varchar(128) null;

/*==============================================================*/
/* Table: MeasurementKey */
/*==============================================================*/
alter table MeasurementKey add COLNUMBER  numeric(9) null;
alter table MeasurementKey add DATATYPE  varchar(50) null;
alter table MeasurementKey add DATASIZE  integer null;
alter table MeasurementKey add DATASCALE  integer null;
alter table MeasurementKey add UNIQUEVALUE numeric(9) null;
alter table MeasurementKey add NULLABLE  integer null;
alter table MeasurementKey add INDEXES  varchar(20) null;
alter table MeasurementKey add INCLUDESQL  integer null;
alter table MeasurementKey add UNIVOBJECT  varchar(35) null;

/*==============================================================*/
/* Table: MeasurementCounter */
/*==============================================================*/
 
alter table MeasurementCounter add COLNUMBER numeric(9) null;
alter table MeasurementCounter add DATATYPE varchar(50) null;
alter table MeasurementCounter add DATASIZE integer null;
alter table MeasurementCounter add DATASCALE integer null;
alter table MeasurementCounter add INCLUDESQL integer null;
alter table MeasurementCounter add UNIVOBJECT varchar(35) null;
alter table MeasurementCounter add UNIVCLASS varchar(35) null;
alter table MeasurementCounter add COUNTERTYPE varchar(16) null;
alter table MeasurementCounter add VENDORRELEASE   varchar(16) null;
alter table MeasurementCounter add VINDEX  numeric  null;
alter table MeasurementCounter add VFROM varchar(255) null;
alter table MeasurementCounter add VTO varchar(255) null;
alter table MeasurementCounter add MEASURE varchar(255) null;
