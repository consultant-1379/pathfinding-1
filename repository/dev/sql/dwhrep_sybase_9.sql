/*==============================================================*/
/* Table: MeasurementKey                                        */
/*==============================================================*/
alter table MeasurementKey add JOINABLE integer null;

/*==============================================================*/
/* Table: ReferenceTable                                        */
/*==============================================================*/
alter table ReferenceTable add TABLE_TYPE varchar(12) null;

/*==============================================================*/
/* Table: ReferenceColumn                                       */
/*==============================================================*/
alter table ReferenceColumn add UNIVERSECLASS varchar(35) null;
alter table ReferenceColumn add UNIVERSEOBJECT varchar(35) null;
alter table ReferenceColumn add UNIVERSECONDITION varchar(255) null;

/*==============================================================*/
/* Table: MeasurementVector                                     */
/*==============================================================*/
create table MeasurementVector (
  TYPEID         varchar(255) not null,
  DATANAME       varchar(128) not null,
  VENDORRELEASE  varchar(16)  not null,
  VINDEX         numeric      not null,
  VFROM          varchar(255) not null,
  VTO            varchar(255) not null,
  MEASURE        varchar(255) not null
) @system@;

alter table MeasurementVector
  add primary key (TYPEID, DATANAME, VENDORRELEASE);
    
/*==============================================================*/
/* Table: MeasurementDeltaCalcSupport                           */
/*==============================================================*/
create table MeasurementDeltaCalcSupport (
  TYPEID		varchar(255) not null,
  VENDORRELEASE		varchar(16)  not null,
  DELTACALCSUPPORT	integer      not null
) @system@;

alter table MeasurementDeltaCalcSupport
  add primary key (TYPEID, VENDORRELEASE);
  
/*==============================================================*/
/* Table: Universename */
/*==============================================================*/
create table UniverseName (
VERSIONID varchar(128) not null,
UNIVERSENAME varchar(30) not null,
UNIVERSEEXTENSION varchar(16) not null
) @system@;

alter table UniverseName
add primary key (VERSIONID,UNIVERSEEXTENSION);
