/*==============================================================*/
/* Table: ExternalStatement                                     */
/*==============================================================*/
create table ExternalStatement (
   VERSIONID varchar(128) not null,
   STATEMENTNAME varchar(255) not null,
   EXECUTIONORDER numeric(9) not null,
   DBCONNECTION varchar(20) not null,
   STATEMENT varchar(32000) null
) @system@;

alter table ExternalStatement
   add primary key (VERSIONID, STATEMENTNAME);
   
/* DWHMANAGER scope begins here */

/*==============================================================*/
/* Table: DWHTechPacks                                          */
/*==============================================================*/
create table DWHTechPacks (
   TECHPACK_NAME varchar(30) not null,
   VERSIONID varchar(128) not null,
   CREATIONDATE datetime null
) @system@;

alter table DWHTechPacks
   add primary key (TECHPACK_NAME);

/*==============================================================*/
/* Table: ExternalStatementStatus                               */
/*==============================================================*/
create table ExternalStatementStatus (
   TECHPACK_NAME varchar(30) not null,
   STATEMENTNAME varchar(255) not null,
   VERSIONID varchar(128) not null,
   STATUS varchar(10) not null,
   EXECTIME datetime null,
   EXECSTATEMENT varchar(32000) null
) @system@;

alter table ExternalStatementStatus
   add primary key (TECHPACK_NAME, STATEMENTNAME);

/*==============================================================*/
/* Table: DWHType                                               */
/*==============================================================*/
create table DWHType (
   TECHPACK_NAME varchar(30) not null,
   TYPENAME varchar(255) not null,
   TABLELEVEL varchar(50) not null,
   STORAGEID varchar(255) not null,
   PARTITIONSIZE numeric(9) not null,
   PARTITIONCOUNT numeric(9) null,
   STATUS varchar(50) not null,
   TYPE varchar(50) not null,
   OWNER varchar(50) null,
   VIEWTEMPLATE varchar(255) not null,
   CREATETEMPLATE varchar(255) not null,
   NEXTPARTITIONTIME timestamp null,
   BASETABLENAME varchar(125) not null,
   DATADATECOLUMN varchar(128) null
) @system@;

alter table DWHType
   add primary key (STORAGEID);
      
/*==============================================================*/
/* Table: DWHColumn                                            */
/*==============================================================*/
create table DWHColumn (
   STORAGEID varchar(255) not null,
   DATANAME varchar(128) not null,
   COLNUMBER numeric(9) not null,
   DATATYPE varchar(50) not null,
   DATASIZE integer not null,
   DATASCALE integer not null,
   UNIQUEVALUE numeric(9) not null,
   NULLABLE integer not null,
   INDEXES varchar(20) not null,
   UNIQUEKEY integer not null,
   STATUS varchar(10) null
) @system@;

alter table DWHColumn
   add primary key (STORAGEID, DATANAME);
         
/*==============================================================*/
/* Table: DWHPartition                                          */
/*==============================================================*/
create table DWHPartition (
   STORAGEID varchar(255) not null,
   TABLENAME varchar(255) not null,
   STARTTIME timestamp null,
   ENDTIME timestamp null,
   STATUS varchar(10) not null
) @system@;

alter table DWHPartition
   add primary key (TABLENAME);
         
/*==============================================================*/
/* Table: AlarmInterface                                        */
/*==============================================================*/
create table AlarmInterface (
	INTERFACEID varchar(50) not null,
	DESCRIPTION varchar(200) not null,
	STATUS varchar(20) not null,
	COLLECTION_SET_ID numeric(38) not null,
	COLLECTION_ID numeric(38) not null
) @system@;

alter table AlarmInterface
   add primary key (INTERFACEID);
   

/*==============================================================*/
/* Table: AlarmReport                                           */
/*==============================================================*/
create table AlarmReport (
	INTERFACEID varchar(50) not null,
	REPORTID varchar(255) not null,
	REPORTNAME varchar(255) not null,
	URL varchar(32000) not null,
	STATUS varchar(10) not null
) @system@;

alter table AlarmReport
   add primary key (REPORTID);
         
/*==============================================================*/
/* Table: TPActivation                                          */
/*==============================================================*/
create table TPActivation (
   TECHPACK_NAME varchar(30)  not null,
   STATUS        varchar(10)  not null,
   VERSIONID     varchar(128) null,
   TYPE          varchar(10)  not null
) @system@;

alter table TPActivation
   add primary key (TECHPACK_NAME);
   
/*==============================================================*/
/* Table: TypeActivation                                        */
/*==============================================================*/
create table TypeActivation (
   TECHPACK_NAME varchar(30)  not null,
   STATUS        varchar(10)  not null,
   TYPENAME      varchar(255) not null,
   TABLELEVEL    varchar(50)  not null,
   STORAGETIME   numeric(9)   null,
   TYPE          varchar(12)  not null
) @system@;

alter table TypeActivation
   add primary key (TECHPACK_NAME,TYPENAME,TABLELEVEL);
   
/*==============================================================*/
/* Table: PartitionPlan                                         */
/*==============================================================*/
create table PartitionPlan (
   PARTITIONPLAN varchar(128) not null,
   DEFAULTSTORAGETIME numeric(9) not null,
   DEFAULTPARTITIONSIZE numeric(9) not null
) @system@;

alter table PartitionPlan
   add primary key (PARTITIONPLAN);
   
/*==============================================================*/
/* Table: AlarmReportParameter                                  */
/*==============================================================*/
create table AlarmReportParameter (
	REPORTID varchar(255) not null,
	NAME varchar(255) not null,
	VALUE varchar(255) not null
) @system@;

alter table AlarmReportParameter
   add primary key (REPORTID,NAME);
