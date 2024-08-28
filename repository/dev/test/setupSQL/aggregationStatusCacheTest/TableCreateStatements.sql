CREATE TABLE systable (
	table_name varchar(32), 
	creator varchar(12), 
	table_type varchar(12)
);
CREATE TABLE sysuser (
	user_id varchar(12), 
	user_name varchar(12)
);

CREATE TABLE META_DATABASES (
	USERNAME VARCHAR(30),
	VERSION_NUMBER VARCHAR(32),
	TYPE_NAME VARCHAR(15),
	CONNECTION_ID NUMERIC(38),
	CONNECTION_NAME VARCHAR(30), 
	CONNECTION_STRING VARCHAR(200), 
	PASSWORD VARCHAR(30), 
	DESCRIPTION VARCHAR(32000), 
	DRIVER_NAME VARCHAR(100), 
	DB_LINK_NAME VARCHAR(128)
);

CREATE TABLE DATAITEM (
DATAFORMATID          VARCHAR(100),
DATANAME              VARCHAR(128),
COLNUMBER             NUMERIC(11),
DATAID                VARCHAR(255),
PROCESS_INSTRUCTION   VARCHAR(128),
DATATYPE              VARCHAR(50), 
DATASIZE              INTEGER, 
DATASCALE             INTEGER
); 

CREATE TABLE REPOSITORYTABLES (
table_qualifier       VARCHAR(50),
creator               VARCHAR(50),
tname                 VARCHAR(50),
table_type            VARCHAR(50),
remarks               VARCHAR(50)
);

create table Aggregation (
   AGGREGATION          varchar(255) not null,
   VERSIONID            varchar(128) not null,
   AGGREGATIONSET       varchar(100) null,
   AGGREGATIONGROUP     varchar(100) null,
   REAGGREGATIONSET     varchar(100) null,
   REAGGREGATIONGROUP   varchar(100) null,
   GROUPORDER           integer      null,
   AGGREGATIONORDER     integer      null,
   AGGREGATIONTYPE      varchar(50)  null,
   AGGREGATIONSCOPE     varchar(50)  null       
);


create table AggregationRule (
   AGGREGATION          varchar(255) not null,
   VERSIONID            varchar(128) not null,
   RULEID               integer      not null,
   TARGET_TYPE          varchar(50)  null,
   TARGET_LEVEL         varchar(50)  null,
   TARGET_TABLE         varchar(255) null,
   TARGET_MTABLEID      varchar(255) null,
   SOURCE_TYPE          varchar(50)  null,
   SOURCE_LEVEL         varchar(50)  null,
   SOURCE_TABLE         varchar(255) null,
   SOURCE_MTABLEID      varchar(255) null,
   RULETYPE             varchar(50)  null,
   AGGREGATIONSCOPE     varchar(50)  null,
   BHTYPE		varchar(50)  null 
);

CREATE TABLE DWHPartition (
	STORAGEID VARCHAR(255),
	TABLENAME VARCHAR(255),
	STARTTIME TIMESTAMP,
	ENDTIME TIMESTAMP,
	STATUS VARCHAR(10),
	LOADORDER INTEGER
);

CREATE TABLE DWHType (
	TECHPACK_NAME VARCHAR(30),
	TYPENAME VARCHAR(255),
	TABLELEVEL VARCHAR(50),
	STORAGEID VARCHAR(255),
	PARTITIONSIZE NUMERIC(9),
	PARTITIONCOUNT NUMERIC(9),
	STATUS VARCHAR(50),
	TYPE VARCHAR(50),
	OWNER VARCHAR(50),
	VIEWTEMPLATE VARCHAR(255),
	CREATETEMPLATE VARCHAR(255),
	NEXTPARTITIONTIME TIMESTAMP,
	BASETABLENAME VARCHAR(125),
	DATADATECOLUMN VARCHAR(128),
	PUBLICVIEWTEMPLATE VARCHAR(255),
	PARTITIONPLAN VARCHAR(128)
);

create table MeasurementColumn (
   MTABLEID             varchar(255) not null,
   DATANAME             varchar(128) not null,
   COLNUMBER            numeric(9)   null,
   DATATYPE             varchar(50)  null,
   DATASIZE             integer      null,
   DATASCALE            integer      null,
   UNIQUEVALUE          numeric(9)   null,
   NULLABLE             integer      null,
   INDEXES              varchar(20)  null,
   DESCRIPTION          varchar(32000) null,
   DATAID               varchar(255) null,
   RELEASEID            varchar(50)  null,
   UNIQUEKEY            integer      null
);

CREATE TABLE PartitionPlan (
	PARTITIONPLAN VARCHAR(128),
	DEFAULTSTORAGETIME NUMERIC(20),
	DEFAULTPARTITIONSIZE NUMERIC(20),
	MAXSTORAGETIME NUMERIC(20),
	PARTITIONTYPE TINYINT
);

CREATE TABLE TPActivation (
	TECHPACK_NAME VARCHAR(30),
	STATUS VARCHAR(10),
	VERSIONID VARCHAR(128),
	TYPE VARCHAR(10),
	MODIFIED INTEGER
);

CREATE TABLE TypeActivation (
	TECHPACK_NAME VARCHAR(30),
	STATUS VARCHAR(10),
	TYPENAME VARCHAR(255),
	TABLELEVEL VARCHAR(50),
	STORAGETIME NUMERIC(15),
	TYPE VARCHAR(12),
	PARTITIONPLAN VARCHAR(128)
);

create table LOG_AGGREGATIONSTATUS(
	TIMELEVEL					varchar(10) 	null,
	DATADATE					date 				null,
	DATE_ID						date				null,
	INITIAL_AGGREGATION	timestamp 	null,
	STATUS						varchar(16) 	null,
	DESCRIPTION				varchar(250) null,
	ROWCOUNT					integer			null,
	AGGREGATIONSCOPE		varchar(50)	null,
	LAST_AGGREGATION		timestamp		null,
	LOOPCOUNT					integer			null,
	THRESHOLD         timestamp null,
	AGGREGATION				varchar(255)	null,
	TYPENAME					varchar(255)	null
);

create table LOG_AggregationStatus_01(
	AGGREGATION	varchar(255),
	TYPENAME	varchar(255),
	TIMELEVEL	varchar(10),
	DATADATE	date,
	DATE_ID		date,
	INITIAL_AGGREGATION	timestamp,
	STATUS		varchar(16),
	DESCRIPTION	varchar(250),
	ROWCOUNT	integer,
	AGGREGATIONSCOPE	varchar(50),
	LAST_AGGREGATION	timestamp,
	LOOPCOUNT	integer,
	THRESHOLD timestamp
);
