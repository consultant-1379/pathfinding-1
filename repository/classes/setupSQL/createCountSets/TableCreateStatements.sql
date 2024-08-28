--/***************************
--  MeasurementType
--/***************************
create table MeasurementType (
	TYPEID varchar(255) not null,
	TYPECLASSID varchar(50) not null,
	TYPENAME varchar(255) not null,
	VENDORID varchar(50) ,
	FOLDERNAME varchar(50) ,
	DESCRIPTION varchar(32000) ,
	STATUS numeric(9) ,
	VERSIONID varchar(128) not null,
	OBJECTID varchar(255) not null,
	OBJECTNAME varchar(255) ,
	OBJECTVERSION integer ,
	OBJECTTYPE varchar(255) ,
	JOINABLE varchar(255) ,
	SIZING varchar(16) ,
	TOTALAGG integer ,
	ELEMENTBHSUPPORT integer ,
	RANKINGTABLE integer ,
	DELTACALCSUPPORT integer ,
	PLAINTABLE integer ,
	UNIVERSEEXTENSION varchar(4) ,
	VECTORSUPPORT integer ,
	DATAFORMATSUPPORT integer ,
	FOLLOWJOHN integer,
	ONEMINAGG int,
	FIFTEENMINAGG int,
	EVENTSCALCTABLE int,
	MIXEDPARTITIONSTABLE int,
	LOADFILE_DUP_CHECK int,		SONAGG int,		SONFIFTEENMINAGG int,		ROPGRPCELL varchar(255)
);

 alter table MeasurementType
       add primary key (TYPEID);

CREATE TABLE MEASUREMENTTABLE (MTABLEID VARCHAR(255), TABLELEVEL VARCHAR(50), TYPEID VARCHAR(255), BASETABLENAME VARCHAR(255), DEFAULT_TEMPLATE VARCHAR(50), PARTITIONPLAN VARCHAR(128));

CREATE TABLE REFERENCETABLE (TYPEID	varchar(50), VERSIONID	varchar(50), TYPENAME	varchar(50), OBJECTID	varchar(50), OBJECTNAME	varchar(50), OBJECTVERSION	varchar(50), OBJECTTYPE	varchar(50), DESCRIPTION	varchar(50), STATUS	int	, UPDATE_POLICY	int	, TABLE_TYPE	varchar(50), DATAFORMATSUPPORT	int	, BASEDEF	int)
CREATE TABLE PartitionPlan (PARTITIONPLAN varchar(128), DEFAULTSTORAGETIME numeric(20), DEFAULTPARTITIONSIZE numeric(20), MAXSTORAGETIME numeric(20), PARTITIONTYPE	tinyint)

