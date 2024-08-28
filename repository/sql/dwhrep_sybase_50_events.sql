/*=====================================================================================*/
/*Contains all changes to upgrade an Eniq Stats base to include the Eniq Events schema */
/*=====================================================================================*/

/* events dwhrep_sybase_37.sql*/
if (select count(*) from sys.syscolumns where tname = 'PartitionPlan' and cname = 'PARTITIONTYPE') = 0
begin
	/*==========================================================================================================*/
	/* Table: PartitionPlan						                    											*/
	/* Added new column PARTITIONTYPE.                      													*/
	/* This will decide what type of partition this is. 0 = Time-Based Partition, 1 = Volume-Based Partition 	*/
	/* If any rows already exist in this table, then let PARTITIONTYPE have a value of 0 for all of these 		*/
	/*==========================================================================================================*/
	alter table PartitionPlan
		add PARTITIONTYPE tinyint null
		
	update PartitionPlan set PARTITIONTYPE=0 where PARTITIONTYPE is null
	
	alter table PartitionPlan
		modify PARTITIONTYPE tinyint not null
		
	alter table PartitionPlan
		modify DEFAULTSTORAGETIME numeric(20)

	alter table PartitionPlan
		modify DEFAULTPARTITIONSIZE numeric(20)
   
	alter table PartitionPlan
		modify MAXSTORAGETIME numeric(20)
end

/* events dwhrep_sybase_37.sql*/
if (select count(*) from sys.syscolumns where tname = 'MeasurementType' and cname = 'ONEMINAGG') = 0
begin
	/*==========================================================================================================*/
	/* Table: MeasurementType						                    										*/
	/* Added new columns ONEMINAGG, FIFTEENMINAGG, EVENTSCALCTABLE, MIXEDPARTITIONSTABLE.   					*/
	/* These will all be used by ENIQ Events.																	*/ 
	/* If any rows already exist in this table, then these new columns will be set to null						*/
	/*==========================================================================================================*/
	alter table MeasurementType 
		add ONEMINAGG integer null

	alter table MeasurementType 
		add FIFTEENMINAGG integer null

	alter table MeasurementType 
		add EVENTSCALCTABLE integer null

	alter table MeasurementType
		add MIXEDPARTITIONSTABLE integer null
end


/* events dwhrep_sybase_38.sql & dwhrep_sybase_43.sql | LOADSTATUS replaced by LOADORDER*/
if(select count(*) from sys.syscolumns where tname = 'DWHPartition' and cname = 'LOADORDER') = 0
begin
	ALTER TABLE DWHPartition ADD LOADORDER INTEGER NULL
end

/* events dwhrep_sybase_39_counting.sql */
if(select count(*) from sys.systable where table_name = 'CountingManagement') = 0
begin
	/*==========================================================================================================*/
	/* Table: VolumeAggregationInfo        								                        */
	/* STORAGEID: The storage id for the measurement.   						    	                        */
	/* TABLENAME: The name of the table/partition. This should be a raw partition.  	                        */
	/* LAST_AGGREGATED_ROW: The last row id for which all required aggregations have been persistently scheduled. */
	/*==========================================================================================================*/

	create table CountingManagement (
	   STORAGEID varchar(255) not null,
	   TABLENAME varchar(255) not null,
	   LASTAGGREGATEDROW bigint not null
	) @system@

	alter table CountingManagement
	   add primary key (TABLENAME)

	   
	/*==========================================================================================================*/
	/* Table: VolumeAggregationLevel        								                				        */
	/* INTERVAL: The interval (duration) expressed in minutes   						    		                        */
	/* INTERVALNAME: The name of the interval									  	                        */
	/* INTERVALDESCRIPTION: A brief description of the interval												 	*/
	/*==========================================================================================================*/   
	create table CountingInterval (  
	  INTERVAL        integer      not null,
	  INTERVALNAME   varchar(16)  not null,
	  INTERVALDESCRIPTION  varchar(255)  not null
	) @system@

	alter table CountingInterval
	  add primary key (INTERVAL)

	insert into CountingInterval (INTERVAL, INTERVALNAME, INTERVALDESCRIPTION) 
	  values (1, '1MIN', 'One Minute Aggregation')


	insert into CountingInterval (INTERVAL, INTERVALNAME, INTERVALDESCRIPTION) 
	  values (15, '15MIN', 'Fifteen Minute Aggregation')


	insert into CountingInterval (INTERVAL, INTERVALNAME, INTERVALDESCRIPTION) 
	  values (1440, 'DAY', 'Total Aggregation')

end

/* events dwhrep_sybase_40.sql & dwhrep_sybase_46_grouptypes.sql*/
if(select count(*) from sys.systable where table_name = 'GroupTypes') = 0
begin
	/*================================================================*/
	/* Changes due to introcuction og GroupManagement in Eniq Events. */
	/*                                                                */
	/*================================================================*/

	/*================================================================*/
	/* Table: GroupTypes		                                      */
	/*================================================================*/
	CREATE TABLE GroupTypes
	(
	  GROUPTYPE varchar(64) NOT NULL,
	  VERSIONID varchar(128) NOT NULL,
	  DATANAME varchar(128) NOT NULL,
	  DATATYPE varchar(50) NOT NULL,
	  DATASIZE integer,
	  DATASCALE integer DEFAULT 0,
	  NULLABLE smallint DEFAULT 0
	) @system@

	/*================================================================*/
	/* Add primary and foreign keys.                                  */
	/*================================================================*/
	alter table GroupTypes
	   add primary key (GROUPTYPE, DATANAME, VERSIONID)

end

/* events dwhrep_sybase_41.sql */
if(select count(*) from sys.syscolumns where tname = 'MeasurementType' and cname = 'LOADFILE_DUP_CHECK') = 0
begin
	alter table MeasurementType
		add LOADFILE_DUP_CHECK int null
end

/* events dwhrep_sybase_42.sql */
if(select count(*) from sys.syscolumns where tname = 'DataInterface' and cname = 'INSTALLDESCRIPTION') = 0
begin
	alter table DataInterface
		add INSTALLDESCRIPTION varchar(32000) NULL
end

/* events dwhrep_sybase_44.sql */
if(select count(*) from sys.systable where table_name = 'ENIQ_EVENTS_ADMIN_PROPERTIES') = 0
begin
	create table ENIQ_EVENTS_ADMIN_PROPERTIES (
		PARAM_NAME		VARCHAR(100)	NOT NULL,
		PARAM_VALUE 	VARCHAR(1000),
		DATE_MODIFIED 	DATETIME		NOT NULL 	DEFAULT getdate(), 
		MODIFIED_BY		VARCHAR(100)	NOT NULL
	) @system@

	alter table ENIQ_EVENTS_ADMIN_PROPERTIES
		add primary key (PARAM_NAME)
end

/* events dwhrep_sybase_45.sql */
if(select length from sys.syscolumns where tname = 'TypeActivation' and cname = 'STORAGETIME') < 15
begin
	alter table TypeActivation modify STORAGETIME numeric(15)
end

/* events dwhrep_sybase_47_mztechpacks.sql */
if(select count(*) from sys.systable where table_name = 'MZTechPacks') = 0
begin
	create table MZTechPacks (
		VERSIONID            varchar(128) not null,
		TECHPACK_NAME        varchar(30)  not null,
		STATUS               varchar(10) null,
		CREATIONDATE         datetime null,
		PRODUCT_NUMBER       varchar(255) null,
		TYPE			     varchar(10) not null,
		TECHPACK_VERSION     varchar(32) null
	) @system@

	alter table dwhrep.MZTechPacks
		add primary key (VERSIONID)
end
