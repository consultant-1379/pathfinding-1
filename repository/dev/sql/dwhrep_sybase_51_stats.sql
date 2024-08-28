/*=====================================================================================*/
/*Contains all changes to upgrade an Eniq Events base to include the Eniq Stats schema */
/*=====================================================================================*/

/* stats dwhrep_sybase_36.sql */
if(select count(*) from sys.syscolumns where tname = 'MeasurementVector' and cname = 'QUANTITY') = 0
begin
	/*==============================================================*/
	/* Table: MeasurementVector                                     */
	/* Added one new column QUANTITY                                */
	/*==============================================================*/
	ALTER TABLE MeasurementVector 
		ADD QUANTITY numeric null
end
/*eo_36 */

/* stats dwhrep_sybase_37.sql */
if (select length from sys.syscolumns where tname = 'Measurementcounter' and cname = 'COUNTAGGREGATION') < 32000
begin
	alter table Measurementcounter 
		modify COUNTAGGREGATION varchar(32000)
end
/*eo_37 */

/* stats dwhrep_sybase_38.sql */
if(select count(*) from sys.syscolumns where tname = 'MeasurementColumn' and cname = 'FOLLOWJOHN') = 0
begin
	ALTER TABLE MeasurementColumn 
		ADD FOLLOWJOHN integer null
end
if(select count(*) from sys.syscolumns where tname = 'MeasurementCounter' and cname = 'FOLLOWJOHN') = 0
begin
	ALTER TABLE MeasurementCounter 
		ADD FOLLOWJOHN integer null
end
/* eo_38 */

/* stats dwhrep_sybase_39.sql */
if(select count(*) from sys.syscolumns where tname = 'MeasurementType' and cname = 'FOLLOWJOHN') = 0
begin
	ALTER TABLE MeasurementType 
		ADD FOLLOWJOHN integer null
end
/* eo_39 */

/* stats busyhour changes -_-
Events already has them but theirdefs arnt up to date....
*/
if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'ENABLE') is null
begin
	alter table Busyhour
		modify ENABLE DEFAULT 1
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'ENABLE') = 'Y'
begin
	alter table Busyhour
		modify ENABLE NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'OFFSET') is null
begin
	alter table Busyhour
		modify OFFSET DEFAULT 0
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'OFFSET') = 'Y'
begin
	alter table Busyhour
		modify OFFSET NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'WINDOWSIZE') is null
begin
	alter table Busyhour
		modify WINDOWSIZE DEFAULT 60
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'WINDOWSIZE') = 'Y'
begin
	alter table Busyhour
		modify WINDOWSIZE NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'LOOKBACK') is null
begin
	alter table Busyhour
		modify LOOKBACK DEFAULT 0
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'LOOKBACK') = 'Y'
begin
	alter table Busyhour
		modify LOOKBACK NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'P_THRESHOLD') is null
begin
	alter table Busyhour
		modify P_THRESHOLD DEFAULT 0
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'P_THRESHOLD') = 'Y'
begin
	alter table Busyhour
		modify P_THRESHOLD NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'N_THRESHOLD') is null
begin
	alter table Busyhour
		modify N_THRESHOLD DEFAULT 0
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'N_THRESHOLD') = 'Y'
begin
	alter table Busyhour
		modify N_THRESHOLD NOT NULL
end

if (select default_value from sys.syscolumns where tname = 'Busyhour' and cname = 'CLAUSE') is null
begin
	alter table Busyhour
		modify CLAUSE DEFAULT ''
end
if (select nulls from sys.syscolumns where tname = 'Busyhour' and cname = 'CLAUSE') = 'Y'
begin
	alter table Busyhour
		modify CLAUSE NOT NULL
end
/* eo_busyhour*/

/* stats dwhrep_sybase_42.sql */
if(select count(*) from sys.syscolumns where tname = 'AlarmReport' and cname = 'SIMULTANEOUS') = 0
begin
	ALTER TABLE AlarmReport 
		ADD SIMULTANEOUS integer null
end
/* eo_42 */

/* stats dwhrep_sybase_43.sql */
if (select length from sys.syscolumns where tname = 'Busyhour' and cname = 'BHLEVEL') < 255
begin
	alter table Busyhour drop primary key
	alter table Busyhour modify BHLEVEL varchar(255) not null
	alter table Busyhour add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE)
	alter table BusyhourMapping drop primary key
	alter table BusyhourMapping modify BHLEVEL varchar(255) not null
	alter table BusyhourMapping
		add primary key (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, TYPEID)
	alter table BusyhourPlaceholders drop primary key
	alter table BusyhourPlaceholders modify BHLEVEL varchar(255) not null
	alter table BusyhourPlaceholders add primary key (VERSIONID, BHLEVEL)
	alter table BusyhourRankkeys drop primary key
	alter table BusyhourRankkeys modify BHLEVEL varchar(255) not null
	alter table BusyhourRankkeys 
		add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, KEYNAME)
	alter table BusyhourSource drop primary key
	alter table BusyhourSource modify BHLEVEL varchar(255) not null
	alter table BusyhourSource 
		add primary key (VERSIONID, TARGETVERSIONID, BHLEVEL, BHOBJECT, BHTYPE, TYPENAME)
end

if (select length from sys.syscolumns where tname = 'DataFormat' and cname = 'FOLDERNAME') < 128
begin
	alter table DataFormat modify FOLDERNAME varchar(128) not null
end

if (select length from sys.syscolumns where tname = 'DefaultTags' and cname = 'TAGID') < 128
begin
	alter table DefaultTags delete primary key
	alter table DefaultTags modify TAGID varchar(128) not null
	alter table DefaultTags add primary key (TAGID, DATAFORMATID)
end

if (select length from sys.syscolumns where tname = 'DataInterface' and cname = 'INTERFACENAME') < 255
begin
	alter table DataInterface drop primary key
	alter table DataInterface modify INTERFACENAME varchar(255) not null
	alter table DataInterface add primary key (INTERFACENAME,INTERFACEVERSION)
end

if (select length from sys.syscolumns where tname = 'InterfaceTechpacks' and cname = 'INTERFACENAME') < 255
begin
	alter table InterfaceTechpacks drop primary key
	alter table InterfaceTechpacks modify INTERFACENAME varchar(255) not null
	alter table InterfaceTechpacks add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION)
end

if (select length from sys.syscolumns where tname = 'InterfaceDependency' and cname = 'INTERFACENAME') < 255
begin
	alter table InterfaceDependency drop primary key
	alter table InterfaceDependency modify INTERFACENAME varchar(255) not null
	alter table InterfaceDependency
		add primary key (INTERFACENAME,INTERFACEVERSION,TECHPACKNAME,TECHPACKVERSION)
end

if (select length from sys.syscolumns where tname = 'InterfaceMeasurement' and cname = 'TAGID') < 128
begin
	alter table InterfaceMeasurement drop primary key
	alter table InterfaceMeasurement modify TAGID varchar(128) not null
	alter table InterfaceMeasurement modify INTERFACENAME varchar(255) not null
	alter table InterfaceMeasurement add primary key (TAGID,INTERFACENAME,INTERFACEVERSION)
end

if (select length from sys.syscolumns where tname = 'MeasurementTypeClass' and cname = 'TYPECLASSID') < 255
begin
	alter table MeasurementTypeClass drop primary key
	alter table MeasurementTypeClass modify TYPECLASSID varchar(255) not null
	alter table MeasurementTypeClass add primary key (TYPECLASSID)
end

if (select length from sys.syscolumns where tname = 'MeasurementType' and cname = 'TYPECLASSID') < 255
begin
	alter table MeasurementType modify TYPECLASSID varchar(255) not null
	alter table MeasurementType modify VENDORID varchar(128) null
	alter table MeasurementType modify FOLDERNAME varchar(128) null
end
/* eo_43 */





























