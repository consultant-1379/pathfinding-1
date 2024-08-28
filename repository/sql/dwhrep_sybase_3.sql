alter table MeasurementColumn add INCLUDESQL integer null;

alter table ReferenceColumn add INCLUDESQL integer null;
alter table ReferenceColumn add INCLUDEUPD integer null;

alter table DWHColumn add INCLUDESQL integer null;

create table PromptImplementor (
  VERSIONID varchar(128) not null,
  PROMPTIMPLEMENTORID integer not null,
  PROMPTCLASSNAME varchar(255) null,
  PRIORITY integer not null
) @system@;

alter table PromptImplementor
  add primary key (VERSIONID,PROMPTIMPLEMENTORID);

create table Prompt (
  VERSIONID varchar(128) not null,
  PROMPTIMPLEMENTORID integer not null,
  PROMPTNAME varchar(255) not null,
  ORDERNUMBER integer null,
  UNREFRESHABLE varchar(32) null) @system@;

alter table Prompt
  add primary key (VERSIONID,PROMPTIMPLEMENTORID,PROMPTNAME);

create table PromptOption (
  VERSIONID varchar(128) not null,
  PROMPTIMPLEMENTORID integer not null,
  OPTIONNAME varchar(255) not null,
  OPTIONVALUE varchar(255) not null) @system@;

alter table PromptOption
  add primary key (VERSIONID,PROMPTIMPLEMENTORID,OPTIONNAME);

create table InfoMessages (
	MSGID integer not null,
	TITLE varchar(50) not null,
	MSGDATE datetime not null,
	NAME varchar(50) not null,
	EMAIL varchar(50) not null,
	STATUS varchar(20) not null,
	MSG varchar(500) null
) @system@;

alter table InfoMessages
	add primary key (MSGID);

create table InterfaceTechpacks (
  INTERFACENAME varchar(50) not null,
  TECHPACKNAME varchar(30) not null
) @system@;

alter table InterfaceTechpacks
  add primary key (INTERFACENAME, TECHPACKNAME);

alter table Versioning add PRODUCT_NUMBER varchar(255) null;

alter table DataInterface add DATAFORMATTYPE varchar(50) null;

alter table AlarmInterface add QUEUE_NUMBER numeric(38) not null;

alter table DWHType add PUBLICVIEWTEMPLATE varchar(255) null;
