
/*==============================================================*/
/* Table: Aggregation                                           */
/*==============================================================*/
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
) @system@;

alter table Aggregation
   add primary key (AGGREGATION, VERSIONID);

/*==============================================================*/
/* Table: AggregationRule                                       */
/*==============================================================*/
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
   BHTYPE				varchar(50)  null 
       
) @system@;

alter table AggregationRule
   add primary key (AGGREGATION, VERSIONID, RULEID);

/*==============================================================*/
/* Table: MeasurementColumn                                     */
/*==============================================================*/
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
) @system@;

alter table MeasurementColumn
   add primary key (MTABLEID, DATANAME);

/*==============================================================*/
/* Table: MeasurementCounter                                    */
/*==============================================================*/
create table MeasurementCounter (
   TYPEID               varchar(255) not null,
   DATANAME             varchar(128) not null,
   DESCRIPTION          varchar(32000) null,
   TIMEAGGREGATION      varchar(50)  null,
   GROUPAGGREGATION     varchar(50)  null,
   COUNTAGGREGATION	varchar(50)  null
) @system@;

alter table MeasurementCounter
   add primary key (TYPEID, DATANAME);

/*==============================================================*/
/* Table: MeasurementKey                                        */
/*==============================================================*/
create table MeasurementKey (
   TYPEID               varchar(255) not null,
   DATANAME             varchar(128) not null,
   DESCRIPTION          varchar(32000) null,
   ISELEMENT            integer      null,
   UNIQUEKEY            integer      null
) @system@;

alter table MeasurementKey
   add primary key (TYPEID, DATANAME);

/*==============================================================*/
/* Table: MeasurementTable                                      */
/*==============================================================*/
create table MeasurementTable (
   MTABLEID             varchar(255) not null,
   TABLELEVEL           varchar(50)  not null,
   TYPEID               varchar(255) null,
   BASETABLENAME        varchar(255) null,
   DEFAULT_TEMPLATE     varchar(50)  null,
   PARTITIONPLAN        varchar(128) null
) @system@;

alter table MeasurementTable
   add primary key (MTABLEID);

/*==============================================================*/
/* Table: MeasurementType                                       */
/*==============================================================*/
create table MeasurementType (
   TYPEID               varchar(255) not null,
   TYPECLASSID          varchar(50)  not null,
   TYPENAME             varchar(255) not null,
   VENDORID             varchar(50)  null,
   FOLDERNAME           varchar(50)  null,
   DESCRIPTION          varchar(32000) null,
   STATUS               numeric(9)   null,
   VERSIONID            varchar(128) not null,
   OBJECTID             varchar(255) not null,
   OBJECTNAME           varchar(255) null,
   OBJECTVERSION        integer      null,
   OBJECTTYPE           varchar(255) null
) @system@;

alter table MeasurementType
   add primary key (TYPEID);

--alter table MeasurementType
--   add unique (OBJECTID);

/*==============================================================*/
/* Table: MeasurementTypeClass                                  */
/*==============================================================*/
create table MeasurementTypeClass (
   TYPECLASSID          varchar(50)  not null,
   VERSIONID            varchar(128) not null,
   DESCRIPTION          varchar(32000)  null
) @system@;

alter table MeasurementTypeClass
   add primary key (TYPECLASSID);

/*==============================================================*/
/* Table: ReferenceColumn                                       */
/*==============================================================*/
create table ReferenceColumn (
   TYPEID               varchar(255) not null,
   DATANAME             varchar(128) not null,
   COLNUMBER            numeric(9)   null,
   DATATYPE             varchar(50)  null,
   DATASIZE             integer      null,
   DATASCALE            integer      null,
   UNIQUEVALUE          numeric(9)   null,
   NULLABLE             integer      null,
   INDEXES              varchar(20)  null,
   UNIQUEKEY            integer      null
) @system@;

alter table ReferenceColumn
   add primary key (TYPEID, DATANAME);

/*==============================================================*/
/* Table: ReferenceTable                                        */
/*==============================================================*/
create table ReferenceTable (
   TYPEID               varchar(255) not null,
   VERSIONID            varchar(128) not null,
   TYPENAME             varchar(255) null,
   OBJECTID             varchar(255) null,
   OBJECTNAME           varchar(255) null,
   OBJECTVERSION        varchar(50)  null,
   OBJECTTYPE           varchar(255) null,
   DESCRIPTION          varchar(32000) null,
   STATUS               numeric(9)   null,
   UPDATE_POLICY		numeric(9)   null
) @system@;

alter table ReferenceTable
   add primary key (TYPEID);

--alter table ReferenceTable
--   add unique (OBJECTID);

/*==============================================================*/
/* Table: Versioning                                            */
/*==============================================================*/
create table Versioning (
   VERSIONID            varchar(128) not null,
   DESCRIPTION          varchar(50)  null,
   STATUS               numeric(9)   null,
   TECHPACK_NAME        varchar(30)  not null,
   TECHPACK_VERSION     varchar(32)  null,
   TECHPACK_TYPE        varchar(10)  null
) @system@;

alter table Versioning
   add primary key (VERSIONID);


/*==============================================================*/
/* Table: Transformer                                            */
/*==============================================================*/      
create table Transformer (
   TRANSFORMERID varchar(255) not null,
   VERSIONID     varchar(128) not null,
   DESCRIPTION   varchar(32000) null
) @system@;

alter table Transformer
   add primary key (TRANSFORMERID);
   
/*==============================================================*/
/* Table: Transformation                                        */
/*==============================================================*/            
create table Transformation (
   TRANSFORMERID varchar(255)   not null,
   ORDERNO       numeric(9)     not null,
   TYPE          varchar(128)   not null,
   SOURCE        varchar(128)   null,
   TARGET        varchar(128)   null,
   CONFIG        varchar(32000) null,
   DESCRIPTION   varchar(32000) null
) @system@;

alter table Transformation
   add primary key (TRANSFORMERID,ORDERNO);
         
/*==============================================================*/
/* Table: DataInterface                                         */
/*==============================================================*/            
create table DataInterface (
   INTERFACENAME    varchar(50)  not null,
   STATUS           numeric(9)   not null,
   INTERFACETYPE    varchar(50)  not null,
   DESCRIPTION      varchar(32000) null
) @system@;

alter table DataInterface
   add primary key (INTERFACENAME);

/*==============================================================*/
/* Table: InterfaceMeasurement                                  */
/*==============================================================*/            
create table InterfaceMeasurement (
   TAGID          varchar(50)  not null,
   DATAFORMATID   varchar(100) not null,
   INTERFACENAME  varchar(50)  not null,
   TRANSFORMERID  varchar(255) null,
   STATUS         numeric(9)   not null,
   MODIFTIME      datetime     null,
   DESCRIPTION    varchar(32000) null
) @system@;

alter table InterfaceMeasurement
   add primary key (TAGID, INTERFACENAME);
   
/*==============================================================*/
/* Table: DefaultTags                                           */
/*==============================================================*/            
create table DefaultTags (
   TAGID         varchar(50)  not null,
   DATAFORMATID  varchar(100) not null,
   DESCRIPTION   varchar(200) null
) @system@;

alter table DefaultTags
   add primary key (TAGID, DATAFORMATID);
   
/*==============================================================*/
/* Table: DataFormat                                            */
/*==============================================================*/            
create table DataFormat (
   DATAFORMATID    varchar(100) not null,
   TYPEID          varchar(255) not null,
   VERSIONID       varchar(128) not null,
   OBJECTTYPE      varchar(255) not null,
   FOLDERNAME      varchar(50)  not null,
   DATAFORMATTYPE  varchar(50)  not null
) @system@;

alter table DataFormat
   add primary key (DATAFORMATID);
   
/*==============================================================*/
/* Table: DataItem                                              */
/*==============================================================*/            
create table DataItem (
   DATAFORMATID  varchar(100) not null,
   DATANAME      varchar(128) not null,
   COLNUMBER     numeric(9)   not null,
   DATAID        varchar(255) not null,
   PROCESS_INSTRUCTION varchar(128) null
) @system@;

alter table DataItem
   add primary key (DATAFORMATID, DATANAME);

