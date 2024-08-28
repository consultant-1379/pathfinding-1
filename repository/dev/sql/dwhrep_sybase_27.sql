
/*==============================================================*/
/* Changes due to modifications of the universe extension size. */
/*==============================================================*/

/*==============================================================*/
/* Table: MeasurementType                                       */
/*==============================================================*/
alter table MeasurementType drop primary key;

create table MeasurementType_new (
   TYPEID               varchar(255)   not null,
   TYPECLASSID          varchar(50)    not null,
   TYPENAME             varchar(255)   not null,
   VENDORID             varchar(50)    null,
   FOLDERNAME           varchar(50)    null,
   DESCRIPTION          varchar(32000) null,
   STATUS               numeric(9)     null,
   VERSIONID            varchar(128)   not null,
   OBJECTID             varchar(255)   not null,
   OBJECTNAME           varchar(255)   null,
   OBJECTVERSION        integer        null,
   OBJECTTYPE           varchar(255)   null,
   JOINABLE             varchar(255)   null,
   SIZING               varchar(16)    null,
   TOTALAGG             integer        null,
   ELEMENTBHSUPPORT     integer        null,
   RANKINGTABLE         integer        null,
   DELTACALCSUPPORT     integer        null,
   PLAINTABLE           integer        null,
   UNIVERSEEXTENSION    varchar(12)    null,
   VECTORSUPPORT        integer        null,
   DATAFORMATSUPPORT    integer        null
) @system@;


insert into MeasurementType_new 
   select * from MeasurementType;

drop table MeasurementType;

alter table MeasurementType_new 
   rename MeasurementType;

alter table MeasurementType
   add primary key (TYPEID);
