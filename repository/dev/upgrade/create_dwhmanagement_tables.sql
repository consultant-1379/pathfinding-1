create table StorageInfo (
   STORAGEID            varchar(255) not null,
   TYPEID               varchar(255) null,
   TABLELEVEL           varchar(50)	 null,
   BASETABLENAME        varchar(255) null,
   STORAGETIME          numeric(9)	 null,
   PARTITIONSIZE        numeric(9)	 null,
   SEEDTIME             timestamp	 null,
   STATUS               numeric(9)	 null,
   TEMPLATE				varchar(50)  null
);

alter table StorageInfo
   add primary key (STORAGEID);
   
create table PhysicalTable (
   TABLEID              numeric(9)      not null,
   STORAGEID            varchar(255) 	null,
   TABLENAME            varchar(50)	    null,
   STARTTIME            timestamp	    null,
   ENDTIME              timestamp	    null,
   STATUS               numeric(9)	    null
);

alter table PhysicalTable
   add primary key (TABLEID);
   
alter table PhysicalTable
   add foreign key FK_PhysicalTable_To_StorageInfo (STORAGEID)
      references StorageInfo (STORAGEID)
      on delete restrict on update restrict;
      
create table MeasurementData (
   STORAGEID            varchar(255) not null,
   DATANAME             varchar(128) not null,
   COLNUMBER            numeric(9)	 null,
   DATATYPE             varchar(50)	 null,
   DATASIZE             integer	     null,
   DATASCALE            integer	     null,
   UNIQUEVALUE          numeric(9)	 null,
   NULLABLE             integer	     null,
   INDEXES              varchar(20)	 null,
   DESCRIPTION          varchar(32000) null,
   DATAID               varchar(255) null,
   RELEASEID            varchar(50)	 null
);

alter table MeasurementData
   add primary key (STORAGEID, DATANAME);
   
alter table MeasurementData
   add foreign key FK_MeasurementData_To_StorageInfo (STORAGEID)
      references StorageInfo (STORAGEID)
      on delete restrict on update restrict;
      
create table ReferenceType (
   TYPEID               varchar(255) not null,
   TYPENAME             varchar(255) null,
   DESCRIPTION          varchar(32000) null,
   STATUS               numeric(9)	 null,
   VERSIONID            varchar(20)  not null
);

alter table ReferenceType
   add primary key (TYPEID);
      
create table ReferenceData (
   TYPEID               varchar(255) not null,
   DATANAME             varchar(128) not null,
   COLNUMBER            numeric(9)	 null,
   DATATYPE             varchar(50)	 null,
   DATASIZE             integer	     null,
   DATASCALE            integer	     null,
   UNIQUEVALUE          numeric(9)	 null,
   NULLABLE             integer	     null,
   INDEXES              varchar(20)	 null,
   UNIQUEKEY            integer	     null
);

alter table ReferenceData
   add primary key (TYPEID, DATANAME);
   
alter table ReferenceData
   add foreign key FK_ReferenceData_To_ReferenceType (TYPEID)
      references ReferenceType (TYPEID)
      on delete restrict on update restrict;

create table DuplicateMonitoring (
   STORAGEID            varchar(255) not null,
   GROUP_FIELD          varchar(50)	 null,
   GROUP_ORDER          integer	     null
);

commit;