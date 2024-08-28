create table Configuration ( 
PARAMNAME varchar(255) not null,
PARAMVALUE varchar(32000) not null) @system@;

alter table Configuration
  add primary key (PARAMNAME);
  
alter table PartitionPlan
  add MAXSTORAGETIME numeric(9) null;
  
alter table TypeActivation
  add PARTITIONPLAN varchar(128) null;

alter table DWHType
  add PARTITIONPLAN varchar(128) null;
  