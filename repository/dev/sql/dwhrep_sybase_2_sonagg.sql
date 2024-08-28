/*==============================================================*/
/* Table: MeasurementType */
/*==============================================================*/
alter table MeasurementType add SONFIFTEENMINAGG integer null;
alter table MeasurementKey add ROPGRPCELL integer null Default 0;
alter table MeasurementType add ROPGRPCELL varchar(255) null;