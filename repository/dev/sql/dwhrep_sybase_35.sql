/*==============================================================*/
/* Table: DataItem                                              */
/* Added three new columns DATATYPE, DATASIZE and DATASCALE     */
/* These get values from MeasurementColumn and ReferenceColumn  */
/*==============================================================*/

ALTER TABLE DataItem ADD DATATYPE varchar(50) null;
ALTER TABLE DataItem ADD DATASIZE integer null;
ALTER TABLE DataItem ADD DATASCALE integer null;
