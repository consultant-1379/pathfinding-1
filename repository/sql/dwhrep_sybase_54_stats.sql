/*=====================================================================================*/

/*xminwal :: Alter table MeasurementVector to add QUANTITY as primary key TR :: HR61390*/

/*=====================================================================================*/
if(select a.pkey from sys.SYSCOLUMN a,sys.SYSTABLE b where a.table_id=b.table_id and b.table_name='MeasurementVector' and column_name='QUANTITY')='N'
begin
UPDATE MeasurementVector SET QUANTITY = 0 WHERE QUANTITY IS NULL
alter table MeasurementVector modify QUANTITY not null DEFAULT 0
alter table MeasurementVector drop primary key
alter table MeasurementVector add primary key (TYPEID, DATANAME, VENDORRELEASE, VINDEX, QUANTITY)
end
