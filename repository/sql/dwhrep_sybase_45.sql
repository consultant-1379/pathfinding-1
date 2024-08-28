/*==========================================================================================*/
/* Modify: InterfaceMeasurement																*/
/*==========================================================================================*/

alter table InterfaceMeasurement drop primary key;
alter table dwhrep.InterfaceMeasurement add primary key (TAGID,INTERFACENAME,INTERFACEVERSION, DATAFORMATID);