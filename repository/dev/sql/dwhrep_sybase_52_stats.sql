/*==========================================================================================*/
/* MODIFY: Universejoin                                                                          */
/*==========================================================================================*/

/* for stats sql dwhrep_sybase_44.sql*/
if (select count(*) from sys.syscolumns where tname = 'Universejoin' and cname = 'UNIVERSEEXTENSION') = 0
begin
	alter table Universejoin add UNIVERSEEXTENSION        varchar(32)    DEFAULT '' null
end

/*==========================================================================================*/
/* Modify: InterfaceMeasurement																*/
/*==========================================================================================*/

/* for stats sql dwhrep_sybase_45.sql*/
alter table InterfaceMeasurement drop primary key;
alter table dwhrep.InterfaceMeasurement add primary key (TAGID,INTERFACENAME,INTERFACEVERSION, DATAFORMATID);