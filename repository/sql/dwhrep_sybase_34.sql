/*==========================================================================================================*/
/* Table: UniverseName						                    											*/
/* Added new column UNIVERSEEXTENSIONNAME.                      											*/
/* IDE #652 problem with the names of the Universes when you have splitted universe, 2009-06-08 eeoidiv 	*/
/*==========================================================================================================*/

alter table UniverseName add UNIVERSEEXTENSIONNAME varchar(35) null;