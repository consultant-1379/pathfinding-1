/*======================================================*/
/* Table: Measurementcounter						    */
/* Extending COUNTAGGREGATION column size to 32000. 	*/
/* Duplicate of 37.sql to allow upgrade work from 11.0.4*/
/* HM34343, 09-08-2010 eeipca,eeoidiv   			 	*/
/*======================================================*/

alter table Measurementcounter modify COUNTAGGREGATION varchar(32000);
