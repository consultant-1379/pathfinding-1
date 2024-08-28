/*======================================================*/
/* Table: Measurementcounter						    */
/* Extending COUNTAGGREGATION column size to 32000. 	*/
/* HM34343, 08-07-2010 eeipca						 	*/
/*======================================================*/

alter table Measurementcounter modify COUNTAGGREGATION varchar(32000);
