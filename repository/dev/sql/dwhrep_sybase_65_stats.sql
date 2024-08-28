/*======================================================*/
/* Table: Versioning								    */
/* Extending LICENSENAME column size to 1023.		 	*/
/* EQEV - 28205, 22-12-2015 xarjsin		   			 	*/
/*======================================================*/

alter table Versioning modify LICENSENAME varchar(1023);
