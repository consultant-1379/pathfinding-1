/*======================================================*/
/* Table: TPNodeVersion								    */
/* Extending NODE_VERSION column size to 50.		 	*/
/* EQEV - 41950, 14-08-2017 xdivykn	   			 	*/
/*======================================================*/

alter table Tpnodeversion modify NODE_VERSION varchar(50) ,modify NODE_TYPE varchar(50)