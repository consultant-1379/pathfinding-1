/*======================================================	*/
/* Table: TPNodeVersion										*/
/* Creating table to hold node versions details for FFU		*/
/* EQEV - 41950, 10-08-2017 xdivykn		   			 		*/
/*======================================================	*/

create table TPNodeVersion (
  TECHPACK_NAME    varchar(30) not null,
  TECHPACK_VERSION varchar(10) ,
  NODE_TYPE      varchar(30) ,
  NODE_VERSION varchar(10),
) @system@;
