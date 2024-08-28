/*======================================================*/
/* Table: RoleTable									    */
/* Creating table with required columns				 	*/
/* EQEV - ???, 03-10-2016 xarjsin		   			 	*/
/*======================================================*/

create table RoleTable (
  ENIQ_ID    varchar(50) not null,
  IP_ADDRESS varchar(15)  not null,
  ROLE      varchar(15)  not null
) @system@;

create table ENIQS_Node_Assignment (
  ENIQ_IDENTIFIER varchar(50) null,
  FDN varchar(250) PRIMARY KEY,
  NETYPE varchar(50) null
) @system@;


create table ENIQS_Policy_Criteria (
  TECHNOLOGY varchar(50),
  NAMINGCONVENTION varchar(250),
  ENIQ_IDENTIFIER varchar(50) not null
) @system@;
