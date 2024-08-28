/*======================================================*/
/* Table: ENIQS_Node_Assignment, ENIQS_Policy_Criteria.			*/
/* Adding ENM_HOSTNAME_ALIAS column in both tables.		 	*/
/* EQEV - 47210, 20-02-2018 ztxxbee	   			 	*/
/*======================================================*/

alter table ENIQS_Policy_Criteria add ENM_HOSTNAME_ALIAS varchar(100) not null;
alter table ENIQS_Node_Assignment add ENM_HOSTNAME_ALIAS varchar(100) not null;

alter table ENIQS_Node_Assignment delete primary key;
alter table ENIQS_Node_Assignment add primary key (FDN, ENM_HOSTNAME_ALIAS);