/*==============================================================*/
/* Table: INFORMATIONSTOREVIEWS                                    */
/* (Table which has details for recreating/Creating a view for Information Store Techpack)  */
/*==============================================================*/

create table InformationStoreViews (

   VIEW_NAME        		varchar(128) not null,
   VIEW_DEFINITION        	varchar(5300) not null,
   TECHPACK_NAME      		varchar(128) not null,
   TABLE_NAME		    	varchar(128) not null,
   TECHPACK_DEFINITION     	varchar(5300) not null,
   CONDITIONS           	varchar(255) null
      
) @system@;