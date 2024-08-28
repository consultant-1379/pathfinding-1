/*==============================================================*/
/* Table: BackupConfiguration                                   */
/*==============================================================*/

create table BackupConfiguration (
   LICENSEID            varchar(15) not null,
   BACKUPLEVEL          varchar(15) not null,
   DESCRIPTION          varchar(50) not null,
   ENABLED_FLAG         varchar(1)  not null,
) @system@;