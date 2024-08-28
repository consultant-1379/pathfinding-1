/*==============================================================*/
/* Table: ExternalStatement                                     */
/* BASEDEF added and included in the primary key.               */
/*==============================================================*/
alter table dwhrep.ExternalStatement drop primary key;

create table dwhrep.ExternalStatement_new (
   VERSIONID varchar(128) not null,
   STATEMENTNAME varchar(255) not null,
   EXECUTIONORDER numeric(9) not null,
   DBCONNECTION varchar(20) not null,
   STATEMENT varchar(32000) null,
   BASEDEF integer not null default 0
) @system@;

insert into dwhrep.ExternalStatement_new (VERSIONID, STATEMENTNAME, EXECUTIONORDER, DBCONNECTION, STATEMENT)
   select VERSIONID, STATEMENTNAME, EXECUTIONORDER, DBCONNECTION, STATEMENT from dwhrep.ExternalStatement;

drop table dwhrep.ExternalStatement;

alter table dwhrep.ExternalStatement_new 
   rename ExternalStatement;
   
alter table dwhrep.ExternalStatement
   add primary key (VERSIONID, STATEMENTNAME, BASEDEF);
