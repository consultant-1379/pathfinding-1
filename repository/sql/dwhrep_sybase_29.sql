/*==============================================================*/
/* Table: VerificationCondition                                 */
/* FACTTABLE size increased.                                    */
/*==============================================================*/
alter table VerificationCondition drop primary key;

create table VerificationCondition_new (
  VERSIONID       varchar(128)  not null,
  FACTTABLE 	  varchar(2560) not null,
  VERLEVEL	      varchar(32)   not null,
  CONDITIONCLASS  varchar(32)   not null,	
  VERCONDITION    varchar(128)  not null,  	
  PROMPTNAME1     varchar(255)  null,	
  PROMPTVALUE1	  varchar(128)  null,
  PROMPTNAME2     varchar(255)  null,	
  PROMPTVALUE2	  varchar(128)  null,
  OBJECTCONDITION varchar(255)  null,	 
  PROMPTNAME3	  varchar(255)  null,
  PROMPTVALUE3    varchar(128)  null
)@system@;

insert into VerificationCondition_new 
   select * from VerificationCondition;

drop table VerificationCondition;

alter table VerificationCondition_new 
   rename VerificationCondition;
   
alter table VerificationCondition
  add primary key (VERSIONID,VERLEVEL,VERCONDITION,CONDITIONCLASS);    
