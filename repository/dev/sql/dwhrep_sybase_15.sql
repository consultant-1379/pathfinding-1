drop table VerificationObject;
drop table VerificationCondition;

/*==============================================================*/
/* Table: VerificationObject                                    */
/*==============================================================*/
create table VerificationObject (
	VERSIONID       varchar(128)  not null,
	MEASTYPE		varchar(128) not null,
	MEASLEVEL		varchar(32)  not null,
	OBJECTCLASS		varchar(32)  not null,
	OBJECTNAME		varchar(32)  not null
) @system@;

alter table VerificationObject
  add primary key (VERSIONID,MEASLEVEL,OBJECTNAME);
    
/*==============================================================*/
/* Table: VerificationCondition                                 */
/*==============================================================*/
create table VerificationCondition (
  VERSIONID       varchar(128)  not null,
  FACTTABLE		  varchar(128) not null,
  VERLEVEL		  varchar(32)  not null,
  CONDITIONCLASS  varchar(32)  not null,	
  VERCONDITION    varchar(128) not null,  	
  PROMPTNAME1     varchar(255) null,	
  PROMPTVALUE1	  varchar(128) null,
  PROMPTNAME2     varchar(255) null,	
  PROMPTVALUE2	  varchar(128) null,
  OBJECTCONDITION varchar(255) null,	 
  PROMPTNAME3	  varchar(255) null,
  PROMPTVALUE3    varchar(128) null
)@system@;

alter table VerificationCondition
  add primary key (VERSIONID,VERLEVEL,VERCONDITION);
