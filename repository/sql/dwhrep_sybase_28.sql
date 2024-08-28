/*==============================================================*/
/* Table: VerificationCondition                                 */
/* Primary key changed.                                         */
/*==============================================================*/

alter table VerificationCondition drop primary key;

alter table VerificationCondition
  add primary key (VERSIONID,VERLEVEL,VERCONDITION,CONDITIONCLASS);

    
