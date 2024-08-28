/*==============================================================*/
/* Table: VerificationObject                                    */
/* Primary key changed: OBJECTCLASS added.                      */
/*==============================================================*/

alter table VerificationObject drop primary key;

alter table VerificationObject
  add primary key (VERSIONID,MEASLEVEL,OBJECTNAME,OBJECTCLASS);
    
