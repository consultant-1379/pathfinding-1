alter table ExternalStatementStatus drop primary key;

alter table ExternalStatementStatus add primary key (techpack_name,statementname,versionid);
