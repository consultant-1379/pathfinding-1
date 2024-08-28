/*=====================================================================================*/

/*20120314 eanguan:: Alter table Externalstatementstatus and Externalstatement for TR HP29366*/

/*=====================================================================================*/

alter table Externalstatement modify STATEMENT text null;
alter table ExternalStatementStatus modify EXECSTATEMENT text null;
