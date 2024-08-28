/* Add SERVICE_NODE column to etlrep.META_EXECUTION_SLOT and etlrep.META_TRANSFER_BATCHES */

if (select count(*) from sys.syscolumns where tname = 
'META_EXECUTION_SLOT' and cname = 'SERVICE_NODE') = 0
begin
	alter table META_EXECUTION_SLOT
		add SERVICE_NODE varchar(64) null
end

if (select count(*) from sys.syscolumns where tname = 
'META_TRANSFER_BATCHES' and cname = 'SERVICE_NODE') = 0
begin
	alter table META_TRANSFER_BATCHES
		add SERVICE_NODE varchar(64) null
end
