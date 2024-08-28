/* etlrep_sybase_dcuser_entry_for_adminUI.sql */
if (select count(*) from META_CONNECTION_TYPES where TYPE_NAME = 'dcuser') = 0
begin
	INSERT INTO META_CONNECTION_TYPES VALUES ('dcuser','N')
end


