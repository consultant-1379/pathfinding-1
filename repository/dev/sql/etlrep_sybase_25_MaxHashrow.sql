/*=====================================================================================*/
/*Same Content as ENIQ Events etlrep_sybase_15_MaxHashrow.sql */
/*=====================================================================================*/

if not exists (select 1 from META_DATABASES where CONNECTION_STRING like '%SQLINITSTRING=SET TEMPORARY OPTION MAX_HASH_ROWS%' and CONNECTION_NAME ='dwh') then
update META_DATABASES
set CONNECTION_STRING = replace(CONNECTION_STRING, CONNECTION_STRING, CONNECTION_STRING||'?SQLINITSTRING=SET TEMPORARY OPTION MAX_HASH_ROWS=18000000')  
where CONNECTION_NAME ='dwh'
end if;

