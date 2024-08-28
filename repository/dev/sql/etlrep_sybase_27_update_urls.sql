if (select count(*) from META_DATABASES where CONNECTION_NAME = 'dwh' and CONNECTION_STRING like '%dwh_reader%') > 0
begin
	UPDATE META_DATABASES set CONNECTION_STRING = 'jdbc:sybase:Tds:dwhdb:@dwh.port@?SQLINITSTRING=SET TEMPORARY OPTION MAX_HASH_ROWS=18000000' where CONNECTION_NAME ='dwh'
end