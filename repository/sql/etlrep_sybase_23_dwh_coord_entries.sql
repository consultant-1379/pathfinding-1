/* etlrep_sybase_dwh_coord_entries.sql */
if (select count(*) from META_DATABASES where CONNECTION_NAME = 'dwh_coor') = 0
begin
	INSERT INTO META_DATABASES VALUES ('dc','0','USER','10','dwh_coor','jdbc:sqlanywhere:host=dwhdb:@dwh.port@','@dwh.password@','The DataWareHouse Co-ordinator Database','@sybase.driver@',null)
	INSERT INTO META_DATABASES VALUES ('dba','0','DBA','11','dwh_coor','jdbc:sqlanywhere:host=dwhdb:@dwh.port@','@dba.password@','The DataWareHouse Co-ordinator Database','@sybase.driver@',null)
	UPDATE META_DATABASES a SET a.PASSWORD = b.PASSWORD FROM META_DATABASES b WHERE b.USERNAME ='dc' AND b.CONNECTION_NAME ='dwh' AND a.USERNAME ='dc' AND a.CONNECTION_NAME ='dwh_coor'
	UPDATE META_DATABASES a SET a.PASSWORD = b.PASSWORD FROM META_DATABASES b WHERE b.USERNAME ='dba' AND b.CONNECTION_NAME ='dwh' AND a.USERNAME ='dba' AND a.CONNECTION_NAME ='dwh_coor'
end

