/*=====================================================================================*/
/*Contains all changes to upgrade an Eniq Stats base to include the Eniq Events schema */
/*=====================================================================================*/

/* events etlrep_sybase_8_priorityqueue.sql*/
if (select count(*) from sys.systable where table_name = 'PriorityQueue') = 0
begin
	/*==========================================================================================================*/
	/* Table: PriorityQueue        														                        */
	/* queueid: The queue id for the element in the queue.   					    	                        */
	/* obj: The actual obj that is being queued.																*/
	/*==========================================================================================================*/
	CREATE TABLE PriorityQueue (
		queueid bigint not null, 
		obj varchar(32000) not null
	) @system@

	alter table PriorityQueue
		add primary key (queueid)
end

/* events etlrep_sybase_9.sql */
if (select count(*) from sys.syscolumns where tname = 'META_TRANSFER_BATCHES' and cname = 'SCHEDULING_INFO') = 0
begin
	alter table META_TRANSFER_BATCHES
		add SCHEDULING_INFO varchar(16000) null
end

/* events etlrep_sybase_10.sql */
if (select count(*) from META_CONNECTION_TYPES where TYPE_NAME = 'MG') = 0
begin
	INSERT INTO META_CONNECTION_TYPES VALUES ('MG','N')
	INSERT INTO META_DATABASES VALUES ('admin','0','MG','6','MG','','dr','Mediation Gateway','',null)
end

/* events etlrep_sybase_11_ldap.sql */
if(select count(*) from META_CONNECTION_TYPES where TYPE_NAME = 'LDAP') = 0
begin
	INSERT INTO META_CONNECTION_TYPES VALUES ('LDAP','N')
	INSERT INTO META_DATABASES VALUES ('LDAP_BIND_PASSWORD','0','LDAP','7','LDAP','','@ldap.password@','LDAP','',null)
end

/* events etlrep_sybase_13_dcbo_dcpublic.sql */
if(select count(*) from META_DATABASES where CONNECTION_NAME = 'dcbo') = 0
begin
	INSERT INTO META_DATABASES VALUES ('dcbo','0','USER','8','dcbo','jdbc:sqlanywhere:host=dwhdb:@dwh.port@','@dcbo.password@','The DataWareHouse Database','@sybase.driver@',null)
end
if(select count(*) from META_DATABASES where CONNECTION_NAME = 'dcpublic') = 0
begin
	INSERT INTO META_DATABASES VALUES ('dcpublic','0','USER','9','dcpublic','jdbc:sqlanywhere:host=dwhdb:@dwh.port@','@dcpublic.password@','The DataWareHouse Database','@sybase.driver@',null)
end
