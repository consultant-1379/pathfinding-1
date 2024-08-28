insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', '0', 'etlrep', 'jdbc:hsqldb:mem:testdb', '', 'ETL Repository Database', 'org.hsqldb.jdbcDriver');
insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', '1', 'dwhrep', 'jdbc:hsqldb:mem:testdb', '', 'DWH Repository Database', 'org.hsqldb.jdbcDriver');
insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', '2', 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver');
insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '3', 'etlrep', 'jdbc:hsqldb:mem:testdb', '', 'ETL Repository Database', 'org.hsqldb.jdbcDriver');
insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '4', 'dwhrep', 'jdbc:hsqldb:mem:testdb', '', 'DWH Repository Database', 'org.hsqldb.jdbcDriver');
insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '5', 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver');

insert into REPOSITORYTABLES (table_qualifier, creator, tname, table_type, remarks) values ('repdb','dwhrep','Aggregation',      'TABLE', null);
insert into REPOSITORYTABLES (table_qualifier, creator, tname, table_type, remarks) values ('repdb','dwhrep','AggregationRule',  'TABLE', null);
insert into REPOSITORYTABLES (table_qualifier, creator, tname, table_type, remarks) values ('repdb','dwhrep','MeasurementColumn','TABLE', null);
insert into REPOSITORYTABLES (table_qualifier, creator, tname, table_type, remarks) values ('repdb','dwhrep','DataItem',         'TABLE', null);

insert into LOG_AGGREGATIONSTATUS (TIMELEVEL, DATADATE, DATE_ID, INITIAL_AGGREGATION, STATUS, DESCRIPTION, ROWCOUNT, AGGREGATIONSCOPE, LAST_AGGREGATION, LOOPCOUNT, THRESHOLD, AGGREGATION, TYPENAME) values ('DAYBH', '2011-01-10', '2011-01-10', null, 'BLOCKED', null, 10, 'DAY', null, 1, '2010-01-10 00:00:00', 'DC_E_CPP_VCLTP_DAYBH_VCLTP', 'DC_E_CPP_VCLTP');
insert into LOG_AGGREGATIONSTATUS (TIMELEVEL, DATADATE, DATE_ID, INITIAL_AGGREGATION, STATUS, DESCRIPTION, ROWCOUNT, AGGREGATIONSCOPE, LAST_AGGREGATION, LOOPCOUNT, THRESHOLD, AGGREGATION, TYPENAME) values ('DAYBH', '2010-11-23', '2010-11-23', null, 'BLOCKED', null, 10, 'DAY', null, 1, '2010-11-23 00:00:00', 'DC_E_CPP_VCLTP_DAYBH_VCLTP', 'DC_E_CPP_VCLTP');

INSERT INTO LOG_AggregationStatus_01 (AGGREGATION,TYPENAME,TIMELEVEL,DATADATE,DATE_ID,STATUS,AGGREGATIONSCOPE) VALUES ('DC_E_CPP_VCLTP_DAYBH_VCLTP','DC_E_CPP_VCLTP','DAYBH','2011-01-10','2011-01-10','NOT_LOADED', 'DAY');

insert into TPActivation (TECHPACK_NAME, STATUS, VERSIONID, TYPE, MODIFIED) values ('DWH_MONITOR', 'ACTIVE', 'DWH_MONITOR:((6))', 'SYSTEM', 0);

insert into DWHType (TECHPACK_NAME, TYPENAME, TABLELEVEL, STORAGEID, PARTITIONSIZE, PARTITIONCOUNT, STATUS, TYPE, OWNER, VIEWTEMPLATE, CREATETEMPLATE, NEXTPARTITIONTIME, BASETABLENAME, DATADATECOLUMN, PUBLICVIEWTEMPLATE, PARTITIONPLAN) values ('DWH_MONITOR', 'LOG_AggregationStatus', 'PLAIN', 'LOG_AggregationStatus:PLAIN', -1, 7, 'ENABLED', 'PARTITIONED', 'dc', 'createview.vm', 'createpartition.vm', '2011-01-01 00:00:00', 'LOG_AggregationStatus', 'DATE_ID', 'createpublicview.vm', 'medium_plain');

insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, LOADORDER) values ('LOG_AggregationStatus:PLAIN', 'LOG_AggregationStatus_01', '2011-01-10 00:00:00', '2011-03-26 00:00:00', 'ACTIVE', null);

insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DWH_MONITOR', 'ACTIVE', 'LOG_AggregationStatus', 'PLAIN', -1, 'Measurement', 'medium_plain');

insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('medium_plain', 90, 384, 250, 0);

