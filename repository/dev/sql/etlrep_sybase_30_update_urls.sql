if (select count(*) from META_DATABASES where DRIVER_NAME != '@sybase.driver@' and DRIVER_NAME != '') > 0
BEGIN

UPDATE META_DATABASES SET CONNECTION_STRING = 'jdbc:sqlanywhere:host=repdb:@rep.port@;InitString=''SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14''''''', DRIVER_NAME = '@sybase.driver@' WHERE TYPE_NAME IN ('DBA', 'USER') AND CONNECTION_NAME IN ('etlrep','dwhrep')

UPDATE META_DATABASES SET CONNECTION_STRING = 'jdbc:sqlanywhere:host=dwhdb:@dwh.port@;InitString=''SET TEMPORARY OPTION MAX_HASH_ROWS=18000000''', DRIVER_NAME = '@sybase.driver@' WHERE TYPE_NAME IN ('DBA', 'USER') AND CONNECTION_NAME IN ('dwh')

UPDATE META_DATABASES SET CONNECTION_STRING = 'jdbc:sqlanywhere:host=dwhdb:@dwh.port@', DRIVER_NAME = '@sybase.driver@' WHERE TYPE_NAME IN ('DBA', 'USER') AND CONNECTION_NAME IN ('dcbo','dcpublic','dwh_coor')

UPDATE META_DATABASES SET CONNECTION_STRING = 'jdbc:sqlanywhere:host=dwh_reader_1:@dwh_reader_1.port@;InitString=''SET TEMPORARY OPTION MAX_HASH_ROWS=18000000''', DRIVER_NAME = '@sybase.driver@' WHERE TYPE_NAME IN ('DBA', 'USER') AND CONNECTION_NAME IN ('dwh_reader_1')

UPDATE META_DATABASES SET CONNECTION_STRING = 'jdbc:sqlanywhere:host=dwh_reader_2:@dwh_reader_1.port@;InitString=''SET TEMPORARY OPTION MAX_HASH_ROWS=18000000''', DRIVER_NAME = '@sybase.driver@' WHERE TYPE_NAME IN ('DBA', 'USER') AND CONNECTION_NAME IN ('dwh_reader_2')

END

