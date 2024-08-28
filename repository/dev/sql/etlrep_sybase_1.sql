create table META_CONNECTION_TYPES
(
    TYPE_NAME                     varchar(15)             not null,
    DATABASE_TYPE_FLAG            varchar(1)              not null,
    primary key (TYPE_NAME)
) @system@;

create table META_PARAMETERS
(
    RB_SEGMENT_NAME               varchar(30)              null    ,
    USE_RB_SEGMENT_FLAG           char(1)              not null,
    DEFAULT_ERROR_MAIL_ADDR       char(100)              null    ,
    DEFAULT_FAIL_MAIL_ADDR        char(100)              null    ,
    DEFAULT_BUG_ERROR_MAIL_ADDR   char(100)              null    ,
    DEFAULT_MAX_ERROR_VALUE       numeric(38)              not null,
    DEFAULT_MAX_FK_ERROR_VALUE    numeric(38)              not null,
    DEFAULT_MAX_COL_LIMIT_VALUE   numeric(38)              not null,
    TEMP_SUM_TABLESPACE           varchar(30)              null    ,
    USE_TEMP_SUM_TABLESPACE_FLAG  varchar(1)              not null,
    BATCH_COLUMN_NAME             varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    primary key (VERSION_NUMBER)
) @system@;

create table META_VERSIONS
(
    VERSION_NUMBER                varchar(32)              not null,
    DESCRIPTION                   varchar(32000)              null    ,
    CURRENT_FLAG                  varchar(1)              not null,
    IS_PREDEFINED                 varchar(1)              not null,
    ENGINE_SERVER                 varchar(50)              null    ,
    MAIL_SERVER                   varchar(100)              null    ,
    SCHEDULER_SERVER              varchar(50)              null    ,
    MAIL_SERVER_PORT              numeric(5)              null    ,
    primary key (VERSION_NUMBER)
) @system@;

create table META_COLLECTION_SETS
(
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_SET_NAME           varchar(128)              not null,
    DESCRIPTION                   varchar(32000)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    ENABLED_FLAG		  varchar(1)		   null	   ,
    TYPE			  varchar(32)		   null,
    primary key (VERSION_NUMBER,COLLECTION_SET_ID )
) @system@;


create table META_DATABASES
(
    USERNAME                      varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    TYPE_NAME                     varchar(15)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    CONNECTION_NAME               varchar(30)              not null,
    CONNECTION_STRING             varchar(400)              not null,
    PASSWORD                      varchar(30)              null    ,
    DESCRIPTION                   varchar(32000)              null    ,
    DRIVER_NAME                   varchar(100)              not null,
    DB_LINK_NAME                  varchar(128)              null    ,
    primary key (VERSION_NUMBER, CONNECTION_ID)
) @system@;

create table META_PARAMETER_TABLES
(
    PAR_NAME                      varchar(30)              not null,
    PAR_VALUE                     varchar(200)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    primary key (VERSION_NUMBER,PAR_NAME)
) @system@;


create table META_TRANSFORMATION_RULES
(
    TRANSFORMATION_ID             numeric(38)              not null,
    TRANSFORMATION_NAME           varchar(10)              not null,
    CODE                          varchar(2000)		           not null,
    DESCRIPTION                   varchar(32000)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    primary key (VERSION_NUMBER,TRANSFORMATION_ID)
) @system@;


create table META_TRANSFORMATION_TABLES
(
    TRANSF_TABLE_ID               numeric(38)              not null,
    TRANSF_TABLE_NAME             varchar(30)              not null,
    DESCRIPTION                   varchar(32000)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    IS_LOOKUP			  varchar(1)		   null,
    CONNECTION_ID		  numeric(38)              null,
    TABLE_ID			  numeric(38)              null,
    KEY_COLUMN_ID 		  numeric(38)		   null,
    VALUE_COLUMN_ID 		  numeric(38)		   null,
    primary key (VERSION_NUMBER,TRANSF_TABLE_ID)
) @system@;


create table META_COLLECTIONS
(
    COLLECTION_ID                 numeric(38)              not null,
    COLLECTION_NAME               varchar(128)              not null,
    COLLECTION                    varchar(200)              null    ,
    MAIL_ERROR_ADDR               varchar(100)              null    ,
    MAIL_FAIL_ADDR                varchar(100)              null    ,
    MAIL_BUG_ADDR                 varchar(100)              null    ,
    MAX_ERRORS                    numeric(38)              not null,
    MAX_FK_ERRORS                 numeric(38)              not null,
    MAX_COL_LIMIT_ERRORS          numeric(38)              not null,
    CHECK_FK_ERROR_FLAG           varchar(1)              not null,
    CHECK_COL_LIMITS_FLAG         varchar(1)              not null,
    LAST_TRANSFER_DATE            datetime              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    USE_BATCH_ID                  varchar(1)              null    ,
    PRIORITY			  numeric(3)		null    ,
    QUEUE_TIME_LIMIT		  numeric(38)		null    ,
    ENABLED_FLAG		  varchar(1)		null    ,
    SETTYPE		 	  varchar(10)		null    ,
    FOLDABLE_FLAG		  varchar(1)		null    ,
    MEASTYPE			  varchar(30)		null    ,
    HOLD_FLAG			  varchar(1)		null    ,
    SCHEDULING_INFO               varchar(2000)              null    ,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID,COLLECTION_ID)
) @system@;



create table META_SCHEDULINGS
(
    VERSION_NUMBER                varchar(32)              null    ,
    ID                            numeric(38)              not null,
    EXECUTION_TYPE                varchar(15)              not null,
    OS_COMMAND                    varchar(2000)              null    ,
    SCHEDULING_MONTH              numeric(2)              null    ,
    SCHEDULING_DAY                numeric(2)              null    ,
    SCHEDULING_HOUR               numeric(2)              null    ,
    SCHEDULING_MIN                numeric(2)              null    ,
    COLLECTION_SET_ID             numeric(38)              null    ,
    COLLECTION_ID                 numeric(38)              null    ,
    MON_FLAG                      varchar(1)              null    ,
    TUE_FLAG                      varchar(1)              null    ,
    WED_FLAG                      varchar(1)              null    ,
    THU_FLAG                      varchar(1)              null    ,
    FRI_FLAG                      varchar(1)              null    ,
    SAT_FLAG                      varchar(1)              null    ,
    SUN_FLAG                      varchar(1)              null    ,
    STATUS                        varchar(20)              null    ,
    LAST_EXECUTION_TIME           datetime              null    ,
    INTERVAL_HOUR                 numeric(2)              null    ,
    INTERVAL_MIN                  numeric(2)              null    ,
    NAME                          varchar(128)              not null,
    HOLD_FLAG			  varchar(1)            null    ,
    PRIORITY			  numeric(3)            null    ,
    SCHEDULING_YEAR		  numeric(4)            null    ,
    TRIGGER_COMMAND		  varchar(2000)            null    ,
    primary key (ID)
) @system@;

create table META_TRANSFER_ACTIONS
(
    VERSION_NUMBER                varchar(32)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    ACTION_TYPE                   varchar(20)              not null,
    TRANSFER_ACTION_NAME          varchar(128)              not null,
    ORDER_BY_NO                   numeric(38)              not null,
    DESCRIPTION                   varchar(32000)              null    ,
    WHERE_CLAUSE                  varchar(32000)              null    ,
    ACTION_CONTENTS               varchar(32000)              null    ,
    ENABLED_FLAG                  varchar(1)              not null,
    CONNECTION_ID                 numeric(38)              null    ,
    primary key (VERSION_NUMBER,   COLLECTION_SET_ID,COLLECTION_ID,TRANSFER_ACTION_ID)
) @system@;



create table META_TRANSFER_BATCHES
(
    ID                            numeric(38)              not null,
    START_DATE                    datetime              not null,
    END_DATE                      datetime              null    ,
    FAIL_FLAG                     varchar(1)              not null,
    STATUS                        varchar(10)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID,ID)
) @system@;


create table META_DEBUGS
(
    ID                            numeric(38)              not null,
    TEXT                          varchar(2000) 	   not null,
    LAST_UPDATED                  datetime              not null,
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_BATCH_ID             numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              null    ,
    primary key (ID)
) @system@;

create table META_ERRORS
(
    ID                            numeric(38)              not null,
    TEXT                          varchar(2000)              null    ,
    METHOD_NAME                   varchar(100)              null    ,
    ERR_TYPE                      varchar(30)              not null,
    LAST_UPDATED                  datetime              not null,
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_BATCH_ID             numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              null    ,
    primary key (ID)
) @system@;


create table META_FILES
(
    FILE_ID                       numeric(38)              not null,
    FILE_NAME                     varchar(100)              not null,
    FILE_CONTENT_TYPE             varchar(20)              not null,
    ROW_DELIM                     varchar(5)              null    ,
    COLUMN_DELIM                  varchar(5)              null    ,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    COMMIT_AFTER_N_ROWS           numeric(10)              null    ,
    IS_SOURCE                     varchar(1)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID,  TRANSFER_ACTION_ID,FILE_ID)
) @system@;



create table META_PLUGINS
(
    PLUGIN_ID                     numeric(38)              not null,
    PLUGIN_NAME                   varchar(30)              not null,
    CONSTRUCTOR_PARAMETER         varchar(200)              null    ,
    IS_SOURCE                     varchar(1)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    COMMIT_AFTER_N_ROWS           numeric(10)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID,  TRANSFER_ACTION_ID,PLUGIN_ID)
) @system@;



create table META_SOURCE_TABLES
(
    LAST_TRANSFER_DATE            datetime              null    ,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    USE_TR_DATE_IN_WHERE_FLAG     varchar(1)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    DISTINCT_FLAG                 varchar(1)              not null,
    AS_SELECT_OPTIONS             varchar(200)              null    ,
    AS_SELECT_TABLESPACE          varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    TIMESTAMP_COLUMN_ID           numeric(38)              null    ,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID,  TRANSFER_ACTION_ID, CONNECTION_ID,TABLE_ID)
) @system@;



create table META_STATUSES
(
    ID                            numeric(38)              not null,
    STATUS_DESCRIPTION            varchar(32000)              not null,
    LAST_UPDATED                  datetime              not null,
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_BATCH_ID             numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              null    ,
    primary key (ID)
) @system@;


create table META_TARGET_TABLES
(
    VERSION_NUMBER                varchar(32)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, TRANSFER_ACTION_ID, CONNECTION_ID, TABLE_ID)
) @system@;


create table META_FK_TABLES
(
    MAX_ERRORS                    numeric(38)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    WHERE_CLAUSE                  varchar(2000)              null    ,
    FILTER_ERRORS_FLAG            varchar(1)              not null,
    REPLACE_ERRORS_FLAG           varchar(1)              not null,
    REPLACE_ERRORS_WITH           varchar(30)              null    ,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    TARGET_TABLE_ID               numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, TRANSFER_ACTION_ID, CONNECTION_ID, TABLE_ID, TARGET_TABLE_ID)
) @system@;


create table META_JOINTS
(
    ID                            numeric(38)              not null,
    IS_PK_COLUMN                  varchar(1)              not null,
    IS_SUM_COLUMN                 varchar(1)              not null,
    IS_GROUP_BY_COLUMN            varchar(1)              not null,
    COLUMN_SPACE_AT_FILE          numeric(10)              null    ,
    FILE_ORDER_BY                 numeric(38)              null    ,
    PLUGIN_METHOD_NAME            varchar(100)              null    ,
    VERSION_NUMBER                varchar(32)              null    ,
    COLLECTION_SET_ID             numeric(38)              null    ,
    COLLECTION_ID                 numeric(38)              null    ,
    TRANSFER_ACTION_ID            numeric(38)              null    ,
    TARGET_CONNECTION_ID          numeric(38)              null    ,
    TARGET_TABLE_ID               numeric(38)              null    ,
    COLUMN_ID_TARGET_COLUMN       numeric(38)              null    ,
    SOURCE_CONNECTION_ID          numeric(38)              null    ,
    SOURCE_TABLE_ID               numeric(38)              null    ,
    COLUMN_ID_SOURCE_COLUMN       numeric(38)              null    ,
    TRANSFORMATION_ID             numeric(38)              null    ,
    TRANSF_TABLE_ID               numeric(38)              null    ,
    PAR_NAME                      varchar(30)              null    ,
    FILE_ID                       numeric(38)              null    ,
    PLUGIN_ID                     numeric(38)              null    ,
    FREE_FORMAT_TRANSFORMAT       varchar(2000)              null    ,
    METHOD_PARAMETER              varchar(200)              null    ,
    primary key (ID)
) @system@;


create table META_SQL_LOADS
(
    INPUT_FILE                    varchar(200)              not null,
    CTL_FILE                      varchar(200)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    DIS_FILE                      varchar(200)              not null,
    BAD_FILE                      varchar(200)              not null,
    LOAD_TYPE                     varchar(30)              not null,
    TEXT                          varchar(2000)              not null,
    DELIM                         varchar(1)              null    ,
    SQLLDR_CMD                    varchar(200)              null    ,
    LOAD_OPTION                   varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    primary key (VERSION_NUMBER,COLLECTION_SET_ID, COLLECTION_ID,   TRANSFER_ACTION_ID,CONNECTION_ID, TABLE_ID)
) @system@;

create table META_TABLES
(
    TABLE_ID                      numeric(38)              not null,
    TABLE_NAME                    varchar(60)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    IS_JOIN                	  varchar(1)               null,
    JOIN_CLAUSE                   varchar(2000)            null,
    TABLES_AND_ALIASES            varchar(2000)            null,
    CONNECTION_ID                 numeric(38)              not null,
    primary key (VERSION_NUMBER, CONNECTION_ID,TABLE_ID )
) @system@;


create table META_COLUMNS
(
    COLUMN_ID                     numeric(38)              not null,
    COLUMN_NAME                   varchar(30)              not null,
    COLUMN_ALIAS_NAME             varchar(60)              null,
    COLUMN_TYPE                   varchar(30)              not null,
    COLUMN_LENGTH                 numeric(38)              not null,
    IS_PK_COLUMN                  varchar(1)              not null,
    VERSION_NUMBER                varchar(32)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    primary key (VERSION_NUMBER, CONNECTION_ID, TABLE_ID,COLUMN_ID)
) @system@;



create table META_COLUMN_CONSTRAINTS
(
    ID                            numeric(38)              not null,
    LOW_VALUE                     varchar(30)              not null,
    HIGH_VALUE                    varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    COLUMN_ID                     numeric(38)              not null,
    primary key (VERSION_NUMBER, CONNECTION_ID, TABLE_ID, COLUMN_ID,ID)
) @system@;

create table META_FK_TABLE_JOINTS
(
    VERSION_NUMBER                varchar(32)              not null,
    CONNECTION_ID                 numeric(38)              not null,
    TABLE_ID                      numeric(38)              not null,
    COLUMN_ID_FK_COLUMN           numeric(38)              not null,
    TARGET_TABLE_ID               numeric(38)              not null,
    COLUMN_ID                     numeric(38)              not null,
    COLLECTION_SET_ID             numeric(38)              not null,
    COLLECTION_ID                 numeric(38)              not null,
    TRANSFER_ACTION_ID            numeric(38)              not null,
    primary key (VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, TRANSFER_ACTION_ID, CONNECTION_ID, TABLE_ID, COLUMN_ID_FK_COLUMN, TARGET_TABLE_ID, COLUMN_ID)
) @system@;


create table META_TRANSF_TABLE_VALUES
(
    OLD_VALUE                     varchar(30)              not null,
    NEW_VALUE                     varchar(30)              null    ,
    VERSION_NUMBER                varchar(32)              not null,
    TRANSF_TABLE_ID               numeric(38)              not null,
    primary key (VERSION_NUMBER, TRANSF_TABLE_ID,OLD_VALUE)
) @system@;


create table META_EXECUTION_SLOT_PROFILE
(
    PROFILE_NAME		   varchar(15)             not null,
    PROFILE_ID			   varchar(38)              not null,
    ACTIVE_FLAG			   varchar(1)	           not null,
    primary key (PROFILE_ID)
) @system@;

create table META_EXECUTION_SLOT
(
    PROFILE_ID			   varchar(38)              not null,
    SLOT_NAME			   varchar(15)             not null,
    SLOT_ID			   varchar(38)              not null,
    ACCEPTED_SET_TYPES	           varchar(2000)	    not null,
    primary key (SLOT_ID)
) @system@;



create table META_SERVERS
(
    HOSTNAME                      varchar(255)             not null,
    TYPE                          varchar(128)             not null,
    STATUS_URL                    varchar(255)             not null,
    primary key (HOSTNAME)
) @system@;

create table META_SYSTEM_MONITORS
(
    MONITOR                       varchar(255)             not null,
    HOSTNAME                      varchar(255)             not null,
    TYPE                          varchar(32)              not null,
    CONFIGURATION                 varchar(32000)           null,
    EXECUTED                      datetime                 not null,
    STATUS                        varchar(10)              not null,
    primary key (MONITOR,HOSTNAME)
) @system@;   

ALTER TABLE META_COLLECTIONS ADD CONSTRAINT CON_NAME_UI UNIQUE(COLLECTION_NAME,
   COLLECTION_SET_ID,
   VERSION_NUMBER);

ALTER TABLE META_COLLECTION_SETS ADD CONSTRAINT CST_NAME_UI UNIQUE(VERSION_NUMBER,
   COLLECTION_SET_NAME);

INSERT INTO META_CONNECTION_TYPES (TYPE_NAME,DATABASE_TYPE_FLAG) VALUES ('USER','Y');
INSERT INTO META_CONNECTION_TYPES (TYPE_NAME,DATABASE_TYPE_FLAG) VALUES ('DBA','Y');
INSERT INTO META_VERSIONS VALUES ('0','Default version','Y','N','localhost',null,'localhost',null);
INSERT INTO META_DATABASES VALUES ('etlrep','0','USER','0','etlrep','jdbc:sqlanywhere:host=repdb:@rep.port@;InitString=''SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14''''''','@etlrep.password@','ETL Repository Database','@sybase.driver@',null);

INSERT INTO META_DATABASES VALUES ('dwhrep','0','USER','1','dwhrep','jdbc:sqlanywhere:host=repdb:@rep.port@;InitString=''SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14''''''','@dwhrep.password@','DWH Repository Database','@sybase.driver@',null);

INSERT INTO META_DATABASES VALUES ('dc','0','USER','2','dwh','jdbc:sqlanywhere:host=dwhdb:@dwh.port@;InitString=''SET TEMPORARY OPTION MAX_HASH_ROWS=18000000''','@dwh.password@','The DataWareHouse Database','@sybase.driver@',null);

INSERT INTO META_DATABASES VALUES ('dba','0','DBA','3','etlrep','jdbc:sqlanywhere:host=repdb:@rep.port@;InitString=''SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14''''''','@dba.password@','ETL Repository Database','@sybase.driver@',null);

INSERT INTO META_DATABASES VALUES ('dba','0','DBA','4','dwhrep','jdbc:sqlanywhere:host=repdb:@rep.port@;InitString=''SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14''''''','@dba.password@','DWH Repository Database','@sybase.driver@',null);

INSERT INTO META_DATABASES VALUES ('dba','0','DBA','5','dwh','jdbc:sqlanywhere:host=dwhdb:@dwh.port@;InitString=''SET TEMPORARY OPTION MAX_HASH_ROWS=18000000''','@dba.password@','The DataWareHouse Database','@sybase.driver@',null);

commit;
