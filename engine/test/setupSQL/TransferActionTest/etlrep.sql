insert into META_COLLECTION_SETS (COLLECTION_SET_ID, COLLECTION_SET_NAME, DESCRIPTION, VERSION_NUMBER, ENABLED_FLAG, TYPE) 
values 
(1, 'DC_E_TEST', 'test', '((6))', 'Y', 'Techpack');


INSERT
INTO
    Meta_collections
    (
        COLLECTION_SET_ID, COLLECTION_ID,
        COLLECTION_NAME,
        MAIL_ERROR_ADDR, MAIL_FAIL_ADDR, MAIL_BUG_ADDR,
		MAX_ERRORS, MAX_FK_ERRORS, MAX_COL_LIMIT_ERRORS,
		CHECK_FK_ERROR_FLAG, CHECK_COL_LIMITS_FLAG,
		LAST_TRANSFER_DATE,
        VERSION_NUMBER,
		USE_BATCH_ID,
        PRIORITY,
        QUEUE_TIME_LIMIT,
        ENABLED_FLAG,
        SETTYPE,
        FOLDABLE_FLAG,
        MEASTYPE,
        HOLD_FLAG,
        SCHEDULING_INFO
    )
    VALUES
    (
        1, 2,
        'testCOLLECTION_NAME',
        'testMAIL_ERROR_ADDR', 'testMAIL_FAIL_ADDR', 'testMAIL_BUG_ADDR',
		1, 1, 1,
		'Y', 'Y',
		'2000-01-01 00:00:00.0',
        'versionnumber',
		'Y',
		1,
		1,
		'Y',
		'SETTYPE',
        'Y',
        'testMEASTYPE',
        'Y',
        'testSCHEDULING_INFO'
	);

INSERT
INTO
    etlrep.META_TRANSFER_ACTIONS
    (
        VERSION_NUMBER,
        TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID,
        ACTION_TYPE, TRANSFER_ACTION_NAME,
        ORDER_BY_NO,
        DESCRIPTION,
        WHERE_CLAUSE_01,
        ACTION_CONTENTS_01,
        ENABLED_FLAG,
        CONNECTION_ID
    )
	VALUES
	(
		'versionnumber',
		3, 2, 1,
		'Loader', 'transferactionname',
		1, 'description',
		'tablename=event_e_sgeh_success
versiondir=123',
		'actioncontents01',
		'Y', 1
	);

INSERT
INTO
    etlrep.META_TRANSFER_ACTIONS
    (
        VERSION_NUMBER,
        TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID,
        ACTION_TYPE, TRANSFER_ACTION_NAME,
        ORDER_BY_NO,
        DESCRIPTION,
        WHERE_CLAUSE_01,
        ACTION_CONTENTS_01,
        ENABLED_FLAG,
        CONNECTION_ID
    )
	VALUES
	(
		'versionnumber',
		4, 2, 1,
		'CreateCollectedData', 'transferactionname',
		1, 'description',
		'tablename=event_e_sgeh_success
versiondir=123',
		'actioncontents01',
		'Y', 1
	);
	
INSERT
INTO
    etlrep.META_TRANSFER_ACTIONS
    (
        VERSION_NUMBER,
        TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID,
        ACTION_TYPE, TRANSFER_ACTION_NAME,
        ORDER_BY_NO,
        DESCRIPTION,
        WHERE_CLAUSE_01,
        ACTION_CONTENTS_01,
        ENABLED_FLAG,
        CONNECTION_ID
    )
	VALUES
	(
		'versionnumber',
		5, 2, 1,
		'UpdateCollectedData', 'transferactionname',
		1, 'description',
		'tablename=event_e_sgeh_success
versiondir=123',
		'actioncontents01',
		'Y', 1
	);
	
INSERT
INTO
    etlrep.META_TRANSFER_ACTIONS
    (
        VERSION_NUMBER,
        TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID,
        ACTION_TYPE, TRANSFER_ACTION_NAME,
        ORDER_BY_NO,
        DESCRIPTION,
        WHERE_CLAUSE_01,
        ACTION_CONTENTS_01,
        ENABLED_FLAG,
        CONNECTION_ID
    )
	VALUES
	(
		'versionnumber',
		6, 2, 1,
		'UpdateHashIds', 'transferactionname',
		1, 'description',
		'tablename=event_e_sgeh_success
versiondir=123',
		'actioncontents01',
		'Y', 1
	);