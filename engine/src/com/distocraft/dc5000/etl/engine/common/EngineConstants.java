package com.distocraft.dc5000.etl.engine.common;

import java.util.TimeZone;

/**
 * This class is String resource pool for dagger.engine All constants are here.
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.2
 */
public final class EngineConstants extends java.lang.Object {

	public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
	public static final int SQL_ERR = 21;
	public static final int OTHER_ERR = 22;
	public static final int DAGGER_ERR = 23;
	public static final int SYSTEM_ERR = 24;
	public static final int FILE_ERR = 25;

	public static final int COL_CONST_ERROR_STOP_CODE = 30;
	public static final int PLUGIN_ERR = 31;

	public static final int EXECUTION_NORMAL_CODE = 0;
	public static final int EXECUTION_STOP_LIMIT_CODE = 10;

	public static final String ERR_TYPE_SQL = "SQL ERROR";
	public static final String ERR_TYPE_DEFINITION = "DEFINITION ERROR";
	public static final String ERR_TYPE_VALIDATION = "VALIDATION ERROR";
	public static final String ERR_TYPE_WARNING = "WARNING";
	public static final String ERR_TYPE_SYSTEM = "SYSTEM ERROR";
	public static final String ERR_TYPE_EXECUTION = "EXECUTION ERROR";

	public static final String SOURCE_TABLE_ALIAS = "SRC";
	public static final String TARGET_TABLE_ALIAS = "TRG";
	public static final String FK_TABLE_ALIAS = "FKT";
	public static final String TRANSF_TABLE_ALIAS = "tables";
	public static final String TRANSF_OUT_TABLE_ALIAS = "out_tables";
	public static final String TRANSF_TABLE_VALUE_ALIAS = "vals";

	public static final String FILE_TYPE_FIXED = "FIXED";

	public static final String EX_WRONG_NUM_OF_COLS_TEXT = "The number of table columns and file data don't match";
	public static final String EX_NO_COL_LENGTH_TEXT = "No column length specified for column: {0}";
	public static final String EX_TOO_LONG_COL_TEXT = "Column length exceeds row length, column: {0}";
	public static final String MAX_ERROR_STOP_TEXT = "Maximum number of errors exceeded, Errors: {0} Max: {1}";
	public static final String FK_ERROR_STOP_TEXT = "Foreign key error max exceeded, Errors: {0} Max: {1}";
	public static final String COL_CONST_ERROR_STOP_TEXT = "Collection Column Constraint error max exceeded, Errors: {0} Max: {1}";
	public static final String COL_FK_ERROR_STOP_TEXT = "Collection Foreign Key error max exceeded, Errors: {0} Max: {1}";
	public static final String NO_COL_DELIM_TEXT = "No column delimiter defined";

	public static final String NO_DEBUG_TEXT = "Nothing to execute";
	public static final String NO_ERROR_TEXT = "No error text available";
	public static final String NO_STATUS_TEXT = "No status available";

	public static final String PLUGIN_ITERATOR_METHOD_NAME = "next";
	public static final String PLUGIN_HASNEXT_METHOD_NAME = "hasNext";
	public static final String PLUGIN_ADDROW_METHOD_NAME = "addRow";
	public static final String PLUGIN_COMMIT_METHOD_NAME = "commit";

	public static final String CANNOT_SEND_MAIL = "Cannot send mail, server:{0} port:{1}";
	public static final String CANNOT_CONNECT_TO_METADATA = "Cannot connect to metadata repository:{0} user:{1}";
	public static final String CANNOT_REMOVE_DATA = "Cannot remove data from table:{0} (Batch column name: {1})";

	public static final String CANNOT_EXECUTE = "Cannot execute action.";
	public static final String CANNOT_SET_TIMESTAMP = "Cannot set timestamp, table:{0}, timestamp column:{1}";

	public static final String CANNOT_READ_METADATA = "Cannot read metadata table {0}";
	public static final String CANNOT_WRITE_DEBUG = "Cannot write debug information";
	public static final String CANNOT_WRITE_STATUS = "Cannot write status information";
	public static final String CANNOT_WRITE_ERROR = "Cannot write error information";
	public static final String CANNOT_WRITE_TRANSFER_BATCH = "Cannot write Transfer Batch information";
	public static final String CANNOT_CREATE_DBCONNECTION = "Cannot connec to DB: {0} user: {1}";

	public static final String NO_PLUGIN_NAME = "No plugin name in Metadata.";
	public static final String NO_PRIMARY_KEY_DEFINED = "No Primary Key Defined Source:{0}, Target:{1}";

	public static final String CANNOT_WRITE_FILE = "Cannot write to file :{0}";
	public static final String CANNOT_READ_FILE = "Cannot read file :{0}";
	public static final String NO_FILE_NAME = "No file name given";

	public static final String STATUS_EXEC_ENDED_WITH_WARN = "Execution ended with warnings";
	public static final String STATUS_EXEC_ENDED_WITH_ERR = "Execution stopped with errors";
	public static final String STATUS_EXEC_START = "Execution Started";
	public static final String STATUS_EXEC_STOP = "Execution Ended";

	public static final String ERR_MAIL_SENDER = "DAGGER";
	public static final String ERR_MAIL_SUBJECT = "DAGGER:";

	public static final String DAGGER_OWNER_NAME_ORACLE = "DAGGER";
	public static final String DAGGER_OWNER_NAME_SYBASE = "meta_tables.dbo";

	public static final String NUMERIC_NAME_ORACLE = "NUMBER";
	public static final String NUMERIC_NAME_SYBASE = "NUMERIC";

	public static final String MORE_THAN_ONE_RUN_VERSIONS = "More than one run version";
	public static final String NO_RUN_VERSION = "No run version available";
  public static final String THRESHOLD_NAME = "GateKeeper.thresholdLimit";

	// Set Context String Constants
	public static final String SET_CONTEXT_STORAGE_ID = "STORAGE_ID"; // Set
																		// Object
																		// is
																		// String
																		// (storage
																		// name)
	public static final String SET_CONTEXT_COUNTING_ROW_INFO = "COUNTING_ROW_INFO"; // Set
																					// Object
																					// is
	// List<Countingmangement>
	public static final String SET_CONTEXT_COUNTING_INTERVALS = "COUNTING_INTERVALS"; // Set
																						// Object
																						// is
	// Map<Integer,Set<String>> or
	// (Map(intervalInMinutes,Set<intervalDateTimes>)


	public final static String LOCK_TABLE = "lockTable";
	public final static String AGG_DATE = "aggDate";
	public final static String TIME_LEVEL = "timelevel";
	public final static String RESTORE_FROM_TO_DATES = "fromToDates";
	
}
