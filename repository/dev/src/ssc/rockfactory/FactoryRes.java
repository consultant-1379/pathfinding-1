/**
 * ETL Repository access library.<br>
 * <br>
 * Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved.<br>
 * 
 * @author lemminkainen
 */
package ssc.rockfactory;

public final class FactoryRes  {

  public static final String CANNOT_GET_TIMESTAMP = "Error when trying to update Timestamp information ";

  public static final String CANNOT_GET_SEQUENCE = "Cannot get sequence data from database : ";

  public static final String CANNOT_GET_TABLE_DATA = "Cannot get data from database for table : ";

  public static final String SOMEONE_HAS_UPDATED = "Someone has updated the record, please query the data again ";

  public static final String NO_PRIMARYKEY_INFO = "No primary key information available";

  public static final String[] NOT_COLUMN_NAMES = { "timeStampName", "primaryKeyNames", "columnsAndSequences",
	  "rockFact", "newItem", "private_table_namexyz" , "modifiedColumns", "validateData", "original", "currentTrans", "otherTrans",
	  "isDecryptionRequired" };

  public static final String TIMESTAMP_METHOD = "gettimeStampName";

  public static final String COLS_SEQS_METHOD = "getcolumnsAndSequences";

  public static final String PRIMARYKEY_METHOD = "getprimaryKeyNames";

  public static final String TABLE_PREFIX = "Rock";

  public static final String GET_TABLE_NAME_METHOD_NAME = "getTableName";

  public static final String VALUE_EQUALS_NULL = "The value cannot be == null";

  public static final String SYBASE_DRIVER_NAME = "SybDriver";

  public static final String ORACLE_DRIVER_NAME = "OracleDriver";

  public static final String SYBASE_IS_NULL_FUNCTION_NAME = "isnull";

  public static final String ORACLE_IS_NULL_FUNCTION_NAME = "NVL";

  public static final int SYBASE_CHILD_RECORD_EXIST_CODE = 547;

  public static final int ORACLE_CHILD_RECORD_EXIST_CODE = 2292;

}
