/**
 * ETL Repository access library.<br>
 * <br>
 * Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved.<br>
 * 
 * @author lemminkainen
 */
package ssc.rockfactory;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

/**
 * An iterface for the both usages of db access: 1. the user user is a database user 2. All users
 * use a single database user database connections
 */
interface RockFactory_I {

  /**
   * insertData Executes an insert SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          dataObj The data object to insert.
   * @param boolean
   *          useTimestamp If true, the current time is automatically updated.
   * @param boolean
   *          useTimestamp If true, the number fields associated with sequences get values
   *          automatically.
   * @return int Number of inserted rows.
   * @exception SQLException
   */
  int insertData(Object dataObj, boolean useTimestamp, boolean useSequence) throws SQLException, RockException;

  /**
   * insertData Executes an insert SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          dataObj The data object to insert.
   * @return int Number of inserted rows.
   * @exception SQLException
   */
  int insertData(Object dataObj) throws SQLException, RockException;

  /**
   * Executes a delete SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          whereObj The data object to delete.
   * @return int Number of deleted rows.
   * @exception SQLException
   */
  int deleteData(boolean isPrimaryKeyDelete, Object whereObj) throws SQLException, RockException;

  /**
   * Executes a delete SQL -clause based on given object data and class information.
   * 
   * @param whereString
   *          whereStr The data for the WHERE -clause ( a sql clause for the where part, does not
   *          include WHERE).
   * @return int Number of deleted rows.
   * @exception SQLException
   */
  int deleteData(boolean isPrimaryKeyDelete, String whereString) throws SQLException, RockException;

  /**
   * Executes an update SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          dataObj The data object to update.
   * @param boolean
   *          isPrimaryKeyUpdate If true, where -clause is created from primary key information.
   * @param Object
   *          whereObj The data for the WHERE -clause.
   * @return int Number of updated rows.
   * @exception SQLException
   */
  int updateData(Object dataObj, boolean isPrimaryKeyUpdate, Object whereObj) throws SQLException, RockException;

  /**
   * Executes an update SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          dataObj The data object to update.
   * @param boolean
   *          isPrimaryKeyUpdate If true, where -clause is created from primary key information.
   * @param whereObj
   *          all field that are != null are used for the WHERE -part
   * @param boolean
   *          useTimestamp If false, it is not checked wheather the data has changed.
   * @return int Number of updated rows.
   * @exception SQLException
   */
  int updateData(Object dataObj, boolean isPrimaryKeyUpdate, Object whereObj, boolean useTimestamp)
      throws SQLException, RockException;

  /**
   * Executes an update SQL -clause based on given object data and class information.
   * 
   * @param Object
   *          dataObj The data object to update.
   * @param boolean
   *          isPrimaryKeyUpdate If true, where -clause is created from primary key information.
   * @param String
   *          whereStr The data for the WHERE -clause ( a sql clause for the where part, does not
   *          include WHERE).
   * @param boolean
   *          useTimestamp If false, it is not checked wheather the data has changed.
   * @return int Number of updated rows.
   * @exception SQLException
   */
  int updateData(Object dataObj, boolean isPrimaryKeyUpdate, String whereStr, boolean useTimestamp)
      throws SQLException, RockException;

  /**
   * Sets the cursor for a SQL -clause
   * 
   * @param isPrimaryKeySelect
   *          if true the primary key data is used for the WHERE -part, else WHERE is constructed
   *          from whereDBParameter
   * @param whereObj
   *          all field that are != null are used for the WHERE -part
   * @param whereString
   *          whereStr The data for the WHERE -clause ( a sql clause for the where part, does not
   *          include WHERE).
   * @return
   * @exception SQLException
   */
  RockResultSet setSelectSQL(String selectString) throws SQLException, RockException;

  /**
   * Sets the cursor for a SQL -clause
   * 
   * @param isPrimaryKeySelect
   *          if true the primary key data is used for the WHERE -part, else WHERE is constructed
   *          from whereDBParameter
   * @param whereDBParameter
   *          all field that are != null are used for the WHERE -part
   * @return
   * @exception SQLException
   */
  RockResultSet setSelectSQL(boolean isPrimaryKeySelect, Object whereObj) throws SQLException, RockException;

  /**
   * Sets the cursor for a SQL -clause
   * 
   * @param isPrimaryKeySelect
   *          if true the primary key data is used for the WHERE -part, else WHERE is constructed
   *          from whereObj
   * @param whereDBParameter
   *          all field that are != null are used for the WHERE -part
   * @return
   * @exception SQLException
   */
  RockResultSet setSelectSQL(boolean isPrimaryKeySelect, Object whereObj, String orderByStr) throws SQLException,
      RockException;

  /**
   * Sets the cursor for a SQL -clause
   * 
   * @param isPrimaryKeySelect
   *          if true the primary key data is used for the WHERE -part, else WHERE is constructed
   *          from whereDBParameter
   * @param whereString
   *          whereStr The data for the WHERE -clause ( a sql clause for the where part, does not
   *          include WHERE). WHERE -part
   * @return
   * @exception SQLException
   */
  RockResultSet setSelectSQL(boolean isPrimaryKeySelect, String whereString, String orderByStr) throws SQLException,
      RockException;
  
  /**
   * Sets the cursor for a SQL -clause
   * 
   * @param isPrimaryKeySelect
   *          if true the primary key data is used for the WHERE -part, else WHERE is constructed
   *          from whereDBParameter
   * @param whereDBParameter
   *          all field that are != null are used for the WHERE -part
   * @return
   * @exception SQLException
   */
  RockResultSet setSelectSQL(boolean isPrimaryKeySelect, Object whereObj, String whereClause, String orderByStr) throws SQLException, RockException;

  /**
   * Gets data from the cursor for a SQL -clause
   * 
   * @param dataObj
   *          holds the data retrieved from the db
   * 
   * @return true while more data is available
   * @exception SQLException
   */
  Iterator getData(Object dataObj, RockResultSet rockResults) throws SQLException, RockException;

  /**
   * Executes a prepared SQL -clause based on String.
   * 
   * @param preparedSqlStr
   *          String to execute
   * @param objVec
   *          vector containig the objects related to the string
   * @exception SQLException
   */
  void executePreparedSql(String preparedSqlStr, Vector rowVec) throws SQLException, RockException;

  /**
   * Executes a prepared SQL -clause based on String. If update cannot be done the an insert is
   * exec.
   * 
   * @param preparedSqlStr
   *          String to execute
   * @param objVec
   *          vector containig the objects related to the string
   * @exception SQLException
   */
  void executePreparedInsAndUpdSql(String preparedUpdStr, Vector rowVec, String preparedInsStr) throws SQLException,
      RockException;

  /**
   * commit Commits the transaction if autocommit is OFF.
   * 
   * @exception SQLException
   */
  void commit() throws SQLException, RockException;

  /**
   * rollback Rollbacks the transaction if autocommit is OFF.
   * 
   * @exception SQLException
   */
  void rollback() throws SQLException, RockException;

  /**
   * begin Begins a transaction (ended by commit/rollback
   * 
   * 
   */
  void begin();

  /**
   * Copies tables child table information
   * 
   * @param tableName
   * @param setValues
   *          key <setMethodName> value {oldValue,newValue}
   */
  void copySchema(String tableName, Hashtable setValues, String packagePath) throws SQLException, RockException;
}