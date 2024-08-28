/**
* ETL Repository access library.<br>
* <br>
* Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved.<br>
*
* @author lemminkainen
*/
package ssc.rockfactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({ "PMD.ConfusingTernary" })
public class RockFactory implements RockFactory_I {

            protected Connection connection;

            protected String dbURL;

            protected String driverName;

            protected String strUserName;

            protected String strPassword;

            protected boolean autoCommit;

            protected String sqlQuote;

            protected String dbType;
            
            private static final Logger LOG = Logger.getLogger("etlengine.repository.RockFactory");
            
            private static final int DEFAULT_QUERY_TIMEOUT = 5*60; //in minutes
            
            private static final int MIN_QUERY_TIMEOUT = DEFAULT_QUERY_TIMEOUT; //in minutes
            
            private static final int MAX_QUERY_TIMEOUT = 24*60; //in minutes
            
            private static final String TIMEOUT_PROPERTY_STRING = "defaultQueryTimeOutInMins";
            
            //validate query timeout
            static {
              int queryTimeout = DEFAULT_QUERY_TIMEOUT;
          	  try {
          		  queryTimeout = Integer.parseInt(System.getProperty(TIMEOUT_PROPERTY_STRING));
          	  } catch (Exception e) {
          		  //LOG.log(Level.WARNING,"defined statsdefaultQueryTimeOutInMins : "+queryTimeout+" is not valid :"
          				 // + ", So setting the default value: "+DEFAULT_QUERY_TIMEOUT);
          		  queryTimeout = DEFAULT_QUERY_TIMEOUT;
          	  }
          	  if (queryTimeout >= MIN_QUERY_TIMEOUT && queryTimeout <= MAX_QUERY_TIMEOUT) {
          		System.setProperty(TIMEOUT_PROPERTY_STRING, String.valueOf(queryTimeout));
          	  } else {
          		  //LOG.log(Level.WARNING,"defined statsdefaultQueryTimeOutInMins : "+queryTimeout+" "
          		  		//+ "is not within the defined boundary : "
          				 // +"("+MIN_QUERY_TIMEOUT+" to "+MAX_QUERY_TIMEOUT+")"
          		  		//+ ", So setting the default value: "+DEFAULT_QUERY_TIMEOUT);
          		  System.setProperty(TIMEOUT_PROPERTY_STRING, String.valueOf(DEFAULT_QUERY_TIMEOUT));
          	  }
            }
            
            public static int DEFAULT_QUERY_TIMEOUT_IN_SECONDS = Integer.parseInt(System.getProperty(
                    "defaultQueryTimeOutInMins", "300")) * 60;

            
            
            public static int SHORT_QUERY_TIMEOUT_IN_SECONDS = Integer.parseInt(System.getProperty("shortQueryTimeOutInMins",
                    "60")) * 60;

            public static int LONG_QUERY_TIMEOUT_IN_SECONDS = Integer.parseInt(System.getProperty("longQueryTimeOutInMins",
                                    "180")) * 60;

            public static int UNLIMITED_QUERY_TIMEOUT_IN_SECONDS = Integer.parseInt(System.getProperty(
                                    "unlimitedQueryTimeOutInMins", "1440")) * 60;
            
            private static final String TEMP_OPTION_REQUEST_TIME = "SET TEMPORARY OPTION request_timeout=";
            
            private static final String CON_STRING_FOR_TIMEOUT = ".+InitString='SET TEMPORARY OPTION MAX_HASH_ROWS=18000000'";
            
            private static final String DC_USER = "dc";

            int maxRetry = 8;

            int errorCode = 0;
			
            Connection conn;
            /* Parameters not required for sajdbc 
            //public static String IS_CONNECTION_CLOSE = "INTERNAL";
            
            //public static String SERIALIZE_REQUESTS = "true";
			*/
	       
            

            /**
            * Create RockFactory with connection name
            *
            * @param dbURL
            *            url
            * @param dbType
            *            type
            * @param strUserName
            *            user
            * @param strPassword
            *            password
            * @param driverName
            *            driver
            * @param conName
            *            conn name
            * @param autoCommit
            *            auto commit
            * @throws RockException
            *             errors
            * @throws java.sql.SQLException
            *             errors
            */
            public RockFactory(final String dbURL, final String dbType, final String strUserName, final String strPassword,
                                    final String driverName, final String conName, final boolean autoCommit) throws SQLException, RockException {
                        this(dbURL, dbType, strUserName, strPassword, driverName, conName, autoCommit, -1);
            }

            /**
            * Create RockFactory with connection name
            *
            * @param dbURL
            *            url
            * @param strUserName
            *            user
            * @param strPassword
            *            password
            * @param driverName
            *            driver
            * @param conName
            *            con name
            * @param autoCommit
            *            auto commit
            * @throws java.sql.SQLException
            *             errors
            * @throws RockException
            *             errors
            */
            public RockFactory(final String dbURL, final String strUserName, final String strPassword, final String driverName,
                                    final String conName, final boolean autoCommit) throws SQLException, RockException {
                        this(dbURL, "sybase", strUserName, strPassword, driverName, conName, autoCommit);
            }

            /**
            * Create RockFactory with connection name
            *
            * @param dbURL
            *            url
            * @param strUserName
            *            user
            * @param strPassword
            *            password
            * @param driverName
            *            driver
            * @param conName
            *            con name
            * @param autoCommit
            *            auto commit
            * @param isolationLevel
            *            not used
            * @throws java.sql.SQLException
            *             errors
            * @throws RockException
            *             errors
            */
            public RockFactory(final String dbURL, final String strUserName, final String strPassword, final String driverName,
                                    final String conName, final boolean autoCommit, final int isolationLevel) throws SQLException,
                                    RockException {
                        this(dbURL, "sybase", strUserName, strPassword, driverName, conName, autoCommit, isolationLevel);
            }

            /**
            * Create RockFactory with connection name
            *
            * @param dbURL
            *            user
            * @param dbType
            *            user type
            * @param strUserName
            *            username
            * @param strPassword
            *            password
            * @param driverName
            *            driver
            * @param conName
            *            con name
            * @param autoCommit
            *            auto commit
            * @param isolationLevel
            *            not used
            * @throws RockException
            *             errors
            * @throws java.sql.SQLException
            *             errors
            */
            public RockFactory(final String dbURL, final String dbType, final String strUserName, final String strPassword,
                                    final String driverName, final String conName, final boolean autoCommit, final int isolationLevel)
                                    throws SQLException, RockException {

                        this.dbURL = dbURL;
                        this.dbType = dbType;
                        this.driverName = driverName;
                        this.strUserName = strUserName;
                        this.strPassword = strPassword;
                        this.autoCommit = autoCommit;
                        this.connection = initiateConnection(conName);
            }

            /**
            * Create a new RockFactory instance using an existing JDBC Connection. Mainly used in the Events Services EJB.
            * Note, using this method will not set the password internally so <code>getPassword()</code> will return a dummy
            * value.
            *
            * @param rawConnection
            *            Raw sql Connection
            * @throws SQLException
            *             If there are errors getting the Connections MetaData
            */
            public RockFactory(final Connection rawConnection) throws SQLException {
                        final DatabaseMetaData meta = rawConnection.getMetaData();
                        this.dbURL = meta.getURL();
                        this.dbType = "sybase";
                        this.driverName = meta.getDriverName();
                        this.strUserName = meta.getUserName();
                        this.strPassword = "?";
                        this.autoCommit = rawConnection.getAutoCommit();
                        this.connection = rawConnection;
            }

            @SuppressWarnings({ "PMD.CloseResource" })
            // eeipca : returns the connection
            private Connection initiateConnection(final String conName) throws SQLException, RockException {

                        for (int i = 0; i < maxRetry; i++) {
                                    try {

                                                LOG.finest("Initiate Connection for " + conName);
                                                final Driver driver = (Driver) Class.forName(driverName).newInstance();
                                                final Properties p = new Properties();
                                                p.put("user", strUserName);
                                                p.put("password", strPassword);
                                                //p.put("INTERNAL_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT_IN_SECONDS);
                                                //p.setProperty("INTERNAL_QUERY_TIMEOUT", Integer.toString(DEFAULT_QUERY_TIMEOUT_IN_SECONDS)); // hsql
                                                                                                                                                                                                                                                                                                // setProperties
                                                //p.put("DEFAULT_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT_IN_SECONDS);
                                                //p.setProperty("DEFAULT_QUERY_TIMEOUT", Integer.toString(DEFAULT_QUERY_TIMEOUT_IN_SECONDS)); // hsql
                                                
                                                //Added IS_CLOSED_TEST parameter to avoid deadlock on JDBC
                                                /*p.put("IS_CLOSED_TEST", IS_CONNECTION_CLOSE);
                                                p.setProperty("IS_CLOSED_TEST", IS_CONNECTION_CLOSE); 
                                                                                                                                                                                                                                                                                                                                    // throws a
                                                // Added SERIALIZE_REQUESTS parameter as suggested by SAP - 1028245 
                                                p.put("SERIALIZE_REQUESTS", SERIALIZE_REQUESTS);
                                                p.setProperty("SERIALIZE_REQUESTS", SERIALIZE_REQUESTS);*/
                                                                                                                                                                                                                                                                              // setProperties
                                                if (dbType.equalsIgnoreCase("sybase")) {
                                                	  //p.put("REMOTEPWD", ";CON=" + conName);
                                                	if (DC_USER.equals(strUserName) && isAppendTimeOut(dbURL)) {
                                                		dbURL = dbURL.substring(0, dbURL.length()-1);
                                                    	dbURL = dbURL + ";" + TEMP_OPTION_REQUEST_TIME+DEFAULT_QUERY_TIMEOUT_IN_SECONDS + "'";
                                                    	LOG.fine("dbURL with Timeout settings :"+dbURL);
                                                	}
                                                	dbURL = dbURL + ";CON=" + conName;
                                                	
                                                	LOG.fine("dbURL Used====" + dbURL);
                                                }
                                                											
                                                conn = driver.connect(dbURL, p);
                                                conn.setAutoCommit(autoCommit);

                                                final DatabaseMetaData metaData = conn.getMetaData();
                                                this.sqlQuote = metaData.getIdentifierQuoteString();
                                                LOG.config("Connection was created successfully for set: " + conName);
                                                // i=maxRetry;
                                                
                                                // If successful then break the for loop
                                                break;
												
												

                                    } catch (final SQLException sqlE) {
                                                if (sqlE.getNextException() != null) {
                                                            LOG.warning("RockFactory:: SQLException occured while making connection for ( Set name :" + conName
                                                                                    + " ). ");
                                                         //Inorder to catch the exception if exception occurs in the last retry.
                                                            int maxRetryValue=maxRetry-1;
                                                            if(i==maxRetryValue){
                                                            	throw new RockException(sqlE.getMessage());
                                                            }
                                                            errorCode = sqlE.getErrorCode();

                                                            // 1601 - Database server connection limit exceeded
                                                            if (errorCode == 1601) {
                                                                        LOG.warning("Database server connection limit exceeded for ( Set name :" + conName
                                                                                                + " ), retrying in 15 seconds.");
                                                                        try {
                                                                                    // Database connections limit exceeded. Retrying after 15 seconds to make connection
                                                                                    Thread.sleep(15000);
																					
                                                                        } catch (final Exception ie) {
                                                                                    throw new RockException(ie.getMessage(), ie);
                                                                        }
                                                            }
                                                            else if((dbURL.contains("repdb")) && (errorCode == 0)){ //Added this condition to avoid Set failure
                                                                                                                                                                                                                                                                        //because of connection error to dwhrep.
                                                                        LOG.warning("Repdb Login failure for ( Set name :" + conName
                                                                                                + " ), retrying in 15 seconds.");
                                                                        try {
                                                                                    // Database connections limit exceeded. Retrying after 10 seconds to make connection
                                                                                    Thread.sleep(10000);
                                                                        } catch (final Exception ie) {
                                                                                    throw new RockException(ie.getMessage(), ie);
                                                                        }
                                                            }
                                                            
                                                            else {
                                                                        LOG.info("Error Code is : " + errorCode);
                                                                        throw new RockException(sqlE.getMessage());
                                                                                                                                    //modifying this to avoid retry 8 times without any sleep
                                                            }
                                                } else {
                                                            LOG.warning("Exception is getting NULL ( " + sqlE.getMessage() + " ).");
                                                }
                                    } catch (final Exception e) {
									
                                                throw new RockException("Driver not found", e);
                                    }
                        }
                        return conn;
            }
            
            private boolean isAppendTimeOut (String dburl) {
            	Pattern p = Pattern.compile(CON_STRING_FOR_TIMEOUT);
            	Matcher m = p.matcher(dburl);
            	return m.matches();
            }

            /**
            * Return the correct is null database function according to the databse type.
            *
            * @return String
            */
            private String getIsNullFName() {

                        if (this.driverName.indexOf(FactoryRes.SYBASE_DRIVER_NAME) > 0) {
                                    return FactoryRes.SYBASE_IS_NULL_FUNCTION_NAME;
                        } else {
                                    return FactoryRes.ORACLE_IS_NULL_FUNCTION_NAME;
                        }

            }

            /**
            * Returns the sql error code when child records exist when trying to delete a row.
            *
            * @return SQL error code
            */
            public int getChildRecorsExistCode() {
                        if (this.driverName.indexOf(FactoryRes.SYBASE_DRIVER_NAME) > 0) {
                                    return FactoryRes.SYBASE_CHILD_RECORD_EXIST_CODE;
                        } else {
                                    return FactoryRes.ORACLE_CHILD_RECORD_EXIST_CODE;
                        }
            }

            public String getDbURL() {
                        return this.dbURL;
            }

            public String getDriverName() {
                        return this.driverName;
            }

            public String getUserName() {
                        return this.strUserName;
            }

            public String getPassword() {
                        return this.strPassword;
            }

            public boolean getAutoCommit() {
                        return this.autoCommit;
            }

            /**
            * isColumnName tests if name is a columns name
            *
            * @param fieldName
            *            field name
            * @param fieldTypeName
            *            field class type
            * @return boolean true if a column name, else false
            */
            protected boolean isColumnName(final String fieldName, final String fieldTypeName) {
                        if (fieldTypeName.equals("Vector")) {
                                    return false;
                        }
                        for (final String element : FactoryRes.NOT_COLUMN_NAMES) {
                                    if (fieldName.equals(element)) {
                                                return false;
                                    }
                        }
                        return true;
            }

            /**
            * getFieldValueWithQuotes returns a string containing quotes if type is ?String or Timestamp.
            *
            * @param obj
            *            instance
            * @param field
            *            field
            * @param method
            *            method
            * @return a SELECT - String containing the WHERE -part of the SQL -clause
            * @throws RockException
            *             errors
            */
            protected String getFieldValueWithQuotes(final Object obj, final Field field, final Method method)
                                    throws RockException {
                        try {
                                    final Object tempObj = method.invoke(obj, (Object[]) null);
                                    String helpString = "NULL";
                                    if (tempObj != null) {
                                                helpString = tempObj.toString();
                                    }

                                    final Class<?> fieldClass = field.getType();

                                    final String className = fieldClass.toString();
                                    int pointLoc = className.lastIndexOf(".") + 1;
                                    final String name = className.substring(pointLoc, className.length());

                                    if (name.equals("Timestamp")) {
                                                if (helpString.equals("NULL")) {
                                                            return helpString;
                                                } else {
                                                            pointLoc = helpString.lastIndexOf(".");
                                                            if (this.driverName.indexOf(FactoryRes.ORACLE_DRIVER_NAME) > 0) {
                                                                        return "TO_DATE('" + helpString.substring(0, pointLoc) + "','yyyy-mm-dd hh24:mi:ss')";
                                                            } else {
                                                                        return "'" + helpString.substring(0, pointLoc) + "'";
                                                            }
                                                }
                                    } else {
                                                if (name.equals("String")) {
                                                            if (helpString.equals("NULL")) {
                                                                        return helpString;
                                                            } else {
                                                                        final String newString = helpString.replaceAll("'", "''");
                                                                        if (newString.length() > 0) {
                                                                                    return "'" + newString + "'";
                                                                        } else {
                                                                                    return "'" + helpString + "'";
                                                                        }
                                                            }
                                                } else {
                                                            return helpString;
                                                }
                                    }
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * createSelectSQL is general SQL -clause parser for all of the SELECT -clauses.
            *
            * @param obj
            *            db object
            * @param c
            *            object class
            * @param whereString
            *            where clause
            * @return a SELECT - String containing the WHERE -part of the SQL -clause
            * @throws RockException
            *             errors
            */
            protected String createSelectSQL(final Object obj, final Class<?> c, final String whereString) throws RockException {

                        String sQLString = "SELECT ";
                        final Field[] classFields = c.getDeclaredFields();

                        for (final Field field : classFields) {

                                    final String fieldType = field.getType().getName();
                                    final String fieldTypeName = fieldType.substring(fieldType.lastIndexOf(".") + 1, fieldType.length());

                                    if (isColumnName(field.getName(), fieldTypeName)) {

                                                if (!sQLString.equals("SELECT ")) {
                                                            sQLString = new StringBuffer(sQLString).append(",").toString();
                                                }
                                                sQLString = new StringBuffer(sQLString).append(field.getName()).toString();
                                    }

                        }

                        sQLString = new StringBuffer(sQLString).append(" FROM ").append(getTableName(obj, c)).toString();
                        sQLString = new StringBuffer(sQLString).append(whereString).toString();

                        return sQLString;

            }

            /**
            * getFieldGetMethod returns the getmethod for a field
            *
            * @param objClass
            *            class
            * @param field
            *            field
            * @param setOrGet
            *            getter or setter
            * @return method get method the a given field.
            * @throws RockException
            *             erros
            */
            protected Method getFieldMethod(final Class<?> objClass, final Field field, final String setOrGet)
                                    throws RockException {
                        try {
                                    final String columnName = field.getName();
                                    final String firstCharOfColumn = columnName.substring(0, 1);
                                    final String restOfTheColumn = columnName.substring(1, columnName.length());

                                    final String getMethodName = setOrGet + firstCharOfColumn.toUpperCase() + restOfTheColumn.toLowerCase();
                                    Class<?>[] parameterTypes = { field.getType() };
                                    if (setOrGet.equals("get")) {
                                                parameterTypes = null;
                                    }
                                    return objClass.getMethod(getMethodName, parameterTypes);
                        } catch (final Exception e) {
                                    e.printStackTrace();
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * createUpdateSQL is general SQL -clause parser for all of the UPDATE -clauses.
            *
            * @param obj
            *            instance
            * @param dataClass
            *            clas tpye
            * @param whereString
            *            wher eclause
            * @return a UPDATE - String containing the WHERE -part of the SQL -clause
            * @throws RockException
            *             errors
            */
            protected String createUpdateSQL(final Object obj, final Class<?> dataClass, final String whereString)
                                    throws RockException {
                        try {
                                    String sQLString = "UPDATE ";
                                    sQLString = new StringBuffer(sQLString).append(getTableName(obj, dataClass)).append(" SET ").toString();
                                    final Field[] classFields = dataClass.getDeclaredFields();
                                    String setStr = " ";

                                    Set<String> modCols = null;

                                    if (obj instanceof RockDBObject) {
                                                modCols = ((RockDBObject) obj).gimmeModifiedColumns();
                                    }

                                    for (final Field field : classFields) {

                                                final String fieldType = field.getType().getName();
                                                final String fieldTypeName = fieldType.substring(fieldType.lastIndexOf(".") + 1, fieldType.length());

                                                if (isColumnName(field.getName(), fieldTypeName)) {

                                                            if ((modCols != null) && !modCols.contains(field.getName())) {
                                                                        continue;
                                                            }

                                                            if (!setStr.equals(" ")) {
                                                                        setStr = new StringBuffer(setStr).append(",").toString();
                                                            }
                                                            setStr = new StringBuffer(setStr).append(field.getName()).append("=").toString();
                                                            final Method method = getFieldMethod(dataClass, field, "get");
                                                            setStr = new StringBuffer(setStr).append(getFieldValueWithQuotes(obj, field, method)).toString();
                                                }
                                    }
                                    sQLString = new StringBuffer(sQLString).append(setStr).toString();
                                    sQLString = new StringBuffer(sQLString).append(whereString).toString();

                                    return sQLString;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * compares a string and a string vector
            *
            * @param fieldName
            *            test string
            * @param colSeqNames
            *            compare sctings
            * @return int index of a string in the string vector
            */

            private int getSeqColIndex(final String fieldName, final String[] colSeqNames) {
                        for (int i = 0; i < (colSeqNames.length - 1); i++) {
                                    if (fieldName.toLowerCase().equals(colSeqNames[i].toLowerCase())) {
                                                return i;
                                    }
                        }
                        return -1;
            }

            /**
            * Check if the parameter column name exists in the class fields
            *
            * @param dataClass
            *            class where the field should be
            * @param columnName
            *            column name to compare
            * @return boolean true if the column existed else false
            */
            private boolean columnExists(final Class<?> dataClass, final String columnName) {
                        final Field[] classFields = dataClass.getDeclaredFields();

                        for (final Field field : classFields) {
                                    if (field.getName().toLowerCase().equals(columnName.toLowerCase())) {
                                                return true;
                                    }
                        }
                        return false;
            }

            /**
            * createInsertSQL is general SQL -clause parser for all of the INSERT -clauses.
            *
            * @param useSequence
            *            useTimestamp If true, the current time is automatically updated.
            * @return a INSERT -String SQL -clause
            * @param obj
            *            obj
            * @param dataClass
            *            class type
            * @param useTimestamp
            *            use timestamps
            * @throws SQLException
            *             errorsa
            * @throws RockException
            *             erros
            */
            protected String createInsertSQL(final Object obj, final Class<?> dataClass, final boolean useTimestamp,
                                    final boolean useSequence) throws SQLException, RockException {
                        try {
                                    String sQLString = "INSERT INTO ";
                                    sQLString = new StringBuffer(sQLString).append(getTableName(obj, dataClass)).append(" (").toString();

                                    final Field[] classFields = dataClass.getDeclaredFields();
                                    String columnStr = " ";
                                    for (final Field field : classFields) {

                                                final String fieldType = field.getType().getName();
                                                final String fieldTypeName = fieldType.substring(fieldType.lastIndexOf(".") + 1, fieldType.length());

                                                if (isColumnName(field.getName(), fieldTypeName)) {

                                                            if (!columnStr.equals(" ")) {
                                                                        columnStr = new StringBuffer(columnStr).append(",").toString();
                                                            }
                                                            columnStr = new StringBuffer(columnStr).append(field.getName()).toString();
                                                }

                                    }

                                    sQLString = new StringBuffer(sQLString).append(columnStr).toString();
                                    sQLString = new StringBuffer(sQLString).append(") VALUES(").toString();
                                    String valuesStr = " ";
                                    final Method timeGetMethod = dataClass.getDeclaredMethod(FactoryRes.TIMESTAMP_METHOD, (Class[]) null);
                                    final String timeStampColumnName = (String) timeGetMethod.invoke(obj, (Object[]) null);
                                    final Method seqGetMethod = dataClass.getDeclaredMethod(FactoryRes.COLS_SEQS_METHOD, (Class[]) null);
                                    final String[] colSeqNames = (String[]) seqGetMethod.invoke(obj, (Object[]) null);

                                    for (final Field field : classFields) {

                                                final String fieldType = field.getType().getName();
                                                final String fieldTypeName = fieldType.substring(fieldType.lastIndexOf(".") + 1, fieldType.length());

                                                if (isColumnName(field.getName(), fieldTypeName)) {

                                                            if (!valuesStr.equals(" ")) {
                                                                        valuesStr = new StringBuffer(valuesStr).append(",").toString();
                                                            }
                                                            // if (useTimestamp) {
                                                            if (field.getName().toUpperCase().equals(timeStampColumnName.toUpperCase())) {
                                                                        final Method timeTsSetMethod = getFieldMethod(dataClass,
                                                                                                dataClass.getDeclaredField(timeStampColumnName), "set");
                                                                        final Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                                                                        final Object[] objVect = { currentTime };
                                                                        timeTsSetMethod.invoke(obj, objVect);
                                                            }
                                                            // }
                                                            if (useSequence) {
                                                                        final int seqColIndex = getSeqColIndex(field.getName(), colSeqNames);
                                                                        if (seqColIndex >= 0) {
                                                                                    final String columnName = colSeqNames[seqColIndex];
                                                                                    final String sequenceName = colSeqNames[seqColIndex + 1];

                                                                                    String sql1 = "";

                                                                                    if (sequenceName.equals("MAXVALUE")) {
                                                                                                sql1 = "SELECT MAX(" + this.getIsNullFName() + "(" + columnName + ",0))+1 FROM "
                                                                                                                        + getTableName(obj, dataClass) + " "
                                                                                                                        + createWhereSQLfromPrimaries(obj, columnName);
                                                                                    } else {
                                                                                                sql1 = "SELECT " + sequenceName + ".NEXTVAL" + " FROM DUAL";
                                                                                    }
                                                                                    final Statement sqlStatement = connection.createStatement();
                                                                                    final ResultSet getSeqResults = sqlStatement.executeQuery(sql1);
                                                                                    try {
                                                                                                int i_fromDBSeq = 0;
                                                                                                long l_fromDBSeq = 0;
                                                                                                if (getSeqResults.next()) {
                                                                                                            if (fieldTypeName.equals("Long")) {
                                                                                                                        l_fromDBSeq = getSeqResults.getLong(1);
                                                                                                            } else {
                                                                                                                        i_fromDBSeq = getSeqResults.getInt(1);
                                                                                                            }
                                                                                                }
                                                                                                final Method colSeqSetMethod = getFieldMethod(dataClass,
                                                                                                                        dataClass.getDeclaredField(columnName), "set");
                                                                                                if (fieldTypeName.equals("Long")) {
                                                                                                            final Object[] objVect = { l_fromDBSeq };
                                                                                                            colSeqSetMethod.invoke(obj, objVect);
                                                                                                } else {
                                                                                                            final Object[] objVect = { i_fromDBSeq };
                                                                                                            colSeqSetMethod.invoke(obj, objVect);
                                                                                                }
                                                                                    } finally {
                                                                                                sqlStatement.close();
                                                                                                getSeqResults.close();
                                                                                    }
                                                                        }
                                                            }

                                                            final Method method = getFieldMethod(dataClass, field, "get");
                                                            valuesStr = new StringBuffer(valuesStr).append(getFieldValueWithQuotes(obj, field, method))
                                                                                    .toString();
                                                }

                                    }
                                    sQLString = new StringBuffer(sQLString).append(valuesStr).append(" )").toString();
                                    return sQLString;
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * getTableName gets the table name from a class name.
            *
            * @param dataClass
            *            dataclass
            * @param obj
            *            .
            * @return String tablename
            * @throws RockException
            *             errors
            */

            protected String getTableName(final Object obj, final Class<?> dataClass) throws RockException {

                        try {
                                    final Method getTableNameMethod = dataClass
                                                            .getMethod(FactoryRes.GET_TABLE_NAME_METHOD_NAME, new Class[] {});
                                    return (String) getTableNameMethod.invoke(obj, new Object[] {});
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }

            }

            /**
            * fieldValueOtherThanInitial tests wheather the fields value is the initial one or not
            *
            * @param obj
            *            obj
            * @param whereClass
            *            whereClass
            * @param field
            *            field
            * @return Boolean false if the field contains the initial value else true
            * @throws RockException
            *             errors
            */
            protected boolean fieldValueOtherThanInitial(final Object obj, final Class<?> whereClass, final Field field)
                                    throws RockException {
                        try {
                                    final Method method = getFieldMethod(whereClass, field, "get");
                                    final String[] argv = null;
                                    final Object tempObj = method.invoke(obj, (Object[]) argv);

                                    // Class<?> tempClass = method.getReturnType();

                                    return tempObj != null;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * createWhereSQL is general SQL -clause parser for all of the WHERE -parts.
            *
            * @param obj
            *            obj
            * @param whereClass
            *            whereClass
            * @return a WHERE -SQL clause
            * @throws RockException
            *             errors
            */
            protected String createWhereSQL(final Object obj, final Class<?> whereClass) throws RockException {
                        try {
                                    String whereString = " ";
                                    final Field[] classFields = whereClass.getDeclaredFields();

                                    for (final Field field : classFields) {

                                                final String fieldType = field.getType().getName();
                                                final String fieldTypeName = fieldType.substring(fieldType.lastIndexOf(".") + 1, fieldType.length());

                                                if (isColumnName(field.getName(), fieldTypeName)) {

                                                            if (fieldValueOtherThanInitial(obj, whereClass, field)) {

                                                                        if (!whereString.equals(" ")) {
                                                                                    whereString = new StringBuffer(whereString).append(" AND ").toString();
                                                                        }

                                                                        final Method method = getFieldMethod(whereClass, field, "get");
                                                                        final String fieldValue = getFieldValueWithQuotes(obj, field, method);

                                                                        // LIKE caused problems with strings containing "%"'s when data was updated
                                                                        // ("ASA Error -606: The pattern is too long")
                                                                        // if (fieldValue.indexOf("%") < 0) {
                                                                        // whereString = new
                                                                        // StringBuffer(whereString).append(field.getName()).append("=").append(fieldValue)
                                                                        // .toString();
                                                                        // } else {
                                                                        // whereString = new
                                                                        // StringBuffer(whereString).append(field.getName()).append(" LIKE ").append(fieldValue)
                                                                        // .toString();
                                                                        // }
                                                                        whereString = new StringBuffer(whereString).append(field.getName()).append("=")
                                                                                                .append(fieldValue).toString();
                                                            }
                                                }
                                    }
                                    if (!whereString.equals(" ")) {
                                                whereString = " WHERE " + whereString;
                                    }

                                    return whereString;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }

            }

            /**
            * createWhereSQLfromPrimaries Creates a SQL clause from primaryKey columns.
            *
            * @param dataObj
            * @param columnNotIncluded
            * @return a WHERE -SQL clause
            * @exception
            * @throws RockException
            */
            private String createWhereSQLfromPrimaries(final Object dataObj, final String columnNotIncluded)
                                    throws RockException {
                        String whereSQL = "";
                        try {
                                    final Method pkGetMethod = dataObj.getClass().getDeclaredMethod(FactoryRes.PRIMARYKEY_METHOD,
                                                            (Class[]) null);
                                    final String[] pkColumnNames = (String[]) pkGetMethod.invoke(dataObj, (Object[]) null);
                                    if (pkColumnNames.length > 0) {
                                                whereSQL = " ";
                                                for (final String pkColumnName : pkColumnNames) {
                                                            final Field field = dataObj.getClass().getDeclaredField(pkColumnName);
                                                            if ((fieldValueOtherThanInitial(dataObj, dataObj.getClass(), field))
                                                                                    && ((columnNotIncluded == null) || (!columnNotIncluded.equals(field.getName())))) {
                                                                        if (!whereSQL.equals(" ")) {
                                                                                    whereSQL = new StringBuffer(whereSQL).append(" AND ").toString();
                                                                        }
                                                                        final Method method = getFieldMethod(dataObj.getClass(), field, "get");
                                                                        final String fieldValue = getFieldValueWithQuotes(dataObj, field, method);

                                                                        if (fieldValue.indexOf("%") < 0) {
                                                                                    whereSQL = new StringBuffer(whereSQL).append(pkColumnName).append("=").append(fieldValue)
                                                                                                            .toString();
                                                                        } else {
                                                                                    whereSQL = new StringBuffer(whereSQL).append(pkColumnName).append(" LIKE ")
                                                                                                            .append(fieldValue).toString();
                                                                        }
                                                            }

                                                }
                                                if (!whereSQL.equals(" ")) {
                                                            whereSQL = " WHERE " + whereSQL;
                                                }
                                    } else {
                                                throw new RockException(FactoryRes.NO_PRIMARYKEY_INFO);
                                    }
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
                        return whereSQL;
            }

            /**
            * insertData Executes an insert SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to insert.
            * @param boolean useTimestamp If true, the current time is automatically updated.
            * @return int Number of inserted rows.
            * @exception SQLException
            * @param dataObj
            * @param useTimestamp
            * @param useSequence
            * @throws RockException
            */
            private int insertDataPriv(final Object dataObj, final boolean useTimestamp, final boolean useSequence)
                                    throws SQLException, RockException {

                        // Log the insert operation performed on the database table
                        TableModificationLogger.instance()
                                                .add("INSERT" + " " + dbURL + " " + getTableName(dataObj, dataObj.getClass()));

                        int records;
                        Statement sqlStatement = null;
                        try {
                                    sqlStatement = connection.createStatement();
                                    final String sql = createInsertSQL(dataObj, dataObj.getClass(), useTimestamp, useSequence);

                                    records = sqlStatement.executeUpdate(sql);
                                    sqlStatement.close();
                                    return records;
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        } finally {
                                    if (sqlStatement != null) {
                                                try {
                                                            sqlStatement.close();
                                                } catch (final Throwable t) {/**/
                                                }
                                    }
                        }
            }

            /**
            * insertData Executes an insert SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to insert.
            * @param boolean useTimestamp If true, the current time is automatically updated.
            * @param boolean useTimestamp If true, the number fields associated with sequences get values automatically.
            * @return int Number of inserted rows.
            * @exception SQLException
            */
            @Override
            public int insertData(final Object dataObj, final boolean useTimestamp, final boolean useSequence)
                                    throws SQLException, RockException {
                        return insertDataPriv(dataObj, useTimestamp, useSequence);
            }

            /**
            * insertData Executes an insert SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to insert.
            * @return int Number of inserted rows.
            * @exception SQLException
            */
            @Override
            public int insertData(final Object dataObj) throws SQLException, RockException {
                        return insertDataPriv(dataObj, true, true);
            }

            /**
            * Executes a delete SQL -clause based on given object data and class information.
            *
            * @param isPrimaryKeyDelete
            * @param whereObj
            *            all field that are != null are used for the WHERE -part
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @return int Number of deleted rows.
            * @exception SQLException
            * @throws RockException
            */
            private int deleteDataPriv(final boolean isPrimaryKeyDelete, final Object whereObj, final String whereString)
                                    throws SQLException, RockException {

                        // Log the delete operation performed on the database table
                        TableModificationLogger.instance().add(
                                                "DELETE" + " " + dbURL + " " + getTableName(whereObj, whereObj.getClass()));

                        int records = -1;
                        try {
                                    String whereSQL = "";
                                    if (isPrimaryKeyDelete) {
                                                whereSQL = createWhereSQLfromPrimaries(whereObj, null);
                                    } else if (whereObj != null) {
                                                whereSQL = createWhereSQL(whereObj, whereObj.getClass());
                                    }
                                    // dead code
                                    // else if (whereString != null)
                                    // if (whereString.equals("") == false)
                                    // whereSQL = " WHERE " + whereString;

                                    final Statement sqlStatement = connection.createStatement();
                                    try {
                                                final String sql = "DELETE FROM " + getTableName(whereObj, whereObj.getClass()) + " " + whereSQL;
                                                records = sqlStatement.executeUpdate(sql);
                                    } finally {
                                                sqlStatement.close();
                                    }
                                    return records;
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * Executes a delete SQL -clause based on given object data and class information.
            *
            * @param Object
            *            whereObj The data object to delete.
            * @return int Number of deleted rows.
            * @exception SQLException
            */
            @Override
            public int deleteData(final boolean isPrimaryKeyDelete, final Object whereObj) throws SQLException, RockException {
                        return deleteDataPriv(isPrimaryKeyDelete, whereObj, null);
            }

            /**
            * Executes a delete SQL -clause based on given object data and class information.
            *
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @return int Number of deleted rows.
            * @exception SQLException
            */
            @Override
            public int deleteData(final boolean isPrimaryKeyDelete, final String whereString) throws SQLException,
                                    RockException {
                        return deleteDataPriv(isPrimaryKeyDelete, null, whereString);
            }

            /**
            * updateData Executes an update SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to update.
            * @param boolean isPrimaryKeyUpdate If true, where -clause is created from primary key information.
            * @param Object
            *            whereObj The data for the WHERE -clause.
            * @param boolean useTimestamp If false, it is not checked wheather the data has changed.
            * @param dataObj
            * @param isPrimaryKeyUpdate
            * @param whereObj
            * @param useTimestamp
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @return int Number of updated rows.
            * @exception SQLException
            * @throws RockException
            */
            private int updateDataPriv(final Object dataObj, final boolean isPrimaryKeyUpdate, final Object whereObj,
                                    final boolean useTimestamp, final String whereString) throws SQLException, RockException {

                        // Log the update operation performed on the database table
                        TableModificationLogger.instance()
                                                .add("UPDATE" + " " + dbURL + " " + getTableName(dataObj, dataObj.getClass()));

                        int records = -1;
                        String whereSQL = "";

                        if (isPrimaryKeyUpdate) {
                                    whereSQL = createWhereSQLfromPrimaries(dataObj, null);
                        } else if (whereObj != null) {
                                    whereSQL = createWhereSQL(whereObj, whereObj.getClass());
                        } else if (whereString != null) {
                                    if (!whereString.equals("")) {
                                                whereSQL = " WHERE " + whereString;
                                    }
                        }

                        Method timeTsSetMethod = null;
                        Timestamp timeStampColumnValue = null;

                        try {
                                    // if (useTimestamp) {
                                    final Method timeGetMethod = dataObj.getClass().getDeclaredMethod(FactoryRes.TIMESTAMP_METHOD,
                                                            (Class[]) null);
                                    final String timeStampColumnName = (String) timeGetMethod.invoke(dataObj, (Object[]) null);

                                    if ((timeStampColumnName != null) && (!timeStampColumnName.equals(""))
                                                            && (columnExists(dataObj.getClass(), timeStampColumnName))) {

                                                final Method timeTsGetMethod = getFieldMethod(dataObj.getClass(),
                                                                        dataObj.getClass().getDeclaredField(timeStampColumnName), "get");

                                                timeStampColumnValue = (Timestamp) timeTsGetMethod.invoke(dataObj, (Object[]) null);

                                                final Statement sqlStatement = connection.createStatement();
                                                ResultSet forUpdateResults = null;
                                                final Timestamp fromDBTs;
                                                try {
                                                            final String sql1 = "SELECT " + timeStampColumnName + " FROM "
                                                                                    + getTableName(dataObj, dataObj.getClass()) + whereSQL + " FOR UPDATE";

                                                            forUpdateResults = sqlStatement.executeQuery(sql1);

                                                            if (forUpdateResults.next()) {
                                                                        fromDBTs = forUpdateResults.getTimestamp(timeStampColumnName);
                                                            } else {
                                                                        throw new RockException(FactoryRes.CANNOT_GET_TIMESTAMP);
                                                            }
                                                } finally {
                                                            forUpdateResults.close();
                                                            sqlStatement.close();
                                                }
                                                if (fromDBTs != null) {
                                                            timeStampColumnValue.setNanos(0);
                                                            if (!fromDBTs.equals(timeStampColumnValue)) {
                                                                        throw new RockException(FactoryRes.SOMEONE_HAS_UPDATED + " Database: " + fromDBTs.toString()
                                                                                                + " Curr.record: " + timeStampColumnValue.toString());
                                                            }
                                                }

                                                timeTsSetMethod = getFieldMethod(dataObj.getClass(),
                                                                        dataObj.getClass().getDeclaredField(timeStampColumnName), "set");

                                                final Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                                                final Object[] objVect = { currentTime };
                                                timeTsSetMethod.invoke(dataObj, objVect);
                                                sqlStatement.close();

                                    }
                                    // }
                                    final Statement sqlStatement = connection.createStatement();
                                    try {
                                                final String sql2 = createUpdateSQL(dataObj, dataObj.getClass(), whereSQL);
                                                records = sqlStatement.executeUpdate(sql2);
                                    } finally {
                                                sqlStatement.close();
                                    }
                                    return records;
                        } catch (final SQLException sqlE) {
                                    // Return the timestamp column value as it were not updated
                                    if ((timeTsSetMethod != null) && (timeStampColumnValue != null)) {
                                                try {
                                                            final Object[] objVect = { timeStampColumnValue };
                                                            timeTsSetMethod.invoke(dataObj, objVect);
                                                } catch (final Exception e) {
                                                            throw new RockException(e.getMessage(), e);
                                                }
                                    }
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * Executes an update SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to update.
            * @param boolean isPrimaryKeyUpdate If true, where -clause is created from primary key information.
            * @param Object
            *            whereObj The data for the WHERE -clause.
            * @return int Number of updated rows.
            * @exception SQLException
            */
            @Override
            public int updateData(final Object dataObj, final boolean isPrimaryKeyUpdate, final Object whereObj)
                                    throws SQLException, RockException {
                        return updateDataPriv(dataObj, isPrimaryKeyUpdate, whereObj, true, null);
            }

            /**
            * Executes an update SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to update.
            * @param boolean isPrimaryKeyUpdate If true, where -clause is created from primary key information.
            * @param whereObj
            *            all field that are != null are used for the WHERE -part
            * @param boolean useTimestamp If false, it is not checked wheather the data has changed.
            * @return int Number of updated rows.
            * @exception SQLException
            */
            @Override
            public int updateData(final Object dataObj, final boolean isPrimaryKeyUpdate, final Object whereObj,
                                    final boolean useTimestamp) throws SQLException, RockException {
                        return updateDataPriv(dataObj, isPrimaryKeyUpdate, whereObj, useTimestamp, null);
            }

            /**
            * Executes an update SQL -clause based on given object data and class information.
            *
            * @param Object
            *            dataObj The data object to update.
            * @param boolean isPrimaryKeyUpdate If true, where -clause is created from primary key information.
            * @param String
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @param boolean useTimestamp If false, it is not checked wheather the data has changed.
            * @return int Number of updated rows.
            * @exception SQLException
            */
            @Override
            public int updateData(final Object dataObj, final boolean isPrimaryKeyUpdate, final String whereStr,
                                    final boolean useTimestamp) throws SQLException, RockException {
                        return updateDataPriv(dataObj, isPrimaryKeyUpdate, null, useTimestamp, whereStr);
            }

            /**
            * Sets the cursor for a SQL -clause
            *
            * @param isPrimaryKeySelect
            *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
            *            whereDBParameter
            * @param whereObj
            *            all field that are != null are used for the WHERE -part
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @param orderByStr
            * @return
            * @exception SQLException
            * @throws RockException
            */
            @SuppressWarnings({ "PMD.CloseResource" })
            private RockResultSet setSelectSQLPriv(final boolean isPrimaryKeySelect, final Object whereObj,
                                    final String whereString, final String orderByStr) throws SQLException, RockException {
                        final RockResultSet rockResults = new RockResultSet();
                        try {
                                    String whereSQL = "";
                                    if (isPrimaryKeySelect) {
                                                whereSQL = createWhereSQLfromPrimaries(whereObj, null);
                                    } else if (whereObj != null) {
                                                whereSQL = createWhereSQL(whereObj, whereObj.getClass());
                                    } 
                                    if (whereString != null) {
                                                if (!whereString.equals("")) {
                                                            whereSQL = " WHERE " + whereString;
                                                }
                                    }
                                    if (orderByStr != null) {
                                                whereSQL = new StringBuffer(whereSQL).append(" ").append(orderByStr).toString();
                                    }
                                    final Statement sqlSelectStatement = connection.createStatement();
                                    final String sql = createSelectSQL(whereObj, whereObj.getClass(), whereSQL);
                                    rockResults.setResultSet(sqlSelectStatement.executeQuery(sql), sqlSelectStatement);
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
                        return rockResults;
            }

            /**
            * Sets the cursor for a SQL -clause
            *
            * @param isPrimaryKeySelect
            *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
            *            whereDBParameter
            * @param whereObj
            *            all field that are != null are used for the WHERE -part
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            * @return
            * @exception SQLException
            */
            @Override
            @SuppressWarnings({ "PMD.CloseResource" })
            public RockResultSet setSelectSQL(final String selectString) throws SQLException, RockException {
                        final RockResultSet rockResults = new RockResultSet();
                        try {
                                    final Statement sqlSelectStatement = connection.createStatement();

                                    rockResults.setResultSet(sqlSelectStatement.executeQuery(selectString), sqlSelectStatement);
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
                        return rockResults;
            }

            /**
            * Sets the cursor for a SQL -clause
            *
            * @param isPrimaryKeySelect
            *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
            *            whereDBParameter
            * @param whereDBParameter
            *            all field that are != null are used for the WHERE -part
            * @return
            * @exception SQLException
            */
            @Override
            public RockResultSet setSelectSQL(final boolean isPrimaryKeySelect, final Object whereObj) throws SQLException,
                                    RockException {
                        return setSelectSQLPriv(isPrimaryKeySelect, whereObj, null, null);
            }

            /**
            * Sets the cursor for a SQL -clause
            *
            * @param isPrimaryKeySelect
            *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
            *            whereDBParameter
            * @param whereDBParameter
            *            all field that are != null are used for the WHERE -part
            * @return
            * @exception SQLException
            */
            @Override
            public RockResultSet setSelectSQL(final boolean isPrimaryKeySelect, final Object whereObj, final String orderByStr)
                                    throws SQLException, RockException {
                        return setSelectSQLPriv(isPrimaryKeySelect, whereObj, null, orderByStr);
            }
            

            /**
            * Sets the cursor for a SQL -clause
            *
            * @param isPrimaryKeySelect
            *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
            *            whereDBParameter
            * @param whereString
            *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
            *            WHERE -part
            * @return
            * @exception SQLException
            */
            @Override
            public RockResultSet setSelectSQL(final boolean isPrimaryKeySelect, final String whereString,
                                    final String orderByStr) throws SQLException, RockException {
                        return setSelectSQLPriv(isPrimaryKeySelect, null, whereString, orderByStr);
            }
            
            /**
             * Sets the cursor for a SQL -clause
             *
             * @param isPrimaryKeySelect
             *            if true the primary key data is used for the WHERE -part, else WHERE is constructed from
             *            whereDBParameter
             * @param whereString
             *            whereStr The data for the WHERE -clause ( a sql clause for the where part, does not include WHERE).
             *            WHERE -part
             * @return
             * @exception SQLException
             */
             @Override
             public RockResultSet setSelectSQL(final boolean isPrimaryKeySelect, final Object whereObj, final String whereString,
                                     final String orderByStr) throws SQLException, RockException {
                         return setSelectSQLPriv(isPrimaryKeySelect, whereObj, whereString, orderByStr);
             }

            /**
            * Gets data from the cursor for a SQL -clause
            *
            * @param dataObj
            *            holds the data retrieved from the db
            *
            * @return true while more data is available
            * @exception SQLException
            */
            @Override
            public Iterator getData(final Object dataObj, final RockResultSet rockResults) throws SQLException, RockException {
                        final ResultSet results = rockResults.getResultSet();
                        final Collection cSql = rockResults.getCollection();
                        try {
                                    while (results.next()) {

                                                final Method cloneMethod = dataObj.getClass().getDeclaredMethod("clone", (Class[]) null);
                                                final Object newObj = cloneMethod.invoke(dataObj, (Object[]) null);

                                                final Class<?> dataClass = newObj.getClass();

                                                final Field[] classFields = dataClass.getDeclaredFields();

                                                for (final Field field : classFields) {

                                                            final String fieldType = field.getType().getName();
                                                            final String fieldTypeName = fieldType
                                                                                    .substring(fieldType.lastIndexOf(".") + 1, fieldType.length());
                                                            if (isColumnName(field.getName(), fieldTypeName)) {

                                                                        final Method setMethod = getFieldMethod(dataClass, field, "set");
                                                                        final String attributeName = field.getName();

                                                                        if (fieldTypeName.equals("Integer")) {
                                                                                    final int resultInt = results.getInt(attributeName);
                                                                                    Integer tempInteger = null;
                                                                                    if (!results.wasNull()) {
                                                                                                tempInteger = resultInt;
                                                                                    }
                                                                                    final Object[] objList = { tempInteger };
                                                                                    setMethod.invoke(newObj, objList);
                                                                        } else {
                                                                                    if (fieldTypeName.equals("Float")) {
                                                                                                final float resultFloat = results.getFloat(attributeName);
                                                                                                Float tempFloat = null;
                                                                                                if (!results.wasNull()) {
                                                                                                            tempFloat = resultFloat;
                                                                                                }
                                                                                                final Object[] objList = { tempFloat };
                                                                                                setMethod.invoke(newObj, objList);
                                                                                    } else {
                                                                                                if (fieldTypeName.equals("Double")) {
                                                                                                            final double resultDouble = results.getDouble(attributeName);
                                                                                                            Double tempDouble = null;
                                                                                                            if (!results.wasNull()) {
                                                                                                                        tempDouble = resultDouble;
                                                                                                            }
                                                                                                            final Object[] objList = { tempDouble };
                                                                                                            setMethod.invoke(newObj, objList);
                                                                                                } else {
                                                                                                            if (fieldTypeName.equals("Short")) {
                                                                                                                        final short resultShort = results.getShort(attributeName);
                                                                                                                        Short tempShort = null;
                                                                                                                        if (!results.wasNull()) {
                                                                                                                                    tempShort = resultShort;
                                                                                                                        }
                                                                                                                        final Object[] objList = { tempShort };
                                                                                                                        setMethod.invoke(newObj, objList);
                                                                                                            } else {
                                                                                                                        if (fieldTypeName.equals("Long")) {
                                                                                                                                    final long resultLong = results.getLong(attributeName);
                                                                                                                                    Long tempLong = null;
                                                                                                                                    if (!results.wasNull()) {
                                                                                                                                                tempLong = resultLong;
                                                                                                                                    }
                                                                                                                                    final Object[] objList = { tempLong };
                                                                                                                                    setMethod.invoke(newObj, objList);
                                                                                                                        } else {
                                                                                                                                    final Object resultObject = results.getObject(attributeName);
                                                                                                                                    final Object[] objList = { resultObject };
                                                                                                                                    setMethod.invoke(newObj, objList);
                                                                                                                        }
                                                                                                            }
                                                                                                }
                                                                                    }
                                                                        }
                                                            }
                                                }
                                                cSql.add(newObj);
                                    }
                                    return cSql.iterator();
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {

                                    if (e instanceof InvocationTargetException) {
                                                if (((InvocationTargetException) e).getTargetException() instanceof InvalidDataException) {
                                                            throw new InvalidDataException(((InvocationTargetException) e).getTargetException().getMessage());
                                                }
                                    }
                                    throw new RockException(e.getMessage(), e);
                        } finally {
                                    results.close();
                        }
            }

            /**
            * Executes an insert SQL -clause based on String.
            *
            * @param sqlStr
            *            String to execute
            * @exception SQLException
            * @throws RockException
            */
            public void executeSql(final String sqlStr) throws SQLException, RockException {
                        Statement sqlStatement = null;
                        try {
                                    sqlStatement = connection.createStatement();
                                    sqlStatement.execute(sqlStr);
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        } finally {
                                    if (sqlStatement != null) {
                                                while (sqlStatement.getMoreResults()) {
                                                            sqlStatement.getResultSet().close();
                                                }
                                                sqlStatement.close();
                                    }
                        }
            }

            /**
            * Executes a prepared SQL -clause based on String.
            *
            * @param preparedSqlStr
            *            String to execute
            * @param objVec
            *            vector containig the objects related to the string
            * @exception SQLException
            */
            @Override
            public void executePreparedSql(final String preparedSqlStr, final Vector rowVec) throws SQLException, RockException {
                        PreparedStatement sqlStatement = null;
                        try {
                                    sqlStatement = connection.prepareStatement(preparedSqlStr);

                                    for (int i = 0; i < rowVec.size(); i++) {
                                                final Object[] objs = (Object[]) rowVec.elementAt(i);
                                                final Vector objVec = (Vector) objs[0];
                                                final Vector pkVec = (Vector) objs[1];

                                                for (int j = 0; j < objVec.size(); j++) {
                                                            final Object[] obj = (Object[]) objVec.elementAt(j);
                                                            if (obj[0] != null) {
                                                                        sqlStatement.setObject(j + 1, obj[0]);
                                                            } else {
                                                                        sqlStatement.setNull(j + 1, ((Integer) obj[1]).intValue());
                                                            }
                                                }

                                                for (int j = 0; j < pkVec.size(); j++) {
                                                            final Object[] obj = (Object[]) pkVec.elementAt(j);
                                                            if (obj[0] != null) {
                                                                        sqlStatement.setObject(objVec.size() + j + 1, obj[0]);
                                                            } else {
                                                                        sqlStatement.setNull(objVec.size() + j + 1, ((Integer) obj[1]).intValue());
                                                            }
                                                }

                                                sqlStatement.executeUpdate();
                                    }
                                    sqlStatement.close();
                                    sqlStatement = null; // JVesey 26/07/2011 Prevents error in finally below
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        } finally {
                                    if (sqlStatement != null) {
                                                while (sqlStatement.getMoreResults()) {
                                                            sqlStatement.getResultSet().close();
                                                }
                                                sqlStatement.close();
                                    }
                        }
            }

            /**
            * Create prepared statement which can be used in executePreparedSqlQuery
            *
            * @param preparedSqlStr
            * @return
            * @throws SQLException
            * @throws RockException
            */
            public PreparedStatement createPreparedSqlQuery(final String preparedSqlStr) throws SQLException, RockException {
                        return connection.prepareStatement(preparedSqlStr);
            }

            /**
            * Close functionality for prepared query
            *
            * @param preparedSqlStmt
            * @throws SQLException
            * @throws RockException
            */
            public void closePreparedSqlQuery(final PreparedStatement preparedSqlStmt) throws SQLException, RockException {
                        preparedSqlStmt.close();
            }

            /**
            * Executes a prepared SQL Query-clause based on String.
            *
            * @param preparedSqlStr
            *            String to execute
            * @param objVec
            *            vector containing the parameters related in the string sql
            * @exception SQLException
            * @param preparedSqlStmt
            * @param parameters
            * @throws RockException
            * @return
            */
            public Vector<Vector<Object>> executePreparedSqlQuery(final PreparedStatement preparedSqlStmt,
                                    final Vector<Object> parameters) throws SQLException, RockException {
                        final Vector<Vector<Object>> results = new Vector<Vector<Object>>();
                        try {
                                    int ind = 1;
                                    for (final Object parameter : parameters) {
                                                preparedSqlStmt.setObject(ind, parameter);
                                                ind++;
                                    }
                                    final ResultSet rs = preparedSqlStmt.executeQuery();
                                    try {
                                                final ResultSetMetaData rsMetaData = rs.getMetaData();
                                                final int numberOfColumns = rsMetaData.getColumnCount();
                                                while (rs.next()) {
                                                            final Vector<Object> result = new Vector<Object>(numberOfColumns);
                                                            for (int jnd = 1; jnd <= numberOfColumns; jnd++) {
                                                                        result.add(rs.getObject(jnd));
                                                            }
                                                            results.add(result);
                                                }
                                    } finally {
                                                rs.close();
                                    }
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
                        return results;
            }

            /**
            * Executes a prepared SQL -clause based on String. If update cannot be done the an insert is exec.
            *
            * @param preparedSqlStr
            *            String to execute
            * @param objVec
            *            vector containig the objects related to the string
            * @exception SQLException
            */
            @Override
            public void executePreparedInsAndUpdSql(final String preparedUpdStr, final Vector rowVec,
                                    final String preparedInsStr) throws SQLException, RockException {
                        try {
                                    final PreparedStatement updStatement = connection.prepareStatement(preparedUpdStr);
                                    final PreparedStatement insStatement = connection.prepareStatement(preparedInsStr);

                                    for (int i = 0; i < rowVec.size(); i++) {
                                                final Object[] objs = (Object[]) rowVec.elementAt(i);
                                                final Vector objVec = (Vector) objs[0];
                                                final Vector pkVec = (Vector) objs[1];

                                                for (int j = 0; j < objVec.size(); j++) {
                                                            final Object[] obj = (Object[]) objVec.elementAt(j);
                                                            if (obj[0] != null) {
                                                                        updStatement.setObject(j + 1, obj[0]);
                                                                        insStatement.setObject(j + 1, obj[0]);
                                                            } else {
                                                                        updStatement.setNull(j + 1, (Integer) obj[1]);
                                                                        insStatement.setNull(j + 1, (Integer) obj[1]);
                                                            }
                                                }

                                                for (int j = 0; j < pkVec.size(); j++) {
                                                            final Object[] obj = (Object[]) pkVec.elementAt(j);
                                                            if (obj[0] != null) {
                                                                        updStatement.setObject(objVec.size() + j + 1, obj[0]);
                                                            } else {
                                                                        updStatement.setNull(objVec.size() + j + 1, (Integer) obj[1]);
                                                            }
                                                }

                                                final int rows = updStatement.executeUpdate();

                                                if (rows <= 0) {

                                                            insStatement.executeUpdate();

                                                }

                                    }
                                    updStatement.close();
                                    insStatement.close();
                        } catch (final SQLException sqlE) {
                                    throw sqlE;
                        } catch (final Exception e) {
                                    throw new RockException(e.getMessage(), e);
                        }
            }

            /**
            * commit Commits the transaction if autocommit is OFF.
            *
            * @exception SQLException
            */
            @Override
            public void commit() throws SQLException {
                        if (!autoCommit) {

                                    connection.commit();
                        }
            }

            /**
            * rollback Rollbacks the transaction if autocommit is OFF.
            *
            * @exception SQLException
            */
            @Override
            public void rollback() throws SQLException {
                        if (!autoCommit) {

                                    connection.rollback();
                        }
            }

            /**
            * begin Begins a transaction (ended by commit/rollback ) DOES NOTHING IN THIS CONTEXT (just to implement the
            * interface needs
            *
            */
            @Override
            public void begin() {
            }

            /**
            * Returns the database connect element
            *
            *
            * @return The connect element
            */
            public Connection getConnection() {
                        return this.connection;
            }

            /**
            * Copies tables child table information
            *
            * @param tableName
            * @param setValues
            *            key <setMethodName> value {oldValue,newValue}
            */
            @Override
            public void copySchema(final String tableName, final Hashtable setValues, final String packagePath)
                                    throws SQLException, RockException {
                        final CopySchema copySchema = new CopySchema(this, "", // catalog
                                                this.strUserName, setValues, packagePath);

                        copySchema.copy(tableName);

                        copySchema.close();
            }

            /**
            * Copies tables child table information
            *
            * @param tableName
            * @param tableNames
            * @param setValues
            *            key <setMethodName> value {oldValue,newValue}
            * @param packagePath
            * @throws RockException
            * @throws java.sql.SQLException
            */
            public void copySchemaNonRecursive(final String[] tableNames, final Hashtable setValues, final String packagePath)
                                    throws SQLException, RockException {

                        final CopySchema copySchema = new CopySchema(this, "", // catalog
                                                this.strUserName, setValues, packagePath);

                        for (final String tableName : tableNames) {

                                    copySchema.copyNonRecursive(tableName);

                        }

                        copySchema.close();

            }
            


}
