package com.ericsson.eniq.Services;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Imports a xml file to DB. USAGE: url userName password dbDriverName
 * outputfile Property file: Uses one property file. ETLCImport.properties there
 * is tablelist (comma delimited) containing all the tables that need some
 * modifications tableList = table1,table2,table3 for each element in the list
 * there can be two types of modifications (reNumber and replace) reNumber
 * indicates a column that is re-numbered according to the max value (starts
 * numbering the added (imported columns) from the max value) of that column in
 * the database. There can be only on renumbered column in one table.
 * table1.reNumber = column1 table3.renumber = column1 replace replaces a string
 * from one to another in one column. there are two sub parameters in replace
 * (column and rule) replace.column indicates the colum where the replacement is
 * done. replace.rule contains number of string pairs (old string = new string)
 * delimited by comma, table2.replace.column = column1 table2.replace.rule =
 * oldString = newString , oldString = newString , oldString = newString
 * 
 * @author savinen Copyright Distocraft 2005 $id$
 */
@Component
public class ETLCImport extends DefaultHandler {

  private Connection con = null;

  // private IDatabaseConnection dbConnection;

  private final HashMap valueCache = new HashMap();

  private String url = null;

  private String userName = null;

  private String password = null;

  private String dbDriverName = null;

  private String charValue;

  private final ArrayList colNameList;

  private final ArrayList colValueList;

  private final ArrayList reOrderList;

  private ArrayList schedulerTables;

  private ArrayList setTables;

  private final HashMap replaceMap;

  private ArrayList restrictedTables;

  private final String lastType = "";

  private Statement stmt;

  private boolean useLogging = true;
  
  private static final int NUMBER_OF_CLAUSE_PARTS = 3;
  
  private static final int ACTION_CONTENT_MAX_SIZE_PER_COL = 100000; //100KB
  
  private static final String ACTION_CONTENTS = "ACTION_CONTENTS";
  
  private static final String WHERE_CLAUSE = "WHERE_CLAUSE";

  /**
   * constructor
   * 
   * @param url
   * @param userName
   * @param password
   * @param dbDriverName
   */
  public ETLCImport(String url, String userName, String password, String dbDriverName) {

    super();

    this.url = url;
    this.userName = userName;
    this.password = password;
    this.dbDriverName = dbDriverName;
    colNameList = new ArrayList();
    colValueList = new ArrayList();

    reOrderList = new ArrayList();
    replaceMap = new HashMap();

    readProperties(null,null);

    try {

      // database connection
      Class driverClass = Class.forName(dbDriverName);
      con = DriverManager.getConnection(url, userName, password);
     // this.con.commit();

    } catch (Exception e) {

      Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR while creating connection: ", e);

    }

  }

  public ETLCImport(String propDir, Connection jdbcConnection,String ETLCImportDirectory) {

    colNameList = new ArrayList();
    colValueList = new ArrayList();

    reOrderList = new ArrayList();
    replaceMap = new HashMap();

    try {

      con = jdbcConnection;
     // this.con.commit();

    } catch (Exception e) {

      Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR while creating connection: ", e);

    }

    readProperties(null,ETLCImportDirectory);
  }

  public ETLCImport(Properties prop, Connection jdbcConnection,String ETLCImportDirectory) {

    colNameList = new ArrayList();
    colValueList = new ArrayList();

    reOrderList = new ArrayList();
    replaceMap = new HashMap();

    try {

      con = jdbcConnection;
      //this.con.commit();

    } catch (Exception e) {

      Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR while creating connection: ", e);

    }

    readProperties(prop,ETLCImportDirectory);
  }

  public ETLCImport() {
	  colNameList = new ArrayList();
	  colValueList = new ArrayList();

	  reOrderList = new ArrayList();
	  replaceMap = new HashMap();
	}

  /**
   * Reads properties from properties file
   */
  private void readProperties(Properties prop,String ETLCImportDirectory) {

    InputStream is = null;

    try {

      java.util.Properties appProps = new java.util.Properties();

      if (prop == null) {

        //ClassLoader cl = this.getClass().getClassLoader();
        is = new FileInputStream(ETLCImportDirectory+"/ETLCImport.properties");
        appProps.load(is);
        is.close();

      } else {
        appProps = prop;
      }

      String tableList = appProps.getProperty("tableList", "");
      StringTokenizer token = new StringTokenizer(tableList, ",");

      while (token.hasMoreElements()) {
        String table = (String) token.nextElement();

        // read replacement orders
        String listStr = appProps.getProperty(table + ".replace.list");

        if (listStr != null) {

          StringTokenizer tokenList = new StringTokenizer(listStr, ",");
          while (tokenList.hasMoreElements()) {

            String id = (String) tokenList.nextElement();
            String column = appProps.getProperty(table + "." + id + ".replace.column");
            String replaceStringOld = appProps.getProperty(table + "." + id + ".replace.old");
            String replaceStringNew = appProps.getProperty(table + "." + id + ".replace.new");

            if (column != null && replaceStringOld != null && replaceStringNew != null) {

              ArrayList tmpList = (ArrayList) replaceMap.get(table + column);
              if (tmpList == null) {
                tmpList = new ArrayList();
              }
              tmpList.add(replaceStringOld);
              tmpList.add(replaceStringNew);
              replaceMap.put(table + column, tmpList);

            }

          }
        }

        // read renumber orders
        String reOrderString = appProps.getProperty(table + ".reNumber");
        if (reOrderString == null) {
          reOrderString = "";
        }
        StringTokenizer token2 = new StringTokenizer(reOrderString, ",");
        while (token2.hasMoreElements()) {
          String col = (String) token2.nextElement();
          reOrderList.add(table + col);
        }

      }

      schedulerTables = new ArrayList();

      // read schduler tables
      String schedulerStrings = appProps.getProperty("scheduler");
      if (schedulerStrings == null) {
        schedulerStrings = "";
      }
      token = new StringTokenizer(schedulerStrings, ",");
      while (token.hasMoreElements()) {
        String table = (String) token.nextElement();
        schedulerTables.add(table);
      }

      setTables = new ArrayList();

      // read set tables
      String setStrings = appProps.getProperty("sets");
      if (setStrings == null) {
        setStrings = "";
      }
      token = new StringTokenizer(setStrings, ",");
      while (token.hasMoreElements()) {
        String table = (String) token.nextElement();
        setTables.add(table);
      }

      restrictedTables = new ArrayList();

    } catch (Exception e) {
      Logger.getLogger("import.readProperties").log(Level.SEVERE,
          "ERROR reading properties file: ETLCImport.properties", e);

    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (Exception e) {
        }
      }

    }

  }

  private void readXML(BufferedReader br) throws Exception {
    //try {

    XMLReader xmlReader = new org.apache.xerces.parsers.SAXParser();
    xmlReader.setContentHandler(this);
    xmlReader.setErrorHandler(this);
    xmlReader.setEntityResolver(this);
    xmlReader.parse(new InputSource(br));
    /*
     } catch (Exception e) {
     // System.out.println("ERROR reading XML file: "+e);
     Logger.getLogger("import.readXML").log(Level.SEVERE, "ERROR reading XML file: ", e);
     throw new Exception (e);
     }
     */
  }

  /**
   * Event handlers
   */
  @Override
  public void startDocument() {
    Logger.getLogger("import.startDocument").log(Level.FINEST, "XML Document started");

  }

  @Override
  public void endDocument() throws SAXException {
    Logger.getLogger("import.endDocument").log(Level.FINEST, "XML Document ended");

  }

  /**
   * Handle element start
   */
  @Override
  public void startElement(String uri, String name, String qName, Attributes atts) throws SAXException {

    Logger.getLogger("import.startElement").log(Level.FINEST, "XML element started: " + name);

    charValue = "";
    
    // if meta_transfer_actions element then we have to split WHERE_CLAUSE and ACTION_CONTENTS attributes into three parts
    if ("META_TRANSFER_ACTIONS".equalsIgnoreCase(qName)){
      // read elements atributes (name and value)
      for (int i = 0; i < atts.getLength(); i++) {
        String attributeName = atts.getQName(i);
        String value = atts.getValue(i);
        if(WHERE_CLAUSE.equalsIgnoreCase(attributeName) || ACTION_CONTENTS.equalsIgnoreCase(attributeName)){
          String[][] splitted = splitColumnIntoThreeParts(attributeName, value);
          for(int y = 0; y < splitted[0].length; y++){
            colNameList.add(splitted[0][y]);
            colValueList.add(splitted[1][y]);
          }







        } else {
          colNameList.add(attributeName);
          colValueList.add(value);
        }
      }
    }
    // skip dataset element
    else if (!qName.equalsIgnoreCase("dataset")) {
      // read elements atributes (name and value)
      for (int i = 0; i < atts.getLength(); i++) {
        colNameList.add(atts.getQName(i));
        colValueList.add(atts.getValue(i));

        // Logger.getLogger("import.startElement").log(Level.FINEST,"XML element
        // key ("+i+"): "+atts.getQName(i));
        // Logger.getLogger("import.startElement").log(Level.FINEST,"XML element
        // value("+i+"): "+atts.getValue(i));

      }
    }

  }

  /**
   * Handle element end
   */
  @Override
  public void endElement(String uri, String name, String qName) throws SAXException {

    Logger.getLogger("import.endElement").log(Level.FINEST, "XML element ended: " + name);

    charValue = "";

    // skip dataset element
    if (!qName.equalsIgnoreCase("dataset")) {

      // if data -> write it to the DB
      if (!colNameList.isEmpty() && !colValueList.isEmpty() && !this.restrictedTables.contains(name)) {

        insertRowToDB(qName);
      }

      // clear arrays..
      colNameList.clear();
      colValueList.clear();

    }

  }

  /**
   * read characters
   */
  @Override
  public void characters(char ch[], int start, int length) {
    for (int i = start; i < start + length; i++) {
      charValue += ch[i];
    }
  }

  /**
   * gets the maximum value of given column and returns max + 1. if error is
   * thrown original value (colValue) is returned.
   * 
   * @param tableName
   * @param colName
   * @param colValue
   * @return
   */
  protected String reOrder(String tableName, String colName, String colValue) {
	Logger.getLogger("ETLCImport.reOrder").log(Level.FINE, "Executing reorder query. " +
			"Table name: " + tableName + ", Column name: " + colName + ", Column value: " + colValue);
	ResultSet rSet = null;
    Statement stmtc = null;
	String returnValue = "";
	final int MAX_RETRIES = 4;

	int retry_count = 1;
	while (retry_count < MAX_RETRIES) {
		if (retry_count > 1) {
			Logger.getLogger("ETLCImport.reOrder").log(Level.WARNING, "Retry " + retry_count + " for Table name: " 
					+ tableName + ", Column name: " + colName + ", Column value: " + colValue);
		}
		
		try {
			stmtc = createStatement();
			rSet = queryDBForMaxValue(stmtc, colName, tableName);
			int maxValue = getMaxValueFromRSet(colName, rSet);

      // cache the new value... key is column name + column old value
			String maxValueString = Integer.toString(maxValue);
			valueCache.put(colName + colValue, maxValueString);
			
			Logger.getLogger("ETLCImport.reOrder").log(Level.FINE, "Returning max value for column " + colName + ": " + maxValue);
			returnValue = maxValueString;
			retry_count = MAX_RETRIES;
		} catch (Exception e) {
			pauseBeforeRetry();
			returnValue = colValue;
			Logger.getLogger("import.reOrder").log(Level.SEVERE,
					"ERROR in reordering column: " + colName + " in table " + tableName + ". Default value from xml has been set. Value is: " + returnValue + "\n",
          e);
    } finally {
      try {
        if (rSet != null) {
          rSet.close();
        }

        if (stmtc != null) {
          stmtc.close();
        }
      } catch (final Exception e) {
        Logger.getLogger("import.reOrder").log(Level.WARNING, "Cleanup error - " + e.toString());
			}
			retry_count++;
    }
	}
		
	return returnValue;

  }
  
	protected void pauseBeforeRetry() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	private ResultSet queryDBForMaxValue(Statement stmtc, String colName, String tableName) throws SQLException {
		String queryClause = "";
		ResultSet rSet = null;
		
		queryClause = "select max(" + colName + ") as " + colName + " from " + tableName;
		rSet = stmtc.executeQuery(queryClause);
		Logger.getLogger("ETLCImport.reOrder").log(Level.FINE, "Finished executing reorder query: " + queryClause);
		return rSet;
	}
  
	private int getMaxValueFromRSet(String colName, ResultSet rSet) throws SQLException {
		int maxValue = 0;
		if (rSet.next()) {
			Logger.getLogger("ETLCImport.reOrder").log(Level.FINE, "Got values from ResultSet.next()");
			maxValue = rSet.getInt(colName) + 1;
		} else {
			Logger.getLogger("ETLCImport.reOrder").log(Level.WARNING, "Result set was empty. No max value found for column " + colName);
		}
		return maxValue;
	}

	protected Statement createStatement() throws SQLException {
		Statement stmtc = this.con.createStatement();
		return stmtc;
  }

  /**
   * Writes data (colNameList and colValueList ) into DB. reordering and value
   * replacements are also done. Re-ordering gets the new value of a column by
   * getting the highest value of a numeric column and adding one (1) to it.
   * reOrderList contains the columns that are re-ordered. Reordered columns are
   * cached so that they can be used in other tables. this way inner relations
   * in one import file are not lost. replacemet replaces a string in column to
   * another. replaceMap contains the replacement data. tablename+column name is
   * the key of the replaceMap and value is a two element list that contains old
   * value (0) and the new value (1)
   * 
   * @param tableName
   * @throws Exception
   */
  private void insertRowToDB(String tableName) throws SAXException {

    // names of the columns
    String names = "";
    // values of the columns
    String values = "";

    // loop all columns
    boolean first = true;
    for (int i = 0; i < colNameList.size(); i++) {

      // get column name and value..
      String colName = (String) colNameList.get(i);
      String colValue = (String) colValueList.get(i);

      // is this column cached..
      if (valueCache.containsKey(colName + colValue)) {
        // get correct value
        colValue = (String) valueCache.get(colName + colValue);
      }

      // if first loop, no need to add commas (,)
      if (!first) {
        names += ",";
        values += ",";
      }
      first = false;

      // add column.
      names += colName;

      // is this column reordered.
      if (reOrderList.contains(tableName + colName)) {

        values += reOrder(tableName, colName, colValue);

      } else {
        // contains this column replacements
        if (replaceMap.containsKey(tableName + colName)) {

          // replace list (arrayList) contains old value (0) and new value(1)
          ArrayList replaceList = (ArrayList) replaceMap.get(tableName + colName);

          Iterator repIter = replaceList.iterator();

          while (repIter.hasNext()) {

            String oldOne = (String) repIter.next();
            String newOne = (String) repIter.next();
            Logger.getLogger("import.insertRowToDB").log(Level.FINEST, "replacing " + oldOne + " with " + newOne);
            Logger.getLogger("import.insertRowToDB").log(Level.FINEST, "orginal: " + colValue);

            if (oldOne.length() == 1 && newOne.length() == 1) {
              colValue = colValue.replace(oldOne.charAt(0), newOne.charAt(0));
            } else {
              colValue = colValue.replaceAll(oldOne, newOne);
            }

            Logger.getLogger("import.insertRowToDB").log(Level.FINEST, "new: " + colValue);

          }

        }

        values += "'" + colValue + "'";
      }

    }

    String insertClause = "insert into " + tableName + "(" + names + ")values(" + values + ")";
    Statement stmt = null;
    try {

      Logger.getLogger("import.executeClause").log(Level.FINEST, "creating statement: ");
      stmt = this.con.createStatement();

      if (this.useLogging == true) {
        Logger.getLogger("import.executeClause").log(Level.INFO, "Executing clause: " + insertClause);
      }
      stmt.executeUpdate(insertClause);

    } catch (SQLException se) {
    	BackupAndRestore bnr=new BackupAndRestore();
		  bnr.restore();
      if (se.getErrorCode() == 548) {
        throw new SAXException("Error while importing table " + tableName + " possible duplicate sets.");
      }

      throw new SAXException(se);

    } catch (Exception e) {

      throw new SAXException(e);

    } finally {

      Logger.getLogger("import.executeClause").log(Level.FINEST, "closing statement");
      try {
        stmt.close();
      } catch (Exception e) {

      }
    }

  }

  /**
   * adds a import files content to the DB
   * 
   * @param filename
   */
  public void doImport(String filename) throws Exception {
    try {
      // this.rockFact = initRock();
      // this.con = this.rockFact.getConnection();

      BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
      readXML(br);
      //this.con.commit();
    } catch (Exception e) {
      // System.out.println("ERROR importing XML file: "+filename+"\n "+e);
      Logger.getLogger("import.doImport").log(Level.SEVERE, "ERROR in importing XML file: " + filename + " ", e);

      try {
        this.con.rollback();
      } catch (Exception ei) {
        Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR in rollback : ", ei);
      }

      throw new Exception(e);

    }

  }

  /**
   * adds a import files content to the DB
   * 
   * @param filename
   */
  public void doImport(StringReader sr) throws Exception {

    try {

      BufferedReader br = new BufferedReader(sr);
      readXML(br);
      //this.con.commit();
    } catch (Exception e) {
      // System.out.println("ERROR importing XML file: "+filename+"\n "+e);
      Logger.getLogger("import.doImport").log(Level.SEVERE, "ERROR in importing XML file: ", e);
      try {
        this.con.rollback();
      } catch (Exception ei) {
        Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR in rollback : ", ei);
      }
      throw new Exception(e);

    }

  }

  /**
   * adds a import files content to the DB
   * 
   * @param filename
   * @param sets
   * @param schedulings
   */
  public void doImport(String filename, boolean sets, boolean schedulings) throws Exception {
    try {

      if (!sets) {
        restrictedTables.addAll(this.setTables);
      }
      if (!schedulings) {
        restrictedTables.addAll(this.schedulerTables);
      }

      BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
      readXML(br);
     // this.con.commit();

    } catch (Exception e) {
      // System.out.println("ERROR importing XML file: "+filename+"\n "+e);
      Logger.getLogger("import.doImport").log(Level.SEVERE, "ERROR in importing XML file: " + filename + " ", e);
      try {
        this.con.rollback();
      } catch (Exception ei) {
        Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR in rollback : ", ei);
      }

      throw e;

    }
  }

  /**
   * Adds a import files content to the DB. This function is called from command line tech pack installer.
   * This function is different because it enables the logging to be turned off.
   * @param filename File to import.
   * @param sets If true import sets.
   * @param schedulings If true import schedulings.
   * @param useLogging If true logging is enabled, otherwise logging is disabled.
   */
  public void doImport(String filename, boolean sets, boolean schedulings, boolean useLogging) throws Exception {
    boolean previousEnableLoggingValue = this.useLogging;
    try {
      this.useLogging = useLogging;

      if (!sets) {
        restrictedTables.addAll(this.setTables);
      }
      if (!schedulings) {
        restrictedTables.addAll(this.schedulerTables);
      }

      BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
      readXML(br);
     // this.con.commit();

    } catch (Exception e) {
      // System.out.println("ERROR importing XML file: "+filename+"\n "+e);
      Logger.getLogger("import.doImport").log(Level.SEVERE, "ERROR in importing XML file: " + filename + " ", e);
      try {
        this.con.rollback();
      } catch (Exception ei) {
        this.useLogging = previousEnableLoggingValue;
        Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR in rollback : ", ei);
      }
      this.useLogging = previousEnableLoggingValue;
      throw e;

    }
    this.useLogging = previousEnableLoggingValue;
  }

  private void closeCon() {
    try {

      this.con.close();

    } catch (Exception e) {

      Logger.getLogger("import.closeCon").log(Level.SEVERE, "ERROR while closing connection: ", e);

    }

  }

  public static void main(String[] args) {

    if (args.length == 5) {

      ETLCImport imp = new ETLCImport(args[0], args[1], args[2], args[3]);

      try {

        imp.doImport(args[4], true, false);

      } catch (Exception e) {

      }
      imp.closeCon();

    } else {

      String tmp = "";
      for (int i = 0; i < args.length; i++) {
        tmp += args[i];
      }

      System.out.println("Wrong number of attributes in " + tmp + "\n Usage: \n" + "url\t" + "userName\t"
          + "password\t" + "dbDriverName\t" + "outputfile\n");
    }

  }
  
  private String[][] splitColumnIntoThreeParts(String attributeName, String value) {
    String[][] splitted = new String[2][NUMBER_OF_CLAUSE_PARTS];
    for(int i = 0; i<NUMBER_OF_CLAUSE_PARTS; i++){
      splitted[0][i] = attributeName + "_" + addLeadingZero(Integer.toString(i+1));
    }
    
    if (ACTION_CONTENTS.equalsIgnoreCase(attributeName)){
    	splitted[1] = splitStringIntoPieces(value, ACTION_CONTENT_MAX_SIZE_PER_COL);
    } else {
    	splitted[1] = splitStringIntoPieces(value, 32000);
    }
    
    
    return splitted;
  }

  private String[] splitStringIntoPieces(String string, int howLongPieces) {
    String[] splitted = new String[NUMBER_OF_CLAUSE_PARTS];
    for(int i=0; i<splitted.length; i++){
      splitted[i] = "";
    }
    
    if (null != string) {
      int stringLength = string.length();
      int intoHowMany = (int) Math.ceil((double)stringLength/(double)howLongPieces);
      for (int i = 0; i < NUMBER_OF_CLAUSE_PARTS; i++) {
        if(i < intoHowMany){
          int endIndex = i * howLongPieces + howLongPieces;
          endIndex = endIndex > stringLength ? stringLength : endIndex;
          splitted[i] = string.substring(i * howLongPieces, endIndex);
        }
      }
    }
    
    return splitted;
  }
  
  private String addLeadingZero(final String number) {
    return (number.length() == 1 ? "0" + number : number);
  }
}

