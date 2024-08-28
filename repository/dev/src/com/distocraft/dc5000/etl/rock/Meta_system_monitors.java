package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.sql.Timestamp; // NOPMD : eeipca : may not be used
import java.util.Date; // NOPMD : eeipca : may not be used
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockDBObject;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;
import ssc.rockfactory.DataValidator;

@SuppressWarnings({"PMD.SuspiciousConstantFieldName"}) // eeipca : columns are in UPPERCASE
public class Meta_system_monitors implements Cloneable,RockDBObject  {

    private String MONITOR;
    private String HOSTNAME;
    private String TYPE;
    private String CONFIGURATION;
    private Timestamp EXECUTED;
    private String STATUS;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "MONITOR"    ,"HOSTNAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_system_monitors original; 

  public Meta_system_monitors(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_system_monitors(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.MONITOR = null;
         this.HOSTNAME = null;
         this.TYPE = null;
         this.CONFIGURATION = null;
         this.EXECUTED = null;
         this.STATUS = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_system_monitors(final RockFactory rockFact   ,final String MONITOR ,final String HOSTNAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.MONITOR = MONITOR;
            this.HOSTNAME = HOSTNAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_system_monitors> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_system_monitors o = it.next();

              this.MONITOR = o.getMonitor();
              this.HOSTNAME = o.getHostname();
              this.TYPE = o.getType();
              this.CONFIGURATION = o.getConfiguration();
              this.EXECUTED = o.getExecuted();
              this.STATUS = o.getStatus();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_system_monitors");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  /**
   * Constructor to select the object according to whereObject from the db NO PRIMARY KEY DEFINED
   * 
   * @param whereObject
   *          the where part is constructed from this object
   * @exception SQLException
   */
  public Meta_system_monitors(final RockFactory rockFact, final Meta_system_monitors whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_system_monitors> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_system_monitors o = it.next();
                this.MONITOR = o.getMonitor();
                this.HOSTNAME = o.getHostname();
                this.TYPE = o.getType();
                this.CONFIGURATION = o.getConfiguration();
                this.EXECUTED = o.getExecuted();
                this.STATUS = o.getStatus();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_system_monitors");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  public Set<String> gimmeModifiedColumns(){   
    return modifiedColumns;  
  }

  public void setModifiedColumns(final Set<String> modifiedColumns){
    this.modifiedColumns = modifiedColumns;  
  }

  public void cleanModifiedColumns(){   
    modifiedColumns.clear();  
  }

  /**
   * Method for getting the table name
   * 
   * @return String name of the corresponding table
   */
  public String getTableName() {
    return "Meta_system_monitors";
  }

  /**
   * Update object contents into database PRIMARY KEY MUST BE DEFINED
   * 
   * @exception SQLException
   */
  public int updateDB() throws SQLException, RockException {
    this.newItem = false;
    return rockFact.updateData(this, true, null);
  }

  /**
   * Update object contents into database PRIMARY KEY MUST BE DEFINED
   * 
   * @param boolean
   *          useTimestamp if false, the timestamp is not used to compare if the data has been
   *          changed during the transaction
   * @exception SQLException
   */
  public int updateDB(final boolean useTimestamp) throws SQLException, RockException {
    this.newItem = false;
    return rockFact.updateData(this, true, null, useTimestamp);
  }

  /**
   * Update object contents into database NO PRIMARY KEY DEFINED
   * 
   * @param boolean
   *          useTimestamp if false, the timestamp is not used to compare if the data has been
   *          changed during the transaction
   * @param <this
   *          object type> whereObject the where part is constructed from this object
   * @exception SQLException
   */
  public int updateDB(final boolean useTimestamp, final Meta_system_monitors whereObject) throws SQLException, RockException {
    this.newItem = false;
    return rockFact.updateData(this, false, whereObject, useTimestamp);
  }

  /**
   * Insert object contents into database
   * 
   * @exception SQLException
   */
  public int insertDB() throws SQLException, RockException {
    this.newItem = false;
    return rockFact.insertData(this);
  }

  /**
   * Insert object contents into database
   * 
   * @param boolean
   *          useTimestamp if false, the timestamp is not used to compare if the data has been
   *          changed during the transaction
   * @param boolean
   *          useSequence if false the sequence columns don't get their values from db sequences
   * @exception SQLException
   */
  public int insertDB(final boolean useTimestamp, final boolean useSequence) throws SQLException, RockException {
    this.newItem = false;
    return rockFact.insertData(this, useTimestamp, useSequence);
  }

  /**
   * Delete object contents from database PRIMARY KEY MUST BE DEFINED
   * 
   * @exception SQLException
   */
  public int deleteDB() throws SQLException, RockException {
    this.newItem = true;
    return rockFact.deleteData(true, this);
  }

  /**
   * Delete object contents from database NO PRIMARY KEY DEFINED
   * 
   * @param <this
   *          object type> whereObject the where part is constructed from this object
   * @exception SQLException
   */
  public int deleteDB(final Meta_system_monitors whereObject) throws SQLException, RockException {
    this.newItem = true;
    return rockFact.deleteData(false, whereObject);
  }

  /**
   * Saves the data into the database
   * 
   * @exception SQLException
   */
  public void saveDB() throws SQLException, RockException {

    if (this.newItem) {
      insertDB();
    } else {
      if (isPrimaryDefined()) {
        rockFact.updateData(this, true, this, true);
      } else {
        throw new RockException("Cannot use rock.Meta_system_monitors.saveDB(), no primary key defined");
      }
    }
    this.newItem = false;
    this.setOriginal(this);
  }

  /**
   * Saves the data into the database (without primary key update)
   * 
   * @exception SQLException
   */
  public void saveToDB() throws SQLException, RockException {

    if (this.newItem) {
      insertDB();
    } else if (this.gimmeModifiedColumns().size() > 0) {
      rockFact.updateData(this, false, this.getOriginal(), true);
    }
    this.newItem = false;
    this.setOriginal(this);
  }


  /**
   * Prints the object out as XML
   * 
   * @exception SQLException
   */
    public String toXML_tag() throws SQLException, RockException {
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("<Meta_system_monitors ");
        sbuff.append("MONITOR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MONITOR),12, true)+"\" ");
        sbuff.append("HOSTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.HOSTNAME),12, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("CONFIGURATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONFIGURATION),12, true)+"\" ");
        sbuff.append("EXECUTED=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTED),93, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("DiffStatus=\"\"");
    sbuff.append(" />\n");  
    return sbuff.toString();
  }

  /**
   * Prints the object out as XML
   * 
   * @exception SQLException
   */
   
    public String toXML_startTag() throws SQLException, RockException {
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("<Meta_system_monitors ");
        sbuff.append("MONITOR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MONITOR),12, true)+"\" ");
        sbuff.append("HOSTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.HOSTNAME),12, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("CONFIGURATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONFIGURATION),12, true)+"\" ");
        sbuff.append("EXECUTED=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTED),93, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("DiffStatus=\"\"");
    sbuff.append(" >\n"); 
    return sbuff.toString();
  }

  /**
   * Prints the object out as XML
   * 
   * @exception SQLException
   */
   
    public String toXML_endTag() throws SQLException, RockException {
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("</Meta_system_monitors>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_system_monitors ( ");
	    		sbuff.append("MONITOR");
		    		sbuff.append(", HOSTNAME");
	    		sbuff.append(", TYPE");
	    		sbuff.append(", CONFIGURATION");
	    		sbuff.append(", EXECUTED");
	    		sbuff.append(", STATUS");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.MONITOR,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.HOSTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONFIGURATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXECUTED,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getMonitor() { 
    return this.MONITOR;
  }
   public String getHostname() { 
    return this.HOSTNAME;
  }
   public String getType() { 
    return this.TYPE;
  }
   public String getConfiguration() { 
    return this.CONFIGURATION;
  }
   public Timestamp getExecuted() { 
    return this.EXECUTED;
  }
   public String getStatus() { 
    return this.STATUS;
  }
 

  public String gettimeStampName() {
    return timeStampName;
  }

  public String[] getcolumnsAndSequences() {
    return columnsAndSequences;
  }

  public String[] getprimaryKeyNames() {
    return primaryKeyNames;
  }

  public RockFactory getRockFactory() {
    return this.rockFact;
  }

  public void removeNulls() {
     if (MONITOR == null) {
      MONITOR = "";
    }
     if (HOSTNAME == null) {
      HOSTNAME = "";
    }
     if (TYPE == null) {
      TYPE = "";
    }
     if (CONFIGURATION == null) {
      CONFIGURATION = "";
    }
     if (EXECUTED == null) {
      EXECUTED = new Timestamp (0);
    }
     if (STATUS == null) {
      STATUS = "";
    }
   }

   public void setMonitor(final String MONITOR) {
    if (validateData){
      DataValidator.validateData((Object)MONITOR,"MONITOR",12,255,0);
    }
    modifiedColumns.add("MONITOR");
    this.MONITOR = MONITOR;
  }
   public void setHostname(final String HOSTNAME) {
    if (validateData){
      DataValidator.validateData((Object)HOSTNAME,"HOSTNAME",12,255,0);
    }
    modifiedColumns.add("HOSTNAME");
    this.HOSTNAME = HOSTNAME;
  }
   public void setType(final String TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TYPE,"TYPE",12,32,0);
    }
    modifiedColumns.add("TYPE");
    this.TYPE = TYPE;
  }
   public void setConfiguration(final String CONFIGURATION) {
    if (validateData){
      DataValidator.validateData((Object)CONFIGURATION,"CONFIGURATION",12,32000,0);
    }
    modifiedColumns.add("CONFIGURATION");
    this.CONFIGURATION = CONFIGURATION;
  }
   public void setExecuted(final Timestamp EXECUTED) {
    if (validateData){
      DataValidator.validateData((Object)EXECUTED,"EXECUTED",93,23,0);
    }
    modifiedColumns.add("EXECUTED");
    this.EXECUTED = EXECUTED;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_system_monitors o) {

         if ((((this.MONITOR == null) || (o.MONITOR == null)) && (this.MONITOR != o.MONITOR))
            || (((this.HOSTNAME == null) || (o.HOSTNAME == null)) && (this.HOSTNAME != o.HOSTNAME))
          ){
    return false;
    } else
         if ((((this.MONITOR != null) && (o.MONITOR != null)) && (this.MONITOR.equals(o.MONITOR) == false))
            || (((this.HOSTNAME != null) && (o.HOSTNAME != null)) && (this.HOSTNAME.equals(o.HOSTNAME) == false))
          ){
    return false;
    } else {
      return true;
    }
  }

  /**
   * equals method test wheather the objects field values and the parametrs objects field values
   * are equal.
   */

  @SuppressWarnings({"PMD.SimplifyBooleanExpressions", "PMD.SimplifyBooleanReturns"})
  public boolean equals(final Meta_system_monitors o) {

         if ((((this.MONITOR == null) || (o.MONITOR == null)) && (this.MONITOR != o.MONITOR))
            || (((this.HOSTNAME == null) || (o.HOSTNAME == null)) && (this.HOSTNAME != o.HOSTNAME))
            || (((this.TYPE == null) || (o.TYPE == null)) && (this.TYPE != o.TYPE))
            || (((this.CONFIGURATION == null) || (o.CONFIGURATION == null)) && (this.CONFIGURATION != o.CONFIGURATION))
            || (((this.EXECUTED == null) || (o.EXECUTED == null)) && (this.EXECUTED != o.EXECUTED))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
          ){
    return false;
    } else
         if ((((this.MONITOR != null) && (o.MONITOR != null)) && (this.MONITOR.equals(o.MONITOR) == false))
            || (((this.HOSTNAME != null) && (o.HOSTNAME != null)) && (this.HOSTNAME.equals(o.HOSTNAME) == false))
            || (((this.TYPE != null) && (o.TYPE != null)) && (this.TYPE.equals(o.TYPE) == false))
            || (((this.CONFIGURATION != null) && (o.CONFIGURATION != null)) && (this.CONFIGURATION.equals(o.CONFIGURATION) == false))
            || (((this.EXECUTED != null) && (o.EXECUTED != null)) && (this.EXECUTED.equals(o.EXECUTED) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
          ){
    return false;
    } else {
      return true;
    }
  }
  
  /**
   * to enable a public clone method inherited from Object class (private method)
   */
  public Object clone() {
    Object o = null;
    try {
      o = super.clone();
    } catch (CloneNotSupportedException e) {
    }
    return o;
  }

  /**
   * Is the primakey defined for this table
   */
  public boolean isPrimaryDefined() {
    return this.primaryKeyNames.length > 0;
  }

  /**
   * Sets the member variables to Db default values
   */
  public void setDefaults() {

  }

  /**
   * Does the the object exist in the database
   * 
   * @return boolean true if exists else false.
   */
  public boolean existsDB() throws SQLException, RockException {
    final RockResultSet results = rockFact.setSelectSQL(false, this);
    final Iterator it = rockFact.getData(this, results);
    if (it.hasNext()) {
      results.close();
      return true;
    }
    results.close();
    return false;
  }

  
  /**
  * get columnSize
  * return 255
  */
  public static int getMonitorColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMonitorDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMonitorSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getHostnameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getHostnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getHostnameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getTypeColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getConfigurationColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConfigurationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConfigurationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getExecutedColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExecutedDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getExecutedSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getStatusColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStatusDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getStatusSQLType() {
    
    return 12;   
  }
    
   public boolean isNewItem() {
    return newItem;
  }

  public void setNewItem(final boolean newItem) {
    this.newItem = newItem;
  }

  public boolean isValidateData() {
    return validateData;
  }

  public void setValidateData(final boolean validateData) {
    this.validateData = validateData;
  }

  public Meta_system_monitors getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_system_monitors original) {
    this.original = (Meta_system_monitors) original.clone();
  }
   
  /**
   * Return true if rock object is new, modified or changed
   *
   */ 
  public boolean isUpdated() {
    return isNew() || isModified() || isChanged();
  }
   
  /**
   * Return true if rock object is new 
   *
   */ 
  public boolean isNew() {
    return (original == null);
  }
   
  /**
   * Return true if rock object is modified (values can be same)
   *
   */ 
  public boolean isModified() {
    return (gimmeModifiedColumns().size() > 0);
  }
   
  /**
   * Return true if rock object is changed 
   *
   */ 
  public boolean isChanged() {
    if (original != null) {
    	return (!this.equals(original));
    }
    return false;
  }

}
