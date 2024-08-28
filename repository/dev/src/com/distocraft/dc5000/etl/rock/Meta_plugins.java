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
public class Meta_plugins implements Cloneable,RockDBObject  {

    private Long PLUGIN_ID;
    private String PLUGIN_NAME;
    private String CONSTRUCTOR_PARAMETER;
    private String IS_SOURCE;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private Long COMMIT_AFTER_N_ROWS;
    private String VERSION_NUMBER;
    private Long TRANSFER_ACTION_ID;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "PLUGIN_ID"    ,"COLLECTION_SET_ID"    ,"COLLECTION_ID"    ,"VERSION_NUMBER"    ,"TRANSFER_ACTION_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_plugins original; 

  public Meta_plugins(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_plugins(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.PLUGIN_ID = null;
         this.PLUGIN_NAME = null;
         this.CONSTRUCTOR_PARAMETER = null;
         this.IS_SOURCE = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.COMMIT_AFTER_N_ROWS = null;
         this.VERSION_NUMBER = null;
         this.TRANSFER_ACTION_ID = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_plugins(final RockFactory rockFact   ,final Long PLUGIN_ID ,final Long COLLECTION_SET_ID ,final Long COLLECTION_ID ,final String VERSION_NUMBER ,final Long TRANSFER_ACTION_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.PLUGIN_ID = PLUGIN_ID;
            this.COLLECTION_SET_ID = COLLECTION_SET_ID;
            this.COLLECTION_ID = COLLECTION_ID;
            this.VERSION_NUMBER = VERSION_NUMBER;
            this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_plugins> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_plugins o = it.next();

              this.PLUGIN_ID = o.getPlugin_id();
              this.PLUGIN_NAME = o.getPlugin_name();
              this.CONSTRUCTOR_PARAMETER = o.getConstructor_parameter();
              this.IS_SOURCE = o.getIs_source();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.COMMIT_AFTER_N_ROWS = o.getCommit_after_n_rows();
              this.VERSION_NUMBER = o.getVersion_number();
              this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_plugins");
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
  public Meta_plugins(final RockFactory rockFact, final Meta_plugins whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_plugins> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_plugins o = it.next();
                this.PLUGIN_ID = o.getPlugin_id();
                this.PLUGIN_NAME = o.getPlugin_name();
                this.CONSTRUCTOR_PARAMETER = o.getConstructor_parameter();
                this.IS_SOURCE = o.getIs_source();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.COMMIT_AFTER_N_ROWS = o.getCommit_after_n_rows();
                this.VERSION_NUMBER = o.getVersion_number();
                this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_plugins");
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
    return "Meta_plugins";
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
  public int updateDB(final boolean useTimestamp, final Meta_plugins whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_plugins whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_plugins.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_plugins ");
        sbuff.append("PLUGIN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_ID),2, true)+"\" ");
        sbuff.append("PLUGIN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_NAME),12, true)+"\" ");
        sbuff.append("CONSTRUCTOR_PARAMETER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONSTRUCTOR_PARAMETER),12, true)+"\" ");
        sbuff.append("IS_SOURCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_SOURCE),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("COMMIT_AFTER_N_ROWS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COMMIT_AFTER_N_ROWS),2, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
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
    sbuff.append("<Meta_plugins ");
        sbuff.append("PLUGIN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_ID),2, true)+"\" ");
        sbuff.append("PLUGIN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_NAME),12, true)+"\" ");
        sbuff.append("CONSTRUCTOR_PARAMETER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONSTRUCTOR_PARAMETER),12, true)+"\" ");
        sbuff.append("IS_SOURCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_SOURCE),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("COMMIT_AFTER_N_ROWS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COMMIT_AFTER_N_ROWS),2, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
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
    sbuff.append("</Meta_plugins>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_plugins ( ");
	    		sbuff.append("PLUGIN_ID");
		    		sbuff.append(", PLUGIN_NAME");
	    		sbuff.append(", CONSTRUCTOR_PARAMETER");
	    		sbuff.append(", IS_SOURCE");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", COMMIT_AFTER_N_ROWS");
	    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", TRANSFER_ACTION_ID");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.PLUGIN_ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.PLUGIN_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONSTRUCTOR_PARAMETER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.IS_SOURCE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COMMIT_AFTER_N_ROWS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFER_ACTION_ID,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getPlugin_id() { 
    return this.PLUGIN_ID;
  }
   public String getPlugin_name() { 
    return this.PLUGIN_NAME;
  }
   public String getConstructor_parameter() { 
    return this.CONSTRUCTOR_PARAMETER;
  }
   public String getIs_source() { 
    return this.IS_SOURCE;
  }
   public Long getCollection_set_id() { 
    return this.COLLECTION_SET_ID;
  }
   public Long getCollection_id() { 
    return this.COLLECTION_ID;
  }
   public Long getCommit_after_n_rows() { 
    return this.COMMIT_AFTER_N_ROWS;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getTransfer_action_id() { 
    return this.TRANSFER_ACTION_ID;
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
     if (PLUGIN_ID == null) {
      PLUGIN_ID = new Long (0);
    }
     if (PLUGIN_NAME == null) {
      PLUGIN_NAME = "";
    }
     if (CONSTRUCTOR_PARAMETER == null) {
      CONSTRUCTOR_PARAMETER = "";
    }
     if (IS_SOURCE == null) {
      IS_SOURCE = "";
    }
     if (COLLECTION_SET_ID == null) {
      COLLECTION_SET_ID = new Long (0);
    }
     if (COLLECTION_ID == null) {
      COLLECTION_ID = new Long (0);
    }
     if (COMMIT_AFTER_N_ROWS == null) {
      COMMIT_AFTER_N_ROWS = new Long (0);
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (TRANSFER_ACTION_ID == null) {
      TRANSFER_ACTION_ID = new Long (0);
    }
   }

   public void setPlugin_id(final Long PLUGIN_ID) {
    if (validateData){
      DataValidator.validateData((Object)PLUGIN_ID,"PLUGIN_ID",2,38,0);
    }
    modifiedColumns.add("PLUGIN_ID");
    this.PLUGIN_ID = PLUGIN_ID;
  }
   public void setPlugin_name(final String PLUGIN_NAME) {
    if (validateData){
      DataValidator.validateData((Object)PLUGIN_NAME,"PLUGIN_NAME",12,30,0);
    }
    modifiedColumns.add("PLUGIN_NAME");
    this.PLUGIN_NAME = PLUGIN_NAME;
  }
   public void setConstructor_parameter(final String CONSTRUCTOR_PARAMETER) {
    if (validateData){
      DataValidator.validateData((Object)CONSTRUCTOR_PARAMETER,"CONSTRUCTOR_PARAMETER",12,200,0);
    }
    modifiedColumns.add("CONSTRUCTOR_PARAMETER");
    this.CONSTRUCTOR_PARAMETER = CONSTRUCTOR_PARAMETER;
  }
   public void setIs_source(final String IS_SOURCE) {
    if (validateData){
      DataValidator.validateData((Object)IS_SOURCE,"IS_SOURCE",12,1,0);
    }
    modifiedColumns.add("IS_SOURCE");
    this.IS_SOURCE = IS_SOURCE;
  }
   public void setCollection_set_id(final Long COLLECTION_SET_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_SET_ID,"COLLECTION_SET_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_SET_ID");
    this.COLLECTION_SET_ID = COLLECTION_SET_ID;
  }
   public void setCollection_id(final Long COLLECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_ID,"COLLECTION_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_ID");
    this.COLLECTION_ID = COLLECTION_ID;
  }
   public void setCommit_after_n_rows(final Long COMMIT_AFTER_N_ROWS) {
    if (validateData){
      DataValidator.validateData((Object)COMMIT_AFTER_N_ROWS,"COMMIT_AFTER_N_ROWS",2,10,0);
    }
    modifiedColumns.add("COMMIT_AFTER_N_ROWS");
    this.COMMIT_AFTER_N_ROWS = COMMIT_AFTER_N_ROWS;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setTransfer_action_id(final Long TRANSFER_ACTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFER_ACTION_ID,"TRANSFER_ACTION_ID",2,38,0);
    }
    modifiedColumns.add("TRANSFER_ACTION_ID");
    this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_plugins o) {

         if ((((this.PLUGIN_ID == null) || (o.PLUGIN_ID == null)) && (this.PLUGIN_ID != o.PLUGIN_ID))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
          ){
    return false;
    } else
         if ((((this.PLUGIN_ID != null) && (o.PLUGIN_ID != null)) && (this.PLUGIN_ID.equals(o.PLUGIN_ID) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
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
  public boolean equals(final Meta_plugins o) {

         if ((((this.PLUGIN_ID == null) || (o.PLUGIN_ID == null)) && (this.PLUGIN_ID != o.PLUGIN_ID))
            || (((this.PLUGIN_NAME == null) || (o.PLUGIN_NAME == null)) && (this.PLUGIN_NAME != o.PLUGIN_NAME))
            || (((this.CONSTRUCTOR_PARAMETER == null) || (o.CONSTRUCTOR_PARAMETER == null)) && (this.CONSTRUCTOR_PARAMETER != o.CONSTRUCTOR_PARAMETER))
            || (((this.IS_SOURCE == null) || (o.IS_SOURCE == null)) && (this.IS_SOURCE != o.IS_SOURCE))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.COMMIT_AFTER_N_ROWS == null) || (o.COMMIT_AFTER_N_ROWS == null)) && (this.COMMIT_AFTER_N_ROWS != o.COMMIT_AFTER_N_ROWS))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
          ){
    return false;
    } else
         if ((((this.PLUGIN_ID != null) && (o.PLUGIN_ID != null)) && (this.PLUGIN_ID.equals(o.PLUGIN_ID) == false))
            || (((this.PLUGIN_NAME != null) && (o.PLUGIN_NAME != null)) && (this.PLUGIN_NAME.equals(o.PLUGIN_NAME) == false))
            || (((this.CONSTRUCTOR_PARAMETER != null) && (o.CONSTRUCTOR_PARAMETER != null)) && (this.CONSTRUCTOR_PARAMETER.equals(o.CONSTRUCTOR_PARAMETER) == false))
            || (((this.IS_SOURCE != null) && (o.IS_SOURCE != null)) && (this.IS_SOURCE.equals(o.IS_SOURCE) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.COMMIT_AFTER_N_ROWS != null) && (o.COMMIT_AFTER_N_ROWS != null)) && (this.COMMIT_AFTER_N_ROWS.equals(o.COMMIT_AFTER_N_ROWS) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
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
  * return 38
  */
  public static int getPlugin_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlugin_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getPlugin_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getPlugin_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlugin_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPlugin_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 200
  */
  public static int getConstructor_parameterColumnSize() {
    
     return 200;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConstructor_parameterDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConstructor_parameterSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getIs_sourceColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_sourceDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_sourceSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getCollection_set_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCollection_set_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getCollection_set_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getCollection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCollection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getCollection_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getCommit_after_n_rowsColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCommit_after_n_rowsDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getCommit_after_n_rowsSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getVersion_numberColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVersion_numberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVersion_numberSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getTransfer_action_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransfer_action_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTransfer_action_idSQLType() {
    
    return 2;   
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

  public Meta_plugins getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_plugins original) {
    this.original = (Meta_plugins) original.clone();
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
