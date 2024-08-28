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
public class Meta_tables implements Cloneable,RockDBObject  {

    private Long TABLE_ID;
    private String TABLE_NAME;
    private String VERSION_NUMBER;
    private String IS_JOIN;
    private String JOIN_CLAUSE;
    private String TABLES_AND_ALIASES;
    private Long CONNECTION_ID;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TABLE_ID"    ,"VERSION_NUMBER"    ,"CONNECTION_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_tables original; 

  public Meta_tables(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_tables(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TABLE_ID = null;
         this.TABLE_NAME = null;
         this.VERSION_NUMBER = null;
         this.IS_JOIN = null;
         this.JOIN_CLAUSE = null;
         this.TABLES_AND_ALIASES = null;
         this.CONNECTION_ID = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_tables(final RockFactory rockFact   ,final Long TABLE_ID ,final String VERSION_NUMBER ,final Long CONNECTION_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TABLE_ID = TABLE_ID;
            this.VERSION_NUMBER = VERSION_NUMBER;
            this.CONNECTION_ID = CONNECTION_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_tables> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_tables o = it.next();

              this.TABLE_ID = o.getTable_id();
              this.TABLE_NAME = o.getTable_name();
              this.VERSION_NUMBER = o.getVersion_number();
              this.IS_JOIN = o.getIs_join();
              this.JOIN_CLAUSE = o.getJoin_clause();
              this.TABLES_AND_ALIASES = o.getTables_and_aliases();
              this.CONNECTION_ID = o.getConnection_id();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_tables");
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
  public Meta_tables(final RockFactory rockFact, final Meta_tables whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_tables> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_tables o = it.next();
                this.TABLE_ID = o.getTable_id();
                this.TABLE_NAME = o.getTable_name();
                this.VERSION_NUMBER = o.getVersion_number();
                this.IS_JOIN = o.getIs_join();
                this.JOIN_CLAUSE = o.getJoin_clause();
                this.TABLES_AND_ALIASES = o.getTables_and_aliases();
                this.CONNECTION_ID = o.getConnection_id();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_tables");
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
    return "Meta_tables";
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
  public int updateDB(final boolean useTimestamp, final Meta_tables whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_tables whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_tables.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_tables ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
        sbuff.append("TABLE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("IS_JOIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_JOIN),12, true)+"\" ");
        sbuff.append("JOIN_CLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.JOIN_CLAUSE),12, true)+"\" ");
        sbuff.append("TABLES_AND_ALIASES=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLES_AND_ALIASES),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
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
    sbuff.append("<Meta_tables ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
        sbuff.append("TABLE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("IS_JOIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_JOIN),12, true)+"\" ");
        sbuff.append("JOIN_CLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.JOIN_CLAUSE),12, true)+"\" ");
        sbuff.append("TABLES_AND_ALIASES=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLES_AND_ALIASES),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
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
    sbuff.append("</Meta_tables>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_tables ( ");
	    		sbuff.append("TABLE_ID");
		    		sbuff.append(", TABLE_NAME");
	    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", IS_JOIN");
	    		sbuff.append(", JOIN_CLAUSE");
	    		sbuff.append(", TABLES_AND_ALIASES");
	    		sbuff.append(", CONNECTION_ID");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TABLE_ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TABLE_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.IS_JOIN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.JOIN_CLAUSE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLES_AND_ALIASES,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_ID,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getTable_id() { 
    return this.TABLE_ID;
  }
   public String getTable_name() { 
    return this.TABLE_NAME;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public String getIs_join() { 
    return this.IS_JOIN;
  }
   public String getJoin_clause() { 
    return this.JOIN_CLAUSE;
  }
   public String getTables_and_aliases() { 
    return this.TABLES_AND_ALIASES;
  }
   public Long getConnection_id() { 
    return this.CONNECTION_ID;
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
     if (TABLE_ID == null) {
      TABLE_ID = new Long (0);
    }
     if (TABLE_NAME == null) {
      TABLE_NAME = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (IS_JOIN == null) {
      IS_JOIN = "";
    }
     if (JOIN_CLAUSE == null) {
      JOIN_CLAUSE = "";
    }
     if (TABLES_AND_ALIASES == null) {
      TABLES_AND_ALIASES = "";
    }
     if (CONNECTION_ID == null) {
      CONNECTION_ID = new Long (0);
    }
   }

   public void setTable_id(final Long TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TABLE_ID,"TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TABLE_ID");
    this.TABLE_ID = TABLE_ID;
  }
   public void setTable_name(final String TABLE_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TABLE_NAME,"TABLE_NAME",12,60,0);
    }
    modifiedColumns.add("TABLE_NAME");
    this.TABLE_NAME = TABLE_NAME;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setIs_join(final String IS_JOIN) {
    if (validateData){
      DataValidator.validateData((Object)IS_JOIN,"IS_JOIN",12,1,0);
    }
    modifiedColumns.add("IS_JOIN");
    this.IS_JOIN = IS_JOIN;
  }
   public void setJoin_clause(final String JOIN_CLAUSE) {
    if (validateData){
      DataValidator.validateData((Object)JOIN_CLAUSE,"JOIN_CLAUSE",12,2000,0);
    }
    modifiedColumns.add("JOIN_CLAUSE");
    this.JOIN_CLAUSE = JOIN_CLAUSE;
  }
   public void setTables_and_aliases(final String TABLES_AND_ALIASES) {
    if (validateData){
      DataValidator.validateData((Object)TABLES_AND_ALIASES,"TABLES_AND_ALIASES",12,2000,0);
    }
    modifiedColumns.add("TABLES_AND_ALIASES");
    this.TABLES_AND_ALIASES = TABLES_AND_ALIASES;
  }
   public void setConnection_id(final Long CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_ID,"CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("CONNECTION_ID");
    this.CONNECTION_ID = CONNECTION_ID;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_tables o) {

         if ((((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
          ){
    return false;
    } else
         if ((((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
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
  public boolean equals(final Meta_tables o) {

         if ((((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
            || (((this.TABLE_NAME == null) || (o.TABLE_NAME == null)) && (this.TABLE_NAME != o.TABLE_NAME))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.IS_JOIN == null) || (o.IS_JOIN == null)) && (this.IS_JOIN != o.IS_JOIN))
            || (((this.JOIN_CLAUSE == null) || (o.JOIN_CLAUSE == null)) && (this.JOIN_CLAUSE != o.JOIN_CLAUSE))
            || (((this.TABLES_AND_ALIASES == null) || (o.TABLES_AND_ALIASES == null)) && (this.TABLES_AND_ALIASES != o.TABLES_AND_ALIASES))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
          ){
    return false;
    } else
         if ((((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
            || (((this.TABLE_NAME != null) && (o.TABLE_NAME != null)) && (this.TABLE_NAME.equals(o.TABLE_NAME) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.IS_JOIN != null) && (o.IS_JOIN != null)) && (this.IS_JOIN.equals(o.IS_JOIN) == false))
            || (((this.JOIN_CLAUSE != null) && (o.JOIN_CLAUSE != null)) && (this.JOIN_CLAUSE.equals(o.JOIN_CLAUSE) == false))
            || (((this.TABLES_AND_ALIASES != null) && (o.TABLES_AND_ALIASES != null)) && (this.TABLES_AND_ALIASES.equals(o.TABLES_AND_ALIASES) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
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
  public static int getTable_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTable_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTable_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 60
  */
  public static int getTable_nameColumnSize() {
    
     return 60;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTable_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTable_nameSQLType() {
    
    return 12;   
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
  * return 1
  */
  public static int getIs_joinColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_joinDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_joinSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getJoin_clauseColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getJoin_clauseDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getJoin_clauseSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getTables_and_aliasesColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTables_and_aliasesDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTables_and_aliasesSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getConnection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConnection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getConnection_idSQLType() {
    
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

  public Meta_tables getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_tables original) {
    this.original = (Meta_tables) original.clone();
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
