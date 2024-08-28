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
public class Meta_columns implements Cloneable,RockDBObject  {

    private Long COLUMN_ID;
    private String COLUMN_NAME;
    private String COLUMN_ALIAS_NAME;
    private String COLUMN_TYPE;
    private Long COLUMN_LENGTH;
    private String IS_PK_COLUMN;
    private String VERSION_NUMBER;
    private Long CONNECTION_ID;
    private Long TABLE_ID;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "COLUMN_ID"    ,"VERSION_NUMBER"    ,"CONNECTION_ID"    ,"TABLE_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_columns original; 

  public Meta_columns(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_columns(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.COLUMN_ID = null;
         this.COLUMN_NAME = null;
         this.COLUMN_ALIAS_NAME = null;
         this.COLUMN_TYPE = null;
         this.COLUMN_LENGTH = null;
         this.IS_PK_COLUMN = null;
         this.VERSION_NUMBER = null;
         this.CONNECTION_ID = null;
         this.TABLE_ID = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_columns(final RockFactory rockFact   ,final Long COLUMN_ID ,final String VERSION_NUMBER ,final Long CONNECTION_ID ,final Long TABLE_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.COLUMN_ID = COLUMN_ID;
            this.VERSION_NUMBER = VERSION_NUMBER;
            this.CONNECTION_ID = CONNECTION_ID;
            this.TABLE_ID = TABLE_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_columns> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_columns o = it.next();

              this.COLUMN_ID = o.getColumn_id();
              this.COLUMN_NAME = o.getColumn_name();
              this.COLUMN_ALIAS_NAME = o.getColumn_alias_name();
              this.COLUMN_TYPE = o.getColumn_type();
              this.COLUMN_LENGTH = o.getColumn_length();
              this.IS_PK_COLUMN = o.getIs_pk_column();
              this.VERSION_NUMBER = o.getVersion_number();
              this.CONNECTION_ID = o.getConnection_id();
              this.TABLE_ID = o.getTable_id();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_columns");
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
  public Meta_columns(final RockFactory rockFact, final Meta_columns whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_columns> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_columns o = it.next();
                this.COLUMN_ID = o.getColumn_id();
                this.COLUMN_NAME = o.getColumn_name();
                this.COLUMN_ALIAS_NAME = o.getColumn_alias_name();
                this.COLUMN_TYPE = o.getColumn_type();
                this.COLUMN_LENGTH = o.getColumn_length();
                this.IS_PK_COLUMN = o.getIs_pk_column();
                this.VERSION_NUMBER = o.getVersion_number();
                this.CONNECTION_ID = o.getConnection_id();
                this.TABLE_ID = o.getTable_id();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_columns");
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
    return "Meta_columns";
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
  public int updateDB(final boolean useTimestamp, final Meta_columns whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_columns whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_columns.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_columns ");
        sbuff.append("COLUMN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID),2, true)+"\" ");
        sbuff.append("COLUMN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_NAME),12, true)+"\" ");
        sbuff.append("COLUMN_ALIAS_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ALIAS_NAME),12, true)+"\" ");
        sbuff.append("COLUMN_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_TYPE),12, true)+"\" ");
        sbuff.append("COLUMN_LENGTH=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_LENGTH),2, true)+"\" ");
        sbuff.append("IS_PK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_PK_COLUMN),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
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
    sbuff.append("<Meta_columns ");
        sbuff.append("COLUMN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID),2, true)+"\" ");
        sbuff.append("COLUMN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_NAME),12, true)+"\" ");
        sbuff.append("COLUMN_ALIAS_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ALIAS_NAME),12, true)+"\" ");
        sbuff.append("COLUMN_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_TYPE),12, true)+"\" ");
        sbuff.append("COLUMN_LENGTH=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_LENGTH),2, true)+"\" ");
        sbuff.append("IS_PK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_PK_COLUMN),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
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
    sbuff.append("</Meta_columns>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_columns ( ");
	    		sbuff.append("COLUMN_ID");
		    		sbuff.append(", COLUMN_NAME");
	    		sbuff.append(", COLUMN_ALIAS_NAME");
	    		sbuff.append(", COLUMN_TYPE");
	    		sbuff.append(", COLUMN_LENGTH");
	    		sbuff.append(", IS_PK_COLUMN");
	    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", CONNECTION_ID");
	    		sbuff.append(", TABLE_ID");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.COLUMN_ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_ALIAS_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_LENGTH,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.IS_PK_COLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLE_ID,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getColumn_id() { 
    return this.COLUMN_ID;
  }
   public String getColumn_name() { 
    return this.COLUMN_NAME;
  }
   public String getColumn_alias_name() { 
    return this.COLUMN_ALIAS_NAME;
  }
   public String getColumn_type() { 
    return this.COLUMN_TYPE;
  }
   public Long getColumn_length() { 
    return this.COLUMN_LENGTH;
  }
   public String getIs_pk_column() { 
    return this.IS_PK_COLUMN;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getConnection_id() { 
    return this.CONNECTION_ID;
  }
   public Long getTable_id() { 
    return this.TABLE_ID;
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
     if (COLUMN_ID == null) {
      COLUMN_ID = new Long (0);
    }
     if (COLUMN_NAME == null) {
      COLUMN_NAME = "";
    }
     if (COLUMN_ALIAS_NAME == null) {
      COLUMN_ALIAS_NAME = "";
    }
     if (COLUMN_TYPE == null) {
      COLUMN_TYPE = "";
    }
     if (COLUMN_LENGTH == null) {
      COLUMN_LENGTH = new Long (0);
    }
     if (IS_PK_COLUMN == null) {
      IS_PK_COLUMN = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (CONNECTION_ID == null) {
      CONNECTION_ID = new Long (0);
    }
     if (TABLE_ID == null) {
      TABLE_ID = new Long (0);
    }
   }

   public void setColumn_id(final Long COLUMN_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ID,"COLUMN_ID",2,38,0);
    }
    modifiedColumns.add("COLUMN_ID");
    this.COLUMN_ID = COLUMN_ID;
  }
   public void setColumn_name(final String COLUMN_NAME) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_NAME,"COLUMN_NAME",12,30,0);
    }
    modifiedColumns.add("COLUMN_NAME");
    this.COLUMN_NAME = COLUMN_NAME;
  }
   public void setColumn_alias_name(final String COLUMN_ALIAS_NAME) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ALIAS_NAME,"COLUMN_ALIAS_NAME",12,60,0);
    }
    modifiedColumns.add("COLUMN_ALIAS_NAME");
    this.COLUMN_ALIAS_NAME = COLUMN_ALIAS_NAME;
  }
   public void setColumn_type(final String COLUMN_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_TYPE,"COLUMN_TYPE",12,30,0);
    }
    modifiedColumns.add("COLUMN_TYPE");
    this.COLUMN_TYPE = COLUMN_TYPE;
  }
   public void setColumn_length(final Long COLUMN_LENGTH) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_LENGTH,"COLUMN_LENGTH",2,38,0);
    }
    modifiedColumns.add("COLUMN_LENGTH");
    this.COLUMN_LENGTH = COLUMN_LENGTH;
  }
   public void setIs_pk_column(final String IS_PK_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)IS_PK_COLUMN,"IS_PK_COLUMN",12,1,0);
    }
    modifiedColumns.add("IS_PK_COLUMN");
    this.IS_PK_COLUMN = IS_PK_COLUMN;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setConnection_id(final Long CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_ID,"CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("CONNECTION_ID");
    this.CONNECTION_ID = CONNECTION_ID;
  }
   public void setTable_id(final Long TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TABLE_ID,"TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TABLE_ID");
    this.TABLE_ID = TABLE_ID;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_columns o) {

         if ((((this.COLUMN_ID == null) || (o.COLUMN_ID == null)) && (this.COLUMN_ID != o.COLUMN_ID))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
          ){
    return false;
    } else
         if ((((this.COLUMN_ID != null) && (o.COLUMN_ID != null)) && (this.COLUMN_ID.equals(o.COLUMN_ID) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
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
  public boolean equals(final Meta_columns o) {

         if ((((this.COLUMN_ID == null) || (o.COLUMN_ID == null)) && (this.COLUMN_ID != o.COLUMN_ID))
            || (((this.COLUMN_NAME == null) || (o.COLUMN_NAME == null)) && (this.COLUMN_NAME != o.COLUMN_NAME))
            || (((this.COLUMN_ALIAS_NAME == null) || (o.COLUMN_ALIAS_NAME == null)) && (this.COLUMN_ALIAS_NAME != o.COLUMN_ALIAS_NAME))
            || (((this.COLUMN_TYPE == null) || (o.COLUMN_TYPE == null)) && (this.COLUMN_TYPE != o.COLUMN_TYPE))
            || (((this.COLUMN_LENGTH == null) || (o.COLUMN_LENGTH == null)) && (this.COLUMN_LENGTH != o.COLUMN_LENGTH))
            || (((this.IS_PK_COLUMN == null) || (o.IS_PK_COLUMN == null)) && (this.IS_PK_COLUMN != o.IS_PK_COLUMN))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
          ){
    return false;
    } else
         if ((((this.COLUMN_ID != null) && (o.COLUMN_ID != null)) && (this.COLUMN_ID.equals(o.COLUMN_ID) == false))
            || (((this.COLUMN_NAME != null) && (o.COLUMN_NAME != null)) && (this.COLUMN_NAME.equals(o.COLUMN_NAME) == false))
            || (((this.COLUMN_ALIAS_NAME != null) && (o.COLUMN_ALIAS_NAME != null)) && (this.COLUMN_ALIAS_NAME.equals(o.COLUMN_ALIAS_NAME) == false))
            || (((this.COLUMN_TYPE != null) && (o.COLUMN_TYPE != null)) && (this.COLUMN_TYPE.equals(o.COLUMN_TYPE) == false))
            || (((this.COLUMN_LENGTH != null) && (o.COLUMN_LENGTH != null)) && (this.COLUMN_LENGTH.equals(o.COLUMN_LENGTH) == false))
            || (((this.IS_PK_COLUMN != null) && (o.IS_PK_COLUMN != null)) && (this.IS_PK_COLUMN.equals(o.IS_PK_COLUMN) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
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
  public static int getColumn_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getColumn_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getColumn_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 60
  */
  public static int getColumn_alias_nameColumnSize() {
    
     return 60;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_alias_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getColumn_alias_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getColumn_typeColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getColumn_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getColumn_lengthColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_lengthDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_lengthSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getIs_pk_columnColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_pk_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_pk_columnSQLType() {
    
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

  public Meta_columns getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_columns original) {
    this.original = (Meta_columns) original.clone();
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
