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
public class Meta_transformation_rules implements Cloneable,RockDBObject  {

    private Long TRANSFORMATION_ID;
    private String TRANSFORMATION_NAME;
    private String CODE;
    private String DESCRIPTION;
    private String VERSION_NUMBER;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TRANSFORMATION_ID"    ,"VERSION_NUMBER"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_transformation_rules original; 

  public Meta_transformation_rules(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_transformation_rules(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TRANSFORMATION_ID = null;
         this.TRANSFORMATION_NAME = null;
         this.CODE = null;
         this.DESCRIPTION = null;
         this.VERSION_NUMBER = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_transformation_rules(final RockFactory rockFact   ,final Long TRANSFORMATION_ID ,final String VERSION_NUMBER ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TRANSFORMATION_ID = TRANSFORMATION_ID;
            this.VERSION_NUMBER = VERSION_NUMBER;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_transformation_rules> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_transformation_rules o = it.next();

              this.TRANSFORMATION_ID = o.getTransformation_id();
              this.TRANSFORMATION_NAME = o.getTransformation_name();
              this.CODE = o.getCode();
              this.DESCRIPTION = o.getDescription();
              this.VERSION_NUMBER = o.getVersion_number();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transformation_rules");
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
  public Meta_transformation_rules(final RockFactory rockFact, final Meta_transformation_rules whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_transformation_rules> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_transformation_rules o = it.next();
                this.TRANSFORMATION_ID = o.getTransformation_id();
                this.TRANSFORMATION_NAME = o.getTransformation_name();
                this.CODE = o.getCode();
                this.DESCRIPTION = o.getDescription();
                this.VERSION_NUMBER = o.getVersion_number();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transformation_rules");
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
    return "Meta_transformation_rules";
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
  public int updateDB(final boolean useTimestamp, final Meta_transformation_rules whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_transformation_rules whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_transformation_rules.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_transformation_rules ");
        sbuff.append("TRANSFORMATION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_ID),2, true)+"\" ");
        sbuff.append("TRANSFORMATION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_NAME),12, true)+"\" ");
        sbuff.append("CODE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CODE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
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
    sbuff.append("<Meta_transformation_rules ");
        sbuff.append("TRANSFORMATION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_ID),2, true)+"\" ");
        sbuff.append("TRANSFORMATION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_NAME),12, true)+"\" ");
        sbuff.append("CODE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CODE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
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
    sbuff.append("</Meta_transformation_rules>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_transformation_rules ( ");
	    		sbuff.append("TRANSFORMATION_ID");
		    		sbuff.append(", TRANSFORMATION_NAME");
	    		sbuff.append(", CODE");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", VERSION_NUMBER");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TRANSFORMATION_ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFORMATION_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CODE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getTransformation_id() { 
    return this.TRANSFORMATION_ID;
  }
   public String getTransformation_name() { 
    return this.TRANSFORMATION_NAME;
  }
   public String getCode() { 
    return this.CODE;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
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
     if (TRANSFORMATION_ID == null) {
      TRANSFORMATION_ID = new Long (0);
    }
     if (TRANSFORMATION_NAME == null) {
      TRANSFORMATION_NAME = "";
    }
     if (CODE == null) {
      CODE = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
   }

   public void setTransformation_id(final Long TRANSFORMATION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFORMATION_ID,"TRANSFORMATION_ID",2,38,0);
    }
    modifiedColumns.add("TRANSFORMATION_ID");
    this.TRANSFORMATION_ID = TRANSFORMATION_ID;
  }
   public void setTransformation_name(final String TRANSFORMATION_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFORMATION_NAME,"TRANSFORMATION_NAME",12,10,0);
    }
    modifiedColumns.add("TRANSFORMATION_NAME");
    this.TRANSFORMATION_NAME = TRANSFORMATION_NAME;
  }
   public void setCode(final String CODE) {
    if (validateData){
      DataValidator.validateData((Object)CODE,"CODE",12,2000,0);
    }
    modifiedColumns.add("CODE");
    this.CODE = CODE;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_transformation_rules o) {

         if ((((this.TRANSFORMATION_ID == null) || (o.TRANSFORMATION_ID == null)) && (this.TRANSFORMATION_ID != o.TRANSFORMATION_ID))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
          ){
    return false;
    } else
         if ((((this.TRANSFORMATION_ID != null) && (o.TRANSFORMATION_ID != null)) && (this.TRANSFORMATION_ID.equals(o.TRANSFORMATION_ID) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
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
  public boolean equals(final Meta_transformation_rules o) {

         if ((((this.TRANSFORMATION_ID == null) || (o.TRANSFORMATION_ID == null)) && (this.TRANSFORMATION_ID != o.TRANSFORMATION_ID))
            || (((this.TRANSFORMATION_NAME == null) || (o.TRANSFORMATION_NAME == null)) && (this.TRANSFORMATION_NAME != o.TRANSFORMATION_NAME))
            || (((this.CODE == null) || (o.CODE == null)) && (this.CODE != o.CODE))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
          ){
    return false;
    } else
         if ((((this.TRANSFORMATION_ID != null) && (o.TRANSFORMATION_ID != null)) && (this.TRANSFORMATION_ID.equals(o.TRANSFORMATION_ID) == false))
            || (((this.TRANSFORMATION_NAME != null) && (o.TRANSFORMATION_NAME != null)) && (this.TRANSFORMATION_NAME.equals(o.TRANSFORMATION_NAME) == false))
            || (((this.CODE != null) && (o.CODE != null)) && (this.CODE.equals(o.CODE) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
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
  public static int getTransformation_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransformation_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTransformation_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getTransformation_nameColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransformation_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTransformation_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getCodeColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCodeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCodeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getDescriptionColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDescriptionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDescriptionSQLType() {
    
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

  public Meta_transformation_rules getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_transformation_rules original) {
    this.original = (Meta_transformation_rules) original.clone();
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
