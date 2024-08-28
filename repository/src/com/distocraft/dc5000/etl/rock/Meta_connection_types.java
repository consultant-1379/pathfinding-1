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
public class Meta_connection_types implements Cloneable,RockDBObject  {

    private String TYPE_NAME;
    private String DATABASE_TYPE_FLAG;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPE_NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_connection_types original; 

  public Meta_connection_types(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_connection_types(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPE_NAME = null;
         this.DATABASE_TYPE_FLAG = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_connection_types(final RockFactory rockFact   ,final String TYPE_NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPE_NAME = TYPE_NAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_connection_types> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_connection_types o = it.next();

              this.TYPE_NAME = o.getType_name();
              this.DATABASE_TYPE_FLAG = o.getDatabase_type_flag();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_connection_types");
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
  public Meta_connection_types(final RockFactory rockFact, final Meta_connection_types whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_connection_types> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_connection_types o = it.next();
                this.TYPE_NAME = o.getType_name();
                this.DATABASE_TYPE_FLAG = o.getDatabase_type_flag();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_connection_types");
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
    return "Meta_connection_types";
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
  public int updateDB(final boolean useTimestamp, final Meta_connection_types whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_connection_types whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_connection_types.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_connection_types ");
        sbuff.append("TYPE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE_NAME),12, true)+"\" ");
        sbuff.append("DATABASE_TYPE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATABASE_TYPE_FLAG),12, true)+"\" ");
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
    sbuff.append("<Meta_connection_types ");
        sbuff.append("TYPE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE_NAME),12, true)+"\" ");
        sbuff.append("DATABASE_TYPE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATABASE_TYPE_FLAG),12, true)+"\" ");
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
    sbuff.append("</Meta_connection_types>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_connection_types ( ");
	    		sbuff.append("TYPE_NAME");
		    		sbuff.append(", DATABASE_TYPE_FLAG");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPE_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DATABASE_TYPE_FLAG,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getType_name() { 
    return this.TYPE_NAME;
  }
   public String getDatabase_type_flag() { 
    return this.DATABASE_TYPE_FLAG;
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
     if (TYPE_NAME == null) {
      TYPE_NAME = "";
    }
     if (DATABASE_TYPE_FLAG == null) {
      DATABASE_TYPE_FLAG = "";
    }
   }

   public void setType_name(final String TYPE_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TYPE_NAME,"TYPE_NAME",12,15,0);
    }
    modifiedColumns.add("TYPE_NAME");
    this.TYPE_NAME = TYPE_NAME;
  }
   public void setDatabase_type_flag(final String DATABASE_TYPE_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)DATABASE_TYPE_FLAG,"DATABASE_TYPE_FLAG",12,1,0);
    }
    modifiedColumns.add("DATABASE_TYPE_FLAG");
    this.DATABASE_TYPE_FLAG = DATABASE_TYPE_FLAG;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_connection_types o) {

         if ((((this.TYPE_NAME == null) || (o.TYPE_NAME == null)) && (this.TYPE_NAME != o.TYPE_NAME))
          ){
    return false;
    } else
         if ((((this.TYPE_NAME != null) && (o.TYPE_NAME != null)) && (this.TYPE_NAME.equals(o.TYPE_NAME) == false))
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
  public boolean equals(final Meta_connection_types o) {

         if ((((this.TYPE_NAME == null) || (o.TYPE_NAME == null)) && (this.TYPE_NAME != o.TYPE_NAME))
            || (((this.DATABASE_TYPE_FLAG == null) || (o.DATABASE_TYPE_FLAG == null)) && (this.DATABASE_TYPE_FLAG != o.DATABASE_TYPE_FLAG))
          ){
    return false;
    } else
         if ((((this.TYPE_NAME != null) && (o.TYPE_NAME != null)) && (this.TYPE_NAME.equals(o.TYPE_NAME) == false))
            || (((this.DATABASE_TYPE_FLAG != null) && (o.DATABASE_TYPE_FLAG != null)) && (this.DATABASE_TYPE_FLAG.equals(o.DATABASE_TYPE_FLAG) == false))
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
  * return 15
  */
  public static int getType_nameColumnSize() {
    
     return 15;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getType_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getType_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getDatabase_type_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatabase_type_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDatabase_type_flagSQLType() {
    
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

  public Meta_connection_types getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_connection_types original) {
    this.original = (Meta_connection_types) original.clone();
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
