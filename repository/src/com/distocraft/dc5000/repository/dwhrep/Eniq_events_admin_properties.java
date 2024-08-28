package com.distocraft.dc5000.repository.dwhrep;

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
public class Eniq_events_admin_properties implements Cloneable,RockDBObject  {

    private String PARAM_NAME;
    private String PARAM_VALUE;
    private Timestamp DATE_MODIFIED;
    private String MODIFIED_BY;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "PARAM_NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Eniq_events_admin_properties original; 

  public Eniq_events_admin_properties(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Eniq_events_admin_properties(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.PARAM_NAME = null;
         this.PARAM_VALUE = null;
         this.DATE_MODIFIED = null;
         this.MODIFIED_BY = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Eniq_events_admin_properties(final RockFactory rockFact   ,final String PARAM_NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.PARAM_NAME = PARAM_NAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Eniq_events_admin_properties> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Eniq_events_admin_properties o = it.next();

              this.PARAM_NAME = o.getParam_name();
              this.PARAM_VALUE = o.getParam_value();
              this.DATE_MODIFIED = o.getDate_modified();
              this.MODIFIED_BY = o.getModified_by();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Eniq_events_admin_properties");
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
  public Eniq_events_admin_properties(final RockFactory rockFact, final Eniq_events_admin_properties whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Eniq_events_admin_properties> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Eniq_events_admin_properties o = it.next();
                this.PARAM_NAME = o.getParam_name();
                this.PARAM_VALUE = o.getParam_value();
                this.DATE_MODIFIED = o.getDate_modified();
                this.MODIFIED_BY = o.getModified_by();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Eniq_events_admin_properties");
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
    return "Eniq_events_admin_properties";
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
  public int updateDB(final boolean useTimestamp, final Eniq_events_admin_properties whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Eniq_events_admin_properties whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Eniq_events_admin_properties.saveDB(), no primary key defined");
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
    sbuff.append("<Eniq_events_admin_properties ");
        sbuff.append("PARAM_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARAM_NAME),12, true)+"\" ");
        sbuff.append("PARAM_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARAM_VALUE),12, true)+"\" ");
        sbuff.append("DATE_MODIFIED=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATE_MODIFIED),93, true)+"\" ");
        sbuff.append("MODIFIED_BY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MODIFIED_BY),12, true)+"\" ");
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
    sbuff.append("<Eniq_events_admin_properties ");
        sbuff.append("PARAM_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARAM_NAME),12, true)+"\" ");
        sbuff.append("PARAM_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARAM_VALUE),12, true)+"\" ");
        sbuff.append("DATE_MODIFIED=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATE_MODIFIED),93, true)+"\" ");
        sbuff.append("MODIFIED_BY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MODIFIED_BY),12, true)+"\" ");
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
    sbuff.append("</Eniq_events_admin_properties>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Eniq_events_admin_properties ( ");
	    		sbuff.append("PARAM_NAME");
		    		sbuff.append(", PARAM_VALUE");
	    		sbuff.append(", DATE_MODIFIED");
	    		sbuff.append(", MODIFIED_BY");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.PARAM_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.PARAM_VALUE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATE_MODIFIED,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MODIFIED_BY,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getParam_name() { 
    return this.PARAM_NAME;
  }
   public String getParam_value() { 
    return this.PARAM_VALUE;
  }
   public Timestamp getDate_modified() { 
    return this.DATE_MODIFIED;
  }
   public String getModified_by() { 
    return this.MODIFIED_BY;
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
     if (PARAM_NAME == null) {
      PARAM_NAME = "";
    }
     if (PARAM_VALUE == null) {
      PARAM_VALUE = "";
    }
     if (DATE_MODIFIED == null) {
      DATE_MODIFIED = new Timestamp (0);
    }
     if (MODIFIED_BY == null) {
      MODIFIED_BY = "";
    }
   }

   public void setParam_name(final String PARAM_NAME) {
    if (validateData){
      DataValidator.validateData((Object)PARAM_NAME,"PARAM_NAME",12,100,0);
    }
    modifiedColumns.add("PARAM_NAME");
    this.PARAM_NAME = PARAM_NAME;
  }
   public void setParam_value(final String PARAM_VALUE) {
    if (validateData){
      DataValidator.validateData((Object)PARAM_VALUE,"PARAM_VALUE",12,1000,0);
    }
    modifiedColumns.add("PARAM_VALUE");
    this.PARAM_VALUE = PARAM_VALUE;
  }
   public void setDate_modified(final Timestamp DATE_MODIFIED) {
    if (validateData){
      DataValidator.validateData((Object)DATE_MODIFIED,"DATE_MODIFIED",93,23,0);
    }
    modifiedColumns.add("DATE_MODIFIED");
    this.DATE_MODIFIED = DATE_MODIFIED;
  }
   public void setModified_by(final String MODIFIED_BY) {
    if (validateData){
      DataValidator.validateData((Object)MODIFIED_BY,"MODIFIED_BY",12,100,0);
    }
    modifiedColumns.add("MODIFIED_BY");
    this.MODIFIED_BY = MODIFIED_BY;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Eniq_events_admin_properties o) {

         if ((((this.PARAM_NAME == null) || (o.PARAM_NAME == null)) && (this.PARAM_NAME != o.PARAM_NAME))
          ){
    return false;
    } else
         if ((((this.PARAM_NAME != null) && (o.PARAM_NAME != null)) && (this.PARAM_NAME.equals(o.PARAM_NAME) == false))
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
  public boolean equals(final Eniq_events_admin_properties o) {

         if ((((this.PARAM_NAME == null) || (o.PARAM_NAME == null)) && (this.PARAM_NAME != o.PARAM_NAME))
            || (((this.PARAM_VALUE == null) || (o.PARAM_VALUE == null)) && (this.PARAM_VALUE != o.PARAM_VALUE))
            || (((this.DATE_MODIFIED == null) || (o.DATE_MODIFIED == null)) && (this.DATE_MODIFIED != o.DATE_MODIFIED))
            || (((this.MODIFIED_BY == null) || (o.MODIFIED_BY == null)) && (this.MODIFIED_BY != o.MODIFIED_BY))
          ){
    return false;
    } else
         if ((((this.PARAM_NAME != null) && (o.PARAM_NAME != null)) && (this.PARAM_NAME.equals(o.PARAM_NAME) == false))
            || (((this.PARAM_VALUE != null) && (o.PARAM_VALUE != null)) && (this.PARAM_VALUE.equals(o.PARAM_VALUE) == false))
            || (((this.DATE_MODIFIED != null) && (o.DATE_MODIFIED != null)) && (this.DATE_MODIFIED.equals(o.DATE_MODIFIED) == false))
            || (((this.MODIFIED_BY != null) && (o.MODIFIED_BY != null)) && (this.MODIFIED_BY.equals(o.MODIFIED_BY) == false))
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
  * return 100
  */
  public static int getParam_nameColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getParam_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getParam_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1000
  */
  public static int getParam_valueColumnSize() {
    
     return 1000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getParam_valueDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getParam_valueSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getDate_modifiedColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDate_modifiedDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getDate_modifiedSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getModified_byColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getModified_byDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getModified_bySQLType() {
    
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

  public Eniq_events_admin_properties getOriginal() {
    return original;
  }
   
  public void setOriginal(final Eniq_events_admin_properties original) {
    this.original = (Eniq_events_admin_properties) original.clone();
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
