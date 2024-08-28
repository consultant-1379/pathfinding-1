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
public class Userpreferences implements Cloneable,RockDBObject  {

    private Integer ID;
    private String USERNAME;
    private Integer VERSION;
    private String SETTINGS;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Userpreferences original; 

  public Userpreferences(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Userpreferences(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.ID = null;
         this.USERNAME = null;
         this.VERSION = null;
         this.SETTINGS = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Userpreferences(final RockFactory rockFact   ,final Integer ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.ID = ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Userpreferences> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Userpreferences o = it.next();

              this.ID = o.getId();
              this.USERNAME = o.getUsername();
              this.VERSION = o.getVersion();
              this.SETTINGS = o.getSettings();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Userpreferences");
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
  public Userpreferences(final RockFactory rockFact, final Userpreferences whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Userpreferences> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Userpreferences o = it.next();
                this.ID = o.getId();
                this.USERNAME = o.getUsername();
                this.VERSION = o.getVersion();
                this.SETTINGS = o.getSettings();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Userpreferences");
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
    return "Userpreferences";
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
  public int updateDB(final boolean useTimestamp, final Userpreferences whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Userpreferences whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Userpreferences.saveDB(), no primary key defined");
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
    sbuff.append("<Userpreferences ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),4, true)+"\" ");
        sbuff.append("USERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USERNAME),12, true)+"\" ");
        sbuff.append("VERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION),4, true)+"\" ");
        sbuff.append("SETTINGS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SETTINGS),-1, true)+"\" ");
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
    sbuff.append("<Userpreferences ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),4, true)+"\" ");
        sbuff.append("USERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USERNAME),12, true)+"\" ");
        sbuff.append("VERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION),4, true)+"\" ");
        sbuff.append("SETTINGS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SETTINGS),-1, true)+"\" ");
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
    sbuff.append("</Userpreferences>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Userpreferences ( ");
	    		sbuff.append("ID");
		    		sbuff.append(", USERNAME");
	    		sbuff.append(", VERSION");
	    		sbuff.append(", SETTINGS");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.ID,4)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.USERNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SETTINGS,-1)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Integer getId() { 
    return this.ID;
  }
   public String getUsername() { 
    return this.USERNAME;
  }
   public Integer getVersion() { 
    return this.VERSION;
  }
   public String getSettings() { 
    return this.SETTINGS;
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
     if (ID == null) {
      ID = new Integer (0);
    }
     if (USERNAME == null) {
      USERNAME = "";
    }
     if (VERSION == null) {
      VERSION = new Integer (0);
    }
     if (SETTINGS == null) {
      SETTINGS = "";
    }
   }

   public void setId(final Integer ID) {
    if (validateData){
      DataValidator.validateData((Object)ID,"ID",4,10,0);
    }
    modifiedColumns.add("ID");
    this.ID = ID;
  }
   public void setUsername(final String USERNAME) {
    if (validateData){
      DataValidator.validateData((Object)USERNAME,"USERNAME",12,20,0);
    }
    modifiedColumns.add("USERNAME");
    this.USERNAME = USERNAME;
  }
   public void setVersion(final Integer VERSION) {
    if (validateData){
      DataValidator.validateData((Object)VERSION,"VERSION",4,10,0);
    }
    modifiedColumns.add("VERSION");
    this.VERSION = VERSION;
  }
   public void setSettings(final String SETTINGS) {
    if (validateData){
      DataValidator.validateData((Object)SETTINGS,"SETTINGS",-1,2147483647,0);
    }
    modifiedColumns.add("SETTINGS");
    this.SETTINGS = SETTINGS;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Userpreferences o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
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
  public boolean equals(final Userpreferences o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
            || (((this.USERNAME == null) || (o.USERNAME == null)) && (this.USERNAME != o.USERNAME))
            || (((this.VERSION == null) || (o.VERSION == null)) && (this.VERSION != o.VERSION))
            || (((this.SETTINGS == null) || (o.SETTINGS == null)) && (this.SETTINGS != o.SETTINGS))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
            || (((this.USERNAME != null) && (o.USERNAME != null)) && (this.USERNAME.equals(o.USERNAME) == false))
            || (((this.VERSION != null) && (o.VERSION != null)) && (this.VERSION.equals(o.VERSION) == false))
            || (((this.SETTINGS != null) && (o.SETTINGS != null)) && (this.SETTINGS.equals(o.SETTINGS) == false))
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
  * return 10
  */
  public static int getIdColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIdDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getIdSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 20
  */
  public static int getUsernameColumnSize() {
    
     return 20;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUsernameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUsernameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getVersionColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVersionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getVersionSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 2147483647
  */
  public static int getSettingsColumnSize() {
    
     return 2147483647;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSettingsDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return -1
  */
  public static int getSettingsSQLType() {
    
    return -1;   
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

  public Userpreferences getOriginal() {
    return original;
  }
   
  public void setOriginal(final Userpreferences original) {
    this.original = (Userpreferences) original.clone();
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
