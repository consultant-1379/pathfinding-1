package com.distocraft.dc5000.repository.dwhrep;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockDBObject;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;
import ssc.rockfactory.DataValidator;


public class Dwhtechpacks implements Cloneable,RockDBObject  {

    private String TECHPACK_NAME;
    private String VERSIONID;
    private Timestamp CREATIONDATE;
   
  private String timeStampName = "LAST_UPDATED";

  private String[] columnsAndSequences = {  };

  private String[] primaryKeyNames = {    "TECHPACK_NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Dwhtechpacks original; 

  public Dwhtechpacks(RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Dwhtechpacks(RockFactory rockFact, boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TECHPACK_NAME = null;
         this.VERSIONID = null;
         this.CREATIONDATE = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Dwhtechpacks(RockFactory rockFact   ,String TECHPACK_NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TECHPACK_NAME = TECHPACK_NAME;
      
      RockResultSet results = rockFact.setSelectSQL(true, this);
      Iterator<Dwhtechpacks> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        Dwhtechpacks o = (Dwhtechpacks) it.next();

              this.TECHPACK_NAME = o.getTechpack_name();
              this.VERSIONID = o.getVersionid();
              this.CREATIONDATE = o.getCreationdate();
              results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhtechpacks");
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
  public Dwhtechpacks( RockFactory rockFact, Dwhtechpacks whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      Iterator<Dwhtechpacks> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        Dwhtechpacks o = (Dwhtechpacks) it.next();
                this.TECHPACK_NAME = o.getTechpack_name();
                this.VERSIONID = o.getVersionid();
                this.CREATIONDATE = o.getCreationdate();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhtechpacks");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  public Set<String> gimmeModifiedColumns(){   
    return modifiedColumns;  
  }

  public void setModifiedColumns(Set<String> modifiedColumns){ 
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
    return "Dwhtechpacks";
  }

  /**
   * Update object contents into database PRIMARY KEY MUST BE DEFINED
   * 
   * @exception SQLException
   */
  public int updateDB() throws SQLException, RockException {
    this.newItem = false; 
    updateCreationDate();
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
  public int updateDB(boolean useTimestamp) throws SQLException, RockException {
    this.newItem = false;
    updateCreationDate();
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
  public int updateDB(boolean useTimestamp, Dwhtechpacks whereObject) throws SQLException, RockException {
    this.newItem = false;
    updateCreationDate();
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
  public int insertDB( boolean useTimestamp, boolean useSequence) throws SQLException, RockException {
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
  public int deleteDB(Dwhtechpacks whereObject) throws SQLException, RockException {
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
    	updateCreationDate();
        rockFact.updateData(this, true, this, true);
      } else {
        throw new RockException("Cannot use rock.Dwhtechpacks.saveDB(), no primary key defined");
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
      updateCreationDate();
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
    StringBuffer sbuff = new StringBuffer();
    sbuff.append("<Dwhtechpacks ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("CREATIONDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CREATIONDATE),93, true)+"\" ");
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
    StringBuffer sbuff = new StringBuffer();
    sbuff.append("<Dwhtechpacks ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("CREATIONDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CREATIONDATE),93, true)+"\" ");
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
    StringBuffer sbuff = new StringBuffer();
    sbuff.append("</Dwhtechpacks>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
    StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Dwhtechpacks ( ");
	    		sbuff.append("TECHPACK_NAME");
		    		sbuff.append(", VERSIONID");
	    		sbuff.append(", CREATIONDATE");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TECHPACK_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CREATIONDATE,93)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTechpack_name() { 
    return this.TECHPACK_NAME;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public Timestamp getCreationdate() { 
    return this.CREATIONDATE;
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
     if (TECHPACK_NAME == null)
      TECHPACK_NAME = new String ("");
     if (VERSIONID == null)
      VERSIONID = new String ("");
     if (CREATIONDATE == null)


      CREATIONDATE = new Timestamp (0);

   }

   public void setTechpack_name(String TECHPACK_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_NAME,"TECHPACK_NAME",12,30,0);
    }
    modifiedColumns.add("TECHPACK_NAME");
    this.TECHPACK_NAME = TECHPACK_NAME;
  }
   public void setVersionid(String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setCreationdate(Timestamp CREATIONDATE) {
    if (validateData){
      DataValidator.validateData((Object)CREATIONDATE,"CREATIONDATE",93,23,0);
    }
    modifiedColumns.add("CREATIONDATE");
    this.CREATIONDATE = CREATIONDATE;
  }
 
  public void setcolumnsAndSequences(String[] newColsAndSeqs) {



    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */

  public boolean dbEquals(Dwhtechpacks o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
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

  public boolean equals(Dwhtechpacks o) {


         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.CREATIONDATE == null) || (o.CREATIONDATE == null)) && (this.CREATIONDATE != o.CREATIONDATE))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.CREATIONDATE != null) && (o.CREATIONDATE != null)) && (this.CREATIONDATE.equals(o.CREATIONDATE) == false))
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
    if (this.primaryKeyNames.length > 0)
      return true;
    else
      return false;
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
    RockResultSet results = rockFact.setSelectSQL(false, this);
    Iterator it = rockFact.getData(this, results);
    if (it.hasNext()) {
      results.close();
      return true;
    }
    results.close();
    return false;
  }

  
  /**
  * get columnSize
  * return 30
  */
  public static int getTechpack_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpack_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpack_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getVersionidColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVersionidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVersionidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getCreationdateColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCreationdateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getCreationdateSQLType() {
    
    return 93;   
  }
    
   public boolean isNewItem() {
    return newItem;
  }

  public void setNewItem(boolean newItem) {
    this.newItem = newItem;
  }

  public boolean isValidateData() {
    return validateData;
  }

  public void setValidateData(boolean validateData) {
    this.validateData = validateData;
  }

  public Dwhtechpacks getOriginal() {
    return original;
  }
   
  public void setOriginal(Dwhtechpacks original) {
    this.original = (Dwhtechpacks) original.clone();
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
  
  /*
   * If Techpack is updated to a newer version , this method 
   * updates the creation date field to Current System Time.
   */
  public void updateCreationDate(){
	 	 if(modifiedColumns.size()>0){
 	        setCreationdate(new Timestamp(System.currentTimeMillis()));
 	       }
  }
   
}