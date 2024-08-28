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
public class Measurementdeltacalcsupport implements Cloneable,RockDBObject  {

    private String TYPEID;
    private String VENDORRELEASE;
    private Integer DELTACALCSUPPORT;
    private String VERSIONID;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPEID"    ,"VENDORRELEASE"    ,"VERSIONID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Measurementdeltacalcsupport original; 

  public Measurementdeltacalcsupport(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Measurementdeltacalcsupport(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPEID = null;
         this.VENDORRELEASE = null;
         this.DELTACALCSUPPORT = null;
         this.VERSIONID = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Measurementdeltacalcsupport(final RockFactory rockFact   ,final String TYPEID ,final String VENDORRELEASE ,final String VERSIONID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPEID = TYPEID;
            this.VENDORRELEASE = VENDORRELEASE;
            this.VERSIONID = VERSIONID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Measurementdeltacalcsupport> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Measurementdeltacalcsupport o = it.next();

              this.TYPEID = o.getTypeid();
              this.VENDORRELEASE = o.getVendorrelease();
              this.DELTACALCSUPPORT = o.getDeltacalcsupport();
              this.VERSIONID = o.getVersionid();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementdeltacalcsupport");
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
  public Measurementdeltacalcsupport(final RockFactory rockFact, final Measurementdeltacalcsupport whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Measurementdeltacalcsupport> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Measurementdeltacalcsupport o = it.next();
                this.TYPEID = o.getTypeid();
                this.VENDORRELEASE = o.getVendorrelease();
                this.DELTACALCSUPPORT = o.getDeltacalcsupport();
                this.VERSIONID = o.getVersionid();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementdeltacalcsupport");
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
    return "Measurementdeltacalcsupport";
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
  public int updateDB(final boolean useTimestamp, final Measurementdeltacalcsupport whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Measurementdeltacalcsupport whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Measurementdeltacalcsupport.saveDB(), no primary key defined");
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
    sbuff.append("<Measurementdeltacalcsupport ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("VENDORRELEASE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORRELEASE),12, true)+"\" ");
        sbuff.append("DELTACALCSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DELTACALCSUPPORT),4, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
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
    sbuff.append("<Measurementdeltacalcsupport ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("VENDORRELEASE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORRELEASE),12, true)+"\" ");
        sbuff.append("DELTACALCSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DELTACALCSUPPORT),4, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
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
    sbuff.append("</Measurementdeltacalcsupport>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Measurementdeltacalcsupport ( ");
	    		sbuff.append("TYPEID");
		    		sbuff.append(", VENDORRELEASE");
	    		sbuff.append(", DELTACALCSUPPORT");
	    		sbuff.append(", VERSIONID");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VENDORRELEASE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DELTACALCSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getVendorrelease() { 
    return this.VENDORRELEASE;
  }
   public Integer getDeltacalcsupport() { 
    return this.DELTACALCSUPPORT;
  }
   public String getVersionid() { 
    return this.VERSIONID;
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
     if (TYPEID == null) {
      TYPEID = "";
    }
     if (VENDORRELEASE == null) {
      VENDORRELEASE = "";
    }
     if (DELTACALCSUPPORT == null) {
      DELTACALCSUPPORT = new Integer (0);
    }
     if (VERSIONID == null) {
      VERSIONID = "";
    }
   }

   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setVendorrelease(final String VENDORRELEASE) {
    if (validateData){
      DataValidator.validateData((Object)VENDORRELEASE,"VENDORRELEASE",12,16,0);
    }
    modifiedColumns.add("VENDORRELEASE");
    this.VENDORRELEASE = VENDORRELEASE;
  }
   public void setDeltacalcsupport(final Integer DELTACALCSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)DELTACALCSUPPORT,"DELTACALCSUPPORT",4,10,0);
    }
    modifiedColumns.add("DELTACALCSUPPORT");
    this.DELTACALCSUPPORT = DELTACALCSUPPORT;
  }
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Measurementdeltacalcsupport o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.VENDORRELEASE == null) || (o.VENDORRELEASE == null)) && (this.VENDORRELEASE != o.VENDORRELEASE))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.VENDORRELEASE != null) && (o.VENDORRELEASE != null)) && (this.VENDORRELEASE.equals(o.VENDORRELEASE) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
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
  public boolean equals(final Measurementdeltacalcsupport o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.VENDORRELEASE == null) || (o.VENDORRELEASE == null)) && (this.VENDORRELEASE != o.VENDORRELEASE))
            || (((this.DELTACALCSUPPORT == null) || (o.DELTACALCSUPPORT == null)) && (this.DELTACALCSUPPORT != o.DELTACALCSUPPORT))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.VENDORRELEASE != null) && (o.VENDORRELEASE != null)) && (this.VENDORRELEASE.equals(o.VENDORRELEASE) == false))
            || (((this.DELTACALCSUPPORT != null) && (o.DELTACALCSUPPORT != null)) && (this.DELTACALCSUPPORT.equals(o.DELTACALCSUPPORT) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
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
  public static int getTypeidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getVendorreleaseColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVendorreleaseDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVendorreleaseSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getDeltacalcsupportColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDeltacalcsupportDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getDeltacalcsupportSQLType() {
    
    return 4;   
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

  public Measurementdeltacalcsupport getOriginal() {
    return original;
  }
   
  public void setOriginal(final Measurementdeltacalcsupport original) {
    this.original = (Measurementdeltacalcsupport) original.clone();
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
