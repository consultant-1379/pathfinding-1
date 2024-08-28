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
public class Universename implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String UNIVERSENAME;
    private String UNIVERSEEXTENSION;
    private Long ORDERNRO;
    private String UNIVERSEEXTENSIONNAME;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"UNIVERSEEXTENSION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Universename original; 

  public Universename(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Universename(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.UNIVERSENAME = null;
         this.UNIVERSEEXTENSION = null;
         this.ORDERNRO = null;
         this.UNIVERSEEXTENSIONNAME = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Universename(final RockFactory rockFact   ,final String VERSIONID ,final String UNIVERSEEXTENSION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Universename> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Universename o = it.next();

              this.VERSIONID = o.getVersionid();
              this.UNIVERSENAME = o.getUniversename();
              this.UNIVERSEEXTENSION = o.getUniverseextension();
              this.ORDERNRO = o.getOrdernro();
              this.UNIVERSEEXTENSIONNAME = o.getUniverseextensionname();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universename");
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
  public Universename(final RockFactory rockFact, final Universename whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Universename> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Universename o = it.next();
                this.VERSIONID = o.getVersionid();
                this.UNIVERSENAME = o.getUniversename();
                this.UNIVERSEEXTENSION = o.getUniverseextension();
                this.ORDERNRO = o.getOrdernro();
                this.UNIVERSEEXTENSIONNAME = o.getUniverseextensionname();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universename");
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
    return "Universename";
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
  public int updateDB(final boolean useTimestamp, final Universename whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Universename whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Universename.saveDB(), no primary key defined");
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
    sbuff.append("<Universename ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("UNIVERSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSENAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSIONNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSIONNAME),12, true)+"\" ");
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
    sbuff.append("<Universename ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("UNIVERSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSENAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSIONNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSIONNAME),12, true)+"\" ");
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
    sbuff.append("</Universename>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Universename ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", UNIVERSENAME");
	    		sbuff.append(", UNIVERSEEXTENSION");
	    		sbuff.append(", ORDERNRO");
	    		sbuff.append(", UNIVERSEEXTENSIONNAME");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ORDERNRO,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSIONNAME,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getUniversename() { 
    return this.UNIVERSENAME;
  }
   public String getUniverseextension() { 
    return this.UNIVERSEEXTENSION;
  }
   public Long getOrdernro() { 
    return this.ORDERNRO;
  }
   public String getUniverseextensionname() { 
    return this.UNIVERSEEXTENSIONNAME;
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
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (UNIVERSENAME == null) {
      UNIVERSENAME = "";
    }
     if (UNIVERSEEXTENSION == null) {
      UNIVERSEEXTENSION = "";
    }
     if (ORDERNRO == null) {
      ORDERNRO = new Long (0);
    }
     if (UNIVERSEEXTENSIONNAME == null) {
      UNIVERSEEXTENSIONNAME = "";
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setUniversename(final String UNIVERSENAME) {
    if (validateData){
      DataValidator.validateData((Object)UNIVERSENAME,"UNIVERSENAME",12,30,0);
    }
    modifiedColumns.add("UNIVERSENAME");
    this.UNIVERSENAME = UNIVERSENAME;
  }
   public void setUniverseextension(final String UNIVERSEEXTENSION) {
    if (validateData){
      DataValidator.validateData((Object)UNIVERSEEXTENSION,"UNIVERSEEXTENSION",12,16,0);
    }
    modifiedColumns.add("UNIVERSEEXTENSION");
    this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
  }
   public void setOrdernro(final Long ORDERNRO) {
    if (validateData){
      DataValidator.validateData((Object)ORDERNRO,"ORDERNRO",2,30,6);
    }
    modifiedColumns.add("ORDERNRO");
    this.ORDERNRO = ORDERNRO;
  }
   public void setUniverseextensionname(final String UNIVERSEEXTENSIONNAME) {
    if (validateData){
      DataValidator.validateData((Object)UNIVERSEEXTENSIONNAME,"UNIVERSEEXTENSIONNAME",12,35,0);
    }
    modifiedColumns.add("UNIVERSEEXTENSIONNAME");
    this.UNIVERSEEXTENSIONNAME = UNIVERSEEXTENSIONNAME;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Universename o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
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
  public boolean equals(final Universename o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.UNIVERSENAME == null) || (o.UNIVERSENAME == null)) && (this.UNIVERSENAME != o.UNIVERSENAME))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
            || (((this.ORDERNRO == null) || (o.ORDERNRO == null)) && (this.ORDERNRO != o.ORDERNRO))
            || (((this.UNIVERSEEXTENSIONNAME == null) || (o.UNIVERSEEXTENSIONNAME == null)) && (this.UNIVERSEEXTENSIONNAME != o.UNIVERSEEXTENSIONNAME))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.UNIVERSENAME != null) && (o.UNIVERSENAME != null)) && (this.UNIVERSENAME.equals(o.UNIVERSENAME) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
            || (((this.ORDERNRO != null) && (o.ORDERNRO != null)) && (this.ORDERNRO.equals(o.ORDERNRO) == false))
            || (((this.UNIVERSEEXTENSIONNAME != null) && (o.UNIVERSEEXTENSIONNAME != null)) && (this.UNIVERSEEXTENSIONNAME.equals(o.UNIVERSEEXTENSIONNAME) == false))
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
  * return 30
  */
  public static int getUniversenameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUniversenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUniversenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getUniverseextensionColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUniverseextensionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUniverseextensionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getOrdernroColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 6
  */
  public static int getOrdernroDecimalDigits() {
    
     return 6;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getOrdernroSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 35
  */
  public static int getUniverseextensionnameColumnSize() {
    
     return 35;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUniverseextensionnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUniverseextensionnameSQLType() {
    
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

  public Universename getOriginal() {
    return original;
  }
   
  public void setOriginal(final Universename original) {
    this.original = (Universename) original.clone();
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
