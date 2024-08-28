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
public class Busyhourmapping implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String BHLEVEL;
    private String BHTYPE;
    private String TARGETVERSIONID;
    private String BHOBJECT;
    private String TYPEID;
    private String BHTARGETTYPE;
    private String BHTARGETLEVEL;
    private Integer ENABLE;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"BHLEVEL"    ,"BHTYPE"    ,"TARGETVERSIONID"    ,"BHOBJECT"    ,"TYPEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Busyhourmapping original; 

  public Busyhourmapping(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Busyhourmapping(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.BHLEVEL = null;
         this.BHTYPE = null;
         this.TARGETVERSIONID = null;
         this.BHOBJECT = null;
         this.TYPEID = null;
         this.BHTARGETTYPE = null;
         this.BHTARGETLEVEL = null;
         this.ENABLE = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Busyhourmapping(final RockFactory rockFact   ,final String VERSIONID ,final String BHLEVEL ,final String BHTYPE ,final String TARGETVERSIONID ,final String BHOBJECT ,final String TYPEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.BHLEVEL = BHLEVEL;
            this.BHTYPE = BHTYPE;
            this.TARGETVERSIONID = TARGETVERSIONID;
            this.BHOBJECT = BHOBJECT;
            this.TYPEID = TYPEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Busyhourmapping> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Busyhourmapping o = it.next();

              this.VERSIONID = o.getVersionid();
              this.BHLEVEL = o.getBhlevel();
              this.BHTYPE = o.getBhtype();
              this.TARGETVERSIONID = o.getTargetversionid();
              this.BHOBJECT = o.getBhobject();
              this.TYPEID = o.getTypeid();
              this.BHTARGETTYPE = o.getBhtargettype();
              this.BHTARGETLEVEL = o.getBhtargetlevel();
              this.ENABLE = o.getEnable();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Busyhourmapping");
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
  public Busyhourmapping(final RockFactory rockFact, final Busyhourmapping whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Busyhourmapping> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Busyhourmapping o = it.next();
                this.VERSIONID = o.getVersionid();
                this.BHLEVEL = o.getBhlevel();
                this.BHTYPE = o.getBhtype();
                this.TARGETVERSIONID = o.getTargetversionid();
                this.BHOBJECT = o.getBhobject();
                this.TYPEID = o.getTypeid();
                this.BHTARGETTYPE = o.getBhtargettype();
                this.BHTARGETLEVEL = o.getBhtargetlevel();
                this.ENABLE = o.getEnable();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Busyhourmapping");
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
    return "Busyhourmapping";
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
  public int updateDB(final boolean useTimestamp, final Busyhourmapping whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Busyhourmapping whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Busyhourmapping.saveDB(), no primary key defined");
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
    sbuff.append("<Busyhourmapping ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("BHLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHLEVEL),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
        sbuff.append("TARGETVERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETVERSIONID),12, true)+"\" ");
        sbuff.append("BHOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHOBJECT),12, true)+"\" ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("BHTARGETTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTARGETTYPE),12, true)+"\" ");
        sbuff.append("BHTARGETLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTARGETLEVEL),12, true)+"\" ");
        sbuff.append("ENABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLE),4, true)+"\" ");
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
    sbuff.append("<Busyhourmapping ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("BHLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHLEVEL),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
        sbuff.append("TARGETVERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETVERSIONID),12, true)+"\" ");
        sbuff.append("BHOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHOBJECT),12, true)+"\" ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("BHTARGETTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTARGETTYPE),12, true)+"\" ");
        sbuff.append("BHTARGETLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTARGETLEVEL),12, true)+"\" ");
        sbuff.append("ENABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLE),4, true)+"\" ");
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
    sbuff.append("</Busyhourmapping>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Busyhourmapping ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", BHLEVEL");
	    		sbuff.append(", BHTYPE");
	    		sbuff.append(", TARGETVERSIONID");
	    		sbuff.append(", BHOBJECT");
	    		sbuff.append(", TYPEID");
	    		sbuff.append(", BHTARGETTYPE");
	    		sbuff.append(", BHTARGETLEVEL");
	    		sbuff.append(", ENABLE");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.BHLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGETVERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHOBJECT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPEID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHTARGETTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHTARGETLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENABLE,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getBhlevel() { 
    return this.BHLEVEL;
  }
   public String getBhtype() { 
    return this.BHTYPE;
  }
   public String getTargetversionid() { 
    return this.TARGETVERSIONID;
  }
   public String getBhobject() { 
    return this.BHOBJECT;
  }
   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getBhtargettype() { 
    return this.BHTARGETTYPE;
  }
   public String getBhtargetlevel() { 
    return this.BHTARGETLEVEL;
  }
   public Integer getEnable() { 
    return this.ENABLE;
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
     if (BHLEVEL == null) {
      BHLEVEL = "";
    }
     if (BHTYPE == null) {
      BHTYPE = "";
    }
     if (TARGETVERSIONID == null) {
      TARGETVERSIONID = "";
    }
     if (BHOBJECT == null) {
      BHOBJECT = "";
    }
     if (TYPEID == null) {
      TYPEID = "";
    }
     if (BHTARGETTYPE == null) {
      BHTARGETTYPE = "";
    }
     if (BHTARGETLEVEL == null) {
      BHTARGETLEVEL = "";
    }
     if (ENABLE == null) {
      ENABLE = new Integer (0);
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setBhlevel(final String BHLEVEL) {
    if (validateData){
      DataValidator.validateData((Object)BHLEVEL,"BHLEVEL",12,255,0);
    }
    modifiedColumns.add("BHLEVEL");
    this.BHLEVEL = BHLEVEL;
  }
   public void setBhtype(final String BHTYPE) {
    if (validateData){
      DataValidator.validateData((Object)BHTYPE,"BHTYPE",12,32,0);
    }
    modifiedColumns.add("BHTYPE");
    this.BHTYPE = BHTYPE;
  }
   public void setTargetversionid(final String TARGETVERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)TARGETVERSIONID,"TARGETVERSIONID",12,128,0);
    }
    modifiedColumns.add("TARGETVERSIONID");
    this.TARGETVERSIONID = TARGETVERSIONID;
  }
   public void setBhobject(final String BHOBJECT) {
    if (validateData){
      DataValidator.validateData((Object)BHOBJECT,"BHOBJECT",12,32,0);
    }
    modifiedColumns.add("BHOBJECT");
    this.BHOBJECT = BHOBJECT;
  }
   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setBhtargettype(final String BHTARGETTYPE) {
    if (validateData){
      DataValidator.validateData((Object)BHTARGETTYPE,"BHTARGETTYPE",12,128,0);
    }
    modifiedColumns.add("BHTARGETTYPE");
    this.BHTARGETTYPE = BHTARGETTYPE;
  }
   public void setBhtargetlevel(final String BHTARGETLEVEL) {
    if (validateData){
      DataValidator.validateData((Object)BHTARGETLEVEL,"BHTARGETLEVEL",12,128,0);
    }
    modifiedColumns.add("BHTARGETLEVEL");
    this.BHTARGETLEVEL = BHTARGETLEVEL;
  }
   public void setEnable(final Integer ENABLE) {
    if (validateData){
      DataValidator.validateData((Object)ENABLE,"ENABLE",4,10,0);
    }
    modifiedColumns.add("ENABLE");
    this.ENABLE = ENABLE;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Busyhourmapping o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.BHLEVEL == null) || (o.BHLEVEL == null)) && (this.BHLEVEL != o.BHLEVEL))
            || (((this.BHTYPE == null) || (o.BHTYPE == null)) && (this.BHTYPE != o.BHTYPE))
            || (((this.TARGETVERSIONID == null) || (o.TARGETVERSIONID == null)) && (this.TARGETVERSIONID != o.TARGETVERSIONID))
            || (((this.BHOBJECT == null) || (o.BHOBJECT == null)) && (this.BHOBJECT != o.BHOBJECT))
            || (((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.BHLEVEL != null) && (o.BHLEVEL != null)) && (this.BHLEVEL.equals(o.BHLEVEL) == false))
            || (((this.BHTYPE != null) && (o.BHTYPE != null)) && (this.BHTYPE.equals(o.BHTYPE) == false))
            || (((this.TARGETVERSIONID != null) && (o.TARGETVERSIONID != null)) && (this.TARGETVERSIONID.equals(o.TARGETVERSIONID) == false))
            || (((this.BHOBJECT != null) && (o.BHOBJECT != null)) && (this.BHOBJECT.equals(o.BHOBJECT) == false))
            || (((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
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
  public boolean equals(final Busyhourmapping o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.BHLEVEL == null) || (o.BHLEVEL == null)) && (this.BHLEVEL != o.BHLEVEL))
            || (((this.BHTYPE == null) || (o.BHTYPE == null)) && (this.BHTYPE != o.BHTYPE))
            || (((this.TARGETVERSIONID == null) || (o.TARGETVERSIONID == null)) && (this.TARGETVERSIONID != o.TARGETVERSIONID))
            || (((this.BHOBJECT == null) || (o.BHOBJECT == null)) && (this.BHOBJECT != o.BHOBJECT))
            || (((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.BHTARGETTYPE == null) || (o.BHTARGETTYPE == null)) && (this.BHTARGETTYPE != o.BHTARGETTYPE))
            || (((this.BHTARGETLEVEL == null) || (o.BHTARGETLEVEL == null)) && (this.BHTARGETLEVEL != o.BHTARGETLEVEL))
            || (((this.ENABLE == null) || (o.ENABLE == null)) && (this.ENABLE != o.ENABLE))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.BHLEVEL != null) && (o.BHLEVEL != null)) && (this.BHLEVEL.equals(o.BHLEVEL) == false))
            || (((this.BHTYPE != null) && (o.BHTYPE != null)) && (this.BHTYPE.equals(o.BHTYPE) == false))
            || (((this.TARGETVERSIONID != null) && (o.TARGETVERSIONID != null)) && (this.TARGETVERSIONID.equals(o.TARGETVERSIONID) == false))
            || (((this.BHOBJECT != null) && (o.BHOBJECT != null)) && (this.BHOBJECT.equals(o.BHOBJECT) == false))
            || (((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.BHTARGETTYPE != null) && (o.BHTARGETTYPE != null)) && (this.BHTARGETTYPE.equals(o.BHTARGETTYPE) == false))
            || (((this.BHTARGETLEVEL != null) && (o.BHTARGETLEVEL != null)) && (this.BHTARGETLEVEL.equals(o.BHTARGETLEVEL) == false))
            || (((this.ENABLE != null) && (o.ENABLE != null)) && (this.ENABLE.equals(o.ENABLE) == false))
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
  * return 255
  */
  public static int getBhlevelColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhlevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhlevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getBhtypeColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhtypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhtypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getTargetversionidColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTargetversionidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTargetversionidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getBhobjectColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhobjectDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhobjectSQLType() {
    
    return 12;   
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
  * return 128
  */
  public static int getBhtargettypeColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhtargettypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhtargettypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getBhtargetlevelColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhtargetlevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhtargetlevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getEnableColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEnableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getEnableSQLType() {
    
    return 4;   
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

  public Busyhourmapping getOriginal() {
    return original;
  }
   
  public void setOriginal(final Busyhourmapping original) {
    this.original = (Busyhourmapping) original.clone();
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
