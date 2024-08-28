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
public class Versioning implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String DESCRIPTION;
    private Long STATUS;
    private String TECHPACK_NAME;
    private String TECHPACK_VERSION;
    private String TECHPACK_TYPE;
    private String PRODUCT_NUMBER;
    private String LOCKEDBY;
    private Timestamp LOCKDATE;
    private String BASEDEFINITION;
    private String BASEVERSION;
    private String INSTALLDESCRIPTION;
    private String UNIVERSENAME;
    private String UNIVERSEEXTENSION;
    private String ENIQ_LEVEL;
    private String LICENSENAME;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Versioning original; 

  public Versioning(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Versioning(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.DESCRIPTION = null;
         this.STATUS = null;
         this.TECHPACK_NAME = null;
         this.TECHPACK_VERSION = null;
         this.TECHPACK_TYPE = null;
         this.PRODUCT_NUMBER = null;
         this.LOCKEDBY = null;
         this.LOCKDATE = null;
         this.BASEDEFINITION = null;
         this.BASEVERSION = null;
         this.INSTALLDESCRIPTION = null;
         this.UNIVERSENAME = null;
         this.UNIVERSEEXTENSION = null;
         this.ENIQ_LEVEL = null;
         this.LICENSENAME = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Versioning(final RockFactory rockFact   ,final String VERSIONID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Versioning> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Versioning o = it.next();

              this.VERSIONID = o.getVersionid();
              this.DESCRIPTION = o.getDescription();
              this.STATUS = o.getStatus();
              this.TECHPACK_NAME = o.getTechpack_name();
              this.TECHPACK_VERSION = o.getTechpack_version();
              this.TECHPACK_TYPE = o.getTechpack_type();
              this.PRODUCT_NUMBER = o.getProduct_number();
              this.LOCKEDBY = o.getLockedby();
              this.LOCKDATE = o.getLockdate();
              this.BASEDEFINITION = o.getBasedefinition();
              this.BASEVERSION = o.getBaseversion();
              this.INSTALLDESCRIPTION = o.getInstalldescription();
              this.UNIVERSENAME = o.getUniversename();
              this.UNIVERSEEXTENSION = o.getUniverseextension();
              this.ENIQ_LEVEL = o.getEniq_level();
              this.LICENSENAME = o.getLicensename();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Versioning");
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
  public Versioning(final RockFactory rockFact, final Versioning whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Versioning> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Versioning o = it.next();
                this.VERSIONID = o.getVersionid();
                this.DESCRIPTION = o.getDescription();
                this.STATUS = o.getStatus();
                this.TECHPACK_NAME = o.getTechpack_name();
                this.TECHPACK_VERSION = o.getTechpack_version();
                this.TECHPACK_TYPE = o.getTechpack_type();
                this.PRODUCT_NUMBER = o.getProduct_number();
                this.LOCKEDBY = o.getLockedby();
                this.LOCKDATE = o.getLockdate();
                this.BASEDEFINITION = o.getBasedefinition();
                this.BASEVERSION = o.getBaseversion();
                this.INSTALLDESCRIPTION = o.getInstalldescription();
                this.UNIVERSENAME = o.getUniversename();
                this.UNIVERSEEXTENSION = o.getUniverseextension();
                this.ENIQ_LEVEL = o.getEniq_level();
                this.LICENSENAME = o.getLicensename();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Versioning");
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
    return "Versioning";
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
  public int updateDB(final boolean useTimestamp, final Versioning whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Versioning whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Versioning.saveDB(), no primary key defined");
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
    sbuff.append("<Versioning ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("TECHPACK_VERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_VERSION),12, true)+"\" ");
        sbuff.append("TECHPACK_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_TYPE),12, true)+"\" ");
        sbuff.append("PRODUCT_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRODUCT_NUMBER),12, true)+"\" ");
        sbuff.append("LOCKEDBY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKEDBY),12, true)+"\" ");
        sbuff.append("LOCKDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKDATE),93, true)+"\" ");
        sbuff.append("BASEDEFINITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEFINITION),12, true)+"\" ");
        sbuff.append("BASEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEVERSION),12, true)+"\" ");
        sbuff.append("INSTALLDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INSTALLDESCRIPTION),12, true)+"\" ");
        sbuff.append("UNIVERSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSENAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("ENIQ_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENIQ_LEVEL),12, true)+"\" ");
        sbuff.append("LICENSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LICENSENAME),12, true)+"\" ");
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
    sbuff.append("<Versioning ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("TECHPACK_VERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_VERSION),12, true)+"\" ");
        sbuff.append("TECHPACK_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_TYPE),12, true)+"\" ");
        sbuff.append("PRODUCT_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRODUCT_NUMBER),12, true)+"\" ");
        sbuff.append("LOCKEDBY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKEDBY),12, true)+"\" ");
        sbuff.append("LOCKDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKDATE),93, true)+"\" ");
        sbuff.append("BASEDEFINITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEFINITION),12, true)+"\" ");
        sbuff.append("BASEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEVERSION),12, true)+"\" ");
        sbuff.append("INSTALLDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INSTALLDESCRIPTION),12, true)+"\" ");
        sbuff.append("UNIVERSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSENAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("ENIQ_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENIQ_LEVEL),12, true)+"\" ");
        sbuff.append("LICENSENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LICENSENAME),12, true)+"\" ");
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
    sbuff.append("</Versioning>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Versioning ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", TECHPACK_NAME");
	    		sbuff.append(", TECHPACK_VERSION");
	    		sbuff.append(", TECHPACK_TYPE");
	    		sbuff.append(", PRODUCT_NUMBER");
	    		sbuff.append(", LOCKEDBY");
	    		sbuff.append(", LOCKDATE");
	    		sbuff.append(", BASEDEFINITION");
	    		sbuff.append(", BASEVERSION");
	    		sbuff.append(", INSTALLDESCRIPTION");
	    		sbuff.append(", UNIVERSENAME");
	    		sbuff.append(", UNIVERSEEXTENSION");
	    		sbuff.append(", ENIQ_LEVEL");
	    		sbuff.append(", LICENSENAME");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACK_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACK_VERSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACK_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PRODUCT_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOCKEDBY,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOCKDATE,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASEDEFINITION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASEVERSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INSTALLDESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENIQ_LEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LICENSENAME,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public Long getStatus() { 
    return this.STATUS;
  }
   public String getTechpack_name() { 
    return this.TECHPACK_NAME;
  }
   public String getTechpack_version() { 
    return this.TECHPACK_VERSION;
  }
   public String getTechpack_type() { 
    return this.TECHPACK_TYPE;
  }
   public String getProduct_number() { 
    return this.PRODUCT_NUMBER;
  }
   public String getLockedby() { 
    return this.LOCKEDBY;
  }
   public Timestamp getLockdate() { 
    return this.LOCKDATE;
  }
   public String getBasedefinition() { 
    return this.BASEDEFINITION;
  }
   public String getBaseversion() { 
    return this.BASEVERSION;
  }
   public String getInstalldescription() { 
    return this.INSTALLDESCRIPTION;
  }
   public String getUniversename() { 
    return this.UNIVERSENAME;
  }
   public String getUniverseextension() { 
    return this.UNIVERSEEXTENSION;
  }
   public String getEniq_level() { 
    return this.ENIQ_LEVEL;
  }
   public String getLicensename() { 
    return this.LICENSENAME;
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
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (STATUS == null) {
      STATUS = new Long (0);
    }
     if (TECHPACK_NAME == null) {
      TECHPACK_NAME = "";
    }
     if (TECHPACK_VERSION == null) {
      TECHPACK_VERSION = "";
    }
     if (TECHPACK_TYPE == null) {
      TECHPACK_TYPE = "";
    }
     if (PRODUCT_NUMBER == null) {
      PRODUCT_NUMBER = "";
    }
     if (LOCKEDBY == null) {
      LOCKEDBY = "";
    }
     if (LOCKDATE == null) {
      LOCKDATE = new Timestamp (0);
    }
     if (BASEDEFINITION == null) {
      BASEDEFINITION = "";
    }
     if (BASEVERSION == null) {
      BASEVERSION = "";
    }
     if (INSTALLDESCRIPTION == null) {
      INSTALLDESCRIPTION = "";
    }
     if (UNIVERSENAME == null) {
      UNIVERSENAME = "";
    }
     if (UNIVERSEEXTENSION == null) {
      UNIVERSEEXTENSION = "";
    }
     if (ENIQ_LEVEL == null) {
      ENIQ_LEVEL = "";
    }
     if (LICENSENAME == null) {
      LICENSENAME = "";
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,50,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setStatus(final Long STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",2,9,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setTechpack_name(final String TECHPACK_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_NAME,"TECHPACK_NAME",12,30,0);
    }
    modifiedColumns.add("TECHPACK_NAME");
    this.TECHPACK_NAME = TECHPACK_NAME;
  }
   public void setTechpack_version(final String TECHPACK_VERSION) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_VERSION,"TECHPACK_VERSION",12,32,0);
    }
    modifiedColumns.add("TECHPACK_VERSION");
    this.TECHPACK_VERSION = TECHPACK_VERSION;
  }
   public void setTechpack_type(final String TECHPACK_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_TYPE,"TECHPACK_TYPE",12,10,0);
    }
    modifiedColumns.add("TECHPACK_TYPE");
    this.TECHPACK_TYPE = TECHPACK_TYPE;
  }
   public void setProduct_number(final String PRODUCT_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)PRODUCT_NUMBER,"PRODUCT_NUMBER",12,255,0);
    }
    modifiedColumns.add("PRODUCT_NUMBER");
    this.PRODUCT_NUMBER = PRODUCT_NUMBER;
  }
   public void setLockedby(final String LOCKEDBY) {
    if (validateData){
      DataValidator.validateData((Object)LOCKEDBY,"LOCKEDBY",12,255,0);
    }
    modifiedColumns.add("LOCKEDBY");
    this.LOCKEDBY = LOCKEDBY;
  }
   public void setLockdate(final Timestamp LOCKDATE) {
    if (validateData){
      DataValidator.validateData((Object)LOCKDATE,"LOCKDATE",93,23,0);
    }
    modifiedColumns.add("LOCKDATE");
    this.LOCKDATE = LOCKDATE;
  }
   public void setBasedefinition(final String BASEDEFINITION) {
    if (validateData){
      DataValidator.validateData((Object)BASEDEFINITION,"BASEDEFINITION",12,128,0);
    }
    modifiedColumns.add("BASEDEFINITION");
    this.BASEDEFINITION = BASEDEFINITION;
  }
   public void setBaseversion(final String BASEVERSION) {
    if (validateData){
      DataValidator.validateData((Object)BASEVERSION,"BASEVERSION",12,16,0);
    }
    modifiedColumns.add("BASEVERSION");
    this.BASEVERSION = BASEVERSION;
  }
   public void setInstalldescription(final String INSTALLDESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)INSTALLDESCRIPTION,"INSTALLDESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("INSTALLDESCRIPTION");
    this.INSTALLDESCRIPTION = INSTALLDESCRIPTION;
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
   public void setEniq_level(final String ENIQ_LEVEL) {
    if (validateData){
      DataValidator.validateData((Object)ENIQ_LEVEL,"ENIQ_LEVEL",12,12,0);
    }
    modifiedColumns.add("ENIQ_LEVEL");
    this.ENIQ_LEVEL = ENIQ_LEVEL;
  }
   public void setLicensename(final String LICENSENAME) {
    if (validateData){
      DataValidator.validateData((Object)LICENSENAME,"LICENSENAME",12,1023,0);
    }
    modifiedColumns.add("LICENSENAME");
    this.LICENSENAME = LICENSENAME;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Versioning o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
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
  public boolean equals(final Versioning o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.TECHPACK_VERSION == null) || (o.TECHPACK_VERSION == null)) && (this.TECHPACK_VERSION != o.TECHPACK_VERSION))
            || (((this.TECHPACK_TYPE == null) || (o.TECHPACK_TYPE == null)) && (this.TECHPACK_TYPE != o.TECHPACK_TYPE))
            || (((this.PRODUCT_NUMBER == null) || (o.PRODUCT_NUMBER == null)) && (this.PRODUCT_NUMBER != o.PRODUCT_NUMBER))
            || (((this.LOCKEDBY == null) || (o.LOCKEDBY == null)) && (this.LOCKEDBY != o.LOCKEDBY))
            || (((this.LOCKDATE == null) || (o.LOCKDATE == null)) && (this.LOCKDATE != o.LOCKDATE))
            || (((this.BASEDEFINITION == null) || (o.BASEDEFINITION == null)) && (this.BASEDEFINITION != o.BASEDEFINITION))
            || (((this.BASEVERSION == null) || (o.BASEVERSION == null)) && (this.BASEVERSION != o.BASEVERSION))
            || (((this.INSTALLDESCRIPTION == null) || (o.INSTALLDESCRIPTION == null)) && (this.INSTALLDESCRIPTION != o.INSTALLDESCRIPTION))
            || (((this.UNIVERSENAME == null) || (o.UNIVERSENAME == null)) && (this.UNIVERSENAME != o.UNIVERSENAME))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
            || (((this.ENIQ_LEVEL == null) || (o.ENIQ_LEVEL == null)) && (this.ENIQ_LEVEL != o.ENIQ_LEVEL))
            || (((this.LICENSENAME == null) || (o.LICENSENAME == null)) && (this.LICENSENAME != o.LICENSENAME))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.TECHPACK_VERSION != null) && (o.TECHPACK_VERSION != null)) && (this.TECHPACK_VERSION.equals(o.TECHPACK_VERSION) == false))
            || (((this.TECHPACK_TYPE != null) && (o.TECHPACK_TYPE != null)) && (this.TECHPACK_TYPE.equals(o.TECHPACK_TYPE) == false))
            || (((this.PRODUCT_NUMBER != null) && (o.PRODUCT_NUMBER != null)) && (this.PRODUCT_NUMBER.equals(o.PRODUCT_NUMBER) == false))
            || (((this.LOCKEDBY != null) && (o.LOCKEDBY != null)) && (this.LOCKEDBY.equals(o.LOCKEDBY) == false))
            || (((this.LOCKDATE != null) && (o.LOCKDATE != null)) && (this.LOCKDATE.equals(o.LOCKDATE) == false))
            || (((this.BASEDEFINITION != null) && (o.BASEDEFINITION != null)) && (this.BASEDEFINITION.equals(o.BASEDEFINITION) == false))
            || (((this.BASEVERSION != null) && (o.BASEVERSION != null)) && (this.BASEVERSION.equals(o.BASEVERSION) == false))
            || (((this.INSTALLDESCRIPTION != null) && (o.INSTALLDESCRIPTION != null)) && (this.INSTALLDESCRIPTION.equals(o.INSTALLDESCRIPTION) == false))
            || (((this.UNIVERSENAME != null) && (o.UNIVERSENAME != null)) && (this.UNIVERSENAME.equals(o.UNIVERSENAME) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
            || (((this.ENIQ_LEVEL != null) && (o.ENIQ_LEVEL != null)) && (this.ENIQ_LEVEL.equals(o.ENIQ_LEVEL) == false))
            || (((this.LICENSENAME != null) && (o.LICENSENAME != null)) && (this.LICENSENAME.equals(o.LICENSENAME) == false))
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
  * return 50
  */
  public static int getDescriptionColumnSize() {
    
     return 50;   
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
  * return 9
  */
  public static int getStatusColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStatusDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getStatusSQLType() {
    
    return 2;   
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
  * return 32
  */
  public static int getTechpack_versionColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpack_versionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpack_versionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getTechpack_typeColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpack_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpack_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getProduct_numberColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getProduct_numberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getProduct_numberSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getLockedbyColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLockedbyDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getLockedbySQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getLockdateColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLockdateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getLockdateSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getBasedefinitionColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBasedefinitionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBasedefinitionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getBaseversionColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBaseversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBaseversionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getInstalldescriptionColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInstalldescriptionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInstalldescriptionSQLType() {
    
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
  * return 12
  */
  public static int getEniq_levelColumnSize() {
    
     return 12;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEniq_levelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getEniq_levelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getLicensenameColumnSize() {
    
     return 1023;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLicensenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getLicensenameSQLType() {
    
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

  public Versioning getOriginal() {
    return original;
  }
   
  public void setOriginal(final Versioning original) {
    this.original = (Versioning) original.clone();
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
