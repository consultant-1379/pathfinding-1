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
public class Datainterface implements Cloneable,RockDBObject  {

    private String INTERFACENAME;
    private Long STATUS;
    private String INTERFACETYPE;
    private String DESCRIPTION;
    private String DATAFORMATTYPE;
    private String INTERFACEVERSION;
    private String LOCKEDBY;
    private Timestamp LOCKDATE;
    private String PRODUCTNUMBER;
    private String ENIQ_LEVEL;
    private String RSTATE;
    private String INSTALLDESCRIPTION;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "INTERFACENAME"    ,"INTERFACEVERSION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Datainterface original; 

  public Datainterface(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Datainterface(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.INTERFACENAME = null;
         this.STATUS = null;
         this.INTERFACETYPE = null;
         this.DESCRIPTION = null;
         this.DATAFORMATTYPE = null;
         this.INTERFACEVERSION = null;
         this.LOCKEDBY = null;
         this.LOCKDATE = null;
         this.PRODUCTNUMBER = null;
         this.ENIQ_LEVEL = null;
         this.RSTATE = null;
         this.INSTALLDESCRIPTION = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Datainterface(final RockFactory rockFact   ,final String INTERFACENAME ,final String INTERFACEVERSION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.INTERFACENAME = INTERFACENAME;
            this.INTERFACEVERSION = INTERFACEVERSION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Datainterface> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Datainterface o = it.next();

              this.INTERFACENAME = o.getInterfacename();
              this.STATUS = o.getStatus();
              this.INTERFACETYPE = o.getInterfacetype();
              this.DESCRIPTION = o.getDescription();
              this.DATAFORMATTYPE = o.getDataformattype();
              this.INTERFACEVERSION = o.getInterfaceversion();
              this.LOCKEDBY = o.getLockedby();
              this.LOCKDATE = o.getLockdate();
              this.PRODUCTNUMBER = o.getProductnumber();
              this.ENIQ_LEVEL = o.getEniq_level();
              this.RSTATE = o.getRstate();
              this.INSTALLDESCRIPTION = o.getInstalldescription();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Datainterface");
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
  public Datainterface(final RockFactory rockFact, final Datainterface whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Datainterface> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Datainterface o = it.next();
                this.INTERFACENAME = o.getInterfacename();
                this.STATUS = o.getStatus();
                this.INTERFACETYPE = o.getInterfacetype();
                this.DESCRIPTION = o.getDescription();
                this.DATAFORMATTYPE = o.getDataformattype();
                this.INTERFACEVERSION = o.getInterfaceversion();
                this.LOCKEDBY = o.getLockedby();
                this.LOCKDATE = o.getLockdate();
                this.PRODUCTNUMBER = o.getProductnumber();
                this.ENIQ_LEVEL = o.getEniq_level();
                this.RSTATE = o.getRstate();
                this.INSTALLDESCRIPTION = o.getInstalldescription();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Datainterface");
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
    return "Datainterface";
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
  public int updateDB(final boolean useTimestamp, final Datainterface whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Datainterface whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Datainterface.saveDB(), no primary key defined");
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
    sbuff.append("<Datainterface ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("INTERFACETYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACETYPE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("DATAFORMATTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATTYPE),12, true)+"\" ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
        sbuff.append("LOCKEDBY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKEDBY),12, true)+"\" ");
        sbuff.append("LOCKDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKDATE),93, true)+"\" ");
        sbuff.append("PRODUCTNUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRODUCTNUMBER),12, true)+"\" ");
        sbuff.append("ENIQ_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENIQ_LEVEL),12, true)+"\" ");
        sbuff.append("RSTATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RSTATE),12, true)+"\" ");
        sbuff.append("INSTALLDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INSTALLDESCRIPTION),12, true)+"\" ");
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
    sbuff.append("<Datainterface ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("INTERFACETYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACETYPE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("DATAFORMATTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATTYPE),12, true)+"\" ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
        sbuff.append("LOCKEDBY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKEDBY),12, true)+"\" ");
        sbuff.append("LOCKDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOCKDATE),93, true)+"\" ");
        sbuff.append("PRODUCTNUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRODUCTNUMBER),12, true)+"\" ");
        sbuff.append("ENIQ_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENIQ_LEVEL),12, true)+"\" ");
        sbuff.append("RSTATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RSTATE),12, true)+"\" ");
        sbuff.append("INSTALLDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INSTALLDESCRIPTION),12, true)+"\" ");
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
    sbuff.append("</Datainterface>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Datainterface ( ");
	    		sbuff.append("INTERFACENAME");
		    		sbuff.append(", STATUS");
	    		sbuff.append(", INTERFACETYPE");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", DATAFORMATTYPE");
	    		sbuff.append(", INTERFACEVERSION");
	    		sbuff.append(", LOCKEDBY");
	    		sbuff.append(", LOCKDATE");
	    		sbuff.append(", PRODUCTNUMBER");
	    		sbuff.append(", ENIQ_LEVEL");
	    		sbuff.append(", RSTATE");
	    		sbuff.append(", INSTALLDESCRIPTION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.INTERFACENAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERFACETYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATAFORMATTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERFACEVERSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOCKEDBY,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOCKDATE,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PRODUCTNUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENIQ_LEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.RSTATE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INSTALLDESCRIPTION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getInterfacename() { 
    return this.INTERFACENAME;
  }
   public Long getStatus() { 
    return this.STATUS;
  }
   public String getInterfacetype() { 
    return this.INTERFACETYPE;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getDataformattype() { 
    return this.DATAFORMATTYPE;
  }
   public String getInterfaceversion() { 
    return this.INTERFACEVERSION;
  }
   public String getLockedby() { 
    return this.LOCKEDBY;
  }
   public Timestamp getLockdate() { 
    return this.LOCKDATE;
  }
   public String getProductnumber() { 
    return this.PRODUCTNUMBER;
  }
   public String getEniq_level() { 
    return this.ENIQ_LEVEL;
  }
   public String getRstate() { 
    return this.RSTATE;
  }
   public String getInstalldescription() { 
    return this.INSTALLDESCRIPTION;
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
     if (INTERFACENAME == null) {
      INTERFACENAME = "";
    }
     if (STATUS == null) {
      STATUS = new Long (0);
    }
     if (INTERFACETYPE == null) {
      INTERFACETYPE = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (DATAFORMATTYPE == null) {
      DATAFORMATTYPE = "";
    }
     if (INTERFACEVERSION == null) {
      INTERFACEVERSION = "";
    }
     if (LOCKEDBY == null) {
      LOCKEDBY = "";
    }
     if (LOCKDATE == null) {
      LOCKDATE = new Timestamp (0);
    }
     if (PRODUCTNUMBER == null) {
      PRODUCTNUMBER = "";
    }
     if (ENIQ_LEVEL == null) {
      ENIQ_LEVEL = "";
    }
     if (RSTATE == null) {
      RSTATE = "";
    }
     if (INSTALLDESCRIPTION == null) {
      INSTALLDESCRIPTION = "";
    }
   }

   public void setInterfacename(final String INTERFACENAME) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACENAME,"INTERFACENAME",12,255,0);
    }
    modifiedColumns.add("INTERFACENAME");
    this.INTERFACENAME = INTERFACENAME;
  }
   public void setStatus(final Long STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",2,9,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setInterfacetype(final String INTERFACETYPE) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACETYPE,"INTERFACETYPE",12,50,0);
    }
    modifiedColumns.add("INTERFACETYPE");
    this.INTERFACETYPE = INTERFACETYPE;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setDataformattype(final String DATAFORMATTYPE) {
    if (validateData){
      DataValidator.validateData((Object)DATAFORMATTYPE,"DATAFORMATTYPE",12,50,0);
    }
    modifiedColumns.add("DATAFORMATTYPE");
    this.DATAFORMATTYPE = DATAFORMATTYPE;
  }
   public void setInterfaceversion(final String INTERFACEVERSION) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACEVERSION,"INTERFACEVERSION",12,32,0);
    }
    modifiedColumns.add("INTERFACEVERSION");
    this.INTERFACEVERSION = INTERFACEVERSION;
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
   public void setProductnumber(final String PRODUCTNUMBER) {
    if (validateData){
      DataValidator.validateData((Object)PRODUCTNUMBER,"PRODUCTNUMBER",12,255,0);
    }
    modifiedColumns.add("PRODUCTNUMBER");
    this.PRODUCTNUMBER = PRODUCTNUMBER;
  }
   public void setEniq_level(final String ENIQ_LEVEL) {
    if (validateData){
      DataValidator.validateData((Object)ENIQ_LEVEL,"ENIQ_LEVEL",12,12,0);
    }
    modifiedColumns.add("ENIQ_LEVEL");
    this.ENIQ_LEVEL = ENIQ_LEVEL;
  }
   public void setRstate(final String RSTATE) {
    if (validateData){
      DataValidator.validateData((Object)RSTATE,"RSTATE",12,255,0);
    }
    modifiedColumns.add("RSTATE");
    this.RSTATE = RSTATE;
  }
   public void setInstalldescription(final String INSTALLDESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)INSTALLDESCRIPTION,"INSTALLDESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("INSTALLDESCRIPTION");
    this.INSTALLDESCRIPTION = INSTALLDESCRIPTION;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Datainterface o) {

         if ((((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
          ){
    return false;
    } else
         if ((((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
            || (((this.INTERFACEVERSION != null) && (o.INTERFACEVERSION != null)) && (this.INTERFACEVERSION.equals(o.INTERFACEVERSION) == false))
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
  public boolean equals(final Datainterface o) {

         if ((((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.INTERFACETYPE == null) || (o.INTERFACETYPE == null)) && (this.INTERFACETYPE != o.INTERFACETYPE))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.DATAFORMATTYPE == null) || (o.DATAFORMATTYPE == null)) && (this.DATAFORMATTYPE != o.DATAFORMATTYPE))
            || (((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
            || (((this.LOCKEDBY == null) || (o.LOCKEDBY == null)) && (this.LOCKEDBY != o.LOCKEDBY))
            || (((this.LOCKDATE == null) || (o.LOCKDATE == null)) && (this.LOCKDATE != o.LOCKDATE))
            || (((this.PRODUCTNUMBER == null) || (o.PRODUCTNUMBER == null)) && (this.PRODUCTNUMBER != o.PRODUCTNUMBER))
            || (((this.ENIQ_LEVEL == null) || (o.ENIQ_LEVEL == null)) && (this.ENIQ_LEVEL != o.ENIQ_LEVEL))
            || (((this.RSTATE == null) || (o.RSTATE == null)) && (this.RSTATE != o.RSTATE))
            || (((this.INSTALLDESCRIPTION == null) || (o.INSTALLDESCRIPTION == null)) && (this.INSTALLDESCRIPTION != o.INSTALLDESCRIPTION))
          ){
    return false;
    } else
         if ((((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.INTERFACETYPE != null) && (o.INTERFACETYPE != null)) && (this.INTERFACETYPE.equals(o.INTERFACETYPE) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.DATAFORMATTYPE != null) && (o.DATAFORMATTYPE != null)) && (this.DATAFORMATTYPE.equals(o.DATAFORMATTYPE) == false))
            || (((this.INTERFACEVERSION != null) && (o.INTERFACEVERSION != null)) && (this.INTERFACEVERSION.equals(o.INTERFACEVERSION) == false))
            || (((this.LOCKEDBY != null) && (o.LOCKEDBY != null)) && (this.LOCKEDBY.equals(o.LOCKEDBY) == false))
            || (((this.LOCKDATE != null) && (o.LOCKDATE != null)) && (this.LOCKDATE.equals(o.LOCKDATE) == false))
            || (((this.PRODUCTNUMBER != null) && (o.PRODUCTNUMBER != null)) && (this.PRODUCTNUMBER.equals(o.PRODUCTNUMBER) == false))
            || (((this.ENIQ_LEVEL != null) && (o.ENIQ_LEVEL != null)) && (this.ENIQ_LEVEL.equals(o.ENIQ_LEVEL) == false))
            || (((this.RSTATE != null) && (o.RSTATE != null)) && (this.RSTATE.equals(o.RSTATE) == false))
            || (((this.INSTALLDESCRIPTION != null) && (o.INSTALLDESCRIPTION != null)) && (this.INSTALLDESCRIPTION.equals(o.INSTALLDESCRIPTION) == false))
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
  public static int getInterfacenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfacenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfacenameSQLType() {
    
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
  * return 50
  */
  public static int getInterfacetypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfacetypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfacetypeSQLType() {
    
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
  * return 50
  */
  public static int getDataformattypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDataformattypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDataformattypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getInterfaceversionColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfaceversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfaceversionSQLType() {
    
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
  * return 255
  */
  public static int getProductnumberColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getProductnumberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getProductnumberSQLType() {
    
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
  public static int getRstateColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRstateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getRstateSQLType() {
    
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

  public Datainterface getOriginal() {
    return original;
  }
   
  public void setOriginal(final Datainterface original) {
    this.original = (Datainterface) original.clone();
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
