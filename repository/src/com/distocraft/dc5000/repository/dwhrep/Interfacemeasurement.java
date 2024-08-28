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
public class Interfacemeasurement implements Cloneable,RockDBObject  {

    private String TAGID;
    private String DATAFORMATID;
    private String INTERFACENAME;
    private String TRANSFORMERID;
    private Long STATUS;
    private Timestamp MODIFTIME;
    private String DESCRIPTION;
    private String TECHPACKVERSION;
    private String INTERFACEVERSION;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private String[] primaryKeyNames = {    "TAGID"    ,"DATAFORMATID"    ,"INTERFACENAME"    ,"INTERFACEVERSION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Interfacemeasurement original; 

  public Interfacemeasurement(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Interfacemeasurement(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TAGID = null;
         this.DATAFORMATID = null;
         this.INTERFACENAME = null;
         this.TRANSFORMERID = null;
         this.STATUS = null;
         this.MODIFTIME = null;
         this.DESCRIPTION = null;
         this.TECHPACKVERSION = null;
         this.INTERFACEVERSION = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Interfacemeasurement(RockFactory rockFact   ,String TAGID ,String DATAFORMATID ,String INTERFACENAME ,String INTERFACEVERSION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TAGID = TAGID;
            this.DATAFORMATID = DATAFORMATID;
            this.INTERFACENAME = INTERFACENAME;
            this.INTERFACEVERSION = INTERFACEVERSION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Interfacemeasurement> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Interfacemeasurement o = it.next();

              this.TAGID = o.getTagid();
              this.DATAFORMATID = o.getDataformatid();
              this.INTERFACENAME = o.getInterfacename();
              this.TRANSFORMERID = o.getTransformerid();
              this.STATUS = o.getStatus();
              this.MODIFTIME = o.getModiftime();
              this.DESCRIPTION = o.getDescription();
              this.TECHPACKVERSION = o.getTechpackversion();
              this.INTERFACEVERSION = o.getInterfaceversion();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Interfacemeasurement");
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
  public Interfacemeasurement(final RockFactory rockFact, final Interfacemeasurement whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Interfacemeasurement> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Interfacemeasurement o = it.next();
                this.TAGID = o.getTagid();
                this.DATAFORMATID = o.getDataformatid();
                this.INTERFACENAME = o.getInterfacename();
                this.TRANSFORMERID = o.getTransformerid();
                this.STATUS = o.getStatus();
                this.MODIFTIME = o.getModiftime();
                this.DESCRIPTION = o.getDescription();
                this.TECHPACKVERSION = o.getTechpackversion();
                this.INTERFACEVERSION = o.getInterfaceversion();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Interfacemeasurement");
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
    return "Interfacemeasurement";
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
  public int updateDB(final boolean useTimestamp, final Interfacemeasurement whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Interfacemeasurement whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Interfacemeasurement.saveDB(), no primary key defined");
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
    sbuff.append("<Interfacemeasurement ");
        sbuff.append("TAGID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TAGID),12, true)+"\" ");
        sbuff.append("DATAFORMATID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATID),12, true)+"\" ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("TRANSFORMERID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMERID),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("MODIFTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MODIFTIME),93, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TECHPACKVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKVERSION),12, true)+"\" ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
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
    sbuff.append("<Interfacemeasurement ");
        sbuff.append("TAGID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TAGID),12, true)+"\" ");
        sbuff.append("DATAFORMATID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATID),12, true)+"\" ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("TRANSFORMERID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMERID),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("MODIFTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MODIFTIME),93, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TECHPACKVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKVERSION),12, true)+"\" ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
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
    sbuff.append("</Interfacemeasurement>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Interfacemeasurement ( ");
	    		sbuff.append("TAGID");
		    		sbuff.append(", DATAFORMATID");
	    		sbuff.append(", INTERFACENAME");
	    		sbuff.append(", TRANSFORMERID");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", MODIFTIME");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", TECHPACKVERSION");
	    		sbuff.append(", INTERFACEVERSION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TAGID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DATAFORMATID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERFACENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFORMERID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MODIFTIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACKVERSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERFACEVERSION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTagid() { 
    return this.TAGID;
  }
   public String getDataformatid() { 
    return this.DATAFORMATID;
  }
   public String getInterfacename() { 
    return this.INTERFACENAME;
  }
   public String getTransformerid() { 
    return this.TRANSFORMERID;
  }
   public Long getStatus() { 
    return this.STATUS;
  }
   public Timestamp getModiftime() { 
    return this.MODIFTIME;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getTechpackversion() { 
    return this.TECHPACKVERSION;
  }
   public String getInterfaceversion() { 
    return this.INTERFACEVERSION;
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
     if (TAGID == null) {
      TAGID = "";
    }
     if (DATAFORMATID == null) {
      DATAFORMATID = "";
    }
     if (INTERFACENAME == null) {
      INTERFACENAME = "";
    }
     if (TRANSFORMERID == null) {
      TRANSFORMERID = "";
    }
     if (STATUS == null) {
      STATUS = new Long (0);
    }
     if (MODIFTIME == null) {
      MODIFTIME = new Timestamp (0);
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (TECHPACKVERSION == null) {
      TECHPACKVERSION = "";
    }
     if (INTERFACEVERSION == null) {
      INTERFACEVERSION = "";
    }
   }

   public void setTagid(final String TAGID) {
    if (validateData){
      DataValidator.validateData((Object)TAGID,"TAGID",12,128,0);
    }
    modifiedColumns.add("TAGID");
    this.TAGID = TAGID;
  }
   public void setDataformatid(final String DATAFORMATID) {
    if (validateData){
      DataValidator.validateData((Object)DATAFORMATID,"DATAFORMATID",12,100,0);
    }
    modifiedColumns.add("DATAFORMATID");
    this.DATAFORMATID = DATAFORMATID;
  }
   public void setInterfacename(final String INTERFACENAME) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACENAME,"INTERFACENAME",12,255,0);
    }
    modifiedColumns.add("INTERFACENAME");
    this.INTERFACENAME = INTERFACENAME;
  }
   public void setTransformerid(final String TRANSFORMERID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFORMERID,"TRANSFORMERID",12,255,0);
    }
    modifiedColumns.add("TRANSFORMERID");
    this.TRANSFORMERID = TRANSFORMERID;
  }
   public void setStatus(final Long STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",2,9,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setModiftime(final Timestamp MODIFTIME) {
    if (validateData){
      DataValidator.validateData((Object)MODIFTIME,"MODIFTIME",93,23,0);
    }
    modifiedColumns.add("MODIFTIME");
    this.MODIFTIME = MODIFTIME;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setTechpackversion(final String TECHPACKVERSION) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACKVERSION,"TECHPACKVERSION",12,32,0);
    }
    modifiedColumns.add("TECHPACKVERSION");
    this.TECHPACKVERSION = TECHPACKVERSION;
  }
   public void setInterfaceversion(final String INTERFACEVERSION) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACEVERSION,"INTERFACEVERSION",12,32,0);
    }
    modifiedColumns.add("INTERFACEVERSION");
    this.INTERFACEVERSION = INTERFACEVERSION;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Interfacemeasurement o) {

         if ((((this.TAGID == null) || (o.TAGID == null)) && (this.TAGID != o.TAGID))
            || (((this.DATAFORMATID == null) || (o.DATAFORMATID == null)) && (this.DATAFORMATID != o.DATAFORMATID))
            || (((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
          ){
    return false;
    } else
         if ((((this.TAGID != null) && (o.TAGID != null)) && (this.TAGID.equals(o.TAGID) == false))
            || (((this.DATAFORMATID != null) && (o.DATAFORMATID != null)) && (this.DATAFORMATID.equals(o.DATAFORMATID) == false))
            || (((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
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
  public boolean equals(final Interfacemeasurement o) {

         if ((((this.TAGID == null) || (o.TAGID == null)) && (this.TAGID != o.TAGID))
            || (((this.DATAFORMATID == null) || (o.DATAFORMATID == null)) && (this.DATAFORMATID != o.DATAFORMATID))
            || (((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.TRANSFORMERID == null) || (o.TRANSFORMERID == null)) && (this.TRANSFORMERID != o.TRANSFORMERID))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.MODIFTIME == null) || (o.MODIFTIME == null)) && (this.MODIFTIME != o.MODIFTIME))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.TECHPACKVERSION == null) || (o.TECHPACKVERSION == null)) && (this.TECHPACKVERSION != o.TECHPACKVERSION))
            || (((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
          ){
    return false;
    } else
         if ((((this.TAGID != null) && (o.TAGID != null)) && (this.TAGID.equals(o.TAGID) == false))
            || (((this.DATAFORMATID != null) && (o.DATAFORMATID != null)) && (this.DATAFORMATID.equals(o.DATAFORMATID) == false))
            || (((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
            || (((this.TRANSFORMERID != null) && (o.TRANSFORMERID != null)) && (this.TRANSFORMERID.equals(o.TRANSFORMERID) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.MODIFTIME != null) && (o.MODIFTIME != null)) && (this.MODIFTIME.equals(o.MODIFTIME) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.TECHPACKVERSION != null) && (o.TECHPACKVERSION != null)) && (this.TECHPACKVERSION.equals(o.TECHPACKVERSION) == false))
            || (((this.INTERFACEVERSION != null) && (o.INTERFACEVERSION != null)) && (this.INTERFACEVERSION.equals(o.INTERFACEVERSION) == false))
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
  public static int getTagidColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTagidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTagidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getDataformatidColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDataformatidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDataformatidSQLType() {
    
    return 12;   
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
  * return 255
  */
  public static int getTransformeridColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransformeridDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTransformeridSQLType() {
    
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
  * return 23
  */
  public static int getModiftimeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getModiftimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getModiftimeSQLType() {
    
    return 93;   
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
  public static int getTechpackversionColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpackversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpackversionSQLType() {
    
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

  public Interfacemeasurement getOriginal() {
    return original;
  }
   
  public void setOriginal(final Interfacemeasurement original) {
    this.original = (Interfacemeasurement) original.clone();
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
