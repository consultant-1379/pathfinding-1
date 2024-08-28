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
public class Aggregationrule implements Cloneable,RockDBObject  {

    private String AGGREGATION;
    private String VERSIONID;
    private Integer RULEID;
    private String TARGET_TYPE;
    private String TARGET_LEVEL;
    private String TARGET_TABLE;
    private String TARGET_MTABLEID;
    private String SOURCE_TYPE;
    private String SOURCE_LEVEL;
    private String SOURCE_TABLE;
    private String SOURCE_MTABLEID;
    private String RULETYPE;
    private String AGGREGATIONSCOPE;
    private String BHTYPE;
    private Integer ENABLE;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "AGGREGATION"    ,"VERSIONID"    ,"RULEID"    ,"RULETYPE"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Aggregationrule original; 

  public Aggregationrule(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Aggregationrule(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.AGGREGATION = null;
         this.VERSIONID = null;
         this.RULEID = null;
         this.TARGET_TYPE = null;
         this.TARGET_LEVEL = null;
         this.TARGET_TABLE = null;
         this.TARGET_MTABLEID = null;
         this.SOURCE_TYPE = null;
         this.SOURCE_LEVEL = null;
         this.SOURCE_TABLE = null;
         this.SOURCE_MTABLEID = null;
         this.RULETYPE = null;
         this.AGGREGATIONSCOPE = null;
         this.BHTYPE = null;
         this.ENABLE = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Aggregationrule(final RockFactory rockFact   ,final String AGGREGATION ,final String VERSIONID ,final Integer RULEID ,final String RULETYPE ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.AGGREGATION = AGGREGATION;
            this.VERSIONID = VERSIONID;
            this.RULEID = RULEID;
            this.RULETYPE = RULETYPE;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Aggregationrule> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Aggregationrule o = it.next();

              this.AGGREGATION = o.getAggregation();
              this.VERSIONID = o.getVersionid();
              this.RULEID = o.getRuleid();
              this.TARGET_TYPE = o.getTarget_type();
              this.TARGET_LEVEL = o.getTarget_level();
              this.TARGET_TABLE = o.getTarget_table();
              this.TARGET_MTABLEID = o.getTarget_mtableid();
              this.SOURCE_TYPE = o.getSource_type();
              this.SOURCE_LEVEL = o.getSource_level();
              this.SOURCE_TABLE = o.getSource_table();
              this.SOURCE_MTABLEID = o.getSource_mtableid();
              this.RULETYPE = o.getRuletype();
              this.AGGREGATIONSCOPE = o.getAggregationscope();
              this.BHTYPE = o.getBhtype();
              this.ENABLE = o.getEnable();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Aggregationrule");
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
  public Aggregationrule(final RockFactory rockFact, final Aggregationrule whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Aggregationrule> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Aggregationrule o = it.next();
                this.AGGREGATION = o.getAggregation();
                this.VERSIONID = o.getVersionid();
                this.RULEID = o.getRuleid();
                this.TARGET_TYPE = o.getTarget_type();
                this.TARGET_LEVEL = o.getTarget_level();
                this.TARGET_TABLE = o.getTarget_table();
                this.TARGET_MTABLEID = o.getTarget_mtableid();
                this.SOURCE_TYPE = o.getSource_type();
                this.SOURCE_LEVEL = o.getSource_level();
                this.SOURCE_TABLE = o.getSource_table();
                this.SOURCE_MTABLEID = o.getSource_mtableid();
                this.RULETYPE = o.getRuletype();
                this.AGGREGATIONSCOPE = o.getAggregationscope();
                this.BHTYPE = o.getBhtype();
                this.ENABLE = o.getEnable();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Aggregationrule");
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
    return "Aggregationrule";
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
  public int updateDB(final boolean useTimestamp, final Aggregationrule whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Aggregationrule whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Aggregationrule.saveDB(), no primary key defined");
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
    sbuff.append("<Aggregationrule ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("RULEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RULEID),4, true)+"\" ");
        sbuff.append("TARGET_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TYPE),12, true)+"\" ");
        sbuff.append("TARGET_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_LEVEL),12, true)+"\" ");
        sbuff.append("TARGET_TABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE),12, true)+"\" ");
        sbuff.append("TARGET_MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_MTABLEID),12, true)+"\" ");
        sbuff.append("SOURCE_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TYPE),12, true)+"\" ");
        sbuff.append("SOURCE_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_LEVEL),12, true)+"\" ");
        sbuff.append("SOURCE_TABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TABLE),12, true)+"\" ");
        sbuff.append("SOURCE_MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_MTABLEID),12, true)+"\" ");
        sbuff.append("RULETYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RULETYPE),12, true)+"\" ");
        sbuff.append("AGGREGATIONSCOPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSCOPE),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
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
    sbuff.append("<Aggregationrule ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("RULEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RULEID),4, true)+"\" ");
        sbuff.append("TARGET_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TYPE),12, true)+"\" ");
        sbuff.append("TARGET_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_LEVEL),12, true)+"\" ");
        sbuff.append("TARGET_TABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE),12, true)+"\" ");
        sbuff.append("TARGET_MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_MTABLEID),12, true)+"\" ");
        sbuff.append("SOURCE_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TYPE),12, true)+"\" ");
        sbuff.append("SOURCE_LEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_LEVEL),12, true)+"\" ");
        sbuff.append("SOURCE_TABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TABLE),12, true)+"\" ");
        sbuff.append("SOURCE_MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_MTABLEID),12, true)+"\" ");
        sbuff.append("RULETYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RULETYPE),12, true)+"\" ");
        sbuff.append("AGGREGATIONSCOPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSCOPE),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
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
    sbuff.append("</Aggregationrule>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Aggregationrule ( ");
	    		sbuff.append("AGGREGATION");
		    		sbuff.append(", VERSIONID");
	    		sbuff.append(", RULEID");
	    		sbuff.append(", TARGET_TYPE");
	    		sbuff.append(", TARGET_LEVEL");
	    		sbuff.append(", TARGET_TABLE");
	    		sbuff.append(", TARGET_MTABLEID");
	    		sbuff.append(", SOURCE_TYPE");
	    		sbuff.append(", SOURCE_LEVEL");
	    		sbuff.append(", SOURCE_TABLE");
	    		sbuff.append(", SOURCE_MTABLEID");
	    		sbuff.append(", RULETYPE");
	    		sbuff.append(", AGGREGATIONSCOPE");
	    		sbuff.append(", BHTYPE");
	    		sbuff.append(", ENABLE");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.AGGREGATION,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.RULEID,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_LEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_TABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_MTABLEID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_LEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_TABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_MTABLEID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.RULETYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONSCOPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENABLE,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getAggregation() { 
    return this.AGGREGATION;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public Integer getRuleid() { 
    return this.RULEID;
  }
   public String getTarget_type() { 
    return this.TARGET_TYPE;
  }
   public String getTarget_level() { 
    return this.TARGET_LEVEL;
  }
   public String getTarget_table() { 
    return this.TARGET_TABLE;
  }
   public String getTarget_mtableid() { 
    return this.TARGET_MTABLEID;
  }
   public String getSource_type() { 
    return this.SOURCE_TYPE;
  }
   public String getSource_level() { 
    return this.SOURCE_LEVEL;
  }
   public String getSource_table() { 
    return this.SOURCE_TABLE;
  }
   public String getSource_mtableid() { 
    return this.SOURCE_MTABLEID;
  }
   public String getRuletype() { 
    return this.RULETYPE;
  }
   public String getAggregationscope() { 
    return this.AGGREGATIONSCOPE;
  }
   public String getBhtype() { 
    return this.BHTYPE;
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
     if (AGGREGATION == null) {
      AGGREGATION = "";
    }
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (RULEID == null) {
      RULEID = new Integer (0);
    }
     if (TARGET_TYPE == null) {
      TARGET_TYPE = "";
    }
     if (TARGET_LEVEL == null) {
      TARGET_LEVEL = "";
    }
     if (TARGET_TABLE == null) {
      TARGET_TABLE = "";
    }
     if (TARGET_MTABLEID == null) {
      TARGET_MTABLEID = "";
    }
     if (SOURCE_TYPE == null) {
      SOURCE_TYPE = "";
    }
     if (SOURCE_LEVEL == null) {
      SOURCE_LEVEL = "";
    }
     if (SOURCE_TABLE == null) {
      SOURCE_TABLE = "";
    }
     if (SOURCE_MTABLEID == null) {
      SOURCE_MTABLEID = "";
    }
     if (RULETYPE == null) {
      RULETYPE = "";
    }
     if (AGGREGATIONSCOPE == null) {
      AGGREGATIONSCOPE = "";
    }
     if (BHTYPE == null) {
      BHTYPE = "";
    }
     if (ENABLE == null) {
      ENABLE = new Integer (0);
    }
   }

   public void setAggregation(final String AGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATION,"AGGREGATION",12,255,0);
    }
    modifiedColumns.add("AGGREGATION");
    this.AGGREGATION = AGGREGATION;
  }
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setRuleid(final Integer RULEID) {
    if (validateData){
      DataValidator.validateData((Object)RULEID,"RULEID",4,10,0);
    }
    modifiedColumns.add("RULEID");
    this.RULEID = RULEID;
  }
   public void setTarget_type(final String TARGET_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_TYPE,"TARGET_TYPE",12,50,0);
    }
    modifiedColumns.add("TARGET_TYPE");
    this.TARGET_TYPE = TARGET_TYPE;
  }
   public void setTarget_level(final String TARGET_LEVEL) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_LEVEL,"TARGET_LEVEL",12,50,0);
    }
    modifiedColumns.add("TARGET_LEVEL");
    this.TARGET_LEVEL = TARGET_LEVEL;
  }
   public void setTarget_table(final String TARGET_TABLE) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_TABLE,"TARGET_TABLE",12,255,0);
    }
    modifiedColumns.add("TARGET_TABLE");
    this.TARGET_TABLE = TARGET_TABLE;
  }
   public void setTarget_mtableid(final String TARGET_MTABLEID) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_MTABLEID,"TARGET_MTABLEID",12,255,0);
    }
    modifiedColumns.add("TARGET_MTABLEID");
    this.TARGET_MTABLEID = TARGET_MTABLEID;
  }
   public void setSource_type(final String SOURCE_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_TYPE,"SOURCE_TYPE",12,50,0);
    }
    modifiedColumns.add("SOURCE_TYPE");
    this.SOURCE_TYPE = SOURCE_TYPE;
  }
   public void setSource_level(final String SOURCE_LEVEL) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_LEVEL,"SOURCE_LEVEL",12,50,0);
    }
    modifiedColumns.add("SOURCE_LEVEL");
    this.SOURCE_LEVEL = SOURCE_LEVEL;
  }
   public void setSource_table(final String SOURCE_TABLE) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_TABLE,"SOURCE_TABLE",12,255,0);
    }
    modifiedColumns.add("SOURCE_TABLE");
    this.SOURCE_TABLE = SOURCE_TABLE;
  }
   public void setSource_mtableid(final String SOURCE_MTABLEID) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_MTABLEID,"SOURCE_MTABLEID",12,255,0);
    }
    modifiedColumns.add("SOURCE_MTABLEID");
    this.SOURCE_MTABLEID = SOURCE_MTABLEID;
  }
   public void setRuletype(final String RULETYPE) {
    if (validateData){
      DataValidator.validateData((Object)RULETYPE,"RULETYPE",12,50,0);
    }
    modifiedColumns.add("RULETYPE");
    this.RULETYPE = RULETYPE;
  }
   public void setAggregationscope(final String AGGREGATIONSCOPE) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONSCOPE,"AGGREGATIONSCOPE",12,50,0);
    }
    modifiedColumns.add("AGGREGATIONSCOPE");
    this.AGGREGATIONSCOPE = AGGREGATIONSCOPE;
  }
   public void setBhtype(final String BHTYPE) {
    if (validateData){
      DataValidator.validateData((Object)BHTYPE,"BHTYPE",12,50,0);
    }
    modifiedColumns.add("BHTYPE");
    this.BHTYPE = BHTYPE;
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
  public boolean dbEquals(final Aggregationrule o) {

         if ((((this.AGGREGATION == null) || (o.AGGREGATION == null)) && (this.AGGREGATION != o.AGGREGATION))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.RULEID == null) || (o.RULEID == null)) && (this.RULEID != o.RULEID))
            || (((this.RULETYPE == null) || (o.RULETYPE == null)) && (this.RULETYPE != o.RULETYPE))
          ){
    return false;
    } else
         if ((((this.AGGREGATION != null) && (o.AGGREGATION != null)) && (this.AGGREGATION.equals(o.AGGREGATION) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.RULEID != null) && (o.RULEID != null)) && (this.RULEID.equals(o.RULEID) == false))
            || (((this.RULETYPE != null) && (o.RULETYPE != null)) && (this.RULETYPE.equals(o.RULETYPE) == false))
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
  public boolean equals(final Aggregationrule o) {

         if ((((this.AGGREGATION == null) || (o.AGGREGATION == null)) && (this.AGGREGATION != o.AGGREGATION))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.RULEID == null) || (o.RULEID == null)) && (this.RULEID != o.RULEID))
            || (((this.TARGET_TYPE == null) || (o.TARGET_TYPE == null)) && (this.TARGET_TYPE != o.TARGET_TYPE))
            || (((this.TARGET_LEVEL == null) || (o.TARGET_LEVEL == null)) && (this.TARGET_LEVEL != o.TARGET_LEVEL))
            || (((this.TARGET_TABLE == null) || (o.TARGET_TABLE == null)) && (this.TARGET_TABLE != o.TARGET_TABLE))
            || (((this.TARGET_MTABLEID == null) || (o.TARGET_MTABLEID == null)) && (this.TARGET_MTABLEID != o.TARGET_MTABLEID))
            || (((this.SOURCE_TYPE == null) || (o.SOURCE_TYPE == null)) && (this.SOURCE_TYPE != o.SOURCE_TYPE))
            || (((this.SOURCE_LEVEL == null) || (o.SOURCE_LEVEL == null)) && (this.SOURCE_LEVEL != o.SOURCE_LEVEL))
            || (((this.SOURCE_TABLE == null) || (o.SOURCE_TABLE == null)) && (this.SOURCE_TABLE != o.SOURCE_TABLE))
            || (((this.SOURCE_MTABLEID == null) || (o.SOURCE_MTABLEID == null)) && (this.SOURCE_MTABLEID != o.SOURCE_MTABLEID))
            || (((this.RULETYPE == null) || (o.RULETYPE == null)) && (this.RULETYPE != o.RULETYPE))
            || (((this.AGGREGATIONSCOPE == null) || (o.AGGREGATIONSCOPE == null)) && (this.AGGREGATIONSCOPE != o.AGGREGATIONSCOPE))
            || (((this.BHTYPE == null) || (o.BHTYPE == null)) && (this.BHTYPE != o.BHTYPE))
            || (((this.ENABLE == null) || (o.ENABLE == null)) && (this.ENABLE != o.ENABLE))
          ){
    return false;
    } else
         if ((((this.AGGREGATION != null) && (o.AGGREGATION != null)) && (this.AGGREGATION.equals(o.AGGREGATION) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.RULEID != null) && (o.RULEID != null)) && (this.RULEID.equals(o.RULEID) == false))
            || (((this.TARGET_TYPE != null) && (o.TARGET_TYPE != null)) && (this.TARGET_TYPE.equals(o.TARGET_TYPE) == false))
            || (((this.TARGET_LEVEL != null) && (o.TARGET_LEVEL != null)) && (this.TARGET_LEVEL.equals(o.TARGET_LEVEL) == false))
            || (((this.TARGET_TABLE != null) && (o.TARGET_TABLE != null)) && (this.TARGET_TABLE.equals(o.TARGET_TABLE) == false))
            || (((this.TARGET_MTABLEID != null) && (o.TARGET_MTABLEID != null)) && (this.TARGET_MTABLEID.equals(o.TARGET_MTABLEID) == false))
            || (((this.SOURCE_TYPE != null) && (o.SOURCE_TYPE != null)) && (this.SOURCE_TYPE.equals(o.SOURCE_TYPE) == false))
            || (((this.SOURCE_LEVEL != null) && (o.SOURCE_LEVEL != null)) && (this.SOURCE_LEVEL.equals(o.SOURCE_LEVEL) == false))
            || (((this.SOURCE_TABLE != null) && (o.SOURCE_TABLE != null)) && (this.SOURCE_TABLE.equals(o.SOURCE_TABLE) == false))
            || (((this.SOURCE_MTABLEID != null) && (o.SOURCE_MTABLEID != null)) && (this.SOURCE_MTABLEID.equals(o.SOURCE_MTABLEID) == false))
            || (((this.RULETYPE != null) && (o.RULETYPE != null)) && (this.RULETYPE.equals(o.RULETYPE) == false))
            || (((this.AGGREGATIONSCOPE != null) && (o.AGGREGATIONSCOPE != null)) && (this.AGGREGATIONSCOPE.equals(o.AGGREGATIONSCOPE) == false))
            || (((this.BHTYPE != null) && (o.BHTYPE != null)) && (this.BHTYPE.equals(o.BHTYPE) == false))
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
  * return 255
  */
  public static int getAggregationColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationSQLType() {
    
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
  * return 10
  */
  public static int getRuleidColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRuleidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getRuleidSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTarget_typeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTarget_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTarget_levelColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_levelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTarget_levelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getTarget_tableColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_tableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTarget_tableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getTarget_mtableidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_mtableidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTarget_mtableidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getSource_typeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSource_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getSource_levelColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_levelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSource_levelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getSource_tableColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_tableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSource_tableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getSource_mtableidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_mtableidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSource_mtableidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getRuletypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRuletypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getRuletypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getAggregationscopeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationscopeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationscopeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getBhtypeColumnSize() {
    
     return 50;   
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

  public Aggregationrule getOriginal() {
    return original;
  }
   
  public void setOriginal(final Aggregationrule original) {
    this.original = (Aggregationrule) original.clone();
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
