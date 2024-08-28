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
public class Busyhour implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String BHLEVEL;
    private String BHTYPE;
    private String BHCRITERIA;
    private String WHERECLAUSE;
    private String DESCRIPTION;
    private String TARGETVERSIONID;
    private String BHOBJECT;
    private Integer BHELEMENT;
    private Integer ENABLE;
    private String AGGREGATIONTYPE;
    private Integer OFFSET;
    private Integer WINDOWSIZE;
    private Integer LOOKBACK;
    private Integer P_THRESHOLD;
    private Integer N_THRESHOLD;
    private String CLAUSE;
    private String PLACEHOLDERTYPE;
    private String GROUPING;
    private Integer REACTIVATEVIEWS;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"BHLEVEL"    ,"BHTYPE"    ,"TARGETVERSIONID"    ,"BHOBJECT"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Busyhour original; 

  public Busyhour(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Busyhour(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.BHLEVEL = null;
         this.BHTYPE = null;
         this.BHCRITERIA = null;
         this.WHERECLAUSE = null;
         this.DESCRIPTION = null;
         this.TARGETVERSIONID = null;
         this.BHOBJECT = null;
         this.BHELEMENT = null;
         this.ENABLE = null;
         this.AGGREGATIONTYPE = null;
         this.OFFSET = null;
         this.WINDOWSIZE = null;
         this.LOOKBACK = null;
         this.P_THRESHOLD = null;
         this.N_THRESHOLD = null;
         this.CLAUSE = null;
         this.PLACEHOLDERTYPE = null;
         this.GROUPING = null;
         this.REACTIVATEVIEWS = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Busyhour(final RockFactory rockFact   ,final String VERSIONID ,final String BHLEVEL ,final String BHTYPE ,final String TARGETVERSIONID ,final String BHOBJECT ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.BHLEVEL = BHLEVEL;
            this.BHTYPE = BHTYPE;
            this.TARGETVERSIONID = TARGETVERSIONID;
            this.BHOBJECT = BHOBJECT;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Busyhour> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Busyhour o = it.next();

              this.VERSIONID = o.getVersionid();
              this.BHLEVEL = o.getBhlevel();
              this.BHTYPE = o.getBhtype();
              this.BHCRITERIA = o.getBhcriteria();
              this.WHERECLAUSE = o.getWhereclause();
              this.DESCRIPTION = o.getDescription();
              this.TARGETVERSIONID = o.getTargetversionid();
              this.BHOBJECT = o.getBhobject();
              this.BHELEMENT = o.getBhelement();
              this.ENABLE = o.getEnable();
              this.AGGREGATIONTYPE = o.getAggregationtype();
              this.OFFSET = o.getOffset();
              this.WINDOWSIZE = o.getWindowsize();
              this.LOOKBACK = o.getLookback();
              this.P_THRESHOLD = o.getP_threshold();
              this.N_THRESHOLD = o.getN_threshold();
              this.CLAUSE = o.getClause();
              this.PLACEHOLDERTYPE = o.getPlaceholdertype();
              this.GROUPING = o.getGrouping();
              this.REACTIVATEVIEWS = o.getReactivateviews();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Busyhour");
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
  public Busyhour(final RockFactory rockFact, final Busyhour whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Busyhour> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Busyhour o = it.next();
                this.VERSIONID = o.getVersionid();
                this.BHLEVEL = o.getBhlevel();
                this.BHTYPE = o.getBhtype();
                this.BHCRITERIA = o.getBhcriteria();
                this.WHERECLAUSE = o.getWhereclause();
                this.DESCRIPTION = o.getDescription();
                this.TARGETVERSIONID = o.getTargetversionid();
                this.BHOBJECT = o.getBhobject();
                this.BHELEMENT = o.getBhelement();
                this.ENABLE = o.getEnable();
                this.AGGREGATIONTYPE = o.getAggregationtype();
                this.OFFSET = o.getOffset();
                this.WINDOWSIZE = o.getWindowsize();
                this.LOOKBACK = o.getLookback();
                this.P_THRESHOLD = o.getP_threshold();
                this.N_THRESHOLD = o.getN_threshold();
                this.CLAUSE = o.getClause();
                this.PLACEHOLDERTYPE = o.getPlaceholdertype();
                this.GROUPING = o.getGrouping();
                this.REACTIVATEVIEWS = o.getReactivateviews();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Busyhour");
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
    return "Busyhour";
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
  public int updateDB(final boolean useTimestamp, final Busyhour whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Busyhour whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Busyhour.saveDB(), no primary key defined");
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
    sbuff.append("<Busyhour ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("BHLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHLEVEL),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
        sbuff.append("BHCRITERIA=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHCRITERIA),12, true)+"\" ");
        sbuff.append("WHERECLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERECLAUSE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TARGETVERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETVERSIONID),12, true)+"\" ");
        sbuff.append("BHOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHOBJECT),12, true)+"\" ");
        sbuff.append("BHELEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHELEMENT),4, true)+"\" ");
        sbuff.append("ENABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLE),4, true)+"\" ");
        sbuff.append("AGGREGATIONTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONTYPE),12, true)+"\" ");
        sbuff.append("OFFSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OFFSET),4, true)+"\" ");
        sbuff.append("WINDOWSIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WINDOWSIZE),4, true)+"\" ");
        sbuff.append("LOOKBACK=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOOKBACK),4, true)+"\" ");
        sbuff.append("P_THRESHOLD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.P_THRESHOLD),4, true)+"\" ");
        sbuff.append("N_THRESHOLD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.N_THRESHOLD),4, true)+"\" ");
        sbuff.append("CLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CLAUSE),12, true)+"\" ");
        sbuff.append("PLACEHOLDERTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLACEHOLDERTYPE),12, true)+"\" ");
        sbuff.append("GROUPING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPING),12, true)+"\" ");
        sbuff.append("REACTIVATEVIEWS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REACTIVATEVIEWS),4, true)+"\" ");
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
    sbuff.append("<Busyhour ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("BHLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHLEVEL),12, true)+"\" ");
        sbuff.append("BHTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHTYPE),12, true)+"\" ");
        sbuff.append("BHCRITERIA=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHCRITERIA),12, true)+"\" ");
        sbuff.append("WHERECLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERECLAUSE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TARGETVERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETVERSIONID),12, true)+"\" ");
        sbuff.append("BHOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHOBJECT),12, true)+"\" ");
        sbuff.append("BHELEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BHELEMENT),4, true)+"\" ");
        sbuff.append("ENABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLE),4, true)+"\" ");
        sbuff.append("AGGREGATIONTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONTYPE),12, true)+"\" ");
        sbuff.append("OFFSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OFFSET),4, true)+"\" ");
        sbuff.append("WINDOWSIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WINDOWSIZE),4, true)+"\" ");
        sbuff.append("LOOKBACK=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOOKBACK),4, true)+"\" ");
        sbuff.append("P_THRESHOLD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.P_THRESHOLD),4, true)+"\" ");
        sbuff.append("N_THRESHOLD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.N_THRESHOLD),4, true)+"\" ");
        sbuff.append("CLAUSE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CLAUSE),12, true)+"\" ");
        sbuff.append("PLACEHOLDERTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLACEHOLDERTYPE),12, true)+"\" ");
        sbuff.append("GROUPING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPING),12, true)+"\" ");
        sbuff.append("REACTIVATEVIEWS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REACTIVATEVIEWS),4, true)+"\" ");
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
    sbuff.append("</Busyhour>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Busyhour ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", BHLEVEL");
	    		sbuff.append(", BHTYPE");
	    		sbuff.append(", BHCRITERIA");
	    		sbuff.append(", WHERECLAUSE");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", TARGETVERSIONID");
	    		sbuff.append(", BHOBJECT");
	    		sbuff.append(", BHELEMENT");
	    		sbuff.append(", ENABLE");
	    		sbuff.append(", AGGREGATIONTYPE");
	    		sbuff.append(", OFFSET");
	    		sbuff.append(", WINDOWSIZE");
	    		sbuff.append(", LOOKBACK");
	    		sbuff.append(", P_THRESHOLD");
	    		sbuff.append(", N_THRESHOLD");
	    		sbuff.append(", CLAUSE");
	    		sbuff.append(", PLACEHOLDERTYPE");
	    		sbuff.append(", GROUPING");
	    		sbuff.append(", REACTIVATEVIEWS");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.BHLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHCRITERIA,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WHERECLAUSE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGETVERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHOBJECT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BHELEMENT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENABLE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OFFSET,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WINDOWSIZE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOOKBACK,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.P_THRESHOLD,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.N_THRESHOLD,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CLAUSE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PLACEHOLDERTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.GROUPING,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.REACTIVATEVIEWS,4)+"");
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
   public String getBhcriteria() { 
    return this.BHCRITERIA;
  }
   public String getWhereclause() { 
    return this.WHERECLAUSE;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getTargetversionid() { 
    return this.TARGETVERSIONID;
  }
   public String getBhobject() { 
    return this.BHOBJECT;
  }
   public Integer getBhelement() { 
    return this.BHELEMENT;
  }
   public Integer getEnable() { 
    return this.ENABLE;
  }
   public String getAggregationtype() { 
    return this.AGGREGATIONTYPE;
  }
   public Integer getOffset() { 
    return this.OFFSET;
  }
   public Integer getWindowsize() { 
    return this.WINDOWSIZE;
  }
   public Integer getLookback() { 
    return this.LOOKBACK;
  }
   public Integer getP_threshold() { 
    return this.P_THRESHOLD;
  }
   public Integer getN_threshold() { 
    return this.N_THRESHOLD;
  }
   public String getClause() { 
    return this.CLAUSE;
  }
   public String getPlaceholdertype() { 
    return this.PLACEHOLDERTYPE;
  }
   public String getGrouping() { 
    return this.GROUPING;
  }
   public Integer getReactivateviews() { 
    return this.REACTIVATEVIEWS;
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
     if (BHCRITERIA == null) {
      BHCRITERIA = "";
    }
     if (WHERECLAUSE == null) {
      WHERECLAUSE = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (TARGETVERSIONID == null) {
      TARGETVERSIONID = "";
    }
     if (BHOBJECT == null) {
      BHOBJECT = "";
    }
     if (BHELEMENT == null) {
      BHELEMENT = new Integer (0);
    }
     if (ENABLE == null) {
      ENABLE = new Integer (0);
    }
     if (AGGREGATIONTYPE == null) {
      AGGREGATIONTYPE = "";
    }
     if (OFFSET == null) {
      OFFSET = new Integer (0);
    }
     if (WINDOWSIZE == null) {
      WINDOWSIZE = new Integer (0);
    }
     if (LOOKBACK == null) {
      LOOKBACK = new Integer (0);
    }
     if (P_THRESHOLD == null) {
      P_THRESHOLD = new Integer (0);
    }
     if (N_THRESHOLD == null) {
      N_THRESHOLD = new Integer (0);
    }
     if (CLAUSE == null) {
      CLAUSE = "";
    }
     if (PLACEHOLDERTYPE == null) {
      PLACEHOLDERTYPE = "";
    }
     if (GROUPING == null) {
      GROUPING = "";
    }
     if (REACTIVATEVIEWS == null) {
      REACTIVATEVIEWS = new Integer (0);
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
   public void setBhcriteria(final String BHCRITERIA) {
    if (validateData){
      DataValidator.validateData((Object)BHCRITERIA,"BHCRITERIA",12,32000,0);
    }
    modifiedColumns.add("BHCRITERIA");
    this.BHCRITERIA = BHCRITERIA;
  }
   public void setWhereclause(final String WHERECLAUSE) {
    if (validateData){
      DataValidator.validateData((Object)WHERECLAUSE,"WHERECLAUSE",12,32000,0);
    }
    modifiedColumns.add("WHERECLAUSE");
    this.WHERECLAUSE = WHERECLAUSE;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
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
   public void setBhelement(final Integer BHELEMENT) {
    if (validateData){
      DataValidator.validateData((Object)BHELEMENT,"BHELEMENT",4,10,0);
    }
    modifiedColumns.add("BHELEMENT");
    this.BHELEMENT = BHELEMENT;
  }
   public void setEnable(final Integer ENABLE) {
    if (validateData){
      DataValidator.validateData((Object)ENABLE,"ENABLE",4,10,0);
    }
    modifiedColumns.add("ENABLE");
    this.ENABLE = ENABLE;
  }
   public void setAggregationtype(final String AGGREGATIONTYPE) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONTYPE,"AGGREGATIONTYPE",12,128,0);
    }
    modifiedColumns.add("AGGREGATIONTYPE");
    this.AGGREGATIONTYPE = AGGREGATIONTYPE;
  }
   public void setOffset(final Integer OFFSET) {
    if (validateData){
      DataValidator.validateData((Object)OFFSET,"OFFSET",4,10,0);
    }
    modifiedColumns.add("OFFSET");
    this.OFFSET = OFFSET;
  }
   public void setWindowsize(final Integer WINDOWSIZE) {
    if (validateData){
      DataValidator.validateData((Object)WINDOWSIZE,"WINDOWSIZE",4,10,0);
    }
    modifiedColumns.add("WINDOWSIZE");
    this.WINDOWSIZE = WINDOWSIZE;
  }
   public void setLookback(final Integer LOOKBACK) {
    if (validateData){
      DataValidator.validateData((Object)LOOKBACK,"LOOKBACK",4,10,0);
    }
    modifiedColumns.add("LOOKBACK");
    this.LOOKBACK = LOOKBACK;
  }
   public void setP_threshold(final Integer P_THRESHOLD) {
    if (validateData){
      DataValidator.validateData((Object)P_THRESHOLD,"P_THRESHOLD",4,10,0);
    }
    modifiedColumns.add("P_THRESHOLD");
    this.P_THRESHOLD = P_THRESHOLD;
  }
   public void setN_threshold(final Integer N_THRESHOLD) {
    if (validateData){
      DataValidator.validateData((Object)N_THRESHOLD,"N_THRESHOLD",4,10,0);
    }
    modifiedColumns.add("N_THRESHOLD");
    this.N_THRESHOLD = N_THRESHOLD;
  }
   public void setClause(final String CLAUSE) {
    if (validateData){
      DataValidator.validateData((Object)CLAUSE,"CLAUSE",12,32000,0);
    }
    modifiedColumns.add("CLAUSE");
    this.CLAUSE = CLAUSE;
  }
   public void setPlaceholdertype(final String PLACEHOLDERTYPE) {
    if (validateData){
      DataValidator.validateData((Object)PLACEHOLDERTYPE,"PLACEHOLDERTYPE",12,128,0);
    }
    modifiedColumns.add("PLACEHOLDERTYPE");
    this.PLACEHOLDERTYPE = PLACEHOLDERTYPE;
  }
   public void setGrouping(final String GROUPING) {
    if (validateData){
      DataValidator.validateData((Object)GROUPING,"GROUPING",12,32,0);
    }
    modifiedColumns.add("GROUPING");
    this.GROUPING = GROUPING;
  }
   public void setReactivateviews(final Integer REACTIVATEVIEWS) {
    if (validateData){
      DataValidator.validateData((Object)REACTIVATEVIEWS,"REACTIVATEVIEWS",4,10,0);
    }
    modifiedColumns.add("REACTIVATEVIEWS");
    this.REACTIVATEVIEWS = REACTIVATEVIEWS;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Busyhour o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.BHLEVEL == null) || (o.BHLEVEL == null)) && (this.BHLEVEL != o.BHLEVEL))
            || (((this.BHTYPE == null) || (o.BHTYPE == null)) && (this.BHTYPE != o.BHTYPE))
            || (((this.TARGETVERSIONID == null) || (o.TARGETVERSIONID == null)) && (this.TARGETVERSIONID != o.TARGETVERSIONID))
            || (((this.BHOBJECT == null) || (o.BHOBJECT == null)) && (this.BHOBJECT != o.BHOBJECT))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.BHLEVEL != null) && (o.BHLEVEL != null)) && (this.BHLEVEL.equals(o.BHLEVEL) == false))
            || (((this.BHTYPE != null) && (o.BHTYPE != null)) && (this.BHTYPE.equals(o.BHTYPE) == false))
            || (((this.TARGETVERSIONID != null) && (o.TARGETVERSIONID != null)) && (this.TARGETVERSIONID.equals(o.TARGETVERSIONID) == false))
            || (((this.BHOBJECT != null) && (o.BHOBJECT != null)) && (this.BHOBJECT.equals(o.BHOBJECT) == false))
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
  public boolean equals(final Busyhour o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.BHLEVEL == null) || (o.BHLEVEL == null)) && (this.BHLEVEL != o.BHLEVEL))
            || (((this.BHTYPE == null) || (o.BHTYPE == null)) && (this.BHTYPE != o.BHTYPE))
            || (((this.BHCRITERIA == null) || (o.BHCRITERIA == null)) && (this.BHCRITERIA != o.BHCRITERIA))
            || (((this.WHERECLAUSE == null) || (o.WHERECLAUSE == null)) && (this.WHERECLAUSE != o.WHERECLAUSE))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.TARGETVERSIONID == null) || (o.TARGETVERSIONID == null)) && (this.TARGETVERSIONID != o.TARGETVERSIONID))
            || (((this.BHOBJECT == null) || (o.BHOBJECT == null)) && (this.BHOBJECT != o.BHOBJECT))
            || (((this.BHELEMENT == null) || (o.BHELEMENT == null)) && (this.BHELEMENT != o.BHELEMENT))
            || (((this.ENABLE == null) || (o.ENABLE == null)) && (this.ENABLE != o.ENABLE))
            || (((this.AGGREGATIONTYPE == null) || (o.AGGREGATIONTYPE == null)) && (this.AGGREGATIONTYPE != o.AGGREGATIONTYPE))
            || (((this.OFFSET == null) || (o.OFFSET == null)) && (this.OFFSET != o.OFFSET))
            || (((this.WINDOWSIZE == null) || (o.WINDOWSIZE == null)) && (this.WINDOWSIZE != o.WINDOWSIZE))
            || (((this.LOOKBACK == null) || (o.LOOKBACK == null)) && (this.LOOKBACK != o.LOOKBACK))
            || (((this.P_THRESHOLD == null) || (o.P_THRESHOLD == null)) && (this.P_THRESHOLD != o.P_THRESHOLD))
            || (((this.N_THRESHOLD == null) || (o.N_THRESHOLD == null)) && (this.N_THRESHOLD != o.N_THRESHOLD))
            || (((this.CLAUSE == null) || (o.CLAUSE == null)) && (this.CLAUSE != o.CLAUSE))
            || (((this.PLACEHOLDERTYPE == null) || (o.PLACEHOLDERTYPE == null)) && (this.PLACEHOLDERTYPE != o.PLACEHOLDERTYPE))
            || (((this.GROUPING == null) || (o.GROUPING == null)) && (this.GROUPING != o.GROUPING))
            || (((this.REACTIVATEVIEWS == null) || (o.REACTIVATEVIEWS == null)) && (this.REACTIVATEVIEWS != o.REACTIVATEVIEWS))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.BHLEVEL != null) && (o.BHLEVEL != null)) && (this.BHLEVEL.equals(o.BHLEVEL) == false))
            || (((this.BHTYPE != null) && (o.BHTYPE != null)) && (this.BHTYPE.equals(o.BHTYPE) == false))
            || (((this.BHCRITERIA != null) && (o.BHCRITERIA != null)) && (this.BHCRITERIA.equals(o.BHCRITERIA) == false))
            || (((this.WHERECLAUSE != null) && (o.WHERECLAUSE != null)) && (this.WHERECLAUSE.equals(o.WHERECLAUSE) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.TARGETVERSIONID != null) && (o.TARGETVERSIONID != null)) && (this.TARGETVERSIONID.equals(o.TARGETVERSIONID) == false))
            || (((this.BHOBJECT != null) && (o.BHOBJECT != null)) && (this.BHOBJECT.equals(o.BHOBJECT) == false))
            || (((this.BHELEMENT != null) && (o.BHELEMENT != null)) && (this.BHELEMENT.equals(o.BHELEMENT) == false))
            || (((this.ENABLE != null) && (o.ENABLE != null)) && (this.ENABLE.equals(o.ENABLE) == false))
            || (((this.AGGREGATIONTYPE != null) && (o.AGGREGATIONTYPE != null)) && (this.AGGREGATIONTYPE.equals(o.AGGREGATIONTYPE) == false))
            || (((this.OFFSET != null) && (o.OFFSET != null)) && (this.OFFSET.equals(o.OFFSET) == false))
            || (((this.WINDOWSIZE != null) && (o.WINDOWSIZE != null)) && (this.WINDOWSIZE.equals(o.WINDOWSIZE) == false))
            || (((this.LOOKBACK != null) && (o.LOOKBACK != null)) && (this.LOOKBACK.equals(o.LOOKBACK) == false))
            || (((this.P_THRESHOLD != null) && (o.P_THRESHOLD != null)) && (this.P_THRESHOLD.equals(o.P_THRESHOLD) == false))
            || (((this.N_THRESHOLD != null) && (o.N_THRESHOLD != null)) && (this.N_THRESHOLD.equals(o.N_THRESHOLD) == false))
            || (((this.CLAUSE != null) && (o.CLAUSE != null)) && (this.CLAUSE.equals(o.CLAUSE) == false))
            || (((this.PLACEHOLDERTYPE != null) && (o.PLACEHOLDERTYPE != null)) && (this.PLACEHOLDERTYPE.equals(o.PLACEHOLDERTYPE) == false))
            || (((this.GROUPING != null) && (o.GROUPING != null)) && (this.GROUPING.equals(o.GROUPING) == false))
            || (((this.REACTIVATEVIEWS != null) && (o.REACTIVATEVIEWS != null)) && (this.REACTIVATEVIEWS.equals(o.REACTIVATEVIEWS) == false))
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
  * return 32000
  */
  public static int getBhcriteriaColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhcriteriaDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBhcriteriaSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getWhereclauseColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWhereclauseDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getWhereclauseSQLType() {
    
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
  * return 10
  */
  public static int getBhelementColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBhelementDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getBhelementSQLType() {
    
    return 4;   
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
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getAggregationtypeColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationtypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationtypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getOffsetColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOffsetDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getOffsetSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getWindowsizeColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWindowsizeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getWindowsizeSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getLookbackColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLookbackDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getLookbackSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getP_thresholdColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getP_thresholdDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getP_thresholdSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getN_thresholdColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getN_thresholdDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getN_thresholdSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getClauseColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getClauseDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getClauseSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPlaceholdertypeColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlaceholdertypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPlaceholdertypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getGroupingColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getGroupingDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getGroupingSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getReactivateviewsColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getReactivateviewsDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getReactivateviewsSQLType() {
    
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

  public Busyhour getOriginal() {
    return original;
  }
   
  public void setOriginal(final Busyhour original) {
    this.original = (Busyhour) original.clone();
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
