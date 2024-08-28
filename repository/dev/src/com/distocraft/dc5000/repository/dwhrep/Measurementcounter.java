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
public class Measurementcounter implements Cloneable,RockDBObject  {

    private String TYPEID;
    private String DATANAME;
    private String DESCRIPTION;
    private String TIMEAGGREGATION;
    private String GROUPAGGREGATION;
    private String COUNTAGGREGATION;
    private Long COLNUMBER;
    private String DATATYPE;
    private Integer DATASIZE;
    private Integer DATASCALE;
    private Integer INCLUDESQL;
    private String UNIVOBJECT;
    private String UNIVCLASS;
    private String COUNTERTYPE;
    private String COUNTERPROCESS;
    private String DATAID;
    private Integer FOLLOWJOHN;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPEID"    ,"DATANAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Measurementcounter original; 

  public Measurementcounter(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Measurementcounter(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPEID = null;
         this.DATANAME = null;
         this.DESCRIPTION = null;
         this.TIMEAGGREGATION = null;
         this.GROUPAGGREGATION = null;
         this.COUNTAGGREGATION = null;
         this.COLNUMBER = null;
         this.DATATYPE = null;
         this.DATASIZE = null;
         this.DATASCALE = null;
         this.INCLUDESQL = null;
         this.UNIVOBJECT = null;
         this.UNIVCLASS = null;
         this.COUNTERTYPE = null;
         this.COUNTERPROCESS = null;
         this.DATAID = null;
         this.FOLLOWJOHN = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Measurementcounter(final RockFactory rockFact   ,final String TYPEID ,final String DATANAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPEID = TYPEID;
            this.DATANAME = DATANAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Measurementcounter> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Measurementcounter o = it.next();

              this.TYPEID = o.getTypeid();
              this.DATANAME = o.getDataname();
              this.DESCRIPTION = o.getDescription();
              this.TIMEAGGREGATION = o.getTimeaggregation();
              this.GROUPAGGREGATION = o.getGroupaggregation();
              this.COUNTAGGREGATION = o.getCountaggregation();
              this.COLNUMBER = o.getColnumber();
              this.DATATYPE = o.getDatatype();
              this.DATASIZE = o.getDatasize();
              this.DATASCALE = o.getDatascale();
              this.INCLUDESQL = o.getIncludesql();
              this.UNIVOBJECT = o.getUnivobject();
              this.UNIVCLASS = o.getUnivclass();
              this.COUNTERTYPE = o.getCountertype();
              this.COUNTERPROCESS = o.getCounterprocess();
              this.DATAID = o.getDataid();
              this.FOLLOWJOHN = o.getFollowjohn();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementcounter");
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
  public Measurementcounter(final RockFactory rockFact, final Measurementcounter whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Measurementcounter> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Measurementcounter o = it.next();
                this.TYPEID = o.getTypeid();
                this.DATANAME = o.getDataname();
                this.DESCRIPTION = o.getDescription();
                this.TIMEAGGREGATION = o.getTimeaggregation();
                this.GROUPAGGREGATION = o.getGroupaggregation();
                this.COUNTAGGREGATION = o.getCountaggregation();
                this.COLNUMBER = o.getColnumber();
                this.DATATYPE = o.getDatatype();
                this.DATASIZE = o.getDatasize();
                this.DATASCALE = o.getDatascale();
                this.INCLUDESQL = o.getIncludesql();
                this.UNIVOBJECT = o.getUnivobject();
                this.UNIVCLASS = o.getUnivclass();
                this.COUNTERTYPE = o.getCountertype();
                this.COUNTERPROCESS = o.getCounterprocess();
                this.DATAID = o.getDataid();
                this.FOLLOWJOHN = o.getFollowjohn();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementcounter");
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
    return "Measurementcounter";
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
  public int updateDB(final boolean useTimestamp, final Measurementcounter whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Measurementcounter whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Measurementcounter.saveDB(), no primary key defined");
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
    sbuff.append("<Measurementcounter ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("DATANAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATANAME),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TIMEAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TIMEAGGREGATION),12, true)+"\" ");
        sbuff.append("GROUPAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPAGGREGATION),12, true)+"\" ");
        sbuff.append("COUNTAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTAGGREGATION),12, true)+"\" ");
        sbuff.append("COLNUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLNUMBER),2, true)+"\" ");
        sbuff.append("DATATYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATATYPE),12, true)+"\" ");
        sbuff.append("DATASIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATASIZE),4, true)+"\" ");
        sbuff.append("DATASCALE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATASCALE),4, true)+"\" ");
        sbuff.append("INCLUDESQL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INCLUDESQL),4, true)+"\" ");
        sbuff.append("UNIVOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVOBJECT),12, true)+"\" ");
        sbuff.append("UNIVCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVCLASS),12, true)+"\" ");
        sbuff.append("COUNTERTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTERTYPE),12, true)+"\" ");
        sbuff.append("COUNTERPROCESS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTERPROCESS),12, true)+"\" ");
        sbuff.append("DATAID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAID),12, true)+"\" ");
        sbuff.append("FOLLOWJOHN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLLOWJOHN),4, true)+"\" ");
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
    sbuff.append("<Measurementcounter ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("DATANAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATANAME),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("TIMEAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TIMEAGGREGATION),12, true)+"\" ");
        sbuff.append("GROUPAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPAGGREGATION),12, true)+"\" ");
        sbuff.append("COUNTAGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTAGGREGATION),12, true)+"\" ");
        sbuff.append("COLNUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLNUMBER),2, true)+"\" ");
        sbuff.append("DATATYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATATYPE),12, true)+"\" ");
        sbuff.append("DATASIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATASIZE),4, true)+"\" ");
        sbuff.append("DATASCALE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATASCALE),4, true)+"\" ");
        sbuff.append("INCLUDESQL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INCLUDESQL),4, true)+"\" ");
        sbuff.append("UNIVOBJECT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVOBJECT),12, true)+"\" ");
        sbuff.append("UNIVCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVCLASS),12, true)+"\" ");
        sbuff.append("COUNTERTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTERTYPE),12, true)+"\" ");
        sbuff.append("COUNTERPROCESS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COUNTERPROCESS),12, true)+"\" ");
        sbuff.append("DATAID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAID),12, true)+"\" ");
        sbuff.append("FOLLOWJOHN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLLOWJOHN),4, true)+"\" ");
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
    sbuff.append("</Measurementcounter>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Measurementcounter ( ");
	    		sbuff.append("TYPEID");
		    		sbuff.append(", DATANAME");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", TIMEAGGREGATION");
	    		sbuff.append(", GROUPAGGREGATION");
	    		sbuff.append(", COUNTAGGREGATION");
	    		sbuff.append(", COLNUMBER");
	    		sbuff.append(", DATATYPE");
	    		sbuff.append(", DATASIZE");
	    		sbuff.append(", DATASCALE");
	    		sbuff.append(", INCLUDESQL");
	    		sbuff.append(", UNIVOBJECT");
	    		sbuff.append(", UNIVCLASS");
	    		sbuff.append(", COUNTERTYPE");
	    		sbuff.append(", COUNTERPROCESS");
	    		sbuff.append(", DATAID");
	    		sbuff.append(", FOLLOWJOHN");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DATANAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TIMEAGGREGATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.GROUPAGGREGATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COUNTAGGREGATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLNUMBER,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATATYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATASIZE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATASCALE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INCLUDESQL,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVOBJECT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVCLASS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COUNTERTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COUNTERPROCESS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATAID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FOLLOWJOHN,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getDataname() { 
    return this.DATANAME;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getTimeaggregation() { 
    return this.TIMEAGGREGATION;
  }
   public String getGroupaggregation() { 
    return this.GROUPAGGREGATION;
  }
   public String getCountaggregation() { 
    return this.COUNTAGGREGATION;
  }
   public Long getColnumber() { 
    return this.COLNUMBER;
  }
   public String getDatatype() { 
    return this.DATATYPE;
  }
   public Integer getDatasize() { 
    return this.DATASIZE;
  }
   public Integer getDatascale() { 
    return this.DATASCALE;
  }
   public Integer getIncludesql() { 
    return this.INCLUDESQL;
  }
   public String getUnivobject() { 
    return this.UNIVOBJECT;
  }
   public String getUnivclass() { 
    return this.UNIVCLASS;
  }
   public String getCountertype() { 
    return this.COUNTERTYPE;
  }
   public String getCounterprocess() { 
    return this.COUNTERPROCESS;
  }
   public String getDataid() { 
    return this.DATAID;
  }
   public Integer getFollowjohn() { 
    return this.FOLLOWJOHN;
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
     if (DATANAME == null) {
      DATANAME = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (TIMEAGGREGATION == null) {
      TIMEAGGREGATION = "";
    }
     if (GROUPAGGREGATION == null) {
      GROUPAGGREGATION = "";
    }
     if (COUNTAGGREGATION == null) {
      COUNTAGGREGATION = "";
    }
     if (COLNUMBER == null) {
      COLNUMBER = new Long (0);
    }
     if (DATATYPE == null) {
      DATATYPE = "";
    }
     if (DATASIZE == null) {
      DATASIZE = new Integer (0);
    }
     if (DATASCALE == null) {
      DATASCALE = new Integer (0);
    }
     if (INCLUDESQL == null) {
      INCLUDESQL = new Integer (0);
    }
     if (UNIVOBJECT == null) {
      UNIVOBJECT = "";
    }
     if (UNIVCLASS == null) {
      UNIVCLASS = "";
    }
     if (COUNTERTYPE == null) {
      COUNTERTYPE = "";
    }
     if (COUNTERPROCESS == null) {
      COUNTERPROCESS = "";
    }
     if (DATAID == null) {
      DATAID = "";
    }
     if (FOLLOWJOHN == null) {
      FOLLOWJOHN = new Integer (0);
    }
   }

   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setDataname(final String DATANAME) {
    if (validateData){
      DataValidator.validateData((Object)DATANAME,"DATANAME",12,128,0);
    }
    modifiedColumns.add("DATANAME");
    this.DATANAME = DATANAME;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setTimeaggregation(final String TIMEAGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)TIMEAGGREGATION,"TIMEAGGREGATION",12,50,0);
    }
    modifiedColumns.add("TIMEAGGREGATION");
    this.TIMEAGGREGATION = TIMEAGGREGATION;
  }
   public void setGroupaggregation(final String GROUPAGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)GROUPAGGREGATION,"GROUPAGGREGATION",12,50,0);
    }
    modifiedColumns.add("GROUPAGGREGATION");
    this.GROUPAGGREGATION = GROUPAGGREGATION;
  }
   public void setCountaggregation(final String COUNTAGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)COUNTAGGREGATION,"COUNTAGGREGATION",12,32000,0);
    }
    modifiedColumns.add("COUNTAGGREGATION");
    this.COUNTAGGREGATION = COUNTAGGREGATION;
  }
   public void setColnumber(final Long COLNUMBER) {
    if (validateData){
      DataValidator.validateData((Object)COLNUMBER,"COLNUMBER",2,9,0);
    }
    modifiedColumns.add("COLNUMBER");
    this.COLNUMBER = COLNUMBER;
  }
   public void setDatatype(final String DATATYPE) {
    if (validateData){
      DataValidator.validateData((Object)DATATYPE,"DATATYPE",12,50,0);
    }
    modifiedColumns.add("DATATYPE");
    this.DATATYPE = DATATYPE;
  }
   public void setDatasize(final Integer DATASIZE) {
    if (validateData){
      DataValidator.validateData((Object)DATASIZE,"DATASIZE",4,10,0);
    }
    modifiedColumns.add("DATASIZE");
    this.DATASIZE = DATASIZE;
  }
   public void setDatascale(final Integer DATASCALE) {
    if (validateData){
      DataValidator.validateData((Object)DATASCALE,"DATASCALE",4,10,0);
    }
    modifiedColumns.add("DATASCALE");
    this.DATASCALE = DATASCALE;
  }
   public void setIncludesql(final Integer INCLUDESQL) {
    if (validateData){
      DataValidator.validateData((Object)INCLUDESQL,"INCLUDESQL",4,10,0);
    }
    modifiedColumns.add("INCLUDESQL");
    this.INCLUDESQL = INCLUDESQL;
  }
   public void setUnivobject(final String UNIVOBJECT) {
    if (validateData){
      DataValidator.validateData((Object)UNIVOBJECT,"UNIVOBJECT",12,128,0);
    }
    modifiedColumns.add("UNIVOBJECT");
    this.UNIVOBJECT = UNIVOBJECT;
  }
   public void setUnivclass(final String UNIVCLASS) {
    if (validateData){
      DataValidator.validateData((Object)UNIVCLASS,"UNIVCLASS",12,35,0);
    }
    modifiedColumns.add("UNIVCLASS");
    this.UNIVCLASS = UNIVCLASS;
  }
   public void setCountertype(final String COUNTERTYPE) {
    if (validateData){
      DataValidator.validateData((Object)COUNTERTYPE,"COUNTERTYPE",12,16,0);
    }
    modifiedColumns.add("COUNTERTYPE");
    this.COUNTERTYPE = COUNTERTYPE;
  }
   public void setCounterprocess(final String COUNTERPROCESS) {
    if (validateData){
      DataValidator.validateData((Object)COUNTERPROCESS,"COUNTERPROCESS",12,16,0);
    }
    modifiedColumns.add("COUNTERPROCESS");
    this.COUNTERPROCESS = COUNTERPROCESS;
  }
   public void setDataid(final String DATAID) {
    if (validateData){
      DataValidator.validateData((Object)DATAID,"DATAID",12,255,0);
    }
    modifiedColumns.add("DATAID");
    this.DATAID = DATAID;
  }
   public void setFollowjohn(final Integer FOLLOWJOHN) {
    if (validateData){
      DataValidator.validateData((Object)FOLLOWJOHN,"FOLLOWJOHN",4,10,0);
    }
    modifiedColumns.add("FOLLOWJOHN");
    this.FOLLOWJOHN = FOLLOWJOHN;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Measurementcounter o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.DATANAME == null) || (o.DATANAME == null)) && (this.DATANAME != o.DATANAME))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.DATANAME != null) && (o.DATANAME != null)) && (this.DATANAME.equals(o.DATANAME) == false))
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
  public boolean equals(final Measurementcounter o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.DATANAME == null) || (o.DATANAME == null)) && (this.DATANAME != o.DATANAME))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.TIMEAGGREGATION == null) || (o.TIMEAGGREGATION == null)) && (this.TIMEAGGREGATION != o.TIMEAGGREGATION))
            || (((this.GROUPAGGREGATION == null) || (o.GROUPAGGREGATION == null)) && (this.GROUPAGGREGATION != o.GROUPAGGREGATION))
            || (((this.COUNTAGGREGATION == null) || (o.COUNTAGGREGATION == null)) && (this.COUNTAGGREGATION != o.COUNTAGGREGATION))
            || (((this.COLNUMBER == null) || (o.COLNUMBER == null)) && (this.COLNUMBER != o.COLNUMBER))
            || (((this.DATATYPE == null) || (o.DATATYPE == null)) && (this.DATATYPE != o.DATATYPE))
            || (((this.DATASIZE == null) || (o.DATASIZE == null)) && (this.DATASIZE != o.DATASIZE))
            || (((this.DATASCALE == null) || (o.DATASCALE == null)) && (this.DATASCALE != o.DATASCALE))
            || (((this.INCLUDESQL == null) || (o.INCLUDESQL == null)) && (this.INCLUDESQL != o.INCLUDESQL))
            || (((this.UNIVOBJECT == null) || (o.UNIVOBJECT == null)) && (this.UNIVOBJECT != o.UNIVOBJECT))
            || (((this.UNIVCLASS == null) || (o.UNIVCLASS == null)) && (this.UNIVCLASS != o.UNIVCLASS))
            || (((this.COUNTERTYPE == null) || (o.COUNTERTYPE == null)) && (this.COUNTERTYPE != o.COUNTERTYPE))
            || (((this.COUNTERPROCESS == null) || (o.COUNTERPROCESS == null)) && (this.COUNTERPROCESS != o.COUNTERPROCESS))
            || (((this.DATAID == null) || (o.DATAID == null)) && (this.DATAID != o.DATAID))
            || (((this.FOLLOWJOHN == null) || (o.FOLLOWJOHN == null)) && (this.FOLLOWJOHN != o.FOLLOWJOHN))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.DATANAME != null) && (o.DATANAME != null)) && (this.DATANAME.equals(o.DATANAME) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.TIMEAGGREGATION != null) && (o.TIMEAGGREGATION != null)) && (this.TIMEAGGREGATION.equals(o.TIMEAGGREGATION) == false))
            || (((this.GROUPAGGREGATION != null) && (o.GROUPAGGREGATION != null)) && (this.GROUPAGGREGATION.equals(o.GROUPAGGREGATION) == false))
            || (((this.COUNTAGGREGATION != null) && (o.COUNTAGGREGATION != null)) && (this.COUNTAGGREGATION.equals(o.COUNTAGGREGATION) == false))
            || (((this.COLNUMBER != null) && (o.COLNUMBER != null)) && (this.COLNUMBER.equals(o.COLNUMBER) == false))
            || (((this.DATATYPE != null) && (o.DATATYPE != null)) && (this.DATATYPE.equals(o.DATATYPE) == false))
            || (((this.DATASIZE != null) && (o.DATASIZE != null)) && (this.DATASIZE.equals(o.DATASIZE) == false))
            || (((this.DATASCALE != null) && (o.DATASCALE != null)) && (this.DATASCALE.equals(o.DATASCALE) == false))
            || (((this.INCLUDESQL != null) && (o.INCLUDESQL != null)) && (this.INCLUDESQL.equals(o.INCLUDESQL) == false))
            || (((this.UNIVOBJECT != null) && (o.UNIVOBJECT != null)) && (this.UNIVOBJECT.equals(o.UNIVOBJECT) == false))
            || (((this.UNIVCLASS != null) && (o.UNIVCLASS != null)) && (this.UNIVCLASS.equals(o.UNIVCLASS) == false))
            || (((this.COUNTERTYPE != null) && (o.COUNTERTYPE != null)) && (this.COUNTERTYPE.equals(o.COUNTERTYPE) == false))
            || (((this.COUNTERPROCESS != null) && (o.COUNTERPROCESS != null)) && (this.COUNTERPROCESS.equals(o.COUNTERPROCESS) == false))
            || (((this.DATAID != null) && (o.DATAID != null)) && (this.DATAID.equals(o.DATAID) == false))
            || (((this.FOLLOWJOHN != null) && (o.FOLLOWJOHN != null)) && (this.FOLLOWJOHN.equals(o.FOLLOWJOHN) == false))
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
  * return 128
  */
  public static int getDatanameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatanameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDatanameSQLType() {
    
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
  public static int getTimeaggregationColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTimeaggregationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTimeaggregationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getGroupaggregationColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getGroupaggregationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getGroupaggregationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getCountaggregationColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCountaggregationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCountaggregationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getColnumberColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColnumberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColnumberSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getDatatypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatatypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDatatypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getDatasizeColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatasizeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getDatasizeSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getDatascaleColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatascaleDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getDatascaleSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getIncludesqlColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIncludesqlDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getIncludesqlSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getUnivobjectColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUnivobjectDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUnivobjectSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 35
  */
  public static int getUnivclassColumnSize() {
    
     return 35;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUnivclassDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUnivclassSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getCountertypeColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCountertypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCountertypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getCounterprocessColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCounterprocessDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCounterprocessSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getDataidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDataidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDataidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getFollowjohnColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFollowjohnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getFollowjohnSQLType() {
    
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

  public Measurementcounter getOriginal() {
    return original;
  }
   
  public void setOriginal(final Measurementcounter original) {
    this.original = (Measurementcounter) original.clone();
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
