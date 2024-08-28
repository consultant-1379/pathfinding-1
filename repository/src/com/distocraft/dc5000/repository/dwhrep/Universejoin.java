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
public class Universejoin implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String SOURCETABLE;
    private String SOURCELEVEL;
    private String SOURCECOLUMN;
    private String TARGETTABLE;
    private String TARGETLEVEL;
    private String TARGETCOLUMN;
    private String EXPRESSION;
    private String CARDINALITY;
    private String CONTEXT;
    private String EXCLUDEDCONTEXTS;
    private Integer TMPCOUNTER;
    private Long ORDERNRO;
    private String UNIVERSEEXTENSION;
    
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"SOURCETABLE"    ,"SOURCECOLUMN"    ,"TARGETTABLE"    ,"TARGETCOLUMN"    ,"TMPCOUNTER"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Universejoin original; 

  public Universejoin(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Universejoin(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.SOURCETABLE = null;
         this.SOURCELEVEL = null;
         this.SOURCECOLUMN = null;
         this.TARGETTABLE = null;
         this.TARGETLEVEL = null;
         this.TARGETCOLUMN = null;
         this.EXPRESSION = null;
         this.CARDINALITY = null;
         this.CONTEXT = null;
         this.EXCLUDEDCONTEXTS = null;
         this.TMPCOUNTER = null;
         this.ORDERNRO = null;
         this.UNIVERSEEXTENSION = null;
         original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Universejoin(final RockFactory rockFact   ,final String VERSIONID ,final String SOURCETABLE ,final String SOURCECOLUMN ,final String TARGETTABLE ,final String TARGETCOLUMN ,final Integer TMPCOUNTER ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.SOURCETABLE = SOURCETABLE;
            this.SOURCECOLUMN = SOURCECOLUMN;
            this.TARGETTABLE = TARGETTABLE;
            this.TARGETCOLUMN = TARGETCOLUMN;
            this.TMPCOUNTER = TMPCOUNTER;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Universejoin> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Universejoin o = it.next();

              this.VERSIONID = o.getVersionid();
              this.SOURCETABLE = o.getSourcetable();
              this.SOURCELEVEL = o.getSourcelevel();
              this.SOURCECOLUMN = o.getSourcecolumn();
              this.TARGETTABLE = o.getTargettable();
              this.TARGETLEVEL = o.getTargetlevel();
              this.TARGETCOLUMN = o.getTargetcolumn();
              this.EXPRESSION = o.getExpression();
              this.CARDINALITY = o.getCardinality();
              this.CONTEXT = o.getContext();
              this.EXCLUDEDCONTEXTS = o.getExcludedcontexts();
              this.TMPCOUNTER = o.getTmpcounter();
              this.ORDERNRO = o.getOrdernro();
              this.UNIVERSEEXTENSION = o.getUniverseextension();
              
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universejoin");
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
  public Universejoin(final RockFactory rockFact, final Universejoin whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Universejoin> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Universejoin o = it.next();
                this.VERSIONID = o.getVersionid();
                this.SOURCETABLE = o.getSourcetable();
                this.SOURCELEVEL = o.getSourcelevel();
                this.SOURCECOLUMN = o.getSourcecolumn();
                this.TARGETTABLE = o.getTargettable();
                this.TARGETLEVEL = o.getTargetlevel();
                this.TARGETCOLUMN = o.getTargetcolumn();
                this.EXPRESSION = o.getExpression();
                this.CARDINALITY = o.getCardinality();
                this.CONTEXT = o.getContext();
                this.EXCLUDEDCONTEXTS = o.getExcludedcontexts();
                this.TMPCOUNTER = o.getTmpcounter();
                this.ORDERNRO = o.getOrdernro();
                this.UNIVERSEEXTENSION = o.getUniverseextension();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universejoin");
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
    return "Universejoin";
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
  public int updateDB(final boolean useTimestamp, final Universejoin whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Universejoin whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Universejoin.saveDB(), no primary key defined");
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
    sbuff.append("<Universejoin ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("SOURCETABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCETABLE),12, true)+"\" ");
        sbuff.append("SOURCELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCELEVEL),12, true)+"\" ");
        sbuff.append("SOURCECOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCECOLUMN),12, true)+"\" ");
        sbuff.append("TARGETTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETTABLE),12, true)+"\" ");
        sbuff.append("TARGETLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETLEVEL),12, true)+"\" ");
        sbuff.append("TARGETCOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETCOLUMN),12, true)+"\" ");
        sbuff.append("EXPRESSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXPRESSION),12, true)+"\" ");
        sbuff.append("CARDINALITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CARDINALITY),12, true)+"\" ");
        sbuff.append("CONTEXT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONTEXT),12, true)+"\" ");
        sbuff.append("EXCLUDEDCONTEXTS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXCLUDEDCONTEXTS),12, true)+"\" ");
        sbuff.append("TMPCOUNTER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TMPCOUNTER),4, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
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
    sbuff.append("<Universejoin ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("SOURCETABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCETABLE),12, true)+"\" ");
        sbuff.append("SOURCELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCELEVEL),12, true)+"\" ");
        sbuff.append("SOURCECOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCECOLUMN),12, true)+"\" ");
        sbuff.append("TARGETTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETTABLE),12, true)+"\" ");
        sbuff.append("TARGETLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETLEVEL),12, true)+"\" ");
        sbuff.append("TARGETCOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGETCOLUMN),12, true)+"\" ");
        sbuff.append("EXPRESSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXPRESSION),12, true)+"\" ");
        sbuff.append("CARDINALITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CARDINALITY),12, true)+"\" ");
        sbuff.append("CONTEXT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONTEXT),12, true)+"\" ");
        sbuff.append("EXCLUDEDCONTEXTS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXCLUDEDCONTEXTS),12, true)+"\" ");
        sbuff.append("TMPCOUNTER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TMPCOUNTER),4, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
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
    sbuff.append("</Universejoin>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Universejoin ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", SOURCETABLE");
	    		sbuff.append(", SOURCELEVEL");
	    		sbuff.append(", SOURCECOLUMN");
	    		sbuff.append(", TARGETTABLE");
	    		sbuff.append(", TARGETLEVEL");
	    		sbuff.append(", TARGETCOLUMN");
	    		sbuff.append(", EXPRESSION");
	    		sbuff.append(", CARDINALITY");
	    		sbuff.append(", CONTEXT");
	    		sbuff.append(", EXCLUDEDCONTEXTS");
	    		sbuff.append(", TMPCOUNTER");
	    		sbuff.append(", ORDERNRO");
	    		sbuff.append(", UNIVERSEEXTENSION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCETABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCELEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCECOLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGETTABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGETLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGETCOLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXPRESSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CARDINALITY,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONTEXT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXCLUDEDCONTEXTS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TMPCOUNTER,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ORDERNRO,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getSourcetable() { 
    return this.SOURCETABLE;
  }
   public String getSourcelevel() { 
    return this.SOURCELEVEL;
  }
   public String getSourcecolumn() { 
    return this.SOURCECOLUMN;
  }
   public String getTargettable() { 
    return this.TARGETTABLE;
  }
   public String getTargetlevel() { 
    return this.TARGETLEVEL;
  }
   public String getTargetcolumn() { 
    return this.TARGETCOLUMN;
  }
   public String getExpression() { 
    return this.EXPRESSION;
  }
   public String getCardinality() { 
    return this.CARDINALITY;
  }
   public String getContext() { 
    return this.CONTEXT;
  }
   public String getExcludedcontexts() { 
    return this.EXCLUDEDCONTEXTS;
  }
   public Integer getTmpcounter() { 
    return this.TMPCOUNTER;
  }
   public Long getOrdernro() { 
    return this.ORDERNRO;
  }
 
  public String getUniverseextension() { 
	     return this.UNIVERSEEXTENSION;
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
     if (SOURCETABLE == null) {
      SOURCETABLE = "";
    }
     if (SOURCELEVEL == null) {
      SOURCELEVEL = "";
    }
     if (SOURCECOLUMN == null) {
      SOURCECOLUMN = "";
    }
     if (TARGETTABLE == null) {
      TARGETTABLE = "";
    }
     if (TARGETLEVEL == null) {
      TARGETLEVEL = "";
    }
     if (TARGETCOLUMN == null) {
      TARGETCOLUMN = "";
    }
     if (EXPRESSION == null) {
      EXPRESSION = "";
    }
     if (CARDINALITY == null) {
      CARDINALITY = "";
    }
     if (CONTEXT == null) {
      CONTEXT = "";
    }
     if (EXCLUDEDCONTEXTS == null) {
      EXCLUDEDCONTEXTS = "";
    }
     if (TMPCOUNTER == null) {
      TMPCOUNTER = new Integer (0);
    }
     if (ORDERNRO == null) {
      ORDERNRO = new Long (0);
    }
     if (UNIVERSEEXTENSION == null){
    	 UNIVERSEEXTENSION = "";
     }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setSourcetable(final String SOURCETABLE) {
    if (validateData){
      DataValidator.validateData((Object)SOURCETABLE,"SOURCETABLE",12,32000,0);
    }
    modifiedColumns.add("SOURCETABLE");
    this.SOURCETABLE = SOURCETABLE;
  }
   public void setSourcelevel(final String SOURCELEVEL) {
    if (validateData){
      DataValidator.validateData((Object)SOURCELEVEL,"SOURCELEVEL",12,255,0);
    }
    modifiedColumns.add("SOURCELEVEL");
    this.SOURCELEVEL = SOURCELEVEL;
  }
   public void setSourcecolumn(final String SOURCECOLUMN) {
    if (validateData){
      DataValidator.validateData((Object)SOURCECOLUMN,"SOURCECOLUMN",12,255,0);
    }
    modifiedColumns.add("SOURCECOLUMN");
    this.SOURCECOLUMN = SOURCECOLUMN;
  }
   public void setTargettable(final String TARGETTABLE) {
    if (validateData){
      DataValidator.validateData((Object)TARGETTABLE,"TARGETTABLE",12,32000,0);
    }
    modifiedColumns.add("TARGETTABLE");
    this.TARGETTABLE = TARGETTABLE;
  }
   public void setTargetlevel(final String TARGETLEVEL) {
    if (validateData){
      DataValidator.validateData((Object)TARGETLEVEL,"TARGETLEVEL",12,255,0);
    }
    modifiedColumns.add("TARGETLEVEL");
    this.TARGETLEVEL = TARGETLEVEL;
  }
   public void setTargetcolumn(final String TARGETCOLUMN) {
    if (validateData){
      DataValidator.validateData((Object)TARGETCOLUMN,"TARGETCOLUMN",12,255,0);
    }
    modifiedColumns.add("TARGETCOLUMN");
    this.TARGETCOLUMN = TARGETCOLUMN;
  }
   public void setExpression(final String EXPRESSION) {
    if (validateData){
      DataValidator.validateData((Object)EXPRESSION,"EXPRESSION",12,255,0);
    }
    modifiedColumns.add("EXPRESSION");
    this.EXPRESSION = EXPRESSION;
  }
   public void setCardinality(final String CARDINALITY) {
    if (validateData){
      DataValidator.validateData((Object)CARDINALITY,"CARDINALITY",12,255,0);
    }
    modifiedColumns.add("CARDINALITY");
    this.CARDINALITY = CARDINALITY;
  }
   public void setContext(final String CONTEXT) {
    if (validateData){
      DataValidator.validateData((Object)CONTEXT,"CONTEXT",12,32000,0);
    }
    modifiedColumns.add("CONTEXT");
    this.CONTEXT = CONTEXT;
  }
   public void setExcludedcontexts(final String EXCLUDEDCONTEXTS) {
    if (validateData){
      DataValidator.validateData((Object)EXCLUDEDCONTEXTS,"EXCLUDEDCONTEXTS",12,32000,0);
    }
    modifiedColumns.add("EXCLUDEDCONTEXTS");
    this.EXCLUDEDCONTEXTS = EXCLUDEDCONTEXTS;
  }
   public void setTmpcounter(final Integer TMPCOUNTER) {
    if (validateData){
      DataValidator.validateData((Object)TMPCOUNTER,"TMPCOUNTER",4,10,0);
    }
    modifiedColumns.add("TMPCOUNTER");
    this.TMPCOUNTER = TMPCOUNTER;
  }
   public void setOrdernro(final Long ORDERNRO) {
    if (validateData){
      DataValidator.validateData((Object)ORDERNRO,"ORDERNRO",2,30,6);
    }
    modifiedColumns.add("ORDERNRO");
    this.ORDERNRO = ORDERNRO;
  }
   public void setUniverseextension(String UNIVERSEEXTENSION) {
	     if (validateData){
	       DataValidator.validateData((Object)UNIVERSEEXTENSION,"UNIVERSEEXTENSION",12,12,0);
	     }
	     modifiedColumns.add("UNIVERSEEXTENSION");
	     this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
	   }

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Universejoin o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.SOURCETABLE == null) || (o.SOURCETABLE == null)) && (this.SOURCETABLE != o.SOURCETABLE))
            || (((this.SOURCECOLUMN == null) || (o.SOURCECOLUMN == null)) && (this.SOURCECOLUMN != o.SOURCECOLUMN))
            || (((this.TARGETTABLE == null) || (o.TARGETTABLE == null)) && (this.TARGETTABLE != o.TARGETTABLE))
            || (((this.TARGETCOLUMN == null) || (o.TARGETCOLUMN == null)) && (this.TARGETCOLUMN != o.TARGETCOLUMN))
            || (((this.TMPCOUNTER == null) || (o.TMPCOUNTER == null)) && (this.TMPCOUNTER != o.TMPCOUNTER))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.SOURCETABLE != null) && (o.SOURCETABLE != null)) && (this.SOURCETABLE.equals(o.SOURCETABLE) == false))
            || (((this.SOURCECOLUMN != null) && (o.SOURCECOLUMN != null)) && (this.SOURCECOLUMN.equals(o.SOURCECOLUMN) == false))
            || (((this.TARGETTABLE != null) && (o.TARGETTABLE != null)) && (this.TARGETTABLE.equals(o.TARGETTABLE) == false))
            || (((this.TARGETCOLUMN != null) && (o.TARGETCOLUMN != null)) && (this.TARGETCOLUMN.equals(o.TARGETCOLUMN) == false))
            || (((this.TMPCOUNTER != null) && (o.TMPCOUNTER != null)) && (this.TMPCOUNTER.equals(o.TMPCOUNTER) == false))
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
  public boolean equals(final Universejoin o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.SOURCETABLE == null) || (o.SOURCETABLE == null)) && (this.SOURCETABLE != o.SOURCETABLE))
            || (((this.SOURCELEVEL == null) || (o.SOURCELEVEL == null)) && (this.SOURCELEVEL != o.SOURCELEVEL))
            || (((this.SOURCECOLUMN == null) || (o.SOURCECOLUMN == null)) && (this.SOURCECOLUMN != o.SOURCECOLUMN))
            || (((this.TARGETTABLE == null) || (o.TARGETTABLE == null)) && (this.TARGETTABLE != o.TARGETTABLE))
            || (((this.TARGETLEVEL == null) || (o.TARGETLEVEL == null)) && (this.TARGETLEVEL != o.TARGETLEVEL))
            || (((this.TARGETCOLUMN == null) || (o.TARGETCOLUMN == null)) && (this.TARGETCOLUMN != o.TARGETCOLUMN))
            || (((this.EXPRESSION == null) || (o.EXPRESSION == null)) && (this.EXPRESSION != o.EXPRESSION))
            || (((this.CARDINALITY == null) || (o.CARDINALITY == null)) && (this.CARDINALITY != o.CARDINALITY))
            || (((this.CONTEXT == null) || (o.CONTEXT == null)) && (this.CONTEXT != o.CONTEXT))
            || (((this.EXCLUDEDCONTEXTS == null) || (o.EXCLUDEDCONTEXTS == null)) && (this.EXCLUDEDCONTEXTS != o.EXCLUDEDCONTEXTS))
            || (((this.TMPCOUNTER == null) || (o.TMPCOUNTER == null)) && (this.TMPCOUNTER != o.TMPCOUNTER))
            || (((this.ORDERNRO == null) || (o.ORDERNRO == null)) && (this.ORDERNRO != o.ORDERNRO))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.SOURCETABLE != null) && (o.SOURCETABLE != null)) && (this.SOURCETABLE.equals(o.SOURCETABLE) == false))
            || (((this.SOURCELEVEL != null) && (o.SOURCELEVEL != null)) && (this.SOURCELEVEL.equals(o.SOURCELEVEL) == false))
            || (((this.SOURCECOLUMN != null) && (o.SOURCECOLUMN != null)) && (this.SOURCECOLUMN.equals(o.SOURCECOLUMN) == false))
            || (((this.TARGETTABLE != null) && (o.TARGETTABLE != null)) && (this.TARGETTABLE.equals(o.TARGETTABLE) == false))
            || (((this.TARGETLEVEL != null) && (o.TARGETLEVEL != null)) && (this.TARGETLEVEL.equals(o.TARGETLEVEL) == false))
            || (((this.TARGETCOLUMN != null) && (o.TARGETCOLUMN != null)) && (this.TARGETCOLUMN.equals(o.TARGETCOLUMN) == false))
            || (((this.EXPRESSION != null) && (o.EXPRESSION != null)) && (this.EXPRESSION.equals(o.EXPRESSION) == false))
            || (((this.CARDINALITY != null) && (o.CARDINALITY != null)) && (this.CARDINALITY.equals(o.CARDINALITY) == false))
            || (((this.CONTEXT != null) && (o.CONTEXT != null)) && (this.CONTEXT.equals(o.CONTEXT) == false))
            || (((this.EXCLUDEDCONTEXTS != null) && (o.EXCLUDEDCONTEXTS != null)) && (this.EXCLUDEDCONTEXTS.equals(o.EXCLUDEDCONTEXTS) == false))
            || (((this.TMPCOUNTER != null) && (o.TMPCOUNTER != null)) && (this.TMPCOUNTER.equals(o.TMPCOUNTER) == false))
            || (((this.ORDERNRO != null) && (o.ORDERNRO != null)) && (this.ORDERNRO.equals(o.ORDERNRO) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
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
  * return 32000
  */
  public static int getSourcetableColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSourcetableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSourcetableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getSourcelevelColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSourcelevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSourcelevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getSourcecolumnColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSourcecolumnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSourcecolumnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getTargettableColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTargettableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTargettableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getTargetlevelColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTargetlevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTargetlevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getTargetcolumnColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTargetcolumnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTargetcolumnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getExpressionColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExpressionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getExpressionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getCardinalityColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCardinalityDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCardinalitySQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getContextColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getContextDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getContextSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getExcludedcontextsColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExcludedcontextsDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getExcludedcontextsSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getTmpcounterColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTmpcounterDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getTmpcounterSQLType() {
    
    return 4;   
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

  public Universejoin getOriginal() {
    return original;
  }
   
  public void setOriginal(final Universejoin original) {
    this.original = (Universejoin) original.clone();
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
  
  /**
   * get SQLType
   * return 12
   */
   public static int getTablenameSQLType() {
     
     return 12;   
   }
     
  
   /**
   * get columnSize
   * return 12
   */
   public static int getUniverseextensionColumnSize() {
     
      return 12;   
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
   
}
