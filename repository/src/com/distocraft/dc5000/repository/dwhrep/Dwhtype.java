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
public class Dwhtype implements Cloneable,RockDBObject  {

    private String TECHPACK_NAME;
    private String TYPENAME;
    private String TABLELEVEL;
    private String STORAGEID;
    private Long PARTITIONSIZE;
    private Long PARTITIONCOUNT;
    private String STATUS;
    private String TYPE;
    private String OWNER;
    private String VIEWTEMPLATE;
    private String CREATETEMPLATE;
    private Timestamp NEXTPARTITIONTIME;
    private String BASETABLENAME;
    private String DATADATECOLUMN;
    private String PUBLICVIEWTEMPLATE;
    private String PARTITIONPLAN;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "STORAGEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Dwhtype original; 

  public Dwhtype(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Dwhtype(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TECHPACK_NAME = null;
         this.TYPENAME = null;
         this.TABLELEVEL = null;
         this.STORAGEID = null;
         this.PARTITIONSIZE = null;
         this.PARTITIONCOUNT = null;
         this.STATUS = null;
         this.TYPE = null;
         this.OWNER = null;
         this.VIEWTEMPLATE = null;
         this.CREATETEMPLATE = null;
         this.NEXTPARTITIONTIME = null;
         this.BASETABLENAME = null;
         this.DATADATECOLUMN = null;
         this.PUBLICVIEWTEMPLATE = null;
         this.PARTITIONPLAN = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Dwhtype(final RockFactory rockFact   ,final String STORAGEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.STORAGEID = STORAGEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Dwhtype> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Dwhtype o = it.next();

              this.TECHPACK_NAME = o.getTechpack_name();
              this.TYPENAME = o.getTypename();
              this.TABLELEVEL = o.getTablelevel();
              this.STORAGEID = o.getStorageid();
              this.PARTITIONSIZE = o.getPartitionsize();
              this.PARTITIONCOUNT = o.getPartitioncount();
              this.STATUS = o.getStatus();
              this.TYPE = o.getType();
              this.OWNER = o.getOwner();
              this.VIEWTEMPLATE = o.getViewtemplate();
              this.CREATETEMPLATE = o.getCreatetemplate();
              this.NEXTPARTITIONTIME = o.getNextpartitiontime();
              this.BASETABLENAME = o.getBasetablename();
              this.DATADATECOLUMN = o.getDatadatecolumn();
              this.PUBLICVIEWTEMPLATE = o.getPublicviewtemplate();
              this.PARTITIONPLAN = o.getPartitionplan();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhtype");
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
  public Dwhtype(final RockFactory rockFact, final Dwhtype whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Dwhtype> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Dwhtype o = it.next();
                this.TECHPACK_NAME = o.getTechpack_name();
                this.TYPENAME = o.getTypename();
                this.TABLELEVEL = o.getTablelevel();
                this.STORAGEID = o.getStorageid();
                this.PARTITIONSIZE = o.getPartitionsize();
                this.PARTITIONCOUNT = o.getPartitioncount();
                this.STATUS = o.getStatus();
                this.TYPE = o.getType();
                this.OWNER = o.getOwner();
                this.VIEWTEMPLATE = o.getViewtemplate();
                this.CREATETEMPLATE = o.getCreatetemplate();
                this.NEXTPARTITIONTIME = o.getNextpartitiontime();
                this.BASETABLENAME = o.getBasetablename();
                this.DATADATECOLUMN = o.getDatadatecolumn();
                this.PUBLICVIEWTEMPLATE = o.getPublicviewtemplate();
                this.PARTITIONPLAN = o.getPartitionplan();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhtype");
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
    return "Dwhtype";
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
  public int updateDB(final boolean useTimestamp, final Dwhtype whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Dwhtype whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Dwhtype.saveDB(), no primary key defined");
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
    sbuff.append("<Dwhtype ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("PARTITIONSIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONSIZE),2, true)+"\" ");
        sbuff.append("PARTITIONCOUNT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONCOUNT),2, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("OWNER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OWNER),12, true)+"\" ");
        sbuff.append("VIEWTEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VIEWTEMPLATE),12, true)+"\" ");
        sbuff.append("CREATETEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CREATETEMPLATE),12, true)+"\" ");
        sbuff.append("NEXTPARTITIONTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NEXTPARTITIONTIME),93, true)+"\" ");
        sbuff.append("BASETABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASETABLENAME),12, true)+"\" ");
        sbuff.append("DATADATECOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATADATECOLUMN),12, true)+"\" ");
        sbuff.append("PUBLICVIEWTEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PUBLICVIEWTEMPLATE),12, true)+"\" ");
        sbuff.append("PARTITIONPLAN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONPLAN),12, true)+"\" ");
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
    sbuff.append("<Dwhtype ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("PARTITIONSIZE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONSIZE),2, true)+"\" ");
        sbuff.append("PARTITIONCOUNT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONCOUNT),2, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("OWNER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OWNER),12, true)+"\" ");
        sbuff.append("VIEWTEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VIEWTEMPLATE),12, true)+"\" ");
        sbuff.append("CREATETEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CREATETEMPLATE),12, true)+"\" ");
        sbuff.append("NEXTPARTITIONTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NEXTPARTITIONTIME),93, true)+"\" ");
        sbuff.append("BASETABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASETABLENAME),12, true)+"\" ");
        sbuff.append("DATADATECOLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATADATECOLUMN),12, true)+"\" ");
        sbuff.append("PUBLICVIEWTEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PUBLICVIEWTEMPLATE),12, true)+"\" ");
        sbuff.append("PARTITIONPLAN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONPLAN),12, true)+"\" ");
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
    sbuff.append("</Dwhtype>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Dwhtype ( ");
	    		sbuff.append("TECHPACK_NAME");
		    		sbuff.append(", TYPENAME");
	    		sbuff.append(", TABLELEVEL");
	    		sbuff.append(", STORAGEID");
	    		sbuff.append(", PARTITIONSIZE");
	    		sbuff.append(", PARTITIONCOUNT");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", TYPE");
	    		sbuff.append(", OWNER");
	    		sbuff.append(", VIEWTEMPLATE");
	    		sbuff.append(", CREATETEMPLATE");
	    		sbuff.append(", NEXTPARTITIONTIME");
	    		sbuff.append(", BASETABLENAME");
	    		sbuff.append(", DATADATECOLUMN");
	    		sbuff.append(", PUBLICVIEWTEMPLATE");
	    		sbuff.append(", PARTITIONPLAN");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TECHPACK_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TYPENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLELEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STORAGEID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARTITIONSIZE,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARTITIONCOUNT,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OWNER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VIEWTEMPLATE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CREATETEMPLATE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.NEXTPARTITIONTIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASETABLENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATADATECOLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PUBLICVIEWTEMPLATE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARTITIONPLAN,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTechpack_name() { 
    return this.TECHPACK_NAME;
  }
   public String getTypename() { 
    return this.TYPENAME;
  }
   public String getTablelevel() { 
    return this.TABLELEVEL;
  }
   public String getStorageid() { 
    return this.STORAGEID;
  }
   public Long getPartitionsize() { 
    return this.PARTITIONSIZE;
  }
   public Long getPartitioncount() { 
    return this.PARTITIONCOUNT;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public String getType() { 
    return this.TYPE;
  }
   public String getOwner() { 
    return this.OWNER;
  }
   public String getViewtemplate() { 
    return this.VIEWTEMPLATE;
  }
   public String getCreatetemplate() { 
    return this.CREATETEMPLATE;
  }
   public Timestamp getNextpartitiontime() { 
    return this.NEXTPARTITIONTIME;
  }
   public String getBasetablename() { 
    return this.BASETABLENAME;
  }
   public String getDatadatecolumn() { 
    return this.DATADATECOLUMN;
  }
   public String getPublicviewtemplate() { 
    return this.PUBLICVIEWTEMPLATE;
  }
   public String getPartitionplan() { 
    return this.PARTITIONPLAN;
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
     if (TECHPACK_NAME == null) {
      TECHPACK_NAME = "";
    }
     if (TYPENAME == null) {
      TYPENAME = "";
    }
     if (TABLELEVEL == null) {
      TABLELEVEL = "";
    }
     if (STORAGEID == null) {
      STORAGEID = "";
    }
     if (PARTITIONSIZE == null) {
      PARTITIONSIZE = new Long (0);
    }
     if (PARTITIONCOUNT == null) {
      PARTITIONCOUNT = new Long (0);
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (TYPE == null) {
      TYPE = "";
    }
     if (OWNER == null) {
      OWNER = "";
    }
     if (VIEWTEMPLATE == null) {
      VIEWTEMPLATE = "";
    }
     if (CREATETEMPLATE == null) {
      CREATETEMPLATE = "";
    }
     if (NEXTPARTITIONTIME == null) {
      NEXTPARTITIONTIME = new Timestamp (0);
    }
     if (BASETABLENAME == null) {
      BASETABLENAME = "";
    }
     if (DATADATECOLUMN == null) {
      DATADATECOLUMN = "";
    }
     if (PUBLICVIEWTEMPLATE == null) {
      PUBLICVIEWTEMPLATE = "";
    }
     if (PARTITIONPLAN == null) {
      PARTITIONPLAN = "";
    }
   }

   public void setTechpack_name(final String TECHPACK_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_NAME,"TECHPACK_NAME",12,30,0);
    }
    modifiedColumns.add("TECHPACK_NAME");
    this.TECHPACK_NAME = TECHPACK_NAME;
  }
   public void setTypename(final String TYPENAME) {
    if (validateData){
      DataValidator.validateData((Object)TYPENAME,"TYPENAME",12,255,0);
    }
    modifiedColumns.add("TYPENAME");
    this.TYPENAME = TYPENAME;
  }
   public void setTablelevel(final String TABLELEVEL) {
    if (validateData){
      DataValidator.validateData((Object)TABLELEVEL,"TABLELEVEL",12,50,0);
    }
    modifiedColumns.add("TABLELEVEL");
    this.TABLELEVEL = TABLELEVEL;
  }
   public void setStorageid(final String STORAGEID) {
    if (validateData){
      DataValidator.validateData((Object)STORAGEID,"STORAGEID",12,255,0);
    }
    modifiedColumns.add("STORAGEID");
    this.STORAGEID = STORAGEID;
  }
   public void setPartitionsize(final Long PARTITIONSIZE) {
    if (validateData){
      DataValidator.validateData((Object)PARTITIONSIZE,"PARTITIONSIZE",2,9,0);
    }
    modifiedColumns.add("PARTITIONSIZE");
    this.PARTITIONSIZE = PARTITIONSIZE;
  }
   public void setPartitioncount(final Long PARTITIONCOUNT) {
    if (validateData){
      DataValidator.validateData((Object)PARTITIONCOUNT,"PARTITIONCOUNT",2,9,0);
    }
    modifiedColumns.add("PARTITIONCOUNT");
    this.PARTITIONCOUNT = PARTITIONCOUNT;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,50,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setType(final String TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TYPE,"TYPE",12,50,0);
    }
    modifiedColumns.add("TYPE");
    this.TYPE = TYPE;
  }
   public void setOwner(final String OWNER) {
    if (validateData){
      DataValidator.validateData((Object)OWNER,"OWNER",12,50,0);
    }
    modifiedColumns.add("OWNER");
    this.OWNER = OWNER;
  }
   public void setViewtemplate(final String VIEWTEMPLATE) {
    if (validateData){
      DataValidator.validateData((Object)VIEWTEMPLATE,"VIEWTEMPLATE",12,255,0);
    }
    modifiedColumns.add("VIEWTEMPLATE");
    this.VIEWTEMPLATE = VIEWTEMPLATE;
  }
   public void setCreatetemplate(final String CREATETEMPLATE) {
    if (validateData){
      DataValidator.validateData((Object)CREATETEMPLATE,"CREATETEMPLATE",12,255,0);
    }
    modifiedColumns.add("CREATETEMPLATE");
    this.CREATETEMPLATE = CREATETEMPLATE;
  }
   public void setNextpartitiontime(final Timestamp NEXTPARTITIONTIME) {
    if (validateData){
      DataValidator.validateData((Object)NEXTPARTITIONTIME,"NEXTPARTITIONTIME",93,23,0);
    }
    modifiedColumns.add("NEXTPARTITIONTIME");
    this.NEXTPARTITIONTIME = NEXTPARTITIONTIME;
  }
   public void setBasetablename(final String BASETABLENAME) {
    if (validateData){
      DataValidator.validateData((Object)BASETABLENAME,"BASETABLENAME",12,125,0);
    }
    modifiedColumns.add("BASETABLENAME");
    this.BASETABLENAME = BASETABLENAME;
  }
   public void setDatadatecolumn(final String DATADATECOLUMN) {
    if (validateData){
      DataValidator.validateData((Object)DATADATECOLUMN,"DATADATECOLUMN",12,128,0);
    }
    modifiedColumns.add("DATADATECOLUMN");
    this.DATADATECOLUMN = DATADATECOLUMN;
  }
   public void setPublicviewtemplate(final String PUBLICVIEWTEMPLATE) {
    if (validateData){
      DataValidator.validateData((Object)PUBLICVIEWTEMPLATE,"PUBLICVIEWTEMPLATE",12,255,0);
    }
    modifiedColumns.add("PUBLICVIEWTEMPLATE");
    this.PUBLICVIEWTEMPLATE = PUBLICVIEWTEMPLATE;
  }
   public void setPartitionplan(final String PARTITIONPLAN) {
    if (validateData){
      DataValidator.validateData((Object)PARTITIONPLAN,"PARTITIONPLAN",12,128,0);
    }
    modifiedColumns.add("PARTITIONPLAN");
    this.PARTITIONPLAN = PARTITIONPLAN;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Dwhtype o) {

         if ((((this.STORAGEID == null) || (o.STORAGEID == null)) && (this.STORAGEID != o.STORAGEID))
          ){
    return false;
    } else
         if ((((this.STORAGEID != null) && (o.STORAGEID != null)) && (this.STORAGEID.equals(o.STORAGEID) == false))
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
  public boolean equals(final Dwhtype o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.TYPENAME == null) || (o.TYPENAME == null)) && (this.TYPENAME != o.TYPENAME))
            || (((this.TABLELEVEL == null) || (o.TABLELEVEL == null)) && (this.TABLELEVEL != o.TABLELEVEL))
            || (((this.STORAGEID == null) || (o.STORAGEID == null)) && (this.STORAGEID != o.STORAGEID))
            || (((this.PARTITIONSIZE == null) || (o.PARTITIONSIZE == null)) && (this.PARTITIONSIZE != o.PARTITIONSIZE))
            || (((this.PARTITIONCOUNT == null) || (o.PARTITIONCOUNT == null)) && (this.PARTITIONCOUNT != o.PARTITIONCOUNT))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.TYPE == null) || (o.TYPE == null)) && (this.TYPE != o.TYPE))
            || (((this.OWNER == null) || (o.OWNER == null)) && (this.OWNER != o.OWNER))
            || (((this.VIEWTEMPLATE == null) || (o.VIEWTEMPLATE == null)) && (this.VIEWTEMPLATE != o.VIEWTEMPLATE))
            || (((this.CREATETEMPLATE == null) || (o.CREATETEMPLATE == null)) && (this.CREATETEMPLATE != o.CREATETEMPLATE))
            || (((this.NEXTPARTITIONTIME == null) || (o.NEXTPARTITIONTIME == null)) && (this.NEXTPARTITIONTIME != o.NEXTPARTITIONTIME))
            || (((this.BASETABLENAME == null) || (o.BASETABLENAME == null)) && (this.BASETABLENAME != o.BASETABLENAME))
            || (((this.DATADATECOLUMN == null) || (o.DATADATECOLUMN == null)) && (this.DATADATECOLUMN != o.DATADATECOLUMN))
            || (((this.PUBLICVIEWTEMPLATE == null) || (o.PUBLICVIEWTEMPLATE == null)) && (this.PUBLICVIEWTEMPLATE != o.PUBLICVIEWTEMPLATE))
            || (((this.PARTITIONPLAN == null) || (o.PARTITIONPLAN == null)) && (this.PARTITIONPLAN != o.PARTITIONPLAN))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.TYPENAME != null) && (o.TYPENAME != null)) && (this.TYPENAME.equals(o.TYPENAME) == false))
            || (((this.TABLELEVEL != null) && (o.TABLELEVEL != null)) && (this.TABLELEVEL.equals(o.TABLELEVEL) == false))
            || (((this.STORAGEID != null) && (o.STORAGEID != null)) && (this.STORAGEID.equals(o.STORAGEID) == false))
            || (((this.PARTITIONSIZE != null) && (o.PARTITIONSIZE != null)) && (this.PARTITIONSIZE.equals(o.PARTITIONSIZE) == false))
            || (((this.PARTITIONCOUNT != null) && (o.PARTITIONCOUNT != null)) && (this.PARTITIONCOUNT.equals(o.PARTITIONCOUNT) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.TYPE != null) && (o.TYPE != null)) && (this.TYPE.equals(o.TYPE) == false))
            || (((this.OWNER != null) && (o.OWNER != null)) && (this.OWNER.equals(o.OWNER) == false))
            || (((this.VIEWTEMPLATE != null) && (o.VIEWTEMPLATE != null)) && (this.VIEWTEMPLATE.equals(o.VIEWTEMPLATE) == false))
            || (((this.CREATETEMPLATE != null) && (o.CREATETEMPLATE != null)) && (this.CREATETEMPLATE.equals(o.CREATETEMPLATE) == false))
            || (((this.NEXTPARTITIONTIME != null) && (o.NEXTPARTITIONTIME != null)) && (this.NEXTPARTITIONTIME.equals(o.NEXTPARTITIONTIME) == false))
            || (((this.BASETABLENAME != null) && (o.BASETABLENAME != null)) && (this.BASETABLENAME.equals(o.BASETABLENAME) == false))
            || (((this.DATADATECOLUMN != null) && (o.DATADATECOLUMN != null)) && (this.DATADATECOLUMN.equals(o.DATADATECOLUMN) == false))
            || (((this.PUBLICVIEWTEMPLATE != null) && (o.PUBLICVIEWTEMPLATE != null)) && (this.PUBLICVIEWTEMPLATE.equals(o.PUBLICVIEWTEMPLATE) == false))
            || (((this.PARTITIONPLAN != null) && (o.PARTITIONPLAN != null)) && (this.PARTITIONPLAN.equals(o.PARTITIONPLAN) == false))
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
  * return 255
  */
  public static int getTypenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTablelevelColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTablelevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTablelevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getStorageidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStorageidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getStorageidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getPartitionsizeColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPartitionsizeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getPartitionsizeSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getPartitioncountColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPartitioncountDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getPartitioncountSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getStatusColumnSize() {
    
     return 50;   
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
  * return 12
  */
  public static int getStatusSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getOwnerColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOwnerDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getOwnerSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getViewtemplateColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getViewtemplateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getViewtemplateSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getCreatetemplateColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCreatetemplateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getCreatetemplateSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getNextpartitiontimeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getNextpartitiontimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getNextpartitiontimeSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 125
  */
  public static int getBasetablenameColumnSize() {
    
     return 125;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBasetablenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBasetablenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getDatadatecolumnColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatadatecolumnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDatadatecolumnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getPublicviewtemplateColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPublicviewtemplateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPublicviewtemplateSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPartitionplanColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPartitionplanDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPartitionplanSQLType() {
    
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

  public Dwhtype getOriginal() {
    return original;
  }
   
  public void setOriginal(final Dwhtype original) {
    this.original = (Dwhtype) original.clone();
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
