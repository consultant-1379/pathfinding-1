package com.distocraft.dc5000.etl.rock;

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
public class Meta_parameters implements Cloneable,RockDBObject  {

    private String RB_SEGMENT_NAME;
    private String USE_RB_SEGMENT_FLAG;
    private String DEFAULT_ERROR_MAIL_ADDR;
    private String DEFAULT_FAIL_MAIL_ADDR;
    private String DEFAULT_BUG_ERROR_MAIL_ADDR;
    private Long DEFAULT_MAX_ERROR_VALUE;
    private Long DEFAULT_MAX_FK_ERROR_VALUE;
    private Long DEFAULT_MAX_COL_LIMIT_VALUE;
    private String TEMP_SUM_TABLESPACE;
    private String USE_TEMP_SUM_TABLESPACE_FLAG;
    private String BATCH_COLUMN_NAME;
    private String VERSION_NUMBER;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSION_NUMBER"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_parameters original; 

  public Meta_parameters(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_parameters(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.RB_SEGMENT_NAME = null;
         this.USE_RB_SEGMENT_FLAG = null;
         this.DEFAULT_ERROR_MAIL_ADDR = null;
         this.DEFAULT_FAIL_MAIL_ADDR = null;
         this.DEFAULT_BUG_ERROR_MAIL_ADDR = null;
         this.DEFAULT_MAX_ERROR_VALUE = null;
         this.DEFAULT_MAX_FK_ERROR_VALUE = null;
         this.DEFAULT_MAX_COL_LIMIT_VALUE = null;
         this.TEMP_SUM_TABLESPACE = null;
         this.USE_TEMP_SUM_TABLESPACE_FLAG = null;
         this.BATCH_COLUMN_NAME = null;
         this.VERSION_NUMBER = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_parameters(final RockFactory rockFact   ,final String VERSION_NUMBER ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSION_NUMBER = VERSION_NUMBER;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_parameters> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_parameters o = it.next();

              this.RB_SEGMENT_NAME = o.getRb_segment_name();
              this.USE_RB_SEGMENT_FLAG = o.getUse_rb_segment_flag();
              this.DEFAULT_ERROR_MAIL_ADDR = o.getDefault_error_mail_addr();
              this.DEFAULT_FAIL_MAIL_ADDR = o.getDefault_fail_mail_addr();
              this.DEFAULT_BUG_ERROR_MAIL_ADDR = o.getDefault_bug_error_mail_addr();
              this.DEFAULT_MAX_ERROR_VALUE = o.getDefault_max_error_value();
              this.DEFAULT_MAX_FK_ERROR_VALUE = o.getDefault_max_fk_error_value();
              this.DEFAULT_MAX_COL_LIMIT_VALUE = o.getDefault_max_col_limit_value();
              this.TEMP_SUM_TABLESPACE = o.getTemp_sum_tablespace();
              this.USE_TEMP_SUM_TABLESPACE_FLAG = o.getUse_temp_sum_tablespace_flag();
              this.BATCH_COLUMN_NAME = o.getBatch_column_name();
              this.VERSION_NUMBER = o.getVersion_number();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_parameters");
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
  public Meta_parameters(final RockFactory rockFact, final Meta_parameters whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_parameters> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_parameters o = it.next();
                this.RB_SEGMENT_NAME = o.getRb_segment_name();
                this.USE_RB_SEGMENT_FLAG = o.getUse_rb_segment_flag();
                this.DEFAULT_ERROR_MAIL_ADDR = o.getDefault_error_mail_addr();
                this.DEFAULT_FAIL_MAIL_ADDR = o.getDefault_fail_mail_addr();
                this.DEFAULT_BUG_ERROR_MAIL_ADDR = o.getDefault_bug_error_mail_addr();
                this.DEFAULT_MAX_ERROR_VALUE = o.getDefault_max_error_value();
                this.DEFAULT_MAX_FK_ERROR_VALUE = o.getDefault_max_fk_error_value();
                this.DEFAULT_MAX_COL_LIMIT_VALUE = o.getDefault_max_col_limit_value();
                this.TEMP_SUM_TABLESPACE = o.getTemp_sum_tablespace();
                this.USE_TEMP_SUM_TABLESPACE_FLAG = o.getUse_temp_sum_tablespace_flag();
                this.BATCH_COLUMN_NAME = o.getBatch_column_name();
                this.VERSION_NUMBER = o.getVersion_number();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_parameters");
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
    return "Meta_parameters";
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
  public int updateDB(final boolean useTimestamp, final Meta_parameters whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_parameters whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_parameters.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_parameters ");
        sbuff.append("RB_SEGMENT_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RB_SEGMENT_NAME),12, true)+"\" ");
        sbuff.append("USE_RB_SEGMENT_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USE_RB_SEGMENT_FLAG),1, true)+"\" ");
        sbuff.append("DEFAULT_ERROR_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_ERROR_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_FAIL_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_FAIL_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_BUG_ERROR_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_BUG_ERROR_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_MAX_ERROR_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_ERROR_VALUE),2, true)+"\" ");
        sbuff.append("DEFAULT_MAX_FK_ERROR_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_FK_ERROR_VALUE),2, true)+"\" ");
        sbuff.append("DEFAULT_MAX_COL_LIMIT_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_COL_LIMIT_VALUE),2, true)+"\" ");
        sbuff.append("TEMP_SUM_TABLESPACE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TEMP_SUM_TABLESPACE),12, true)+"\" ");
        sbuff.append("USE_TEMP_SUM_TABLESPACE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USE_TEMP_SUM_TABLESPACE_FLAG),12, true)+"\" ");
        sbuff.append("BATCH_COLUMN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BATCH_COLUMN_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
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
    sbuff.append("<Meta_parameters ");
        sbuff.append("RB_SEGMENT_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RB_SEGMENT_NAME),12, true)+"\" ");
        sbuff.append("USE_RB_SEGMENT_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USE_RB_SEGMENT_FLAG),1, true)+"\" ");
        sbuff.append("DEFAULT_ERROR_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_ERROR_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_FAIL_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_FAIL_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_BUG_ERROR_MAIL_ADDR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_BUG_ERROR_MAIL_ADDR),1, true)+"\" ");
        sbuff.append("DEFAULT_MAX_ERROR_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_ERROR_VALUE),2, true)+"\" ");
        sbuff.append("DEFAULT_MAX_FK_ERROR_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_FK_ERROR_VALUE),2, true)+"\" ");
        sbuff.append("DEFAULT_MAX_COL_LIMIT_VALUE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_MAX_COL_LIMIT_VALUE),2, true)+"\" ");
        sbuff.append("TEMP_SUM_TABLESPACE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TEMP_SUM_TABLESPACE),12, true)+"\" ");
        sbuff.append("USE_TEMP_SUM_TABLESPACE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USE_TEMP_SUM_TABLESPACE_FLAG),12, true)+"\" ");
        sbuff.append("BATCH_COLUMN_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BATCH_COLUMN_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
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
    sbuff.append("</Meta_parameters>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_parameters ( ");
	    		sbuff.append("RB_SEGMENT_NAME");
		    		sbuff.append(", USE_RB_SEGMENT_FLAG");
	    		sbuff.append(", DEFAULT_ERROR_MAIL_ADDR");
	    		sbuff.append(", DEFAULT_FAIL_MAIL_ADDR");
	    		sbuff.append(", DEFAULT_BUG_ERROR_MAIL_ADDR");
	    		sbuff.append(", DEFAULT_MAX_ERROR_VALUE");
	    		sbuff.append(", DEFAULT_MAX_FK_ERROR_VALUE");
	    		sbuff.append(", DEFAULT_MAX_COL_LIMIT_VALUE");
	    		sbuff.append(", TEMP_SUM_TABLESPACE");
	    		sbuff.append(", USE_TEMP_SUM_TABLESPACE_FLAG");
	    		sbuff.append(", BATCH_COLUMN_NAME");
	    		sbuff.append(", VERSION_NUMBER");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.RB_SEGMENT_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.USE_RB_SEGMENT_FLAG,1)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_ERROR_MAIL_ADDR,1)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_FAIL_MAIL_ADDR,1)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_BUG_ERROR_MAIL_ADDR,1)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_MAX_ERROR_VALUE,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_MAX_FK_ERROR_VALUE,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_MAX_COL_LIMIT_VALUE,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TEMP_SUM_TABLESPACE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.USE_TEMP_SUM_TABLESPACE_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BATCH_COLUMN_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getRb_segment_name() { 
    return this.RB_SEGMENT_NAME;
  }
   public String getUse_rb_segment_flag() { 
    return this.USE_RB_SEGMENT_FLAG;
  }
   public String getDefault_error_mail_addr() { 
    return this.DEFAULT_ERROR_MAIL_ADDR;
  }
   public String getDefault_fail_mail_addr() { 
    return this.DEFAULT_FAIL_MAIL_ADDR;
  }
   public String getDefault_bug_error_mail_addr() { 
    return this.DEFAULT_BUG_ERROR_MAIL_ADDR;
  }
   public Long getDefault_max_error_value() { 
    return this.DEFAULT_MAX_ERROR_VALUE;
  }
   public Long getDefault_max_fk_error_value() { 
    return this.DEFAULT_MAX_FK_ERROR_VALUE;
  }
   public Long getDefault_max_col_limit_value() { 
    return this.DEFAULT_MAX_COL_LIMIT_VALUE;
  }
   public String getTemp_sum_tablespace() { 
    return this.TEMP_SUM_TABLESPACE;
  }
   public String getUse_temp_sum_tablespace_flag() { 
    return this.USE_TEMP_SUM_TABLESPACE_FLAG;
  }
   public String getBatch_column_name() { 
    return this.BATCH_COLUMN_NAME;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
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
     if (RB_SEGMENT_NAME == null) {
      RB_SEGMENT_NAME = "";
    }
     if (USE_RB_SEGMENT_FLAG == null) {
      USE_RB_SEGMENT_FLAG = "";
    }
     if (DEFAULT_ERROR_MAIL_ADDR == null) {
      DEFAULT_ERROR_MAIL_ADDR = "";
    }
     if (DEFAULT_FAIL_MAIL_ADDR == null) {
      DEFAULT_FAIL_MAIL_ADDR = "";
    }
     if (DEFAULT_BUG_ERROR_MAIL_ADDR == null) {
      DEFAULT_BUG_ERROR_MAIL_ADDR = "";
    }
     if (DEFAULT_MAX_ERROR_VALUE == null) {
      DEFAULT_MAX_ERROR_VALUE = new Long (0);
    }
     if (DEFAULT_MAX_FK_ERROR_VALUE == null) {
      DEFAULT_MAX_FK_ERROR_VALUE = new Long (0);
    }
     if (DEFAULT_MAX_COL_LIMIT_VALUE == null) {
      DEFAULT_MAX_COL_LIMIT_VALUE = new Long (0);
    }
     if (TEMP_SUM_TABLESPACE == null) {
      TEMP_SUM_TABLESPACE = "";
    }
     if (USE_TEMP_SUM_TABLESPACE_FLAG == null) {
      USE_TEMP_SUM_TABLESPACE_FLAG = "";
    }
     if (BATCH_COLUMN_NAME == null) {
      BATCH_COLUMN_NAME = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
   }

   public void setRb_segment_name(final String RB_SEGMENT_NAME) {
    if (validateData){
      DataValidator.validateData((Object)RB_SEGMENT_NAME,"RB_SEGMENT_NAME",12,30,0);
    }
    modifiedColumns.add("RB_SEGMENT_NAME");
    this.RB_SEGMENT_NAME = RB_SEGMENT_NAME;
  }
   public void setUse_rb_segment_flag(final String USE_RB_SEGMENT_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)USE_RB_SEGMENT_FLAG,"USE_RB_SEGMENT_FLAG",1,1,0);
    }
    modifiedColumns.add("USE_RB_SEGMENT_FLAG");
    this.USE_RB_SEGMENT_FLAG = USE_RB_SEGMENT_FLAG;
  }
   public void setDefault_error_mail_addr(final String DEFAULT_ERROR_MAIL_ADDR) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_ERROR_MAIL_ADDR,"DEFAULT_ERROR_MAIL_ADDR",1,100,0);
    }
    modifiedColumns.add("DEFAULT_ERROR_MAIL_ADDR");
    this.DEFAULT_ERROR_MAIL_ADDR = DEFAULT_ERROR_MAIL_ADDR;
  }
   public void setDefault_fail_mail_addr(final String DEFAULT_FAIL_MAIL_ADDR) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_FAIL_MAIL_ADDR,"DEFAULT_FAIL_MAIL_ADDR",1,100,0);
    }
    modifiedColumns.add("DEFAULT_FAIL_MAIL_ADDR");
    this.DEFAULT_FAIL_MAIL_ADDR = DEFAULT_FAIL_MAIL_ADDR;
  }
   public void setDefault_bug_error_mail_addr(final String DEFAULT_BUG_ERROR_MAIL_ADDR) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_BUG_ERROR_MAIL_ADDR,"DEFAULT_BUG_ERROR_MAIL_ADDR",1,100,0);
    }
    modifiedColumns.add("DEFAULT_BUG_ERROR_MAIL_ADDR");
    this.DEFAULT_BUG_ERROR_MAIL_ADDR = DEFAULT_BUG_ERROR_MAIL_ADDR;
  }
   public void setDefault_max_error_value(final Long DEFAULT_MAX_ERROR_VALUE) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_MAX_ERROR_VALUE,"DEFAULT_MAX_ERROR_VALUE",2,38,0);
    }
    modifiedColumns.add("DEFAULT_MAX_ERROR_VALUE");
    this.DEFAULT_MAX_ERROR_VALUE = DEFAULT_MAX_ERROR_VALUE;
  }
   public void setDefault_max_fk_error_value(final Long DEFAULT_MAX_FK_ERROR_VALUE) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_MAX_FK_ERROR_VALUE,"DEFAULT_MAX_FK_ERROR_VALUE",2,38,0);
    }
    modifiedColumns.add("DEFAULT_MAX_FK_ERROR_VALUE");
    this.DEFAULT_MAX_FK_ERROR_VALUE = DEFAULT_MAX_FK_ERROR_VALUE;
  }
   public void setDefault_max_col_limit_value(final Long DEFAULT_MAX_COL_LIMIT_VALUE) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_MAX_COL_LIMIT_VALUE,"DEFAULT_MAX_COL_LIMIT_VALUE",2,38,0);
    }
    modifiedColumns.add("DEFAULT_MAX_COL_LIMIT_VALUE");
    this.DEFAULT_MAX_COL_LIMIT_VALUE = DEFAULT_MAX_COL_LIMIT_VALUE;
  }
   public void setTemp_sum_tablespace(final String TEMP_SUM_TABLESPACE) {
    if (validateData){
      DataValidator.validateData((Object)TEMP_SUM_TABLESPACE,"TEMP_SUM_TABLESPACE",12,30,0);
    }
    modifiedColumns.add("TEMP_SUM_TABLESPACE");
    this.TEMP_SUM_TABLESPACE = TEMP_SUM_TABLESPACE;
  }
   public void setUse_temp_sum_tablespace_flag(final String USE_TEMP_SUM_TABLESPACE_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)USE_TEMP_SUM_TABLESPACE_FLAG,"USE_TEMP_SUM_TABLESPACE_FLAG",12,1,0);
    }
    modifiedColumns.add("USE_TEMP_SUM_TABLESPACE_FLAG");
    this.USE_TEMP_SUM_TABLESPACE_FLAG = USE_TEMP_SUM_TABLESPACE_FLAG;
  }
   public void setBatch_column_name(final String BATCH_COLUMN_NAME) {
    if (validateData){
      DataValidator.validateData((Object)BATCH_COLUMN_NAME,"BATCH_COLUMN_NAME",12,30,0);
    }
    modifiedColumns.add("BATCH_COLUMN_NAME");
    this.BATCH_COLUMN_NAME = BATCH_COLUMN_NAME;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_parameters o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
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
  public boolean equals(final Meta_parameters o) {

         if ((((this.RB_SEGMENT_NAME == null) || (o.RB_SEGMENT_NAME == null)) && (this.RB_SEGMENT_NAME != o.RB_SEGMENT_NAME))
            || (((this.USE_RB_SEGMENT_FLAG == null) || (o.USE_RB_SEGMENT_FLAG == null)) && (this.USE_RB_SEGMENT_FLAG != o.USE_RB_SEGMENT_FLAG))
            || (((this.DEFAULT_ERROR_MAIL_ADDR == null) || (o.DEFAULT_ERROR_MAIL_ADDR == null)) && (this.DEFAULT_ERROR_MAIL_ADDR != o.DEFAULT_ERROR_MAIL_ADDR))
            || (((this.DEFAULT_FAIL_MAIL_ADDR == null) || (o.DEFAULT_FAIL_MAIL_ADDR == null)) && (this.DEFAULT_FAIL_MAIL_ADDR != o.DEFAULT_FAIL_MAIL_ADDR))
            || (((this.DEFAULT_BUG_ERROR_MAIL_ADDR == null) || (o.DEFAULT_BUG_ERROR_MAIL_ADDR == null)) && (this.DEFAULT_BUG_ERROR_MAIL_ADDR != o.DEFAULT_BUG_ERROR_MAIL_ADDR))
            || (((this.DEFAULT_MAX_ERROR_VALUE == null) || (o.DEFAULT_MAX_ERROR_VALUE == null)) && (this.DEFAULT_MAX_ERROR_VALUE != o.DEFAULT_MAX_ERROR_VALUE))
            || (((this.DEFAULT_MAX_FK_ERROR_VALUE == null) || (o.DEFAULT_MAX_FK_ERROR_VALUE == null)) && (this.DEFAULT_MAX_FK_ERROR_VALUE != o.DEFAULT_MAX_FK_ERROR_VALUE))
            || (((this.DEFAULT_MAX_COL_LIMIT_VALUE == null) || (o.DEFAULT_MAX_COL_LIMIT_VALUE == null)) && (this.DEFAULT_MAX_COL_LIMIT_VALUE != o.DEFAULT_MAX_COL_LIMIT_VALUE))
            || (((this.TEMP_SUM_TABLESPACE == null) || (o.TEMP_SUM_TABLESPACE == null)) && (this.TEMP_SUM_TABLESPACE != o.TEMP_SUM_TABLESPACE))
            || (((this.USE_TEMP_SUM_TABLESPACE_FLAG == null) || (o.USE_TEMP_SUM_TABLESPACE_FLAG == null)) && (this.USE_TEMP_SUM_TABLESPACE_FLAG != o.USE_TEMP_SUM_TABLESPACE_FLAG))
            || (((this.BATCH_COLUMN_NAME == null) || (o.BATCH_COLUMN_NAME == null)) && (this.BATCH_COLUMN_NAME != o.BATCH_COLUMN_NAME))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
          ){
    return false;
    } else
         if ((((this.RB_SEGMENT_NAME != null) && (o.RB_SEGMENT_NAME != null)) && (this.RB_SEGMENT_NAME.equals(o.RB_SEGMENT_NAME) == false))
            || (((this.USE_RB_SEGMENT_FLAG != null) && (o.USE_RB_SEGMENT_FLAG != null)) && (this.USE_RB_SEGMENT_FLAG.equals(o.USE_RB_SEGMENT_FLAG) == false))
            || (((this.DEFAULT_ERROR_MAIL_ADDR != null) && (o.DEFAULT_ERROR_MAIL_ADDR != null)) && (this.DEFAULT_ERROR_MAIL_ADDR.equals(o.DEFAULT_ERROR_MAIL_ADDR) == false))
            || (((this.DEFAULT_FAIL_MAIL_ADDR != null) && (o.DEFAULT_FAIL_MAIL_ADDR != null)) && (this.DEFAULT_FAIL_MAIL_ADDR.equals(o.DEFAULT_FAIL_MAIL_ADDR) == false))
            || (((this.DEFAULT_BUG_ERROR_MAIL_ADDR != null) && (o.DEFAULT_BUG_ERROR_MAIL_ADDR != null)) && (this.DEFAULT_BUG_ERROR_MAIL_ADDR.equals(o.DEFAULT_BUG_ERROR_MAIL_ADDR) == false))
            || (((this.DEFAULT_MAX_ERROR_VALUE != null) && (o.DEFAULT_MAX_ERROR_VALUE != null)) && (this.DEFAULT_MAX_ERROR_VALUE.equals(o.DEFAULT_MAX_ERROR_VALUE) == false))
            || (((this.DEFAULT_MAX_FK_ERROR_VALUE != null) && (o.DEFAULT_MAX_FK_ERROR_VALUE != null)) && (this.DEFAULT_MAX_FK_ERROR_VALUE.equals(o.DEFAULT_MAX_FK_ERROR_VALUE) == false))
            || (((this.DEFAULT_MAX_COL_LIMIT_VALUE != null) && (o.DEFAULT_MAX_COL_LIMIT_VALUE != null)) && (this.DEFAULT_MAX_COL_LIMIT_VALUE.equals(o.DEFAULT_MAX_COL_LIMIT_VALUE) == false))
            || (((this.TEMP_SUM_TABLESPACE != null) && (o.TEMP_SUM_TABLESPACE != null)) && (this.TEMP_SUM_TABLESPACE.equals(o.TEMP_SUM_TABLESPACE) == false))
            || (((this.USE_TEMP_SUM_TABLESPACE_FLAG != null) && (o.USE_TEMP_SUM_TABLESPACE_FLAG != null)) && (this.USE_TEMP_SUM_TABLESPACE_FLAG.equals(o.USE_TEMP_SUM_TABLESPACE_FLAG) == false))
            || (((this.BATCH_COLUMN_NAME != null) && (o.BATCH_COLUMN_NAME != null)) && (this.BATCH_COLUMN_NAME.equals(o.BATCH_COLUMN_NAME) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
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
  public static int getRb_segment_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRb_segment_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getRb_segment_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getUse_rb_segment_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUse_rb_segment_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 1
  */
  public static int getUse_rb_segment_flagSQLType() {
    
    return 1;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getDefault_error_mail_addrColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_error_mail_addrDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 1
  */
  public static int getDefault_error_mail_addrSQLType() {
    
    return 1;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getDefault_fail_mail_addrColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_fail_mail_addrDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 1
  */
  public static int getDefault_fail_mail_addrSQLType() {
    
    return 1;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getDefault_bug_error_mail_addrColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_bug_error_mail_addrDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 1
  */
  public static int getDefault_bug_error_mail_addrSQLType() {
    
    return 1;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getDefault_max_error_valueColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_max_error_valueDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getDefault_max_error_valueSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getDefault_max_fk_error_valueColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_max_fk_error_valueDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getDefault_max_fk_error_valueSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getDefault_max_col_limit_valueColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_max_col_limit_valueDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getDefault_max_col_limit_valueSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getTemp_sum_tablespaceColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTemp_sum_tablespaceDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTemp_sum_tablespaceSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getUse_temp_sum_tablespace_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUse_temp_sum_tablespace_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUse_temp_sum_tablespace_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getBatch_column_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBatch_column_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBatch_column_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getVersion_numberColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVersion_numberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVersion_numberSQLType() {
    
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

  public Meta_parameters getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_parameters original) {
    this.original = (Meta_parameters) original.clone();
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
