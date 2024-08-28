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
public class Meta_schedulings implements Cloneable,RockDBObject  {

    private String VERSION_NUMBER;
    private Long ID;
    private String EXECUTION_TYPE;
    private String OS_COMMAND;
    private Long SCHEDULING_MONTH;
    private Long SCHEDULING_DAY;
    private Long SCHEDULING_HOUR;
    private Long SCHEDULING_MIN;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private String MON_FLAG;
    private String TUE_FLAG;
    private String WED_FLAG;
    private String THU_FLAG;
    private String FRI_FLAG;
    private String SAT_FLAG;
    private String SUN_FLAG;
    private String STATUS;
    private Timestamp LAST_EXECUTION_TIME;
    private Long INTERVAL_HOUR;
    private Long INTERVAL_MIN;
    private String NAME;
    private String HOLD_FLAG;
    private Long PRIORITY;
    private Long SCHEDULING_YEAR;
    private String TRIGGER_COMMAND;
    private Long LAST_EXEC_TIME_MS;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_schedulings original; 

  public Meta_schedulings(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_schedulings(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSION_NUMBER = null;
         this.ID = null;
         this.EXECUTION_TYPE = null;
         this.OS_COMMAND = null;
         this.SCHEDULING_MONTH = null;
         this.SCHEDULING_DAY = null;
         this.SCHEDULING_HOUR = null;
         this.SCHEDULING_MIN = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.MON_FLAG = null;
         this.TUE_FLAG = null;
         this.WED_FLAG = null;
         this.THU_FLAG = null;
         this.FRI_FLAG = null;
         this.SAT_FLAG = null;
         this.SUN_FLAG = null;
         this.STATUS = null;
         this.LAST_EXECUTION_TIME = null;
         this.INTERVAL_HOUR = null;
         this.INTERVAL_MIN = null;
         this.NAME = null;
         this.HOLD_FLAG = null;
         this.PRIORITY = null;
         this.SCHEDULING_YEAR = null;
         this.TRIGGER_COMMAND = null;
         this.LAST_EXEC_TIME_MS = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_schedulings(final RockFactory rockFact   ,final Long ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.ID = ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_schedulings> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_schedulings o = it.next();

              this.VERSION_NUMBER = o.getVersion_number();
              this.ID = o.getId();
              this.EXECUTION_TYPE = o.getExecution_type();
              this.OS_COMMAND = o.getOs_command();
              this.SCHEDULING_MONTH = o.getScheduling_month();
              this.SCHEDULING_DAY = o.getScheduling_day();
              this.SCHEDULING_HOUR = o.getScheduling_hour();
              this.SCHEDULING_MIN = o.getScheduling_min();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.MON_FLAG = o.getMon_flag();
              this.TUE_FLAG = o.getTue_flag();
              this.WED_FLAG = o.getWed_flag();
              this.THU_FLAG = o.getThu_flag();
              this.FRI_FLAG = o.getFri_flag();
              this.SAT_FLAG = o.getSat_flag();
              this.SUN_FLAG = o.getSun_flag();
              this.STATUS = o.getStatus();
              this.LAST_EXECUTION_TIME = o.getLast_execution_time();
              this.INTERVAL_HOUR = o.getInterval_hour();
              this.INTERVAL_MIN = o.getInterval_min();
              this.NAME = o.getName();
              this.HOLD_FLAG = o.getHold_flag();
              this.PRIORITY = o.getPriority();
              this.SCHEDULING_YEAR = o.getScheduling_year();
              this.TRIGGER_COMMAND = o.getTrigger_command();
              this.LAST_EXEC_TIME_MS = o.getLast_exec_time_ms();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_schedulings");
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
  public Meta_schedulings(final RockFactory rockFact, final Meta_schedulings whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_schedulings> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_schedulings o = it.next();
                this.VERSION_NUMBER = o.getVersion_number();
                this.ID = o.getId();
                this.EXECUTION_TYPE = o.getExecution_type();
                this.OS_COMMAND = o.getOs_command();
                this.SCHEDULING_MONTH = o.getScheduling_month();
                this.SCHEDULING_DAY = o.getScheduling_day();
                this.SCHEDULING_HOUR = o.getScheduling_hour();
                this.SCHEDULING_MIN = o.getScheduling_min();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.MON_FLAG = o.getMon_flag();
                this.TUE_FLAG = o.getTue_flag();
                this.WED_FLAG = o.getWed_flag();
                this.THU_FLAG = o.getThu_flag();
                this.FRI_FLAG = o.getFri_flag();
                this.SAT_FLAG = o.getSat_flag();
                this.SUN_FLAG = o.getSun_flag();
                this.STATUS = o.getStatus();
                this.LAST_EXECUTION_TIME = o.getLast_execution_time();
                this.INTERVAL_HOUR = o.getInterval_hour();
                this.INTERVAL_MIN = o.getInterval_min();
                this.NAME = o.getName();
                this.HOLD_FLAG = o.getHold_flag();
                this.PRIORITY = o.getPriority();
                this.SCHEDULING_YEAR = o.getScheduling_year();
                this.TRIGGER_COMMAND = o.getTrigger_command();
                this.LAST_EXEC_TIME_MS = o.getLast_exec_time_ms();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_schedulings");
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
    return "Meta_schedulings";
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
  public int updateDB(final boolean useTimestamp, final Meta_schedulings whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_schedulings whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_schedulings.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_schedulings ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("EXECUTION_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTION_TYPE),12, true)+"\" ");
        sbuff.append("OS_COMMAND=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OS_COMMAND),12, true)+"\" ");
        sbuff.append("SCHEDULING_MONTH=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_MONTH),2, true)+"\" ");
        sbuff.append("SCHEDULING_DAY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_DAY),2, true)+"\" ");
        sbuff.append("SCHEDULING_HOUR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_HOUR),2, true)+"\" ");
        sbuff.append("SCHEDULING_MIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_MIN),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("MON_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MON_FLAG),12, true)+"\" ");
        sbuff.append("TUE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TUE_FLAG),12, true)+"\" ");
        sbuff.append("WED_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WED_FLAG),12, true)+"\" ");
        sbuff.append("THU_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.THU_FLAG),12, true)+"\" ");
        sbuff.append("FRI_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FRI_FLAG),12, true)+"\" ");
        sbuff.append("SAT_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SAT_FLAG),12, true)+"\" ");
        sbuff.append("SUN_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SUN_FLAG),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("LAST_EXECUTION_TIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LAST_EXECUTION_TIME),93, true)+"\" ");
        sbuff.append("INTERVAL_HOUR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL_HOUR),2, true)+"\" ");
        sbuff.append("INTERVAL_MIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL_MIN),2, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("HOLD_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.HOLD_FLAG),12, true)+"\" ");
        sbuff.append("PRIORITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRIORITY),2, true)+"\" ");
        sbuff.append("SCHEDULING_YEAR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_YEAR),2, true)+"\" ");
        sbuff.append("TRIGGER_COMMAND=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRIGGER_COMMAND),12, true)+"\" ");
        sbuff.append("LAST_EXEC_TIME_MS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LAST_EXEC_TIME_MS),2, true)+"\" ");
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
    sbuff.append("<Meta_schedulings ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("EXECUTION_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTION_TYPE),12, true)+"\" ");
        sbuff.append("OS_COMMAND=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OS_COMMAND),12, true)+"\" ");
        sbuff.append("SCHEDULING_MONTH=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_MONTH),2, true)+"\" ");
        sbuff.append("SCHEDULING_DAY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_DAY),2, true)+"\" ");
        sbuff.append("SCHEDULING_HOUR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_HOUR),2, true)+"\" ");
        sbuff.append("SCHEDULING_MIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_MIN),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("MON_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MON_FLAG),12, true)+"\" ");
        sbuff.append("TUE_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TUE_FLAG),12, true)+"\" ");
        sbuff.append("WED_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WED_FLAG),12, true)+"\" ");
        sbuff.append("THU_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.THU_FLAG),12, true)+"\" ");
        sbuff.append("FRI_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FRI_FLAG),12, true)+"\" ");
        sbuff.append("SAT_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SAT_FLAG),12, true)+"\" ");
        sbuff.append("SUN_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SUN_FLAG),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("LAST_EXECUTION_TIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LAST_EXECUTION_TIME),93, true)+"\" ");
        sbuff.append("INTERVAL_HOUR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL_HOUR),2, true)+"\" ");
        sbuff.append("INTERVAL_MIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL_MIN),2, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("HOLD_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.HOLD_FLAG),12, true)+"\" ");
        sbuff.append("PRIORITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PRIORITY),2, true)+"\" ");
        sbuff.append("SCHEDULING_YEAR=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_YEAR),2, true)+"\" ");
        sbuff.append("TRIGGER_COMMAND=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRIGGER_COMMAND),12, true)+"\" ");
        sbuff.append("LAST_EXEC_TIME_MS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LAST_EXEC_TIME_MS),2, true)+"\" ");
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
    sbuff.append("</Meta_schedulings>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_schedulings ( ");
	    		sbuff.append("VERSION_NUMBER");
		    		sbuff.append(", ID");
	    		sbuff.append(", EXECUTION_TYPE");
	    		sbuff.append(", OS_COMMAND");
	    		sbuff.append(", SCHEDULING_MONTH");
	    		sbuff.append(", SCHEDULING_DAY");
	    		sbuff.append(", SCHEDULING_HOUR");
	    		sbuff.append(", SCHEDULING_MIN");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", MON_FLAG");
	    		sbuff.append(", TUE_FLAG");
	    		sbuff.append(", WED_FLAG");
	    		sbuff.append(", THU_FLAG");
	    		sbuff.append(", FRI_FLAG");
	    		sbuff.append(", SAT_FLAG");
	    		sbuff.append(", SUN_FLAG");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", LAST_EXECUTION_TIME");
	    		sbuff.append(", INTERVAL_HOUR");
	    		sbuff.append(", INTERVAL_MIN");
	    		sbuff.append(", NAME");
	    		sbuff.append(", HOLD_FLAG");
	    		sbuff.append(", PRIORITY");
	    		sbuff.append(", SCHEDULING_YEAR");
	    		sbuff.append(", TRIGGER_COMMAND");
	    		sbuff.append(", LAST_EXEC_TIME_MS");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXECUTION_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OS_COMMAND,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_MONTH,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_DAY,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_HOUR,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_MIN,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MON_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TUE_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WED_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.THU_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FRI_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SAT_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SUN_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LAST_EXECUTION_TIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERVAL_HOUR,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERVAL_MIN,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.HOLD_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PRIORITY,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_YEAR,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRIGGER_COMMAND,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LAST_EXEC_TIME_MS,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getId() { 
    return this.ID;
  }
   public String getExecution_type() { 
    return this.EXECUTION_TYPE;
  }
   public String getOs_command() { 
    return this.OS_COMMAND;
  }
   public Long getScheduling_month() { 
    return this.SCHEDULING_MONTH;
  }
   public Long getScheduling_day() { 
    return this.SCHEDULING_DAY;
  }
   public Long getScheduling_hour() { 
    return this.SCHEDULING_HOUR;
  }
   public Long getScheduling_min() { 
    return this.SCHEDULING_MIN;
  }
   public Long getCollection_set_id() { 
    return this.COLLECTION_SET_ID;
  }
   public Long getCollection_id() { 
    return this.COLLECTION_ID;
  }
   public String getMon_flag() { 
    return this.MON_FLAG;
  }
   public String getTue_flag() { 
    return this.TUE_FLAG;
  }
   public String getWed_flag() { 
    return this.WED_FLAG;
  }
   public String getThu_flag() { 
    return this.THU_FLAG;
  }
   public String getFri_flag() { 
    return this.FRI_FLAG;
  }
   public String getSat_flag() { 
    return this.SAT_FLAG;
  }
   public String getSun_flag() { 
    return this.SUN_FLAG;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public Timestamp getLast_execution_time() { 
    return this.LAST_EXECUTION_TIME;
  }
   public Long getInterval_hour() { 
    return this.INTERVAL_HOUR;
  }
   public Long getInterval_min() { 
    return this.INTERVAL_MIN;
  }
   public String getName() { 
    return this.NAME;
  }
   public String getHold_flag() { 
    return this.HOLD_FLAG;
  }
   public Long getPriority() { 
    return this.PRIORITY;
  }
   public Long getScheduling_year() { 
    return this.SCHEDULING_YEAR;
  }
   public String getTrigger_command() { 
    return this.TRIGGER_COMMAND;
  }
   public Long getLast_exec_time_ms() { 
    return this.LAST_EXEC_TIME_MS;
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
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (ID == null) {
      ID = new Long (0);
    }
     if (EXECUTION_TYPE == null) {
      EXECUTION_TYPE = "";
    }
     if (OS_COMMAND == null) {
      OS_COMMAND = "";
    }
     if (SCHEDULING_MONTH == null) {
      SCHEDULING_MONTH = new Long (0);
    }
     if (SCHEDULING_DAY == null) {
      SCHEDULING_DAY = new Long (0);
    }
     if (SCHEDULING_HOUR == null) {
      SCHEDULING_HOUR = new Long (0);
    }
     if (SCHEDULING_MIN == null) {
      SCHEDULING_MIN = new Long (0);
    }
     if (COLLECTION_SET_ID == null) {
      COLLECTION_SET_ID = new Long (0);
    }
     if (COLLECTION_ID == null) {
      COLLECTION_ID = new Long (0);
    }
     if (MON_FLAG == null) {
      MON_FLAG = "";
    }
     if (TUE_FLAG == null) {
      TUE_FLAG = "";
    }
     if (WED_FLAG == null) {
      WED_FLAG = "";
    }
     if (THU_FLAG == null) {
      THU_FLAG = "";
    }
     if (FRI_FLAG == null) {
      FRI_FLAG = "";
    }
     if (SAT_FLAG == null) {
      SAT_FLAG = "";
    }
     if (SUN_FLAG == null) {
      SUN_FLAG = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (LAST_EXECUTION_TIME == null) {
      LAST_EXECUTION_TIME = new Timestamp (0);
    }
     if (INTERVAL_HOUR == null) {
      INTERVAL_HOUR = new Long (0);
    }
     if (INTERVAL_MIN == null) {
      INTERVAL_MIN = new Long (0);
    }
     if (NAME == null) {
      NAME = "";
    }
     if (HOLD_FLAG == null) {
      HOLD_FLAG = "";
    }
     if (PRIORITY == null) {
      PRIORITY = new Long (0);
    }
     if (SCHEDULING_YEAR == null) {
      SCHEDULING_YEAR = new Long (0);
    }
     if (TRIGGER_COMMAND == null) {
      TRIGGER_COMMAND = "";
    }
     if (LAST_EXEC_TIME_MS == null) {
      LAST_EXEC_TIME_MS = new Long (0);
    }
   }

   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setId(final Long ID) {
    if (validateData){
      DataValidator.validateData((Object)ID,"ID",2,38,0);
    }
    modifiedColumns.add("ID");
    this.ID = ID;
  }
   public void setExecution_type(final String EXECUTION_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)EXECUTION_TYPE,"EXECUTION_TYPE",12,15,0);
    }
    modifiedColumns.add("EXECUTION_TYPE");
    this.EXECUTION_TYPE = EXECUTION_TYPE;
  }
   public void setOs_command(final String OS_COMMAND) {
    if (validateData){
      DataValidator.validateData((Object)OS_COMMAND,"OS_COMMAND",12,2000,0);
    }
    modifiedColumns.add("OS_COMMAND");
    this.OS_COMMAND = OS_COMMAND;
  }
   public void setScheduling_month(final Long SCHEDULING_MONTH) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_MONTH,"SCHEDULING_MONTH",2,2,0);
    }
    modifiedColumns.add("SCHEDULING_MONTH");
    this.SCHEDULING_MONTH = SCHEDULING_MONTH;
  }
   public void setScheduling_day(final Long SCHEDULING_DAY) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_DAY,"SCHEDULING_DAY",2,2,0);
    }
    modifiedColumns.add("SCHEDULING_DAY");
    this.SCHEDULING_DAY = SCHEDULING_DAY;
  }
   public void setScheduling_hour(final Long SCHEDULING_HOUR) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_HOUR,"SCHEDULING_HOUR",2,2,0);
    }
    modifiedColumns.add("SCHEDULING_HOUR");
    this.SCHEDULING_HOUR = SCHEDULING_HOUR;
  }
   public void setScheduling_min(final Long SCHEDULING_MIN) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_MIN,"SCHEDULING_MIN",2,2,0);
    }
    modifiedColumns.add("SCHEDULING_MIN");
    this.SCHEDULING_MIN = SCHEDULING_MIN;
  }
   public void setCollection_set_id(final Long COLLECTION_SET_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_SET_ID,"COLLECTION_SET_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_SET_ID");
    this.COLLECTION_SET_ID = COLLECTION_SET_ID;
  }
   public void setCollection_id(final Long COLLECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_ID,"COLLECTION_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_ID");
    this.COLLECTION_ID = COLLECTION_ID;
  }
   public void setMon_flag(final String MON_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)MON_FLAG,"MON_FLAG",12,1,0);
    }
    modifiedColumns.add("MON_FLAG");
    this.MON_FLAG = MON_FLAG;
  }
   public void setTue_flag(final String TUE_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)TUE_FLAG,"TUE_FLAG",12,1,0);
    }
    modifiedColumns.add("TUE_FLAG");
    this.TUE_FLAG = TUE_FLAG;
  }
   public void setWed_flag(final String WED_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)WED_FLAG,"WED_FLAG",12,1,0);
    }
    modifiedColumns.add("WED_FLAG");
    this.WED_FLAG = WED_FLAG;
  }
   public void setThu_flag(final String THU_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)THU_FLAG,"THU_FLAG",12,1,0);
    }
    modifiedColumns.add("THU_FLAG");
    this.THU_FLAG = THU_FLAG;
  }
   public void setFri_flag(final String FRI_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)FRI_FLAG,"FRI_FLAG",12,1,0);
    }
    modifiedColumns.add("FRI_FLAG");
    this.FRI_FLAG = FRI_FLAG;
  }
   public void setSat_flag(final String SAT_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)SAT_FLAG,"SAT_FLAG",12,1,0);
    }
    modifiedColumns.add("SAT_FLAG");
    this.SAT_FLAG = SAT_FLAG;
  }
   public void setSun_flag(final String SUN_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)SUN_FLAG,"SUN_FLAG",12,1,0);
    }
    modifiedColumns.add("SUN_FLAG");
    this.SUN_FLAG = SUN_FLAG;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,20,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setLast_execution_time(final Timestamp LAST_EXECUTION_TIME) {
    if (validateData){
      DataValidator.validateData((Object)LAST_EXECUTION_TIME,"LAST_EXECUTION_TIME",93,23,0);
    }
    modifiedColumns.add("LAST_EXECUTION_TIME");
    this.LAST_EXECUTION_TIME = LAST_EXECUTION_TIME;
  }
   public void setInterval_hour(final Long INTERVAL_HOUR) {
    if (validateData){
      DataValidator.validateData((Object)INTERVAL_HOUR,"INTERVAL_HOUR",2,2,0);
    }
    modifiedColumns.add("INTERVAL_HOUR");
    this.INTERVAL_HOUR = INTERVAL_HOUR;
  }
   public void setInterval_min(final Long INTERVAL_MIN) {
    if (validateData){
      DataValidator.validateData((Object)INTERVAL_MIN,"INTERVAL_MIN",2,2,0);
    }
    modifiedColumns.add("INTERVAL_MIN");
    this.INTERVAL_MIN = INTERVAL_MIN;
  }
   public void setName(final String NAME) {
    if (validateData){
      DataValidator.validateData((Object)NAME,"NAME",12,128,0);
    }
    modifiedColumns.add("NAME");
    this.NAME = NAME;
  }
   public void setHold_flag(final String HOLD_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)HOLD_FLAG,"HOLD_FLAG",12,1,0);
    }
    modifiedColumns.add("HOLD_FLAG");
    this.HOLD_FLAG = HOLD_FLAG;
  }
   public void setPriority(final Long PRIORITY) {
    if (validateData){
      DataValidator.validateData((Object)PRIORITY,"PRIORITY",2,3,0);
    }
    modifiedColumns.add("PRIORITY");
    this.PRIORITY = PRIORITY;
  }
   public void setScheduling_year(final Long SCHEDULING_YEAR) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_YEAR,"SCHEDULING_YEAR",2,4,0);
    }
    modifiedColumns.add("SCHEDULING_YEAR");
    this.SCHEDULING_YEAR = SCHEDULING_YEAR;
  }
   public void setTrigger_command(final String TRIGGER_COMMAND) {
    if (validateData){
      DataValidator.validateData((Object)TRIGGER_COMMAND,"TRIGGER_COMMAND",12,2000,0);
    }
    modifiedColumns.add("TRIGGER_COMMAND");
    this.TRIGGER_COMMAND = TRIGGER_COMMAND;
  }
   public void setLast_exec_time_ms(final Long LAST_EXEC_TIME_MS) {
    if (validateData){
      DataValidator.validateData((Object)LAST_EXEC_TIME_MS,"LAST_EXEC_TIME_MS",2,38,0);
    }
    modifiedColumns.add("LAST_EXEC_TIME_MS");
    this.LAST_EXEC_TIME_MS = LAST_EXEC_TIME_MS;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_schedulings o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
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
  public boolean equals(final Meta_schedulings o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
            || (((this.EXECUTION_TYPE == null) || (o.EXECUTION_TYPE == null)) && (this.EXECUTION_TYPE != o.EXECUTION_TYPE))
            || (((this.OS_COMMAND == null) || (o.OS_COMMAND == null)) && (this.OS_COMMAND != o.OS_COMMAND))
            || (((this.SCHEDULING_MONTH == null) || (o.SCHEDULING_MONTH == null)) && (this.SCHEDULING_MONTH != o.SCHEDULING_MONTH))
            || (((this.SCHEDULING_DAY == null) || (o.SCHEDULING_DAY == null)) && (this.SCHEDULING_DAY != o.SCHEDULING_DAY))
            || (((this.SCHEDULING_HOUR == null) || (o.SCHEDULING_HOUR == null)) && (this.SCHEDULING_HOUR != o.SCHEDULING_HOUR))
            || (((this.SCHEDULING_MIN == null) || (o.SCHEDULING_MIN == null)) && (this.SCHEDULING_MIN != o.SCHEDULING_MIN))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.MON_FLAG == null) || (o.MON_FLAG == null)) && (this.MON_FLAG != o.MON_FLAG))
            || (((this.TUE_FLAG == null) || (o.TUE_FLAG == null)) && (this.TUE_FLAG != o.TUE_FLAG))
            || (((this.WED_FLAG == null) || (o.WED_FLAG == null)) && (this.WED_FLAG != o.WED_FLAG))
            || (((this.THU_FLAG == null) || (o.THU_FLAG == null)) && (this.THU_FLAG != o.THU_FLAG))
            || (((this.FRI_FLAG == null) || (o.FRI_FLAG == null)) && (this.FRI_FLAG != o.FRI_FLAG))
            || (((this.SAT_FLAG == null) || (o.SAT_FLAG == null)) && (this.SAT_FLAG != o.SAT_FLAG))
            || (((this.SUN_FLAG == null) || (o.SUN_FLAG == null)) && (this.SUN_FLAG != o.SUN_FLAG))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.LAST_EXECUTION_TIME == null) || (o.LAST_EXECUTION_TIME == null)) && (this.LAST_EXECUTION_TIME != o.LAST_EXECUTION_TIME))
            || (((this.INTERVAL_HOUR == null) || (o.INTERVAL_HOUR == null)) && (this.INTERVAL_HOUR != o.INTERVAL_HOUR))
            || (((this.INTERVAL_MIN == null) || (o.INTERVAL_MIN == null)) && (this.INTERVAL_MIN != o.INTERVAL_MIN))
            || (((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
            || (((this.HOLD_FLAG == null) || (o.HOLD_FLAG == null)) && (this.HOLD_FLAG != o.HOLD_FLAG))
            || (((this.PRIORITY == null) || (o.PRIORITY == null)) && (this.PRIORITY != o.PRIORITY))
            || (((this.SCHEDULING_YEAR == null) || (o.SCHEDULING_YEAR == null)) && (this.SCHEDULING_YEAR != o.SCHEDULING_YEAR))
            || (((this.TRIGGER_COMMAND == null) || (o.TRIGGER_COMMAND == null)) && (this.TRIGGER_COMMAND != o.TRIGGER_COMMAND))
            || (((this.LAST_EXEC_TIME_MS == null) || (o.LAST_EXEC_TIME_MS == null)) && (this.LAST_EXEC_TIME_MS != o.LAST_EXEC_TIME_MS))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
            || (((this.EXECUTION_TYPE != null) && (o.EXECUTION_TYPE != null)) && (this.EXECUTION_TYPE.equals(o.EXECUTION_TYPE) == false))
            || (((this.OS_COMMAND != null) && (o.OS_COMMAND != null)) && (this.OS_COMMAND.equals(o.OS_COMMAND) == false))
            || (((this.SCHEDULING_MONTH != null) && (o.SCHEDULING_MONTH != null)) && (this.SCHEDULING_MONTH.equals(o.SCHEDULING_MONTH) == false))
            || (((this.SCHEDULING_DAY != null) && (o.SCHEDULING_DAY != null)) && (this.SCHEDULING_DAY.equals(o.SCHEDULING_DAY) == false))
            || (((this.SCHEDULING_HOUR != null) && (o.SCHEDULING_HOUR != null)) && (this.SCHEDULING_HOUR.equals(o.SCHEDULING_HOUR) == false))
            || (((this.SCHEDULING_MIN != null) && (o.SCHEDULING_MIN != null)) && (this.SCHEDULING_MIN.equals(o.SCHEDULING_MIN) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.MON_FLAG != null) && (o.MON_FLAG != null)) && (this.MON_FLAG.equals(o.MON_FLAG) == false))
            || (((this.TUE_FLAG != null) && (o.TUE_FLAG != null)) && (this.TUE_FLAG.equals(o.TUE_FLAG) == false))
            || (((this.WED_FLAG != null) && (o.WED_FLAG != null)) && (this.WED_FLAG.equals(o.WED_FLAG) == false))
            || (((this.THU_FLAG != null) && (o.THU_FLAG != null)) && (this.THU_FLAG.equals(o.THU_FLAG) == false))
            || (((this.FRI_FLAG != null) && (o.FRI_FLAG != null)) && (this.FRI_FLAG.equals(o.FRI_FLAG) == false))
            || (((this.SAT_FLAG != null) && (o.SAT_FLAG != null)) && (this.SAT_FLAG.equals(o.SAT_FLAG) == false))
            || (((this.SUN_FLAG != null) && (o.SUN_FLAG != null)) && (this.SUN_FLAG.equals(o.SUN_FLAG) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.LAST_EXECUTION_TIME != null) && (o.LAST_EXECUTION_TIME != null)) && (this.LAST_EXECUTION_TIME.equals(o.LAST_EXECUTION_TIME) == false))
            || (((this.INTERVAL_HOUR != null) && (o.INTERVAL_HOUR != null)) && (this.INTERVAL_HOUR.equals(o.INTERVAL_HOUR) == false))
            || (((this.INTERVAL_MIN != null) && (o.INTERVAL_MIN != null)) && (this.INTERVAL_MIN.equals(o.INTERVAL_MIN) == false))
            || (((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
            || (((this.HOLD_FLAG != null) && (o.HOLD_FLAG != null)) && (this.HOLD_FLAG.equals(o.HOLD_FLAG) == false))
            || (((this.PRIORITY != null) && (o.PRIORITY != null)) && (this.PRIORITY.equals(o.PRIORITY) == false))
            || (((this.SCHEDULING_YEAR != null) && (o.SCHEDULING_YEAR != null)) && (this.SCHEDULING_YEAR.equals(o.SCHEDULING_YEAR) == false))
            || (((this.TRIGGER_COMMAND != null) && (o.TRIGGER_COMMAND != null)) && (this.TRIGGER_COMMAND.equals(o.TRIGGER_COMMAND) == false))
            || (((this.LAST_EXEC_TIME_MS != null) && (o.LAST_EXEC_TIME_MS != null)) && (this.LAST_EXEC_TIME_MS.equals(o.LAST_EXEC_TIME_MS) == false))
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
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getIdColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIdDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getIdSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 15
  */
  public static int getExecution_typeColumnSize() {
    
     return 15;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExecution_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getExecution_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getOs_commandColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOs_commandDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getOs_commandSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getScheduling_monthColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_monthDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getScheduling_monthSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getScheduling_dayColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_dayDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getScheduling_daySQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getScheduling_hourColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_hourDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getScheduling_hourSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getScheduling_minColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_minDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getScheduling_minSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getCollection_set_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCollection_set_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getCollection_set_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getCollection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getCollection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getCollection_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getMon_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMon_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMon_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getTue_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTue_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTue_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getWed_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWed_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getWed_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getThu_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getThu_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getThu_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getFri_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFri_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFri_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getSat_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSat_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSat_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getSun_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSun_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSun_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 20
  */
  public static int getStatusColumnSize() {
    
     return 20;   
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
  * return 23
  */
  public static int getLast_execution_timeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLast_execution_timeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getLast_execution_timeSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getInterval_hourColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterval_hourDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getInterval_hourSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2
  */
  public static int getInterval_minColumnSize() {
    
     return 2;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterval_minDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getInterval_minSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getNameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getNameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getNameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getHold_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getHold_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getHold_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 3
  */
  public static int getPriorityColumnSize() {
    
     return 3;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPriorityDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getPrioritySQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 4
  */
  public static int getScheduling_yearColumnSize() {
    
     return 4;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_yearDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getScheduling_yearSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getTrigger_commandColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTrigger_commandDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTrigger_commandSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getLast_exec_time_msColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLast_exec_time_msDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getLast_exec_time_msSQLType() {
    
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

  public Meta_schedulings getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_schedulings original) {
    this.original = (Meta_schedulings) original.clone();
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
