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
public class Meta_transfer_batches implements Cloneable,RockDBObject  {

    private Long ID;
    private Timestamp START_DATE;
    private Timestamp END_DATE;
    private String FAIL_FLAG;
    private String STATUS;
    private String VERSION_NUMBER;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private String META_COLLECTION_NAME;
    private String META_COLLECTION_SET_NAME;
    private String SETTYPE;
    private Integer SLOT_ID;
    private String SCHEDULING_INFO;
    private String SERVICE_NODE;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "ID"    ,"VERSION_NUMBER"    ,"META_COLLECTION_NAME"    ,"META_COLLECTION_SET_NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_transfer_batches original; 

  public Meta_transfer_batches(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_transfer_batches(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.ID = null;
         this.START_DATE = null;
         this.END_DATE = null;
         this.FAIL_FLAG = null;
         this.STATUS = null;
         this.VERSION_NUMBER = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.META_COLLECTION_NAME = null;
         this.META_COLLECTION_SET_NAME = null;
         this.SETTYPE = null;
         this.SLOT_ID = null;
         this.SCHEDULING_INFO = null;
         this.SERVICE_NODE = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_transfer_batches(final RockFactory rockFact   ,final Long ID ,final String VERSION_NUMBER ,final String META_COLLECTION_NAME ,final String META_COLLECTION_SET_NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.ID = ID;
            this.VERSION_NUMBER = VERSION_NUMBER;
            this.META_COLLECTION_NAME = META_COLLECTION_NAME;
            this.META_COLLECTION_SET_NAME = META_COLLECTION_SET_NAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_transfer_batches> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_transfer_batches o = it.next();

              this.ID = o.getId();
              this.START_DATE = o.getStart_date();
              this.END_DATE = o.getEnd_date();
              this.FAIL_FLAG = o.getFail_flag();
              this.STATUS = o.getStatus();
              this.VERSION_NUMBER = o.getVersion_number();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.META_COLLECTION_NAME = o.getMeta_collection_name();
              this.META_COLLECTION_SET_NAME = o.getMeta_collection_set_name();
              this.SETTYPE = o.getSettype();
              this.SLOT_ID = o.getSlot_id();
              this.SCHEDULING_INFO = o.getScheduling_info();
              this.SERVICE_NODE = o.getService_node();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transfer_batches");
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
  public Meta_transfer_batches(final RockFactory rockFact, final Meta_transfer_batches whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_transfer_batches> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_transfer_batches o = it.next();
                this.ID = o.getId();
                this.START_DATE = o.getStart_date();
                this.END_DATE = o.getEnd_date();
                this.FAIL_FLAG = o.getFail_flag();
                this.STATUS = o.getStatus();
                this.VERSION_NUMBER = o.getVersion_number();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.META_COLLECTION_NAME = o.getMeta_collection_name();
                this.META_COLLECTION_SET_NAME = o.getMeta_collection_set_name();
                this.SETTYPE = o.getSettype();
                this.SLOT_ID = o.getSlot_id();
                this.SCHEDULING_INFO = o.getScheduling_info();
                this.SERVICE_NODE = o.getService_node();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transfer_batches");
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
    return "Meta_transfer_batches";
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
  public int updateDB(final boolean useTimestamp, final Meta_transfer_batches whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_transfer_batches whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_transfer_batches.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_transfer_batches ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("START_DATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.START_DATE),93, true)+"\" ");
        sbuff.append("END_DATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.END_DATE),93, true)+"\" ");
        sbuff.append("FAIL_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FAIL_FLAG),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("META_COLLECTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.META_COLLECTION_NAME),12, true)+"\" ");
        sbuff.append("META_COLLECTION_SET_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.META_COLLECTION_SET_NAME),12, true)+"\" ");
        sbuff.append("SETTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SETTYPE),12, true)+"\" ");
        sbuff.append("SLOT_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SLOT_ID),4, true)+"\" ");
        sbuff.append("SCHEDULING_INFO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_INFO),12, true)+"\" ");
        sbuff.append("SERVICE_NODE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SERVICE_NODE),12, true)+"\" ");
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
    sbuff.append("<Meta_transfer_batches ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("START_DATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.START_DATE),93, true)+"\" ");
        sbuff.append("END_DATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.END_DATE),93, true)+"\" ");
        sbuff.append("FAIL_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FAIL_FLAG),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("META_COLLECTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.META_COLLECTION_NAME),12, true)+"\" ");
        sbuff.append("META_COLLECTION_SET_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.META_COLLECTION_SET_NAME),12, true)+"\" ");
        sbuff.append("SETTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SETTYPE),12, true)+"\" ");
        sbuff.append("SLOT_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SLOT_ID),4, true)+"\" ");
        sbuff.append("SCHEDULING_INFO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SCHEDULING_INFO),12, true)+"\" ");
        sbuff.append("SERVICE_NODE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SERVICE_NODE),12, true)+"\" ");
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
    sbuff.append("</Meta_transfer_batches>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_transfer_batches ( ");
	    		sbuff.append("ID");
		    		sbuff.append(", START_DATE");
	    		sbuff.append(", END_DATE");
	    		sbuff.append(", FAIL_FLAG");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", META_COLLECTION_NAME");
	    		sbuff.append(", META_COLLECTION_SET_NAME");
	    		sbuff.append(", SETTYPE");
	    		sbuff.append(", SLOT_ID");
	    		sbuff.append(", SCHEDULING_INFO");
	    		sbuff.append(", SERVICE_NODE");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.START_DATE,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.END_DATE,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FAIL_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.META_COLLECTION_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.META_COLLECTION_SET_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SETTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SLOT_ID,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SCHEDULING_INFO,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SERVICE_NODE,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getId() { 
    return this.ID;
  }
   public Timestamp getStart_date() { 
    return this.START_DATE;
  }
   public Timestamp getEnd_date() { 
    return this.END_DATE;
  }
   public String getFail_flag() { 
    return this.FAIL_FLAG;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getCollection_set_id() { 
    return this.COLLECTION_SET_ID;
  }
   public Long getCollection_id() { 
    return this.COLLECTION_ID;
  }
   public String getMeta_collection_name() { 
    return this.META_COLLECTION_NAME;
  }
   public String getMeta_collection_set_name() { 
    return this.META_COLLECTION_SET_NAME;
  }
   public String getSettype() { 
    return this.SETTYPE;
  }
   public Integer getSlot_id() { 
    return this.SLOT_ID;
  }
   public String getScheduling_info() { 
    return this.SCHEDULING_INFO;
  }
   public String getService_node() { 
    return this.SERVICE_NODE;
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
     if (ID == null) {
      ID = new Long (0);
    }
     if (START_DATE == null) {
      START_DATE = new Timestamp (0);
    }
     if (END_DATE == null) {
      END_DATE = new Timestamp (0);
    }
     if (FAIL_FLAG == null) {
      FAIL_FLAG = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (COLLECTION_SET_ID == null) {
      COLLECTION_SET_ID = new Long (0);
    }
     if (COLLECTION_ID == null) {
      COLLECTION_ID = new Long (0);
    }
     if (META_COLLECTION_NAME == null) {
      META_COLLECTION_NAME = "";
    }
     if (META_COLLECTION_SET_NAME == null) {
      META_COLLECTION_SET_NAME = "";
    }
     if (SETTYPE == null) {
      SETTYPE = "";
    }
     if (SLOT_ID == null) {
      SLOT_ID = new Integer (0);
    }
     if (SCHEDULING_INFO == null) {
      SCHEDULING_INFO = "";
    }
     if (SERVICE_NODE == null) {
      SERVICE_NODE = "";
    }
   }

   public void setId(final Long ID) {
    if (validateData){
      DataValidator.validateData((Object)ID,"ID",2,38,0);
    }
    modifiedColumns.add("ID");
    this.ID = ID;
  }
   public void setStart_date(final Timestamp START_DATE) {
    if (validateData){
      DataValidator.validateData((Object)START_DATE,"START_DATE",93,23,0);
    }
    modifiedColumns.add("START_DATE");
    this.START_DATE = START_DATE;
  }
   public void setEnd_date(final Timestamp END_DATE) {
    if (validateData){
      DataValidator.validateData((Object)END_DATE,"END_DATE",93,23,0);
    }
    modifiedColumns.add("END_DATE");
    this.END_DATE = END_DATE;
  }
   public void setFail_flag(final String FAIL_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)FAIL_FLAG,"FAIL_FLAG",12,1,0);
    }
    modifiedColumns.add("FAIL_FLAG");
    this.FAIL_FLAG = FAIL_FLAG;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
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
   public void setMeta_collection_name(final String META_COLLECTION_NAME) {
    if (validateData){
      DataValidator.validateData((Object)META_COLLECTION_NAME,"META_COLLECTION_NAME",12,128,0);
    }
    modifiedColumns.add("META_COLLECTION_NAME");
    this.META_COLLECTION_NAME = META_COLLECTION_NAME;
  }
   public void setMeta_collection_set_name(final String META_COLLECTION_SET_NAME) {
    if (validateData){
      DataValidator.validateData((Object)META_COLLECTION_SET_NAME,"META_COLLECTION_SET_NAME",12,128,0);
    }
    modifiedColumns.add("META_COLLECTION_SET_NAME");
    this.META_COLLECTION_SET_NAME = META_COLLECTION_SET_NAME;
  }
   public void setSettype(final String SETTYPE) {
    if (validateData){
      DataValidator.validateData((Object)SETTYPE,"SETTYPE",12,10,0);
    }
    modifiedColumns.add("SETTYPE");
    this.SETTYPE = SETTYPE;
  }
   public void setSlot_id(final Integer SLOT_ID) {
    if (validateData){
      DataValidator.validateData((Object)SLOT_ID,"SLOT_ID",4,10,0);
    }
    modifiedColumns.add("SLOT_ID");
    this.SLOT_ID = SLOT_ID;
  }
   public void setScheduling_info(final String SCHEDULING_INFO) {
    if (validateData){
      DataValidator.validateData((Object)SCHEDULING_INFO,"SCHEDULING_INFO",12,16000,0);
    }
    modifiedColumns.add("SCHEDULING_INFO");
    this.SCHEDULING_INFO = SCHEDULING_INFO;
  }
   public void setService_node(final String SERVICE_NODE) {
    if (validateData){
      DataValidator.validateData((Object)SERVICE_NODE,"SERVICE_NODE",12,64,0);
    }
    modifiedColumns.add("SERVICE_NODE");
    this.SERVICE_NODE = SERVICE_NODE;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_transfer_batches o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.META_COLLECTION_NAME == null) || (o.META_COLLECTION_NAME == null)) && (this.META_COLLECTION_NAME != o.META_COLLECTION_NAME))
            || (((this.META_COLLECTION_SET_NAME == null) || (o.META_COLLECTION_SET_NAME == null)) && (this.META_COLLECTION_SET_NAME != o.META_COLLECTION_SET_NAME))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.META_COLLECTION_NAME != null) && (o.META_COLLECTION_NAME != null)) && (this.META_COLLECTION_NAME.equals(o.META_COLLECTION_NAME) == false))
            || (((this.META_COLLECTION_SET_NAME != null) && (o.META_COLLECTION_SET_NAME != null)) && (this.META_COLLECTION_SET_NAME.equals(o.META_COLLECTION_SET_NAME) == false))
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
  public boolean equals(final Meta_transfer_batches o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
            || (((this.START_DATE == null) || (o.START_DATE == null)) && (this.START_DATE != o.START_DATE))
            || (((this.END_DATE == null) || (o.END_DATE == null)) && (this.END_DATE != o.END_DATE))
            || (((this.FAIL_FLAG == null) || (o.FAIL_FLAG == null)) && (this.FAIL_FLAG != o.FAIL_FLAG))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.META_COLLECTION_NAME == null) || (o.META_COLLECTION_NAME == null)) && (this.META_COLLECTION_NAME != o.META_COLLECTION_NAME))
            || (((this.META_COLLECTION_SET_NAME == null) || (o.META_COLLECTION_SET_NAME == null)) && (this.META_COLLECTION_SET_NAME != o.META_COLLECTION_SET_NAME))
            || (((this.SETTYPE == null) || (o.SETTYPE == null)) && (this.SETTYPE != o.SETTYPE))
            || (((this.SLOT_ID == null) || (o.SLOT_ID == null)) && (this.SLOT_ID != o.SLOT_ID))
            || (((this.SCHEDULING_INFO == null) || (o.SCHEDULING_INFO == null)) && (this.SCHEDULING_INFO != o.SCHEDULING_INFO))
            || (((this.SERVICE_NODE == null) || (o.SERVICE_NODE == null)) && (this.SERVICE_NODE != o.SERVICE_NODE))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
            || (((this.START_DATE != null) && (o.START_DATE != null)) && (this.START_DATE.equals(o.START_DATE) == false))
            || (((this.END_DATE != null) && (o.END_DATE != null)) && (this.END_DATE.equals(o.END_DATE) == false))
            || (((this.FAIL_FLAG != null) && (o.FAIL_FLAG != null)) && (this.FAIL_FLAG.equals(o.FAIL_FLAG) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.META_COLLECTION_NAME != null) && (o.META_COLLECTION_NAME != null)) && (this.META_COLLECTION_NAME.equals(o.META_COLLECTION_NAME) == false))
            || (((this.META_COLLECTION_SET_NAME != null) && (o.META_COLLECTION_SET_NAME != null)) && (this.META_COLLECTION_SET_NAME.equals(o.META_COLLECTION_SET_NAME) == false))
            || (((this.SETTYPE != null) && (o.SETTYPE != null)) && (this.SETTYPE.equals(o.SETTYPE) == false))
            || (((this.SLOT_ID != null) && (o.SLOT_ID != null)) && (this.SLOT_ID.equals(o.SLOT_ID) == false))
            || (((this.SCHEDULING_INFO != null) && (o.SCHEDULING_INFO != null)) && (this.SCHEDULING_INFO.equals(o.SCHEDULING_INFO) == false))
            || (((this.SERVICE_NODE != null) && (o.SERVICE_NODE != null)) && (this.SERVICE_NODE.equals(o.SERVICE_NODE) == false))
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
  * return 23
  */
  public static int getStart_dateColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStart_dateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getStart_dateSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getEnd_dateColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEnd_dateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getEnd_dateSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getFail_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFail_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFail_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getStatusColumnSize() {
    
     return 10;   
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
  * return 128
  */
  public static int getMeta_collection_nameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMeta_collection_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMeta_collection_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getMeta_collection_set_nameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMeta_collection_set_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMeta_collection_set_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getSettypeColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSettypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSettypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getSlot_idColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSlot_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getSlot_idSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 16000
  */
  public static int getScheduling_infoColumnSize() {
    
     return 16000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getScheduling_infoDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getScheduling_infoSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 64
  */
  public static int getService_nodeColumnSize() {
    
     return 64;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getService_nodeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getService_nodeSQLType() {
    
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

  public Meta_transfer_batches getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_transfer_batches original) {
    this.original = (Meta_transfer_batches) original.clone();
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
