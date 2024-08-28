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
public class Meta_joints implements Cloneable,RockDBObject  {

    private Long ID;
    private String IS_PK_COLUMN;
    private String IS_SUM_COLUMN;
    private String IS_GROUP_BY_COLUMN;
    private Long COLUMN_SPACE_AT_FILE;
    private Long FILE_ORDER_BY;
    private String PLUGIN_METHOD_NAME;
    private String VERSION_NUMBER;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private Long TRANSFER_ACTION_ID;
    private Long TARGET_CONNECTION_ID;
    private Long TARGET_TABLE_ID;
    private Long COLUMN_ID_TARGET_COLUMN;
    private Long SOURCE_CONNECTION_ID;
    private Long SOURCE_TABLE_ID;
    private Long COLUMN_ID_SOURCE_COLUMN;
    private Long TRANSFORMATION_ID;
    private Long TRANSF_TABLE_ID;
    private String PAR_NAME;
    private Long FILE_ID;
    private Long PLUGIN_ID;
    private String FREE_FORMAT_TRANSFORMAT;
    private String METHOD_PARAMETER;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_joints original; 

  public Meta_joints(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_joints(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.ID = null;
         this.IS_PK_COLUMN = null;
         this.IS_SUM_COLUMN = null;
         this.IS_GROUP_BY_COLUMN = null;
         this.COLUMN_SPACE_AT_FILE = null;
         this.FILE_ORDER_BY = null;
         this.PLUGIN_METHOD_NAME = null;
         this.VERSION_NUMBER = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.TRANSFER_ACTION_ID = null;
         this.TARGET_CONNECTION_ID = null;
         this.TARGET_TABLE_ID = null;
         this.COLUMN_ID_TARGET_COLUMN = null;
         this.SOURCE_CONNECTION_ID = null;
         this.SOURCE_TABLE_ID = null;
         this.COLUMN_ID_SOURCE_COLUMN = null;
         this.TRANSFORMATION_ID = null;
         this.TRANSF_TABLE_ID = null;
         this.PAR_NAME = null;
         this.FILE_ID = null;
         this.PLUGIN_ID = null;
         this.FREE_FORMAT_TRANSFORMAT = null;
         this.METHOD_PARAMETER = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_joints(final RockFactory rockFact   ,final Long ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.ID = ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_joints> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_joints o = it.next();

              this.ID = o.getId();
              this.IS_PK_COLUMN = o.getIs_pk_column();
              this.IS_SUM_COLUMN = o.getIs_sum_column();
              this.IS_GROUP_BY_COLUMN = o.getIs_group_by_column();
              this.COLUMN_SPACE_AT_FILE = o.getColumn_space_at_file();
              this.FILE_ORDER_BY = o.getFile_order_by();
              this.PLUGIN_METHOD_NAME = o.getPlugin_method_name();
              this.VERSION_NUMBER = o.getVersion_number();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
              this.TARGET_CONNECTION_ID = o.getTarget_connection_id();
              this.TARGET_TABLE_ID = o.getTarget_table_id();
              this.COLUMN_ID_TARGET_COLUMN = o.getColumn_id_target_column();
              this.SOURCE_CONNECTION_ID = o.getSource_connection_id();
              this.SOURCE_TABLE_ID = o.getSource_table_id();
              this.COLUMN_ID_SOURCE_COLUMN = o.getColumn_id_source_column();
              this.TRANSFORMATION_ID = o.getTransformation_id();
              this.TRANSF_TABLE_ID = o.getTransf_table_id();
              this.PAR_NAME = o.getPar_name();
              this.FILE_ID = o.getFile_id();
              this.PLUGIN_ID = o.getPlugin_id();
              this.FREE_FORMAT_TRANSFORMAT = o.getFree_format_transformat();
              this.METHOD_PARAMETER = o.getMethod_parameter();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_joints");
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
  public Meta_joints(final RockFactory rockFact, final Meta_joints whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_joints> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_joints o = it.next();
                this.ID = o.getId();
                this.IS_PK_COLUMN = o.getIs_pk_column();
                this.IS_SUM_COLUMN = o.getIs_sum_column();
                this.IS_GROUP_BY_COLUMN = o.getIs_group_by_column();
                this.COLUMN_SPACE_AT_FILE = o.getColumn_space_at_file();
                this.FILE_ORDER_BY = o.getFile_order_by();
                this.PLUGIN_METHOD_NAME = o.getPlugin_method_name();
                this.VERSION_NUMBER = o.getVersion_number();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
                this.TARGET_CONNECTION_ID = o.getTarget_connection_id();
                this.TARGET_TABLE_ID = o.getTarget_table_id();
                this.COLUMN_ID_TARGET_COLUMN = o.getColumn_id_target_column();
                this.SOURCE_CONNECTION_ID = o.getSource_connection_id();
                this.SOURCE_TABLE_ID = o.getSource_table_id();
                this.COLUMN_ID_SOURCE_COLUMN = o.getColumn_id_source_column();
                this.TRANSFORMATION_ID = o.getTransformation_id();
                this.TRANSF_TABLE_ID = o.getTransf_table_id();
                this.PAR_NAME = o.getPar_name();
                this.FILE_ID = o.getFile_id();
                this.PLUGIN_ID = o.getPlugin_id();
                this.FREE_FORMAT_TRANSFORMAT = o.getFree_format_transformat();
                this.METHOD_PARAMETER = o.getMethod_parameter();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_joints");
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
    return "Meta_joints";
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
  public int updateDB(final boolean useTimestamp, final Meta_joints whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_joints whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_joints.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_joints ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("IS_PK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_PK_COLUMN),12, true)+"\" ");
        sbuff.append("IS_SUM_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_SUM_COLUMN),12, true)+"\" ");
        sbuff.append("IS_GROUP_BY_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_GROUP_BY_COLUMN),12, true)+"\" ");
        sbuff.append("COLUMN_SPACE_AT_FILE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_SPACE_AT_FILE),2, true)+"\" ");
        sbuff.append("FILE_ORDER_BY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FILE_ORDER_BY),2, true)+"\" ");
        sbuff.append("PLUGIN_METHOD_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_METHOD_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
        sbuff.append("TARGET_CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TARGET_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_TARGET_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_TARGET_COLUMN),2, true)+"\" ");
        sbuff.append("SOURCE_CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_CONNECTION_ID),2, true)+"\" ");
        sbuff.append("SOURCE_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_SOURCE_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_SOURCE_COLUMN),2, true)+"\" ");
        sbuff.append("TRANSFORMATION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_ID),2, true)+"\" ");
        sbuff.append("TRANSF_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSF_TABLE_ID),2, true)+"\" ");
        sbuff.append("PAR_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PAR_NAME),12, true)+"\" ");
        sbuff.append("FILE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FILE_ID),2, true)+"\" ");
        sbuff.append("PLUGIN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_ID),2, true)+"\" ");
        sbuff.append("FREE_FORMAT_TRANSFORMAT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FREE_FORMAT_TRANSFORMAT),12, true)+"\" ");
        sbuff.append("METHOD_PARAMETER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.METHOD_PARAMETER),12, true)+"\" ");
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
    sbuff.append("<Meta_joints ");
        sbuff.append("ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ID),2, true)+"\" ");
        sbuff.append("IS_PK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_PK_COLUMN),12, true)+"\" ");
        sbuff.append("IS_SUM_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_SUM_COLUMN),12, true)+"\" ");
        sbuff.append("IS_GROUP_BY_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.IS_GROUP_BY_COLUMN),12, true)+"\" ");
        sbuff.append("COLUMN_SPACE_AT_FILE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_SPACE_AT_FILE),2, true)+"\" ");
        sbuff.append("FILE_ORDER_BY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FILE_ORDER_BY),2, true)+"\" ");
        sbuff.append("PLUGIN_METHOD_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_METHOD_NAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
        sbuff.append("TARGET_CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TARGET_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_TARGET_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_TARGET_COLUMN),2, true)+"\" ");
        sbuff.append("SOURCE_CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_CONNECTION_ID),2, true)+"\" ");
        sbuff.append("SOURCE_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_SOURCE_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_SOURCE_COLUMN),2, true)+"\" ");
        sbuff.append("TRANSFORMATION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMATION_ID),2, true)+"\" ");
        sbuff.append("TRANSF_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSF_TABLE_ID),2, true)+"\" ");
        sbuff.append("PAR_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PAR_NAME),12, true)+"\" ");
        sbuff.append("FILE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FILE_ID),2, true)+"\" ");
        sbuff.append("PLUGIN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLUGIN_ID),2, true)+"\" ");
        sbuff.append("FREE_FORMAT_TRANSFORMAT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FREE_FORMAT_TRANSFORMAT),12, true)+"\" ");
        sbuff.append("METHOD_PARAMETER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.METHOD_PARAMETER),12, true)+"\" ");
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
    sbuff.append("</Meta_joints>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_joints ( ");
	    		sbuff.append("ID");
		    		sbuff.append(", IS_PK_COLUMN");
	    		sbuff.append(", IS_SUM_COLUMN");
	    		sbuff.append(", IS_GROUP_BY_COLUMN");
	    		sbuff.append(", COLUMN_SPACE_AT_FILE");
	    		sbuff.append(", FILE_ORDER_BY");
	    		sbuff.append(", PLUGIN_METHOD_NAME");
	    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", TRANSFER_ACTION_ID");
	    		sbuff.append(", TARGET_CONNECTION_ID");
	    		sbuff.append(", TARGET_TABLE_ID");
	    		sbuff.append(", COLUMN_ID_TARGET_COLUMN");
	    		sbuff.append(", SOURCE_CONNECTION_ID");
	    		sbuff.append(", SOURCE_TABLE_ID");
	    		sbuff.append(", COLUMN_ID_SOURCE_COLUMN");
	    		sbuff.append(", TRANSFORMATION_ID");
	    		sbuff.append(", TRANSF_TABLE_ID");
	    		sbuff.append(", PAR_NAME");
	    		sbuff.append(", FILE_ID");
	    		sbuff.append(", PLUGIN_ID");
	    		sbuff.append(", FREE_FORMAT_TRANSFORMAT");
	    		sbuff.append(", METHOD_PARAMETER");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.ID,2)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.IS_PK_COLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.IS_SUM_COLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.IS_GROUP_BY_COLUMN,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_SPACE_AT_FILE,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FILE_ORDER_BY,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PLUGIN_METHOD_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFER_ACTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_TABLE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_ID_TARGET_COLUMN,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE_TABLE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_ID_SOURCE_COLUMN,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFORMATION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSF_TABLE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PAR_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FILE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PLUGIN_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FREE_FORMAT_TRANSFORMAT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.METHOD_PARAMETER,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Long getId() { 
    return this.ID;
  }
   public String getIs_pk_column() { 
    return this.IS_PK_COLUMN;
  }
   public String getIs_sum_column() { 
    return this.IS_SUM_COLUMN;
  }
   public String getIs_group_by_column() { 
    return this.IS_GROUP_BY_COLUMN;
  }
   public Long getColumn_space_at_file() { 
    return this.COLUMN_SPACE_AT_FILE;
  }
   public Long getFile_order_by() { 
    return this.FILE_ORDER_BY;
  }
   public String getPlugin_method_name() { 
    return this.PLUGIN_METHOD_NAME;
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
   public Long getTransfer_action_id() { 
    return this.TRANSFER_ACTION_ID;
  }
   public Long getTarget_connection_id() { 
    return this.TARGET_CONNECTION_ID;
  }
   public Long getTarget_table_id() { 
    return this.TARGET_TABLE_ID;
  }
   public Long getColumn_id_target_column() { 
    return this.COLUMN_ID_TARGET_COLUMN;
  }
   public Long getSource_connection_id() { 
    return this.SOURCE_CONNECTION_ID;
  }
   public Long getSource_table_id() { 
    return this.SOURCE_TABLE_ID;
  }
   public Long getColumn_id_source_column() { 
    return this.COLUMN_ID_SOURCE_COLUMN;
  }
   public Long getTransformation_id() { 
    return this.TRANSFORMATION_ID;
  }
   public Long getTransf_table_id() { 
    return this.TRANSF_TABLE_ID;
  }
   public String getPar_name() { 
    return this.PAR_NAME;
  }
   public Long getFile_id() { 
    return this.FILE_ID;
  }
   public Long getPlugin_id() { 
    return this.PLUGIN_ID;
  }
   public String getFree_format_transformat() { 
    return this.FREE_FORMAT_TRANSFORMAT;
  }
   public String getMethod_parameter() { 
    return this.METHOD_PARAMETER;
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
     if (IS_PK_COLUMN == null) {
      IS_PK_COLUMN = "";
    }
     if (IS_SUM_COLUMN == null) {
      IS_SUM_COLUMN = "";
    }
     if (IS_GROUP_BY_COLUMN == null) {
      IS_GROUP_BY_COLUMN = "";
    }
     if (COLUMN_SPACE_AT_FILE == null) {
      COLUMN_SPACE_AT_FILE = new Long (0);
    }
     if (FILE_ORDER_BY == null) {
      FILE_ORDER_BY = new Long (0);
    }
     if (PLUGIN_METHOD_NAME == null) {
      PLUGIN_METHOD_NAME = "";
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
     if (TRANSFER_ACTION_ID == null) {
      TRANSFER_ACTION_ID = new Long (0);
    }
     if (TARGET_CONNECTION_ID == null) {
      TARGET_CONNECTION_ID = new Long (0);
    }
     if (TARGET_TABLE_ID == null) {
      TARGET_TABLE_ID = new Long (0);
    }
     if (COLUMN_ID_TARGET_COLUMN == null) {
      COLUMN_ID_TARGET_COLUMN = new Long (0);
    }
     if (SOURCE_CONNECTION_ID == null) {
      SOURCE_CONNECTION_ID = new Long (0);
    }
     if (SOURCE_TABLE_ID == null) {
      SOURCE_TABLE_ID = new Long (0);
    }
     if (COLUMN_ID_SOURCE_COLUMN == null) {
      COLUMN_ID_SOURCE_COLUMN = new Long (0);
    }
     if (TRANSFORMATION_ID == null) {
      TRANSFORMATION_ID = new Long (0);
    }
     if (TRANSF_TABLE_ID == null) {
      TRANSF_TABLE_ID = new Long (0);
    }
     if (PAR_NAME == null) {
      PAR_NAME = "";
    }
     if (FILE_ID == null) {
      FILE_ID = new Long (0);
    }
     if (PLUGIN_ID == null) {
      PLUGIN_ID = new Long (0);
    }
     if (FREE_FORMAT_TRANSFORMAT == null) {
      FREE_FORMAT_TRANSFORMAT = "";
    }
     if (METHOD_PARAMETER == null) {
      METHOD_PARAMETER = "";
    }
   }

   public void setId(final Long ID) {
    if (validateData){
      DataValidator.validateData((Object)ID,"ID",2,38,0);
    }
    modifiedColumns.add("ID");
    this.ID = ID;
  }
   public void setIs_pk_column(final String IS_PK_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)IS_PK_COLUMN,"IS_PK_COLUMN",12,1,0);
    }
    modifiedColumns.add("IS_PK_COLUMN");
    this.IS_PK_COLUMN = IS_PK_COLUMN;
  }
   public void setIs_sum_column(final String IS_SUM_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)IS_SUM_COLUMN,"IS_SUM_COLUMN",12,1,0);
    }
    modifiedColumns.add("IS_SUM_COLUMN");
    this.IS_SUM_COLUMN = IS_SUM_COLUMN;
  }
   public void setIs_group_by_column(final String IS_GROUP_BY_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)IS_GROUP_BY_COLUMN,"IS_GROUP_BY_COLUMN",12,1,0);
    }
    modifiedColumns.add("IS_GROUP_BY_COLUMN");
    this.IS_GROUP_BY_COLUMN = IS_GROUP_BY_COLUMN;
  }
   public void setColumn_space_at_file(final Long COLUMN_SPACE_AT_FILE) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_SPACE_AT_FILE,"COLUMN_SPACE_AT_FILE",2,10,0);
    }
    modifiedColumns.add("COLUMN_SPACE_AT_FILE");
    this.COLUMN_SPACE_AT_FILE = COLUMN_SPACE_AT_FILE;
  }
   public void setFile_order_by(final Long FILE_ORDER_BY) {
    if (validateData){
      DataValidator.validateData((Object)FILE_ORDER_BY,"FILE_ORDER_BY",2,38,0);
    }
    modifiedColumns.add("FILE_ORDER_BY");
    this.FILE_ORDER_BY = FILE_ORDER_BY;
  }
   public void setPlugin_method_name(final String PLUGIN_METHOD_NAME) {
    if (validateData){
      DataValidator.validateData((Object)PLUGIN_METHOD_NAME,"PLUGIN_METHOD_NAME",12,100,0);
    }
    modifiedColumns.add("PLUGIN_METHOD_NAME");
    this.PLUGIN_METHOD_NAME = PLUGIN_METHOD_NAME;
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
   public void setTransfer_action_id(final Long TRANSFER_ACTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFER_ACTION_ID,"TRANSFER_ACTION_ID",2,38,0);
    }
    modifiedColumns.add("TRANSFER_ACTION_ID");
    this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
  }
   public void setTarget_connection_id(final Long TARGET_CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_CONNECTION_ID,"TARGET_CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("TARGET_CONNECTION_ID");
    this.TARGET_CONNECTION_ID = TARGET_CONNECTION_ID;
  }
   public void setTarget_table_id(final Long TARGET_TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_TABLE_ID,"TARGET_TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TARGET_TABLE_ID");
    this.TARGET_TABLE_ID = TARGET_TABLE_ID;
  }
   public void setColumn_id_target_column(final Long COLUMN_ID_TARGET_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ID_TARGET_COLUMN,"COLUMN_ID_TARGET_COLUMN",2,38,0);
    }
    modifiedColumns.add("COLUMN_ID_TARGET_COLUMN");
    this.COLUMN_ID_TARGET_COLUMN = COLUMN_ID_TARGET_COLUMN;
  }
   public void setSource_connection_id(final Long SOURCE_CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_CONNECTION_ID,"SOURCE_CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("SOURCE_CONNECTION_ID");
    this.SOURCE_CONNECTION_ID = SOURCE_CONNECTION_ID;
  }
   public void setSource_table_id(final Long SOURCE_TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE_TABLE_ID,"SOURCE_TABLE_ID",2,38,0);
    }
    modifiedColumns.add("SOURCE_TABLE_ID");
    this.SOURCE_TABLE_ID = SOURCE_TABLE_ID;
  }
   public void setColumn_id_source_column(final Long COLUMN_ID_SOURCE_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ID_SOURCE_COLUMN,"COLUMN_ID_SOURCE_COLUMN",2,38,0);
    }
    modifiedColumns.add("COLUMN_ID_SOURCE_COLUMN");
    this.COLUMN_ID_SOURCE_COLUMN = COLUMN_ID_SOURCE_COLUMN;
  }
   public void setTransformation_id(final Long TRANSFORMATION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFORMATION_ID,"TRANSFORMATION_ID",2,38,0);
    }
    modifiedColumns.add("TRANSFORMATION_ID");
    this.TRANSFORMATION_ID = TRANSFORMATION_ID;
  }
   public void setTransf_table_id(final Long TRANSF_TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSF_TABLE_ID,"TRANSF_TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TRANSF_TABLE_ID");
    this.TRANSF_TABLE_ID = TRANSF_TABLE_ID;
  }
   public void setPar_name(final String PAR_NAME) {
    if (validateData){
      DataValidator.validateData((Object)PAR_NAME,"PAR_NAME",12,30,0);
    }
    modifiedColumns.add("PAR_NAME");
    this.PAR_NAME = PAR_NAME;
  }
   public void setFile_id(final Long FILE_ID) {
    if (validateData){
      DataValidator.validateData((Object)FILE_ID,"FILE_ID",2,38,0);
    }
    modifiedColumns.add("FILE_ID");
    this.FILE_ID = FILE_ID;
  }
   public void setPlugin_id(final Long PLUGIN_ID) {
    if (validateData){
      DataValidator.validateData((Object)PLUGIN_ID,"PLUGIN_ID",2,38,0);
    }
    modifiedColumns.add("PLUGIN_ID");
    this.PLUGIN_ID = PLUGIN_ID;
  }
   public void setFree_format_transformat(final String FREE_FORMAT_TRANSFORMAT) {
    if (validateData){
      DataValidator.validateData((Object)FREE_FORMAT_TRANSFORMAT,"FREE_FORMAT_TRANSFORMAT",12,2000,0);
    }
    modifiedColumns.add("FREE_FORMAT_TRANSFORMAT");
    this.FREE_FORMAT_TRANSFORMAT = FREE_FORMAT_TRANSFORMAT;
  }
   public void setMethod_parameter(final String METHOD_PARAMETER) {
    if (validateData){
      DataValidator.validateData((Object)METHOD_PARAMETER,"METHOD_PARAMETER",12,200,0);
    }
    modifiedColumns.add("METHOD_PARAMETER");
    this.METHOD_PARAMETER = METHOD_PARAMETER;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_joints o) {

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
  public boolean equals(final Meta_joints o) {

         if ((((this.ID == null) || (o.ID == null)) && (this.ID != o.ID))
            || (((this.IS_PK_COLUMN == null) || (o.IS_PK_COLUMN == null)) && (this.IS_PK_COLUMN != o.IS_PK_COLUMN))
            || (((this.IS_SUM_COLUMN == null) || (o.IS_SUM_COLUMN == null)) && (this.IS_SUM_COLUMN != o.IS_SUM_COLUMN))
            || (((this.IS_GROUP_BY_COLUMN == null) || (o.IS_GROUP_BY_COLUMN == null)) && (this.IS_GROUP_BY_COLUMN != o.IS_GROUP_BY_COLUMN))
            || (((this.COLUMN_SPACE_AT_FILE == null) || (o.COLUMN_SPACE_AT_FILE == null)) && (this.COLUMN_SPACE_AT_FILE != o.COLUMN_SPACE_AT_FILE))
            || (((this.FILE_ORDER_BY == null) || (o.FILE_ORDER_BY == null)) && (this.FILE_ORDER_BY != o.FILE_ORDER_BY))
            || (((this.PLUGIN_METHOD_NAME == null) || (o.PLUGIN_METHOD_NAME == null)) && (this.PLUGIN_METHOD_NAME != o.PLUGIN_METHOD_NAME))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
            || (((this.TARGET_CONNECTION_ID == null) || (o.TARGET_CONNECTION_ID == null)) && (this.TARGET_CONNECTION_ID != o.TARGET_CONNECTION_ID))
            || (((this.TARGET_TABLE_ID == null) || (o.TARGET_TABLE_ID == null)) && (this.TARGET_TABLE_ID != o.TARGET_TABLE_ID))
            || (((this.COLUMN_ID_TARGET_COLUMN == null) || (o.COLUMN_ID_TARGET_COLUMN == null)) && (this.COLUMN_ID_TARGET_COLUMN != o.COLUMN_ID_TARGET_COLUMN))
            || (((this.SOURCE_CONNECTION_ID == null) || (o.SOURCE_CONNECTION_ID == null)) && (this.SOURCE_CONNECTION_ID != o.SOURCE_CONNECTION_ID))
            || (((this.SOURCE_TABLE_ID == null) || (o.SOURCE_TABLE_ID == null)) && (this.SOURCE_TABLE_ID != o.SOURCE_TABLE_ID))
            || (((this.COLUMN_ID_SOURCE_COLUMN == null) || (o.COLUMN_ID_SOURCE_COLUMN == null)) && (this.COLUMN_ID_SOURCE_COLUMN != o.COLUMN_ID_SOURCE_COLUMN))
            || (((this.TRANSFORMATION_ID == null) || (o.TRANSFORMATION_ID == null)) && (this.TRANSFORMATION_ID != o.TRANSFORMATION_ID))
            || (((this.TRANSF_TABLE_ID == null) || (o.TRANSF_TABLE_ID == null)) && (this.TRANSF_TABLE_ID != o.TRANSF_TABLE_ID))
            || (((this.PAR_NAME == null) || (o.PAR_NAME == null)) && (this.PAR_NAME != o.PAR_NAME))
            || (((this.FILE_ID == null) || (o.FILE_ID == null)) && (this.FILE_ID != o.FILE_ID))
            || (((this.PLUGIN_ID == null) || (o.PLUGIN_ID == null)) && (this.PLUGIN_ID != o.PLUGIN_ID))
            || (((this.FREE_FORMAT_TRANSFORMAT == null) || (o.FREE_FORMAT_TRANSFORMAT == null)) && (this.FREE_FORMAT_TRANSFORMAT != o.FREE_FORMAT_TRANSFORMAT))
            || (((this.METHOD_PARAMETER == null) || (o.METHOD_PARAMETER == null)) && (this.METHOD_PARAMETER != o.METHOD_PARAMETER))
          ){
    return false;
    } else
         if ((((this.ID != null) && (o.ID != null)) && (this.ID.equals(o.ID) == false))
            || (((this.IS_PK_COLUMN != null) && (o.IS_PK_COLUMN != null)) && (this.IS_PK_COLUMN.equals(o.IS_PK_COLUMN) == false))
            || (((this.IS_SUM_COLUMN != null) && (o.IS_SUM_COLUMN != null)) && (this.IS_SUM_COLUMN.equals(o.IS_SUM_COLUMN) == false))
            || (((this.IS_GROUP_BY_COLUMN != null) && (o.IS_GROUP_BY_COLUMN != null)) && (this.IS_GROUP_BY_COLUMN.equals(o.IS_GROUP_BY_COLUMN) == false))
            || (((this.COLUMN_SPACE_AT_FILE != null) && (o.COLUMN_SPACE_AT_FILE != null)) && (this.COLUMN_SPACE_AT_FILE.equals(o.COLUMN_SPACE_AT_FILE) == false))
            || (((this.FILE_ORDER_BY != null) && (o.FILE_ORDER_BY != null)) && (this.FILE_ORDER_BY.equals(o.FILE_ORDER_BY) == false))
            || (((this.PLUGIN_METHOD_NAME != null) && (o.PLUGIN_METHOD_NAME != null)) && (this.PLUGIN_METHOD_NAME.equals(o.PLUGIN_METHOD_NAME) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
            || (((this.TARGET_CONNECTION_ID != null) && (o.TARGET_CONNECTION_ID != null)) && (this.TARGET_CONNECTION_ID.equals(o.TARGET_CONNECTION_ID) == false))
            || (((this.TARGET_TABLE_ID != null) && (o.TARGET_TABLE_ID != null)) && (this.TARGET_TABLE_ID.equals(o.TARGET_TABLE_ID) == false))
            || (((this.COLUMN_ID_TARGET_COLUMN != null) && (o.COLUMN_ID_TARGET_COLUMN != null)) && (this.COLUMN_ID_TARGET_COLUMN.equals(o.COLUMN_ID_TARGET_COLUMN) == false))
            || (((this.SOURCE_CONNECTION_ID != null) && (o.SOURCE_CONNECTION_ID != null)) && (this.SOURCE_CONNECTION_ID.equals(o.SOURCE_CONNECTION_ID) == false))
            || (((this.SOURCE_TABLE_ID != null) && (o.SOURCE_TABLE_ID != null)) && (this.SOURCE_TABLE_ID.equals(o.SOURCE_TABLE_ID) == false))
            || (((this.COLUMN_ID_SOURCE_COLUMN != null) && (o.COLUMN_ID_SOURCE_COLUMN != null)) && (this.COLUMN_ID_SOURCE_COLUMN.equals(o.COLUMN_ID_SOURCE_COLUMN) == false))
            || (((this.TRANSFORMATION_ID != null) && (o.TRANSFORMATION_ID != null)) && (this.TRANSFORMATION_ID.equals(o.TRANSFORMATION_ID) == false))
            || (((this.TRANSF_TABLE_ID != null) && (o.TRANSF_TABLE_ID != null)) && (this.TRANSF_TABLE_ID.equals(o.TRANSF_TABLE_ID) == false))
            || (((this.PAR_NAME != null) && (o.PAR_NAME != null)) && (this.PAR_NAME.equals(o.PAR_NAME) == false))
            || (((this.FILE_ID != null) && (o.FILE_ID != null)) && (this.FILE_ID.equals(o.FILE_ID) == false))
            || (((this.PLUGIN_ID != null) && (o.PLUGIN_ID != null)) && (this.PLUGIN_ID.equals(o.PLUGIN_ID) == false))
            || (((this.FREE_FORMAT_TRANSFORMAT != null) && (o.FREE_FORMAT_TRANSFORMAT != null)) && (this.FREE_FORMAT_TRANSFORMAT.equals(o.FREE_FORMAT_TRANSFORMAT) == false))
            || (((this.METHOD_PARAMETER != null) && (o.METHOD_PARAMETER != null)) && (this.METHOD_PARAMETER.equals(o.METHOD_PARAMETER) == false))
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
  * return 1
  */
  public static int getIs_pk_columnColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_pk_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_pk_columnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getIs_sum_columnColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_sum_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_sum_columnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getIs_group_by_columnColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIs_group_by_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIs_group_by_columnSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getColumn_space_at_fileColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_space_at_fileDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_space_at_fileSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getFile_order_byColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFile_order_byDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getFile_order_bySQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getPlugin_method_nameColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlugin_method_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPlugin_method_nameSQLType() {
    
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
  * return 38
  */
  public static int getTransfer_action_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransfer_action_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTransfer_action_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getTarget_connection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_connection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTarget_connection_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getTarget_table_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTarget_table_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTarget_table_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getColumn_id_target_columnColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_id_target_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_id_target_columnSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getSource_connection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_connection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getSource_connection_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getSource_table_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSource_table_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getSource_table_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getColumn_id_source_columnColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_id_source_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_id_source_columnSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getTransformation_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransformation_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTransformation_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getTransf_table_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransf_table_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTransf_table_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getPar_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPar_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPar_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getFile_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFile_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getFile_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getPlugin_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlugin_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getPlugin_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 2000
  */
  public static int getFree_format_transformatColumnSize() {
    
     return 2000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFree_format_transformatDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFree_format_transformatSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 200
  */
  public static int getMethod_parameterColumnSize() {
    
     return 200;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMethod_parameterDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMethod_parameterSQLType() {
    
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

  public Meta_joints getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_joints original) {
    this.original = (Meta_joints) original.clone();
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
