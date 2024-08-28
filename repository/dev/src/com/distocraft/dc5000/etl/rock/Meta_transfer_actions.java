package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.sql.Timestamp; // NOPMD : eeipca : may not be used
import java.util.Date; // NOPMD : eeipca : may not be used
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import ssc.rockfactory.DataValidator;
import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockDBObject;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

@SuppressWarnings({"PMD.SuspiciousConstantFieldName"}) // eeipca : columns are in UPPERCASE
public class Meta_transfer_actions implements Cloneable,RockDBObject  {

    private String VERSION_NUMBER;
    private Long TRANSFER_ACTION_ID;
    private Long COLLECTION_ID;
    private Long COLLECTION_SET_ID;
    private String ACTION_TYPE;
    private String TRANSFER_ACTION_NAME;
    private Long ORDER_BY_NO;
    private String DESCRIPTION;
    private String WHERE_CLAUSE_01;
    private String ACTION_CONTENTS_01;
    private String ENABLED_FLAG;
    private Long CONNECTION_ID;
    private String WHERE_CLAUSE_02;
    private String WHERE_CLAUSE_03;
    private String ACTION_CONTENTS_02;
    private String ACTION_CONTENTS_03;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSION_NUMBER"    ,"TRANSFER_ACTION_ID"    ,"COLLECTION_ID"    ,"COLLECTION_SET_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private boolean isDecryptionRequired = true;
  
  private Meta_transfer_actions original; 
  
  public Meta_transfer_actions(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_transfer_actions(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSION_NUMBER = null;
         this.TRANSFER_ACTION_ID = null;
         this.COLLECTION_ID = null;
         this.COLLECTION_SET_ID = null;
         this.ACTION_TYPE = null;
         this.TRANSFER_ACTION_NAME = null;
         this.ORDER_BY_NO = null;
         this.DESCRIPTION = null;
         this.WHERE_CLAUSE_01 = null;
         this.ACTION_CONTENTS_01 = null;
         this.ENABLED_FLAG = null;
         this.CONNECTION_ID = null;
         this.WHERE_CLAUSE_02 = null;
         this.WHERE_CLAUSE_03 = null;
         this.ACTION_CONTENTS_02 = null;
         this.ACTION_CONTENTS_03 = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_transfer_actions(final RockFactory rockFact   ,final String VERSION_NUMBER ,final Long TRANSFER_ACTION_ID ,final Long COLLECTION_ID ,final Long COLLECTION_SET_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSION_NUMBER = VERSION_NUMBER;
            this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
            this.COLLECTION_ID = COLLECTION_ID;
            this.COLLECTION_SET_ID = COLLECTION_SET_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_transfer_actions> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_transfer_actions o = it.next();

              this.VERSION_NUMBER = o.getVersion_number();
              this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.ACTION_TYPE = o.getAction_type();
              this.TRANSFER_ACTION_NAME = o.getTransfer_action_name();
              this.ORDER_BY_NO = o.getOrder_by_no();
              this.DESCRIPTION = o.getDescription();
              this.WHERE_CLAUSE_01 = o.getWhere_clause_01();
              this.ACTION_CONTENTS_01 = o.getAction_contents_01();
              this.ENABLED_FLAG = o.getEnabled_flag();
              this.CONNECTION_ID = o.getConnection_id();
              this.WHERE_CLAUSE_02 = o.getWhere_clause_02();
              this.WHERE_CLAUSE_03 = o.getWhere_clause_03();
              this.ACTION_CONTENTS_02 = o.getAction_contents_02();
              this.ACTION_CONTENTS_03 = o.getAction_contents_03();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transfer_actions");
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
  public Meta_transfer_actions(final RockFactory rockFact, final Meta_transfer_actions whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_transfer_actions> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_transfer_actions o = it.next();
                this.VERSION_NUMBER = o.getVersion_number();
                this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.ACTION_TYPE = o.getAction_type();
                this.TRANSFER_ACTION_NAME = o.getTransfer_action_name();
                this.ORDER_BY_NO = o.getOrder_by_no();
                this.DESCRIPTION = o.getDescription();
                this.WHERE_CLAUSE_01 = o.getWhere_clause_01();
                this.ACTION_CONTENTS_01 = o.getAction_contents_01();
                this.ENABLED_FLAG = o.getEnabled_flag();
                this.CONNECTION_ID = o.getConnection_id();
                this.WHERE_CLAUSE_02 = o.getWhere_clause_02();
                this.WHERE_CLAUSE_03 = o.getWhere_clause_03();
                this.ACTION_CONTENTS_02 = o.getAction_contents_02();
                this.ACTION_CONTENTS_03 = o.getAction_contents_03();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_transfer_actions");
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
    return "Meta_transfer_actions";
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
  public int updateDB(final boolean useTimestamp, final Meta_transfer_actions whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_transfer_actions whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_transfer_actions.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_transfer_actions ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("ACTION_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_TYPE),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_NAME),12, true)+"\" ");
        sbuff.append("ORDER_BY_NO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDER_BY_NO),2, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_01=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_01),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_01=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_01),12, true)+"\" ");
        sbuff.append("ENABLED_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLED_FLAG),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_02=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_02),12, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_03=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_03),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_02=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_02),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_03=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_03),12, true)+"\" ");
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
    sbuff.append("<Meta_transfer_actions ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("ACTION_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_TYPE),12, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_NAME),12, true)+"\" ");
        sbuff.append("ORDER_BY_NO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDER_BY_NO),2, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_01=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_01),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_01=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_01),12, true)+"\" ");
        sbuff.append("ENABLED_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENABLED_FLAG),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_02=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_02),12, true)+"\" ");
        sbuff.append("WHERE_CLAUSE_03=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.WHERE_CLAUSE_03),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_02=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_02),12, true)+"\" ");
        sbuff.append("ACTION_CONTENTS_03=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ACTION_CONTENTS_03),12, true)+"\" ");
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
    sbuff.append("</Meta_transfer_actions>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_transfer_actions ( ");
	    		sbuff.append("VERSION_NUMBER");
		    		sbuff.append(", TRANSFER_ACTION_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", ACTION_TYPE");
	    		sbuff.append(", TRANSFER_ACTION_NAME");
	    		sbuff.append(", ORDER_BY_NO");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", WHERE_CLAUSE_01");
	    		sbuff.append(", ACTION_CONTENTS_01");
	    		sbuff.append(", ENABLED_FLAG");
	    		sbuff.append(", CONNECTION_ID");
	    		sbuff.append(", WHERE_CLAUSE_02");
	    		sbuff.append(", WHERE_CLAUSE_03");
	    		sbuff.append(", ACTION_CONTENTS_02");
	    		sbuff.append(", ACTION_CONTENTS_03");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFER_ACTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ACTION_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFER_ACTION_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ORDER_BY_NO,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WHERE_CLAUSE_01,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ACTION_CONTENTS_01,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENABLED_FLAG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WHERE_CLAUSE_02,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.WHERE_CLAUSE_03,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ACTION_CONTENTS_02,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ACTION_CONTENTS_03,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getTransfer_action_id() { 
    return this.TRANSFER_ACTION_ID;
  }
   public Long getCollection_id() { 
    return this.COLLECTION_ID;
  }
   public Long getCollection_set_id() { 
    return this.COLLECTION_SET_ID;
  }
   public String getAction_type() { 
    return this.ACTION_TYPE;
  }
   public String getTransfer_action_name() { 
    return this.TRANSFER_ACTION_NAME;
  }
   public Long getOrder_by_no() { 
    return this.ORDER_BY_NO;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getWhere_clause_01() { 
    return this.WHERE_CLAUSE_01;
  }
   public String getAction_contents_01() { 
	if (this.ACTION_CONTENTS_01 == null)
		  return this.ACTION_CONTENTS_01; 
	return decryptPassInActionContents(this.ACTION_CONTENTS_01);
  }
   public String getEnabled_flag() { 
    return this.ENABLED_FLAG;
  }
   public Long getConnection_id() { 
    return this.CONNECTION_ID;
  }
   public String getWhere_clause_02() { 
    return this.WHERE_CLAUSE_02;
  }
   public String getWhere_clause_03() { 
    return this.WHERE_CLAUSE_03;
  }
   public String getAction_contents_02() { 
    return this.ACTION_CONTENTS_02;
  }
   public String getAction_contents_03() { 
    return this.ACTION_CONTENTS_03;
  }
 
  public String getWhere_clause() {
    final java.util.List<String> whereClauseList = new java.util.ArrayList<String>();
    whereClauseList.add((null == this.WHERE_CLAUSE_01) ? "" : this.WHERE_CLAUSE_01);
    whereClauseList.add((null == this.WHERE_CLAUSE_02) ? "" : this.WHERE_CLAUSE_02);
    whereClauseList.add((null == this.WHERE_CLAUSE_03) ? "" : this.WHERE_CLAUSE_03);
    return joinStringsIntoOne(whereClauseList);
  }

  public String getAction_contents() {
    final java.util.List<String> actionContentsList = new java.util.ArrayList<String>();
    actionContentsList.add((null == this.ACTION_CONTENTS_01) ? "" : this.ACTION_CONTENTS_01);
    actionContentsList.add((null == this.ACTION_CONTENTS_02) ? "" : this.ACTION_CONTENTS_02);
    actionContentsList.add((null == this.ACTION_CONTENTS_03) ? "" : this.ACTION_CONTENTS_03);
    String actionContents = joinStringsIntoOne(actionContentsList);
    actionContents = decryptPassInActionContents(actionContents);
    return actionContents;
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
     if (TRANSFER_ACTION_ID == null) {
      TRANSFER_ACTION_ID = new Long (0);
    }
     if (COLLECTION_ID == null) {
      COLLECTION_ID = new Long (0);
    }
     if (COLLECTION_SET_ID == null) {
      COLLECTION_SET_ID = new Long (0);
    }
     if (ACTION_TYPE == null) {
      ACTION_TYPE = "";
    }
     if (TRANSFER_ACTION_NAME == null) {
      TRANSFER_ACTION_NAME = "";
    }
     if (ORDER_BY_NO == null) {
      ORDER_BY_NO = new Long (0);
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (WHERE_CLAUSE_01 == null) {
      WHERE_CLAUSE_01 = "";
    }
     if (ACTION_CONTENTS_01 == null) {
      ACTION_CONTENTS_01 = "";
    }
     if (ENABLED_FLAG == null) {
      ENABLED_FLAG = "";
    }
     if (CONNECTION_ID == null) {
      CONNECTION_ID = new Long (0);
    }
     if (WHERE_CLAUSE_02 == null) {
      WHERE_CLAUSE_02 = "";
    }
     if (WHERE_CLAUSE_03 == null) {
      WHERE_CLAUSE_03 = "";
    }
     if (ACTION_CONTENTS_02 == null) {
      ACTION_CONTENTS_02 = "";
    }
     if (ACTION_CONTENTS_03 == null) {
      ACTION_CONTENTS_03 = "";
    }
   }

   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setTransfer_action_id(final Long TRANSFER_ACTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFER_ACTION_ID,"TRANSFER_ACTION_ID",2,38,0);
    }
    modifiedColumns.add("TRANSFER_ACTION_ID");
    this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
  }
   public void setCollection_id(final Long COLLECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_ID,"COLLECTION_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_ID");
    this.COLLECTION_ID = COLLECTION_ID;
  }
   public void setCollection_set_id(final Long COLLECTION_SET_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLLECTION_SET_ID,"COLLECTION_SET_ID",2,38,0);
    }
    modifiedColumns.add("COLLECTION_SET_ID");
    this.COLLECTION_SET_ID = COLLECTION_SET_ID;
  }
   public void setAction_type(final String ACTION_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)ACTION_TYPE,"ACTION_TYPE",12,20,0);
    }
    modifiedColumns.add("ACTION_TYPE");
    this.ACTION_TYPE = ACTION_TYPE;
  }
   public void setTransfer_action_name(final String TRANSFER_ACTION_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFER_ACTION_NAME,"TRANSFER_ACTION_NAME",12,128,0);
    }
    modifiedColumns.add("TRANSFER_ACTION_NAME");
    this.TRANSFER_ACTION_NAME = TRANSFER_ACTION_NAME;
  }
   public void setOrder_by_no(final Long ORDER_BY_NO) {
    if (validateData){
      DataValidator.validateData((Object)ORDER_BY_NO,"ORDER_BY_NO",2,38,0);
    }
    modifiedColumns.add("ORDER_BY_NO");
    this.ORDER_BY_NO = ORDER_BY_NO;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setWhere_clause_01(final String WHERE_CLAUSE_01) {
    if (validateData){
      DataValidator.validateData((Object)WHERE_CLAUSE_01,"WHERE_CLAUSE_01",12,32000,0);
    }
    modifiedColumns.add("WHERE_CLAUSE_01");
    this.WHERE_CLAUSE_01 = WHERE_CLAUSE_01;
  }
   public void setAction_contents_01(final String ACTION_CONTENTS_01) {
    if (validateData){
      DataValidator.validateData((Object)ACTION_CONTENTS_01,"ACTION_CONTENTS_01",12,32000,0);
    }
    modifiedColumns.add("ACTION_CONTENTS_01");
    this.ACTION_CONTENTS_01 = ACTION_CONTENTS_01;
  }
   public void setEnabled_flag(final String ENABLED_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)ENABLED_FLAG,"ENABLED_FLAG",12,1,0);
    }
    modifiedColumns.add("ENABLED_FLAG");
    this.ENABLED_FLAG = ENABLED_FLAG;
  }
   public void setConnection_id(final Long CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_ID,"CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("CONNECTION_ID");
    this.CONNECTION_ID = CONNECTION_ID;
  }
   public void setWhere_clause_02(final String WHERE_CLAUSE_02) {
    if (validateData){
      DataValidator.validateData((Object)WHERE_CLAUSE_02,"WHERE_CLAUSE_02",12,32000,0);
    }
    modifiedColumns.add("WHERE_CLAUSE_02");
    this.WHERE_CLAUSE_02 = WHERE_CLAUSE_02;
  }
   public void setWhere_clause_03(final String WHERE_CLAUSE_03) {
    if (validateData){
      DataValidator.validateData((Object)WHERE_CLAUSE_03,"WHERE_CLAUSE_03",12,32000,0);
    }
    modifiedColumns.add("WHERE_CLAUSE_03");
    this.WHERE_CLAUSE_03 = WHERE_CLAUSE_03;
  }
   public void setAction_contents_02(final String ACTION_CONTENTS_02) {
    if (validateData){
      DataValidator.validateData((Object)ACTION_CONTENTS_02,"ACTION_CONTENTS_02",12,32000,0);
    }
    modifiedColumns.add("ACTION_CONTENTS_02");
    this.ACTION_CONTENTS_02 = ACTION_CONTENTS_02;
  }
   public void setAction_contents_03(final String ACTION_CONTENTS_03) {
    if (validateData){
      DataValidator.validateData((Object)ACTION_CONTENTS_03,"ACTION_CONTENTS_03",12,32000,0);
    }
    modifiedColumns.add("ACTION_CONTENTS_03");
    this.ACTION_CONTENTS_03 = ACTION_CONTENTS_03;
  }
 

      public void setWhere_clause(final String WHERE_CLAUSE) {
    if (stringTooLong(WHERE_CLAUSE.length())) {
      final java.util.logging.Logger log = java.util.logging.Logger.getLogger("etl.engine.RockFactory");
      log.severe("On setting META_TRANSFER_ACTION (TRANSFER_ACTION_ID=" + this.TRANSFER_ACTION_ID + ", TRANSFER_ACTION_NAME="
          + this.TRANSFER_ACTION_NAME + ", COLLECTION_ID=" + this.COLLECTION_ID + ", COLLECTION_SET_ID="
          + this.COLLECTION_SET_ID + ") the WHERE_CLAUSE is longer than maximum length ("
          + getTotalSQLClauseMaxLength() + "). Rest of the clause is not stored.");
    }
    final String[] splittedWhereClause = splitStringIntoPieces(WHERE_CLAUSE, 32000);
    setWhere_clause_01(splittedWhereClause[0]);
    setWhere_clause_02(splittedWhereClause[1]);
    setWhere_clause_03(splittedWhereClause[2]);
  }

  public void setAction_contents(final String value) {
	  if (stringTooLong(value.length())) {
	      final java.util.logging.Logger log = java.util.logging.Logger.getLogger("etl.engine.RockFactory");
	      log.severe("On setting META_TRANSFER_ACTION (TRANSFER_ACTION_ID=" + this.TRANSFER_ACTION_ID + ", TRANSFER_ACTION_NAME="
	          + this.TRANSFER_ACTION_NAME + ", COLLECTION_ID=" + this.COLLECTION_ID + ", COLLECTION_SET_ID="
	          + this.COLLECTION_SET_ID + ") the ACTION_CONTENTS is longer than maximum length ("
	          + getTotalSQLClauseMaxLength() + "). Rest of the clause is not stored.");
	    }
	  	String actionContents = encryptpassInActionContents(value);
	    final String[] splittedActionContents = splitStringIntoPieces(actionContents, 100000);
	    setAction_contents_01(splittedActionContents[0]);
	    setAction_contents_02(splittedActionContents[1]);
	    setAction_contents_03(splittedActionContents[2]);
  }
  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_transfer_actions o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
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
  public boolean equals(final Meta_transfer_actions o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.ACTION_TYPE == null) || (o.ACTION_TYPE == null)) && (this.ACTION_TYPE != o.ACTION_TYPE))
            || (((this.TRANSFER_ACTION_NAME == null) || (o.TRANSFER_ACTION_NAME == null)) && (this.TRANSFER_ACTION_NAME != o.TRANSFER_ACTION_NAME))
            || (((this.ORDER_BY_NO == null) || (o.ORDER_BY_NO == null)) && (this.ORDER_BY_NO != o.ORDER_BY_NO))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.WHERE_CLAUSE_01 == null) || (o.WHERE_CLAUSE_01 == null)) && (this.WHERE_CLAUSE_01 != o.WHERE_CLAUSE_01))
            || (((this.ACTION_CONTENTS_01 == null) || (o.ACTION_CONTENTS_01 == null)) && (this.ACTION_CONTENTS_01 != o.ACTION_CONTENTS_01))
            || (((this.ENABLED_FLAG == null) || (o.ENABLED_FLAG == null)) && (this.ENABLED_FLAG != o.ENABLED_FLAG))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.WHERE_CLAUSE_02 == null) || (o.WHERE_CLAUSE_02 == null)) && (this.WHERE_CLAUSE_02 != o.WHERE_CLAUSE_02))
            || (((this.WHERE_CLAUSE_03 == null) || (o.WHERE_CLAUSE_03 == null)) && (this.WHERE_CLAUSE_03 != o.WHERE_CLAUSE_03))
            || (((this.ACTION_CONTENTS_02 == null) || (o.ACTION_CONTENTS_02 == null)) && (this.ACTION_CONTENTS_02 != o.ACTION_CONTENTS_02))
            || (((this.ACTION_CONTENTS_03 == null) || (o.ACTION_CONTENTS_03 == null)) && (this.ACTION_CONTENTS_03 != o.ACTION_CONTENTS_03))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.ACTION_TYPE != null) && (o.ACTION_TYPE != null)) && (this.ACTION_TYPE.equals(o.ACTION_TYPE) == false))
            || (((this.TRANSFER_ACTION_NAME != null) && (o.TRANSFER_ACTION_NAME != null)) && (this.TRANSFER_ACTION_NAME.equals(o.TRANSFER_ACTION_NAME) == false))
            || (((this.ORDER_BY_NO != null) && (o.ORDER_BY_NO != null)) && (this.ORDER_BY_NO.equals(o.ORDER_BY_NO) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.WHERE_CLAUSE_01 != null) && (o.WHERE_CLAUSE_01 != null)) && (this.WHERE_CLAUSE_01.equals(o.WHERE_CLAUSE_01) == false))
            || (((this.ACTION_CONTENTS_01 != null) && (o.ACTION_CONTENTS_01 != null)) && (this.ACTION_CONTENTS_01.equals(o.ACTION_CONTENTS_01) == false))
            || (((this.ENABLED_FLAG != null) && (o.ENABLED_FLAG != null)) && (this.ENABLED_FLAG.equals(o.ENABLED_FLAG) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.WHERE_CLAUSE_02 != null) && (o.WHERE_CLAUSE_02 != null)) && (this.WHERE_CLAUSE_02.equals(o.WHERE_CLAUSE_02) == false))
            || (((this.WHERE_CLAUSE_03 != null) && (o.WHERE_CLAUSE_03 != null)) && (this.WHERE_CLAUSE_03.equals(o.WHERE_CLAUSE_03) == false))
            || (((this.ACTION_CONTENTS_02 != null) && (o.ACTION_CONTENTS_02 != null)) && (this.ACTION_CONTENTS_02.equals(o.ACTION_CONTENTS_02) == false))
            || (((this.ACTION_CONTENTS_03 != null) && (o.ACTION_CONTENTS_03 != null)) && (this.ACTION_CONTENTS_03.equals(o.ACTION_CONTENTS_03) == false))
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
  * return 20
  */
  public static int getAction_typeColumnSize() {
    
     return 20;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAction_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAction_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getTransfer_action_nameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransfer_action_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTransfer_action_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getOrder_by_noColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOrder_by_noDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getOrder_by_noSQLType() {
    
    return 2;   
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
  * return 32000
  */
  public static int getWhere_clause_01ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWhere_clause_01DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getWhere_clause_01SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getAction_contents_01ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAction_contents_01DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAction_contents_01SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 1
  */
  public static int getEnabled_flagColumnSize() {
    
     return 1;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEnabled_flagDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getEnabled_flagSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getConnection_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConnection_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getConnection_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getWhere_clause_02ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWhere_clause_02DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getWhere_clause_02SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getWhere_clause_03ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getWhere_clause_03DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getWhere_clause_03SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getAction_contents_02ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAction_contents_02DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAction_contents_02SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getAction_contents_03ColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAction_contents_03DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAction_contents_03SQLType() {
    
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

  public Meta_transfer_actions getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_transfer_actions original) {
    this.original = (Meta_transfer_actions) original.clone();
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

  private String[] splitStringIntoPieces(final String string, final int howLongPieces) {
    final int NUMBER_OF_CLAUSE_PARTS = 3;
    final String[] splitted = new String[NUMBER_OF_CLAUSE_PARTS];
    for(int i=0; i<splitted.length; i++){
      splitted[i] = "";
    }

    if (null != string) {
      final int stringLength = string.length();
      final int intoHowMany = (int) Math.ceil((double)stringLength/(double)howLongPieces);
      for (int i = 0; i < NUMBER_OF_CLAUSE_PARTS; i++) {
        if(i < intoHowMany){
          int endIndex = i * howLongPieces + howLongPieces;
          endIndex = endIndex > stringLength ? stringLength : endIndex;
          splitted[i] = string.substring(i * howLongPieces, endIndex);
        }
      }
    }

    return splitted;
  }

  private String joinStringsIntoOne(final java.util.List strings) {
    final StringBuffer returnString = new StringBuffer();
    returnString.append("");

    if(null != strings){
      for (Object string : strings) {
        if (null != string) {
          returnString.append(string.toString());
        }
      }
    }
    return returnString.toString();
  }

  private boolean stringTooLong(final int length) {
    return (getTotalSQLClauseMaxLength() < length);
  }

  private int getTotalSQLClauseMaxLength() {
    return 32000 * 3;
  }
  
	public boolean getIsDecryptionRequired() {
		return isDecryptionRequired;
	}

	public void setIsDecryptionRequired(boolean isDecryptionRequired) {
		this.isDecryptionRequired = isDecryptionRequired;
	}

	private String decryptPassInActionContents(String Action_Contents) {
		final java.util.logging.Logger log = java.util.logging.Logger.getLogger("etl.engine.RockFactory");
		String actionContents = Action_Contents;
		String[] splitActionContents = actionContents.split("\n");
		String encryptionFlag = "";
		String password = "";
		for (String field : splitActionContents) {
			if (field.contains("EncryptionFlag")) {
				encryptionFlag = field.substring(field.indexOf('=') + 1).trim();
			} else if (field.contains("password")) {
				password = field.substring(field.indexOf('=') + 1).trim();
			}
		}
		log.finest("actionContents before decrypting" + actionContents);
		if ("Y".equalsIgnoreCase(encryptionFlag) && getIsDecryptionRequired()) {
			Base64.Decoder decoder = Base64.getDecoder();
			String decryptedPassword = new String(decoder.decode(password));
			actionContents = actionContents.replace("password=" + password, "password=" + decryptedPassword);
			actionContents = actionContents.replace("EncryptionFlag=" + encryptionFlag, "EncryptionFlag=N");
		}
		log.finest("actionContents after decrypting" + actionContents);
		return actionContents;
	}

	private String encryptpassInActionContents(String Action_Contents) {
		final java.util.logging.Logger log = java.util.logging.Logger.getLogger("etl.engine.RockFactory");
		String actionContents = Action_Contents;
		String[] splitActionContents = actionContents.split("\n");
		String encryptionFlag = "";
		String password = "";
		for (String field : splitActionContents) {
			if (field.contains("EncryptionFlag")) {
				encryptionFlag = field.substring(field.indexOf('=') + 1).trim();
			} else if (field.contains("password")) {
				password = field.substring(field.indexOf('=') + 1).trim();
			}
		}
		log.finest("actionContents before encrypting" + actionContents);
		if ("N".equalsIgnoreCase(encryptionFlag)) {
			Base64.Encoder encoder = Base64.getEncoder();
			String encryptedPassword = encoder.encodeToString(password.getBytes());
			actionContents = actionContents.replace("password=" + password, "password=" + encryptedPassword);
			actionContents = actionContents.replace("EncryptionFlag=" + encryptionFlag, "EncryptionFlag=Y");
		}
		log.finest("actionContents after encrypting" + actionContents);
		return actionContents;
	}
}
