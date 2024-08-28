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
public class Meta_fk_table_joints implements Cloneable,RockDBObject  {

    private String VERSION_NUMBER;
    private Long CONNECTION_ID;
    private Long TABLE_ID;
    private Long COLUMN_ID_FK_COLUMN;
    private Long TARGET_TABLE_ID;
    private Long COLUMN_ID;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private Long TRANSFER_ACTION_ID;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSION_NUMBER"    ,"CONNECTION_ID"    ,"TABLE_ID"    ,"COLUMN_ID_FK_COLUMN"    ,"TARGET_TABLE_ID"    ,"COLUMN_ID"    ,"COLLECTION_SET_ID"    ,"COLLECTION_ID"    ,"TRANSFER_ACTION_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Meta_fk_table_joints original; 

  public Meta_fk_table_joints(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_fk_table_joints(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSION_NUMBER = null;
         this.CONNECTION_ID = null;
         this.TABLE_ID = null;
         this.COLUMN_ID_FK_COLUMN = null;
         this.TARGET_TABLE_ID = null;
         this.COLUMN_ID = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.TRANSFER_ACTION_ID = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_fk_table_joints(final RockFactory rockFact   ,final String VERSION_NUMBER ,final Long CONNECTION_ID ,final Long TABLE_ID ,final Long COLUMN_ID_FK_COLUMN ,final Long TARGET_TABLE_ID ,final Long COLUMN_ID ,final Long COLLECTION_SET_ID ,final Long COLLECTION_ID ,final Long TRANSFER_ACTION_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSION_NUMBER = VERSION_NUMBER;
            this.CONNECTION_ID = CONNECTION_ID;
            this.TABLE_ID = TABLE_ID;
            this.COLUMN_ID_FK_COLUMN = COLUMN_ID_FK_COLUMN;
            this.TARGET_TABLE_ID = TARGET_TABLE_ID;
            this.COLUMN_ID = COLUMN_ID;
            this.COLLECTION_SET_ID = COLLECTION_SET_ID;
            this.COLLECTION_ID = COLLECTION_ID;
            this.TRANSFER_ACTION_ID = TRANSFER_ACTION_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_fk_table_joints> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_fk_table_joints o = it.next();

              this.VERSION_NUMBER = o.getVersion_number();
              this.CONNECTION_ID = o.getConnection_id();
              this.TABLE_ID = o.getTable_id();
              this.COLUMN_ID_FK_COLUMN = o.getColumn_id_fk_column();
              this.TARGET_TABLE_ID = o.getTarget_table_id();
              this.COLUMN_ID = o.getColumn_id();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_fk_table_joints");
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
  public Meta_fk_table_joints(final RockFactory rockFact, final Meta_fk_table_joints whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_fk_table_joints> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_fk_table_joints o = it.next();
                this.VERSION_NUMBER = o.getVersion_number();
                this.CONNECTION_ID = o.getConnection_id();
                this.TABLE_ID = o.getTable_id();
                this.COLUMN_ID_FK_COLUMN = o.getColumn_id_fk_column();
                this.TARGET_TABLE_ID = o.getTarget_table_id();
                this.COLUMN_ID = o.getColumn_id();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.TRANSFER_ACTION_ID = o.getTransfer_action_id();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_fk_table_joints");
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
    return "Meta_fk_table_joints";
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
  public int updateDB(final boolean useTimestamp, final Meta_fk_table_joints whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_fk_table_joints whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_fk_table_joints.saveDB(), no primary key defined");
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
    sbuff.append("<Meta_fk_table_joints ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_FK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_FK_COLUMN),2, true)+"\" ");
        sbuff.append("TARGET_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
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
    sbuff.append("<Meta_fk_table_joints ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID_FK_COLUMN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID_FK_COLUMN),2, true)+"\" ");
        sbuff.append("TARGET_TABLE_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET_TABLE_ID),2, true)+"\" ");
        sbuff.append("COLUMN_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLUMN_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("TRANSFER_ACTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFER_ACTION_ID),2, true)+"\" ");
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
    sbuff.append("</Meta_fk_table_joints>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_fk_table_joints ( ");
	    		sbuff.append("VERSION_NUMBER");
		    		sbuff.append(", CONNECTION_ID");
	    		sbuff.append(", TABLE_ID");
	    		sbuff.append(", COLUMN_ID_FK_COLUMN");
	    		sbuff.append(", TARGET_TABLE_ID");
	    		sbuff.append(", COLUMN_ID");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", TRANSFER_ACTION_ID");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_ID_FK_COLUMN,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET_TABLE_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLUMN_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TRANSFER_ACTION_ID,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public Long getConnection_id() { 
    return this.CONNECTION_ID;
  }
   public Long getTable_id() { 
    return this.TABLE_ID;
  }
   public Long getColumn_id_fk_column() { 
    return this.COLUMN_ID_FK_COLUMN;
  }
   public Long getTarget_table_id() { 
    return this.TARGET_TABLE_ID;
  }
   public Long getColumn_id() { 
    return this.COLUMN_ID;
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
     if (CONNECTION_ID == null) {
      CONNECTION_ID = new Long (0);
    }
     if (TABLE_ID == null) {
      TABLE_ID = new Long (0);
    }
     if (COLUMN_ID_FK_COLUMN == null) {
      COLUMN_ID_FK_COLUMN = new Long (0);
    }
     if (TARGET_TABLE_ID == null) {
      TARGET_TABLE_ID = new Long (0);
    }
     if (COLUMN_ID == null) {
      COLUMN_ID = new Long (0);
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
   }

   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setConnection_id(final Long CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_ID,"CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("CONNECTION_ID");
    this.CONNECTION_ID = CONNECTION_ID;
  }
   public void setTable_id(final Long TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TABLE_ID,"TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TABLE_ID");
    this.TABLE_ID = TABLE_ID;
  }
   public void setColumn_id_fk_column(final Long COLUMN_ID_FK_COLUMN) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ID_FK_COLUMN,"COLUMN_ID_FK_COLUMN",2,38,0);
    }
    modifiedColumns.add("COLUMN_ID_FK_COLUMN");
    this.COLUMN_ID_FK_COLUMN = COLUMN_ID_FK_COLUMN;
  }
   public void setTarget_table_id(final Long TARGET_TABLE_ID) {
    if (validateData){
      DataValidator.validateData((Object)TARGET_TABLE_ID,"TARGET_TABLE_ID",2,38,0);
    }
    modifiedColumns.add("TARGET_TABLE_ID");
    this.TARGET_TABLE_ID = TARGET_TABLE_ID;
  }
   public void setColumn_id(final Long COLUMN_ID) {
    if (validateData){
      DataValidator.validateData((Object)COLUMN_ID,"COLUMN_ID",2,38,0);
    }
    modifiedColumns.add("COLUMN_ID");
    this.COLUMN_ID = COLUMN_ID;
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
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_fk_table_joints o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
            || (((this.COLUMN_ID_FK_COLUMN == null) || (o.COLUMN_ID_FK_COLUMN == null)) && (this.COLUMN_ID_FK_COLUMN != o.COLUMN_ID_FK_COLUMN))
            || (((this.TARGET_TABLE_ID == null) || (o.TARGET_TABLE_ID == null)) && (this.TARGET_TABLE_ID != o.TARGET_TABLE_ID))
            || (((this.COLUMN_ID == null) || (o.COLUMN_ID == null)) && (this.COLUMN_ID != o.COLUMN_ID))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
            || (((this.COLUMN_ID_FK_COLUMN != null) && (o.COLUMN_ID_FK_COLUMN != null)) && (this.COLUMN_ID_FK_COLUMN.equals(o.COLUMN_ID_FK_COLUMN) == false))
            || (((this.TARGET_TABLE_ID != null) && (o.TARGET_TABLE_ID != null)) && (this.TARGET_TABLE_ID.equals(o.TARGET_TABLE_ID) == false))
            || (((this.COLUMN_ID != null) && (o.COLUMN_ID != null)) && (this.COLUMN_ID.equals(o.COLUMN_ID) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
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
  public boolean equals(final Meta_fk_table_joints o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.TABLE_ID == null) || (o.TABLE_ID == null)) && (this.TABLE_ID != o.TABLE_ID))
            || (((this.COLUMN_ID_FK_COLUMN == null) || (o.COLUMN_ID_FK_COLUMN == null)) && (this.COLUMN_ID_FK_COLUMN != o.COLUMN_ID_FK_COLUMN))
            || (((this.TARGET_TABLE_ID == null) || (o.TARGET_TABLE_ID == null)) && (this.TARGET_TABLE_ID != o.TARGET_TABLE_ID))
            || (((this.COLUMN_ID == null) || (o.COLUMN_ID == null)) && (this.COLUMN_ID != o.COLUMN_ID))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.TRANSFER_ACTION_ID == null) || (o.TRANSFER_ACTION_ID == null)) && (this.TRANSFER_ACTION_ID != o.TRANSFER_ACTION_ID))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.TABLE_ID != null) && (o.TABLE_ID != null)) && (this.TABLE_ID.equals(o.TABLE_ID) == false))
            || (((this.COLUMN_ID_FK_COLUMN != null) && (o.COLUMN_ID_FK_COLUMN != null)) && (this.COLUMN_ID_FK_COLUMN.equals(o.COLUMN_ID_FK_COLUMN) == false))
            || (((this.TARGET_TABLE_ID != null) && (o.TARGET_TABLE_ID != null)) && (this.TARGET_TABLE_ID.equals(o.TARGET_TABLE_ID) == false))
            || (((this.COLUMN_ID != null) && (o.COLUMN_ID != null)) && (this.COLUMN_ID.equals(o.COLUMN_ID) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.TRANSFER_ACTION_ID != null) && (o.TRANSFER_ACTION_ID != null)) && (this.TRANSFER_ACTION_ID.equals(o.TRANSFER_ACTION_ID) == false))
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
  * return 38
  */
  public static int getTable_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTable_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getTable_idSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 38
  */
  public static int getColumn_id_fk_columnColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_id_fk_columnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_id_fk_columnSQLType() {
    
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
  public static int getColumn_idColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getColumn_idDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getColumn_idSQLType() {
    
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

  public Meta_fk_table_joints getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_fk_table_joints original) {
    this.original = (Meta_fk_table_joints) original.clone();
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
