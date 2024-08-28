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
public class Alarminterface implements Cloneable,RockDBObject  {

    private String INTERFACEID;
    private String DESCRIPTION;
    private String STATUS;
    private Long COLLECTION_SET_ID;
    private Long COLLECTION_ID;
    private Long QUEUE_NUMBER;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "INTERFACEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Alarminterface original; 

  public Alarminterface(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Alarminterface(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.INTERFACEID = null;
         this.DESCRIPTION = null;
         this.STATUS = null;
         this.COLLECTION_SET_ID = null;
         this.COLLECTION_ID = null;
         this.QUEUE_NUMBER = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Alarminterface(final RockFactory rockFact   ,final String INTERFACEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.INTERFACEID = INTERFACEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Alarminterface> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Alarminterface o = it.next();

              this.INTERFACEID = o.getInterfaceid();
              this.DESCRIPTION = o.getDescription();
              this.STATUS = o.getStatus();
              this.COLLECTION_SET_ID = o.getCollection_set_id();
              this.COLLECTION_ID = o.getCollection_id();
              this.QUEUE_NUMBER = o.getQueue_number();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Alarminterface");
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
  public Alarminterface(final RockFactory rockFact, final Alarminterface whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Alarminterface> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Alarminterface o = it.next();
                this.INTERFACEID = o.getInterfaceid();
                this.DESCRIPTION = o.getDescription();
                this.STATUS = o.getStatus();
                this.COLLECTION_SET_ID = o.getCollection_set_id();
                this.COLLECTION_ID = o.getCollection_id();
                this.QUEUE_NUMBER = o.getQueue_number();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Alarminterface");
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
    return "Alarminterface";
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
  public int updateDB(final boolean useTimestamp, final Alarminterface whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Alarminterface whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Alarminterface.saveDB(), no primary key defined");
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
    sbuff.append("<Alarminterface ");
        sbuff.append("INTERFACEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEID),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("QUEUE_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUEUE_NUMBER),2, true)+"\" ");
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
    sbuff.append("<Alarminterface ");
        sbuff.append("INTERFACEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEID),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("COLLECTION_SET_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_SET_ID),2, true)+"\" ");
        sbuff.append("COLLECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.COLLECTION_ID),2, true)+"\" ");
        sbuff.append("QUEUE_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUEUE_NUMBER),2, true)+"\" ");
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
    sbuff.append("</Alarminterface>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Alarminterface ( ");
	    		sbuff.append("INTERFACEID");
		    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", COLLECTION_SET_ID");
	    		sbuff.append(", COLLECTION_ID");
	    		sbuff.append(", QUEUE_NUMBER");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.INTERFACEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_SET_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.COLLECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.QUEUE_NUMBER,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getInterfaceid() { 
    return this.INTERFACEID;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public Long getCollection_set_id() { 
    return this.COLLECTION_SET_ID;
  }
   public Long getCollection_id() { 
    return this.COLLECTION_ID;
  }
   public Long getQueue_number() { 
    return this.QUEUE_NUMBER;
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
     if (INTERFACEID == null) {
      INTERFACEID = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (COLLECTION_SET_ID == null) {
      COLLECTION_SET_ID = new Long (0);
    }
     if (COLLECTION_ID == null) {
      COLLECTION_ID = new Long (0);
    }
     if (QUEUE_NUMBER == null) {
      QUEUE_NUMBER = new Long (0);
    }
   }

   public void setInterfaceid(final String INTERFACEID) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACEID,"INTERFACEID",12,50,0);
    }
    modifiedColumns.add("INTERFACEID");
    this.INTERFACEID = INTERFACEID;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,200,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,20,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
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
   public void setQueue_number(final Long QUEUE_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)QUEUE_NUMBER,"QUEUE_NUMBER",2,38,0);
    }
    modifiedColumns.add("QUEUE_NUMBER");
    this.QUEUE_NUMBER = QUEUE_NUMBER;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Alarminterface o) {

         if ((((this.INTERFACEID == null) || (o.INTERFACEID == null)) && (this.INTERFACEID != o.INTERFACEID))
          ){
    return false;
    } else
         if ((((this.INTERFACEID != null) && (o.INTERFACEID != null)) && (this.INTERFACEID.equals(o.INTERFACEID) == false))
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
  public boolean equals(final Alarminterface o) {

         if ((((this.INTERFACEID == null) || (o.INTERFACEID == null)) && (this.INTERFACEID != o.INTERFACEID))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.COLLECTION_SET_ID == null) || (o.COLLECTION_SET_ID == null)) && (this.COLLECTION_SET_ID != o.COLLECTION_SET_ID))
            || (((this.COLLECTION_ID == null) || (o.COLLECTION_ID == null)) && (this.COLLECTION_ID != o.COLLECTION_ID))
            || (((this.QUEUE_NUMBER == null) || (o.QUEUE_NUMBER == null)) && (this.QUEUE_NUMBER != o.QUEUE_NUMBER))
          ){
    return false;
    } else
         if ((((this.INTERFACEID != null) && (o.INTERFACEID != null)) && (this.INTERFACEID.equals(o.INTERFACEID) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.COLLECTION_SET_ID != null) && (o.COLLECTION_SET_ID != null)) && (this.COLLECTION_SET_ID.equals(o.COLLECTION_SET_ID) == false))
            || (((this.COLLECTION_ID != null) && (o.COLLECTION_ID != null)) && (this.COLLECTION_ID.equals(o.COLLECTION_ID) == false))
            || (((this.QUEUE_NUMBER != null) && (o.QUEUE_NUMBER != null)) && (this.QUEUE_NUMBER.equals(o.QUEUE_NUMBER) == false))
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
  * return 50
  */
  public static int getInterfaceidColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfaceidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfaceidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 200
  */
  public static int getDescriptionColumnSize() {
    
     return 200;   
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
  public static int getQueue_numberColumnSize() {
    
     return 38;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getQueue_numberDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getQueue_numberSQLType() {
    
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

  public Alarminterface getOriginal() {
    return original;
  }
   
  public void setOriginal(final Alarminterface original) {
    this.original = (Alarminterface) original.clone();
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
