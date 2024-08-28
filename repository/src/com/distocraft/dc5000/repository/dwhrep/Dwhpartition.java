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
public class Dwhpartition implements Cloneable,RockDBObject  {

    private String STORAGEID;
    private String TABLENAME;
    private Timestamp STARTTIME;
    private Timestamp ENDTIME;
    private String STATUS;
    private Integer LOADORDER;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TABLENAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Dwhpartition original; 

  public Dwhpartition(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Dwhpartition(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.STORAGEID = null;
         this.TABLENAME = null;
         this.STARTTIME = null;
         this.ENDTIME = null;
         this.STATUS = null;
         this.LOADORDER = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Dwhpartition(final RockFactory rockFact   ,final String TABLENAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TABLENAME = TABLENAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Dwhpartition> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Dwhpartition o = it.next();

              this.STORAGEID = o.getStorageid();
              this.TABLENAME = o.getTablename();
              this.STARTTIME = o.getStarttime();
              this.ENDTIME = o.getEndtime();
              this.STATUS = o.getStatus();
              this.LOADORDER = o.getLoadorder();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhpartition");
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
  public Dwhpartition(final RockFactory rockFact, final Dwhpartition whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Dwhpartition> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Dwhpartition o = it.next();
                this.STORAGEID = o.getStorageid();
                this.TABLENAME = o.getTablename();
                this.STARTTIME = o.getStarttime();
                this.ENDTIME = o.getEndtime();
                this.STATUS = o.getStatus();
                this.LOADORDER = o.getLoadorder();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Dwhpartition");
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
    return "Dwhpartition";
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
  public int updateDB(final boolean useTimestamp, final Dwhpartition whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Dwhpartition whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Dwhpartition.saveDB(), no primary key defined");
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
    sbuff.append("<Dwhpartition ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("TABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLENAME),12, true)+"\" ");
        sbuff.append("STARTTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STARTTIME),93, true)+"\" ");
        sbuff.append("ENDTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENDTIME),93, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("LOADORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOADORDER),4, true)+"\" ");
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
    sbuff.append("<Dwhpartition ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("TABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLENAME),12, true)+"\" ");
        sbuff.append("STARTTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STARTTIME),93, true)+"\" ");
        sbuff.append("ENDTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENDTIME),93, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("LOADORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOADORDER),4, true)+"\" ");
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
    sbuff.append("</Dwhpartition>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Dwhpartition ( ");
	    		sbuff.append("STORAGEID");
		    		sbuff.append(", TABLENAME");
	    		sbuff.append(", STARTTIME");
	    		sbuff.append(", ENDTIME");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", LOADORDER");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.STORAGEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TABLENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STARTTIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENDTIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOADORDER,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getStorageid() { 
    return this.STORAGEID;
  }
   public String getTablename() { 
    return this.TABLENAME;
  }
   public Timestamp getStarttime() { 
    return this.STARTTIME;
  }
   public Timestamp getEndtime() { 
    return this.ENDTIME;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public Integer getLoadorder() { 
    return this.LOADORDER;
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
     if (STORAGEID == null) {
      STORAGEID = "";
    }
     if (TABLENAME == null) {
      TABLENAME = "";
    }
     if (STARTTIME == null) {
      STARTTIME = new Timestamp (0);
    }
     if (ENDTIME == null) {
      ENDTIME = new Timestamp (0);
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (LOADORDER == null) {
      LOADORDER = new Integer (0);
    }
   }

   public void setStorageid(final String STORAGEID) {
    if (validateData){
      DataValidator.validateData((Object)STORAGEID,"STORAGEID",12,255,0);
    }
    modifiedColumns.add("STORAGEID");
    this.STORAGEID = STORAGEID;
  }
   public void setTablename(final String TABLENAME) {
    if (validateData){
      DataValidator.validateData((Object)TABLENAME,"TABLENAME",12,255,0);
    }
    modifiedColumns.add("TABLENAME");
    this.TABLENAME = TABLENAME;
  }
   public void setStarttime(final Timestamp STARTTIME) {
    if (validateData){
      DataValidator.validateData((Object)STARTTIME,"STARTTIME",93,23,0);
    }
    modifiedColumns.add("STARTTIME");
    this.STARTTIME = STARTTIME;
  }
   public void setEndtime(final Timestamp ENDTIME) {
    if (validateData){
      DataValidator.validateData((Object)ENDTIME,"ENDTIME",93,23,0);
    }
    modifiedColumns.add("ENDTIME");
    this.ENDTIME = ENDTIME;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setLoadorder(final Integer LOADORDER) {
    if (validateData){
      DataValidator.validateData((Object)LOADORDER,"LOADORDER",4,10,0);
    }
    modifiedColumns.add("LOADORDER");
    this.LOADORDER = LOADORDER;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Dwhpartition o) {

         if ((((this.TABLENAME == null) || (o.TABLENAME == null)) && (this.TABLENAME != o.TABLENAME))
          ){
    return false;
    } else
         if ((((this.TABLENAME != null) && (o.TABLENAME != null)) && (this.TABLENAME.equals(o.TABLENAME) == false))
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
  public boolean equals(final Dwhpartition o) {

         if ((((this.STORAGEID == null) || (o.STORAGEID == null)) && (this.STORAGEID != o.STORAGEID))
            || (((this.TABLENAME == null) || (o.TABLENAME == null)) && (this.TABLENAME != o.TABLENAME))
            || (((this.STARTTIME == null) || (o.STARTTIME == null)) && (this.STARTTIME != o.STARTTIME))
            || (((this.ENDTIME == null) || (o.ENDTIME == null)) && (this.ENDTIME != o.ENDTIME))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.LOADORDER == null) || (o.LOADORDER == null)) && (this.LOADORDER != o.LOADORDER))
          ){
    return false;
    } else
         if ((((this.STORAGEID != null) && (o.STORAGEID != null)) && (this.STORAGEID.equals(o.STORAGEID) == false))
            || (((this.TABLENAME != null) && (o.TABLENAME != null)) && (this.TABLENAME.equals(o.TABLENAME) == false))
            || (((this.STARTTIME != null) && (o.STARTTIME != null)) && (this.STARTTIME.equals(o.STARTTIME) == false))
            || (((this.ENDTIME != null) && (o.ENDTIME != null)) && (this.ENDTIME.equals(o.ENDTIME) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.LOADORDER != null) && (o.LOADORDER != null)) && (this.LOADORDER.equals(o.LOADORDER) == false))
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
  * return 255
  */
  public static int getTablenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTablenameDecimalDigits() {
    
     return 0;   
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
  * return 23
  */
  public static int getStarttimeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStarttimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getStarttimeSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getEndtimeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEndtimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getEndtimeSQLType() {
    
    return 93;   
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
  * return 10
  */
  public static int getLoadorderColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLoadorderDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getLoadorderSQLType() {
    
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

  public Dwhpartition getOriginal() {
    return original;
  }
   
  public void setOriginal(final Dwhpartition original) {
    this.original = (Dwhpartition) original.clone();
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
