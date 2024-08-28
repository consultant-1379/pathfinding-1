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
public class Typeactivation implements Cloneable,RockDBObject  {

    private String TECHPACK_NAME;
    private String STATUS;
    private String TYPENAME;
    private String TABLELEVEL;
    private Long STORAGETIME;
    private String TYPE;
    private String PARTITIONPLAN;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TECHPACK_NAME"    ,"TYPENAME"    ,"TABLELEVEL"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Typeactivation original; 

  public Typeactivation(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Typeactivation(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TECHPACK_NAME = null;
         this.STATUS = null;
         this.TYPENAME = null;
         this.TABLELEVEL = null;
         this.STORAGETIME = null;
         this.TYPE = null;
         this.PARTITIONPLAN = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Typeactivation(final RockFactory rockFact   ,final String TECHPACK_NAME ,final String TYPENAME ,final String TABLELEVEL ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TECHPACK_NAME = TECHPACK_NAME;
            this.TYPENAME = TYPENAME;
            this.TABLELEVEL = TABLELEVEL;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Typeactivation> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Typeactivation o = it.next();

              this.TECHPACK_NAME = o.getTechpack_name();
              this.STATUS = o.getStatus();
              this.TYPENAME = o.getTypename();
              this.TABLELEVEL = o.getTablelevel();
              this.STORAGETIME = o.getStoragetime();
              this.TYPE = o.getType();
              this.PARTITIONPLAN = o.getPartitionplan();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Typeactivation");
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
  public Typeactivation(final RockFactory rockFact, final Typeactivation whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Typeactivation> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Typeactivation o = it.next();
                this.TECHPACK_NAME = o.getTechpack_name();
                this.STATUS = o.getStatus();
                this.TYPENAME = o.getTypename();
                this.TABLELEVEL = o.getTablelevel();
                this.STORAGETIME = o.getStoragetime();
                this.TYPE = o.getType();
                this.PARTITIONPLAN = o.getPartitionplan();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Typeactivation");
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
    return "Typeactivation";
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
  public int updateDB(final boolean useTimestamp, final Typeactivation whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Typeactivation whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Typeactivation.saveDB(), no primary key defined");
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
    sbuff.append("<Typeactivation ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("STORAGETIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGETIME),2, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
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
    sbuff.append("<Typeactivation ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("STORAGETIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGETIME),2, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
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
    sbuff.append("</Typeactivation>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Typeactivation ( ");
	    		sbuff.append("TECHPACK_NAME");
		    		sbuff.append(", STATUS");
	    		sbuff.append(", TYPENAME");
	    		sbuff.append(", TABLELEVEL");
	    		sbuff.append(", STORAGETIME");
	    		sbuff.append(", TYPE");
	    		sbuff.append(", PARTITIONPLAN");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TECHPACK_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLELEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STORAGETIME,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARTITIONPLAN,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTechpack_name() { 
    return this.TECHPACK_NAME;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public String getTypename() { 
    return this.TYPENAME;
  }
   public String getTablelevel() { 
    return this.TABLELEVEL;
  }
   public Long getStoragetime() { 
    return this.STORAGETIME;
  }
   public String getType() { 
    return this.TYPE;
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
     if (STATUS == null) {
      STATUS = "";
    }
     if (TYPENAME == null) {
      TYPENAME = "";
    }
     if (TABLELEVEL == null) {
      TABLELEVEL = "";
    }
     if (STORAGETIME == null) {
      STORAGETIME = new Long (0);
    }
     if (TYPE == null) {
      TYPE = "";
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
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
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
   public void setStoragetime(final Long STORAGETIME) {
    if (validateData){
      DataValidator.validateData((Object)STORAGETIME,"STORAGETIME",2,15,0);
    }
    modifiedColumns.add("STORAGETIME");
    this.STORAGETIME = STORAGETIME;
  }
   public void setType(final String TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TYPE,"TYPE",12,12,0);
    }
    modifiedColumns.add("TYPE");
    this.TYPE = TYPE;
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
  public boolean dbEquals(final Typeactivation o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.TYPENAME == null) || (o.TYPENAME == null)) && (this.TYPENAME != o.TYPENAME))
            || (((this.TABLELEVEL == null) || (o.TABLELEVEL == null)) && (this.TABLELEVEL != o.TABLELEVEL))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.TYPENAME != null) && (o.TYPENAME != null)) && (this.TYPENAME.equals(o.TYPENAME) == false))
            || (((this.TABLELEVEL != null) && (o.TABLELEVEL != null)) && (this.TABLELEVEL.equals(o.TABLELEVEL) == false))
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
  public boolean equals(final Typeactivation o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.TYPENAME == null) || (o.TYPENAME == null)) && (this.TYPENAME != o.TYPENAME))
            || (((this.TABLELEVEL == null) || (o.TABLELEVEL == null)) && (this.TABLELEVEL != o.TABLELEVEL))
            || (((this.STORAGETIME == null) || (o.STORAGETIME == null)) && (this.STORAGETIME != o.STORAGETIME))
            || (((this.TYPE == null) || (o.TYPE == null)) && (this.TYPE != o.TYPE))
            || (((this.PARTITIONPLAN == null) || (o.PARTITIONPLAN == null)) && (this.PARTITIONPLAN != o.PARTITIONPLAN))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.TYPENAME != null) && (o.TYPENAME != null)) && (this.TYPENAME.equals(o.TYPENAME) == false))
            || (((this.TABLELEVEL != null) && (o.TABLELEVEL != null)) && (this.TABLELEVEL.equals(o.TABLELEVEL) == false))
            || (((this.STORAGETIME != null) && (o.STORAGETIME != null)) && (this.STORAGETIME.equals(o.STORAGETIME) == false))
            || (((this.TYPE != null) && (o.TYPE != null)) && (this.TYPE.equals(o.TYPE) == false))
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
  * return 15
  */
  public static int getStoragetimeColumnSize() {
    
     return 15;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStoragetimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getStoragetimeSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 12
  */
  public static int getTypeColumnSize() {
    
     return 12;   
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

  public Typeactivation getOriginal() {
    return original;
  }
   
  public void setOriginal(final Typeactivation original) {
    this.original = (Typeactivation) original.clone();
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
