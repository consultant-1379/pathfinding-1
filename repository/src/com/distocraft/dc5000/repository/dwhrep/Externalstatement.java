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
public class Externalstatement implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String STATEMENTNAME;
    private Long EXECUTIONORDER;
    private String DBCONNECTION;
    private String STATEMENT;
    private Integer BASEDEF;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"STATEMENTNAME"    ,"BASEDEF"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Externalstatement original; 

  public Externalstatement(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Externalstatement(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.STATEMENTNAME = null;
         this.EXECUTIONORDER = null;
         this.DBCONNECTION = null;
         this.STATEMENT = null;
         this.BASEDEF = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Externalstatement(final RockFactory rockFact   ,final String VERSIONID ,final String STATEMENTNAME ,final Integer BASEDEF ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.STATEMENTNAME = STATEMENTNAME;
            this.BASEDEF = BASEDEF;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Externalstatement> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Externalstatement o = it.next();

              this.VERSIONID = o.getVersionid();
              this.STATEMENTNAME = o.getStatementname();
              this.EXECUTIONORDER = o.getExecutionorder();
              this.DBCONNECTION = o.getDbconnection();
              this.STATEMENT = o.getStatement();
              this.BASEDEF = o.getBasedef();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Externalstatement");
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
  public Externalstatement(final RockFactory rockFact, final Externalstatement whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Externalstatement> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Externalstatement o = it.next();
                this.VERSIONID = o.getVersionid();
                this.STATEMENTNAME = o.getStatementname();
                this.EXECUTIONORDER = o.getExecutionorder();
                this.DBCONNECTION = o.getDbconnection();
                this.STATEMENT = o.getStatement();
                this.BASEDEF = o.getBasedef();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Externalstatement");
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
    return "Externalstatement";
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
  public int updateDB(final boolean useTimestamp, final Externalstatement whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Externalstatement whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Externalstatement.saveDB(), no primary key defined");
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
    sbuff.append("<Externalstatement ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("STATEMENTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENTNAME),12, true)+"\" ");
        sbuff.append("EXECUTIONORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTIONORDER),2, true)+"\" ");
        sbuff.append("DBCONNECTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DBCONNECTION),12, true)+"\" ");
        sbuff.append("STATEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENT),-1, true)+"\" ");
        sbuff.append("BASEDEF=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEF),4, true)+"\" ");
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
    sbuff.append("<Externalstatement ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("STATEMENTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENTNAME),12, true)+"\" ");
        sbuff.append("EXECUTIONORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECUTIONORDER),2, true)+"\" ");
        sbuff.append("DBCONNECTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DBCONNECTION),12, true)+"\" ");
        sbuff.append("STATEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENT),-1, true)+"\" ");
        sbuff.append("BASEDEF=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEF),4, true)+"\" ");
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
    sbuff.append("</Externalstatement>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Externalstatement ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", STATEMENTNAME");
	    		sbuff.append(", EXECUTIONORDER");
	    		sbuff.append(", DBCONNECTION");
	    		sbuff.append(", STATEMENT");
	    		sbuff.append(", BASEDEF");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.STATEMENTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXECUTIONORDER,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DBCONNECTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATEMENT,-1)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASEDEF,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getStatementname() { 
    return this.STATEMENTNAME;
  }
   public Long getExecutionorder() { 
    return this.EXECUTIONORDER;
  }
   public String getDbconnection() { 
    return this.DBCONNECTION;
  }
   public String getStatement() { 
    return this.STATEMENT;
  }
   public Integer getBasedef() { 
    return this.BASEDEF;
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
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (STATEMENTNAME == null) {
      STATEMENTNAME = "";
    }
     if (EXECUTIONORDER == null) {
      EXECUTIONORDER = new Long (0);
    }
     if (DBCONNECTION == null) {
      DBCONNECTION = "";
    }
     if (STATEMENT == null) {
      STATEMENT = "";
    }
     if (BASEDEF == null) {
      BASEDEF = new Integer (0);
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setStatementname(final String STATEMENTNAME) {
    if (validateData){
      DataValidator.validateData((Object)STATEMENTNAME,"STATEMENTNAME",12,255,0);
    }
    modifiedColumns.add("STATEMENTNAME");
    this.STATEMENTNAME = STATEMENTNAME;
  }
   public void setExecutionorder(final Long EXECUTIONORDER) {
    if (validateData){
      DataValidator.validateData((Object)EXECUTIONORDER,"EXECUTIONORDER",2,9,0);
    }
    modifiedColumns.add("EXECUTIONORDER");
    this.EXECUTIONORDER = EXECUTIONORDER;
  }
   public void setDbconnection(final String DBCONNECTION) {
    if (validateData){
      DataValidator.validateData((Object)DBCONNECTION,"DBCONNECTION",12,20,0);
    }
    modifiedColumns.add("DBCONNECTION");
    this.DBCONNECTION = DBCONNECTION;
  }
   public void setStatement(final String STATEMENT) {
    if (validateData){
      DataValidator.validateData((Object)STATEMENT,"STATEMENT",-1,2147483647,0);
    }
    modifiedColumns.add("STATEMENT");
    this.STATEMENT = STATEMENT;
  }
   public void setBasedef(final Integer BASEDEF) {
    if (validateData){
      DataValidator.validateData((Object)BASEDEF,"BASEDEF",4,10,0);
    }
    modifiedColumns.add("BASEDEF");
    this.BASEDEF = BASEDEF;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Externalstatement o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.STATEMENTNAME == null) || (o.STATEMENTNAME == null)) && (this.STATEMENTNAME != o.STATEMENTNAME))
            || (((this.BASEDEF == null) || (o.BASEDEF == null)) && (this.BASEDEF != o.BASEDEF))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.STATEMENTNAME != null) && (o.STATEMENTNAME != null)) && (this.STATEMENTNAME.equals(o.STATEMENTNAME) == false))
            || (((this.BASEDEF != null) && (o.BASEDEF != null)) && (this.BASEDEF.equals(o.BASEDEF) == false))
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
  public boolean equals(final Externalstatement o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.STATEMENTNAME == null) || (o.STATEMENTNAME == null)) && (this.STATEMENTNAME != o.STATEMENTNAME))
            || (((this.EXECUTIONORDER == null) || (o.EXECUTIONORDER == null)) && (this.EXECUTIONORDER != o.EXECUTIONORDER))
            || (((this.DBCONNECTION == null) || (o.DBCONNECTION == null)) && (this.DBCONNECTION != o.DBCONNECTION))
            || (((this.STATEMENT == null) || (o.STATEMENT == null)) && (this.STATEMENT != o.STATEMENT))
            || (((this.BASEDEF == null) || (o.BASEDEF == null)) && (this.BASEDEF != o.BASEDEF))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.STATEMENTNAME != null) && (o.STATEMENTNAME != null)) && (this.STATEMENTNAME.equals(o.STATEMENTNAME) == false))
            || (((this.EXECUTIONORDER != null) && (o.EXECUTIONORDER != null)) && (this.EXECUTIONORDER.equals(o.EXECUTIONORDER) == false))
            || (((this.DBCONNECTION != null) && (o.DBCONNECTION != null)) && (this.DBCONNECTION.equals(o.DBCONNECTION) == false))
            || (((this.STATEMENT != null) && (o.STATEMENT != null)) && (this.STATEMENT.equals(o.STATEMENT) == false))
            || (((this.BASEDEF != null) && (o.BASEDEF != null)) && (this.BASEDEF.equals(o.BASEDEF) == false))
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
  * return 128
  */
  public static int getVersionidColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVersionidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVersionidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getStatementnameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStatementnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getStatementnameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getExecutionorderColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExecutionorderDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getExecutionorderSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 20
  */
  public static int getDbconnectionColumnSize() {
    
     return 20;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDbconnectionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDbconnectionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 2147483647
  */
  public static int getStatementColumnSize() {
    
     return 2147483647;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStatementDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return -1
  */
  public static int getStatementSQLType() {
    
    return -1;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getBasedefColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBasedefDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getBasedefSQLType() {
    
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

  public Externalstatement getOriginal() {
    return original;
  }
   
  public void setOriginal(final Externalstatement original) {
    this.original = (Externalstatement) original.clone();
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
