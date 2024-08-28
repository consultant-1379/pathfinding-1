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
public class Externalstatementstatus implements Cloneable,RockDBObject  {

    private String TECHPACK_NAME;
    private String STATEMENTNAME;
    private String VERSIONID;
    private String STATUS;
    private Timestamp EXECTIME;
    private String EXECSTATEMENT;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TECHPACK_NAME"    ,"STATEMENTNAME"    ,"VERSIONID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Externalstatementstatus original; 

  public Externalstatementstatus(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Externalstatementstatus(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TECHPACK_NAME = null;
         this.STATEMENTNAME = null;
         this.VERSIONID = null;
         this.STATUS = null;
         this.EXECTIME = null;
         this.EXECSTATEMENT = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Externalstatementstatus(final RockFactory rockFact   ,final String TECHPACK_NAME ,final String STATEMENTNAME ,final String VERSIONID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TECHPACK_NAME = TECHPACK_NAME;
            this.STATEMENTNAME = STATEMENTNAME;
            this.VERSIONID = VERSIONID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Externalstatementstatus> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Externalstatementstatus o = it.next();

              this.TECHPACK_NAME = o.getTechpack_name();
              this.STATEMENTNAME = o.getStatementname();
              this.VERSIONID = o.getVersionid();
              this.STATUS = o.getStatus();
              this.EXECTIME = o.getExectime();
              this.EXECSTATEMENT = o.getExecstatement();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Externalstatementstatus");
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
  public Externalstatementstatus(final RockFactory rockFact, final Externalstatementstatus whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Externalstatementstatus> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Externalstatementstatus o = it.next();
                this.TECHPACK_NAME = o.getTechpack_name();
                this.STATEMENTNAME = o.getStatementname();
                this.VERSIONID = o.getVersionid();
                this.STATUS = o.getStatus();
                this.EXECTIME = o.getExectime();
                this.EXECSTATEMENT = o.getExecstatement();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Externalstatementstatus");
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
    return "Externalstatementstatus";
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
  public int updateDB(final boolean useTimestamp, final Externalstatementstatus whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Externalstatementstatus whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Externalstatementstatus.saveDB(), no primary key defined");
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
    sbuff.append("<Externalstatementstatus ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("STATEMENTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENTNAME),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("EXECTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECTIME),93, true)+"\" ");
        sbuff.append("EXECSTATEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECSTATEMENT),-1, true)+"\" ");
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
    sbuff.append("<Externalstatementstatus ");
        sbuff.append("TECHPACK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_NAME),12, true)+"\" ");
        sbuff.append("STATEMENTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATEMENTNAME),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("EXECTIME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECTIME),93, true)+"\" ");
        sbuff.append("EXECSTATEMENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EXECSTATEMENT),-1, true)+"\" ");
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
    sbuff.append("</Externalstatementstatus>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Externalstatementstatus ( ");
	    		sbuff.append("TECHPACK_NAME");
		    		sbuff.append(", STATEMENTNAME");
	    		sbuff.append(", VERSIONID");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", EXECTIME");
	    		sbuff.append(", EXECSTATEMENT");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TECHPACK_NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.STATEMENTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXECTIME,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EXECSTATEMENT,-1)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTechpack_name() { 
    return this.TECHPACK_NAME;
  }
   public String getStatementname() { 
    return this.STATEMENTNAME;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public Timestamp getExectime() { 
    return this.EXECTIME;
  }
   public String getExecstatement() { 
    return this.EXECSTATEMENT;
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
     if (STATEMENTNAME == null) {
      STATEMENTNAME = "";
    }
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (EXECTIME == null) {
      EXECTIME = new Timestamp (0);
    }
     if (EXECSTATEMENT == null) {
      EXECSTATEMENT = "";
    }
   }

   public void setTechpack_name(final String TECHPACK_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_NAME,"TECHPACK_NAME",12,30,0);
    }
    modifiedColumns.add("TECHPACK_NAME");
    this.TECHPACK_NAME = TECHPACK_NAME;
  }
   public void setStatementname(final String STATEMENTNAME) {
    if (validateData){
      DataValidator.validateData((Object)STATEMENTNAME,"STATEMENTNAME",12,255,0);
    }
    modifiedColumns.add("STATEMENTNAME");
    this.STATEMENTNAME = STATEMENTNAME;
  }
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setExectime(final Timestamp EXECTIME) {
    if (validateData){
      DataValidator.validateData((Object)EXECTIME,"EXECTIME",93,23,0);
    }
    modifiedColumns.add("EXECTIME");
    this.EXECTIME = EXECTIME;
  }
   public void setExecstatement(final String EXECSTATEMENT) {
    if (validateData){
      DataValidator.validateData((Object)EXECSTATEMENT,"EXECSTATEMENT",-1,2147483647,0);
    }
    modifiedColumns.add("EXECSTATEMENT");
    this.EXECSTATEMENT = EXECSTATEMENT;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Externalstatementstatus o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.STATEMENTNAME == null) || (o.STATEMENTNAME == null)) && (this.STATEMENTNAME != o.STATEMENTNAME))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.STATEMENTNAME != null) && (o.STATEMENTNAME != null)) && (this.STATEMENTNAME.equals(o.STATEMENTNAME) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
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
  public boolean equals(final Externalstatementstatus o) {

         if ((((this.TECHPACK_NAME == null) || (o.TECHPACK_NAME == null)) && (this.TECHPACK_NAME != o.TECHPACK_NAME))
            || (((this.STATEMENTNAME == null) || (o.STATEMENTNAME == null)) && (this.STATEMENTNAME != o.STATEMENTNAME))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.EXECTIME == null) || (o.EXECTIME == null)) && (this.EXECTIME != o.EXECTIME))
            || (((this.EXECSTATEMENT == null) || (o.EXECSTATEMENT == null)) && (this.EXECSTATEMENT != o.EXECSTATEMENT))
          ){
    return false;
    } else
         if ((((this.TECHPACK_NAME != null) && (o.TECHPACK_NAME != null)) && (this.TECHPACK_NAME.equals(o.TECHPACK_NAME) == false))
            || (((this.STATEMENTNAME != null) && (o.STATEMENTNAME != null)) && (this.STATEMENTNAME.equals(o.STATEMENTNAME) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.EXECTIME != null) && (o.EXECTIME != null)) && (this.EXECTIME.equals(o.EXECTIME) == false))
            || (((this.EXECSTATEMENT != null) && (o.EXECSTATEMENT != null)) && (this.EXECSTATEMENT.equals(o.EXECSTATEMENT) == false))
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
  * return 23
  */
  public static int getExectimeColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExectimeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getExectimeSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 2147483647
  */
  public static int getExecstatementColumnSize() {
    
     return 2147483647;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getExecstatementDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return -1
  */
  public static int getExecstatementSQLType() {
    
    return -1;   
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

  public Externalstatementstatus getOriginal() {
    return original;
  }
   
  public void setOriginal(final Externalstatementstatus original) {
    this.original = (Externalstatementstatus) original.clone();
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
