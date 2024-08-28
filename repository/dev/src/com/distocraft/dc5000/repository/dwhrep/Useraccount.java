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
public class Useraccount implements Cloneable,RockDBObject  {

    private String NAME;
    private String PASSWORD;
    private String ROLE;
    private Timestamp LASTLOGIN;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Useraccount original; 

  public Useraccount(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Useraccount(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.NAME = null;
         this.PASSWORD = null;
         this.ROLE = null;
         this.LASTLOGIN = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Useraccount(final RockFactory rockFact   ,final String NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.NAME = NAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Useraccount> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Useraccount o = it.next();

              this.NAME = o.getName();
              this.PASSWORD = o.getPassword();
              this.ROLE = o.getRole();
              this.LASTLOGIN = o.getLastlogin();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Useraccount");
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
  public Useraccount(final RockFactory rockFact, final Useraccount whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Useraccount> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Useraccount o = it.next();
                this.NAME = o.getName();
                this.PASSWORD = o.getPassword();
                this.ROLE = o.getRole();
                this.LASTLOGIN = o.getLastlogin();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Useraccount");
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
    return "Useraccount";
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
  public int updateDB(final boolean useTimestamp, final Useraccount whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Useraccount whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Useraccount.saveDB(), no primary key defined");
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
    sbuff.append("<Useraccount ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("PASSWORD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PASSWORD),12, true)+"\" ");
        sbuff.append("ROLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ROLE),12, true)+"\" ");
        sbuff.append("LASTLOGIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LASTLOGIN),93, true)+"\" ");
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
    sbuff.append("<Useraccount ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("PASSWORD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PASSWORD),12, true)+"\" ");
        sbuff.append("ROLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ROLE),12, true)+"\" ");
        sbuff.append("LASTLOGIN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LASTLOGIN),93, true)+"\" ");
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
    sbuff.append("</Useraccount>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Useraccount ( ");
	    		sbuff.append("NAME");
		    		sbuff.append(", PASSWORD");
	    		sbuff.append(", ROLE");
	    		sbuff.append(", LASTLOGIN");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.NAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.PASSWORD,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ROLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LASTLOGIN,93)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getName() { 
    return this.NAME;
  }
   public String getPassword() { 
    return this.PASSWORD;
  }
   public String getRole() { 
    return this.ROLE;
  }
   public Timestamp getLastlogin() { 
    return this.LASTLOGIN;
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
     if (NAME == null) {
      NAME = "";
    }
     if (PASSWORD == null) {
      PASSWORD = "";
    }
     if (ROLE == null) {
      ROLE = "";
    }
     if (LASTLOGIN == null) {
      LASTLOGIN = new Timestamp (0);
    }
   }

   public void setName(final String NAME) {
    if (validateData){
      DataValidator.validateData((Object)NAME,"NAME",12,255,0);
    }
    modifiedColumns.add("NAME");
    this.NAME = NAME;
  }
   public void setPassword(final String PASSWORD) {
    if (validateData){
      DataValidator.validateData((Object)PASSWORD,"PASSWORD",12,16,0);
    }
    modifiedColumns.add("PASSWORD");
    this.PASSWORD = PASSWORD;
  }
   public void setRole(final String ROLE) {
    if (validateData){
      DataValidator.validateData((Object)ROLE,"ROLE",12,16,0);
    }
    modifiedColumns.add("ROLE");
    this.ROLE = ROLE;
  }
   public void setLastlogin(final Timestamp LASTLOGIN) {
    if (validateData){
      DataValidator.validateData((Object)LASTLOGIN,"LASTLOGIN",93,23,0);
    }
    modifiedColumns.add("LASTLOGIN");
    this.LASTLOGIN = LASTLOGIN;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Useraccount o) {

         if ((((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
          ){
    return false;
    } else
         if ((((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
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
  public boolean equals(final Useraccount o) {

         if ((((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
            || (((this.PASSWORD == null) || (o.PASSWORD == null)) && (this.PASSWORD != o.PASSWORD))
            || (((this.ROLE == null) || (o.ROLE == null)) && (this.ROLE != o.ROLE))
            || (((this.LASTLOGIN == null) || (o.LASTLOGIN == null)) && (this.LASTLOGIN != o.LASTLOGIN))
          ){
    return false;
    } else
         if ((((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
            || (((this.PASSWORD != null) && (o.PASSWORD != null)) && (this.PASSWORD.equals(o.PASSWORD) == false))
            || (((this.ROLE != null) && (o.ROLE != null)) && (this.ROLE.equals(o.ROLE) == false))
            || (((this.LASTLOGIN != null) && (o.LASTLOGIN != null)) && (this.LASTLOGIN.equals(o.LASTLOGIN) == false))
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
  public static int getNameColumnSize() {
    
     return 255;   
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
  * return 16
  */
  public static int getPasswordColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPasswordDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPasswordSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getRoleColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRoleDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getRoleSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getLastloginColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLastloginDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getLastloginSQLType() {
    
    return 93;   
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

  public Useraccount getOriginal() {
    return original;
  }
   
  public void setOriginal(final Useraccount original) {
    this.original = (Useraccount) original.clone();
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
