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
public class Infomessages implements Cloneable,RockDBObject  {

    private Integer MSGID;
    private String TITLE;
    private Timestamp MSGDATE;
    private String NAME;
    private String EMAIL;
    private String STATUS;
    private String MSG;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "MSGID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Infomessages original; 

  public Infomessages(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Infomessages(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.MSGID = null;
         this.TITLE = null;
         this.MSGDATE = null;
         this.NAME = null;
         this.EMAIL = null;
         this.STATUS = null;
         this.MSG = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Infomessages(final RockFactory rockFact   ,final Integer MSGID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.MSGID = MSGID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Infomessages> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Infomessages o = it.next();

              this.MSGID = o.getMsgid();
              this.TITLE = o.getTitle();
              this.MSGDATE = o.getMsgdate();
              this.NAME = o.getName();
              this.EMAIL = o.getEmail();
              this.STATUS = o.getStatus();
              this.MSG = o.getMsg();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Infomessages");
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
  public Infomessages(final RockFactory rockFact, final Infomessages whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Infomessages> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Infomessages o = it.next();
                this.MSGID = o.getMsgid();
                this.TITLE = o.getTitle();
                this.MSGDATE = o.getMsgdate();
                this.NAME = o.getName();
                this.EMAIL = o.getEmail();
                this.STATUS = o.getStatus();
                this.MSG = o.getMsg();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Infomessages");
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
    return "Infomessages";
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
  public int updateDB(final boolean useTimestamp, final Infomessages whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Infomessages whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Infomessages.saveDB(), no primary key defined");
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
    sbuff.append("<Infomessages ");
        sbuff.append("MSGID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSGID),4, true)+"\" ");
        sbuff.append("TITLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TITLE),12, true)+"\" ");
        sbuff.append("MSGDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSGDATE),93, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("EMAIL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EMAIL),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("MSG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSG),12, true)+"\" ");
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
    sbuff.append("<Infomessages ");
        sbuff.append("MSGID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSGID),4, true)+"\" ");
        sbuff.append("TITLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TITLE),12, true)+"\" ");
        sbuff.append("MSGDATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSGDATE),93, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("EMAIL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EMAIL),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("MSG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MSG),12, true)+"\" ");
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
    sbuff.append("</Infomessages>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Infomessages ( ");
	    		sbuff.append("MSGID");
		    		sbuff.append(", TITLE");
	    		sbuff.append(", MSGDATE");
	    		sbuff.append(", NAME");
	    		sbuff.append(", EMAIL");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", MSG");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.MSGID,4)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TITLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MSGDATE,93)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EMAIL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MSG,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Integer getMsgid() { 
    return this.MSGID;
  }
   public String getTitle() { 
    return this.TITLE;
  }
   public Timestamp getMsgdate() { 
    return this.MSGDATE;
  }
   public String getName() { 
    return this.NAME;
  }
   public String getEmail() { 
    return this.EMAIL;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public String getMsg() { 
    return this.MSG;
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
     if (MSGID == null) {
      MSGID = new Integer (0);
    }
     if (TITLE == null) {
      TITLE = "";
    }
     if (MSGDATE == null) {
      MSGDATE = new Timestamp (0);
    }
     if (NAME == null) {
      NAME = "";
    }
     if (EMAIL == null) {
      EMAIL = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (MSG == null) {
      MSG = "";
    }
   }

   public void setMsgid(final Integer MSGID) {
    if (validateData){
      DataValidator.validateData((Object)MSGID,"MSGID",4,10,0);
    }
    modifiedColumns.add("MSGID");
    this.MSGID = MSGID;
  }
   public void setTitle(final String TITLE) {
    if (validateData){
      DataValidator.validateData((Object)TITLE,"TITLE",12,50,0);
    }
    modifiedColumns.add("TITLE");
    this.TITLE = TITLE;
  }
   public void setMsgdate(final Timestamp MSGDATE) {
    if (validateData){
      DataValidator.validateData((Object)MSGDATE,"MSGDATE",93,23,0);
    }
    modifiedColumns.add("MSGDATE");
    this.MSGDATE = MSGDATE;
  }
   public void setName(final String NAME) {
    if (validateData){
      DataValidator.validateData((Object)NAME,"NAME",12,50,0);
    }
    modifiedColumns.add("NAME");
    this.NAME = NAME;
  }
   public void setEmail(final String EMAIL) {
    if (validateData){
      DataValidator.validateData((Object)EMAIL,"EMAIL",12,50,0);
    }
    modifiedColumns.add("EMAIL");
    this.EMAIL = EMAIL;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,20,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setMsg(final String MSG) {
    if (validateData){
      DataValidator.validateData((Object)MSG,"MSG",12,500,0);
    }
    modifiedColumns.add("MSG");
    this.MSG = MSG;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Infomessages o) {

         if ((((this.MSGID == null) || (o.MSGID == null)) && (this.MSGID != o.MSGID))
          ){
    return false;
    } else
         if ((((this.MSGID != null) && (o.MSGID != null)) && (this.MSGID.equals(o.MSGID) == false))
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
  public boolean equals(final Infomessages o) {

         if ((((this.MSGID == null) || (o.MSGID == null)) && (this.MSGID != o.MSGID))
            || (((this.TITLE == null) || (o.TITLE == null)) && (this.TITLE != o.TITLE))
            || (((this.MSGDATE == null) || (o.MSGDATE == null)) && (this.MSGDATE != o.MSGDATE))
            || (((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
            || (((this.EMAIL == null) || (o.EMAIL == null)) && (this.EMAIL != o.EMAIL))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.MSG == null) || (o.MSG == null)) && (this.MSG != o.MSG))
          ){
    return false;
    } else
         if ((((this.MSGID != null) && (o.MSGID != null)) && (this.MSGID.equals(o.MSGID) == false))
            || (((this.TITLE != null) && (o.TITLE != null)) && (this.TITLE.equals(o.TITLE) == false))
            || (((this.MSGDATE != null) && (o.MSGDATE != null)) && (this.MSGDATE.equals(o.MSGDATE) == false))
            || (((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
            || (((this.EMAIL != null) && (o.EMAIL != null)) && (this.EMAIL.equals(o.EMAIL) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.MSG != null) && (o.MSG != null)) && (this.MSG.equals(o.MSG) == false))
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
  * return 10
  */
  public static int getMsgidColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMsgidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getMsgidSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTitleColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTitleDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTitleSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 23
  */
  public static int getMsgdateColumnSize() {
    
     return 23;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMsgdateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 93
  */
  public static int getMsgdateSQLType() {
    
    return 93;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getNameColumnSize() {
    
     return 50;   
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
  * return 50
  */
  public static int getEmailColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEmailDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getEmailSQLType() {
    
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
  * return 500
  */
  public static int getMsgColumnSize() {
    
     return 500;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMsgDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMsgSQLType() {
    
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

  public Infomessages getOriginal() {
    return original;
  }
   
  public void setOriginal(final Infomessages original) {
    this.original = (Infomessages) original.clone();
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
