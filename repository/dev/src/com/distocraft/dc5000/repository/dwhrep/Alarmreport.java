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
public class Alarmreport implements Cloneable,RockDBObject  {

    private String INTERFACEID;
    private String REPORTID;
    private String REPORTNAME;
    private String URL;
    private String STATUS;
    private Integer SIMULTANEOUS;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "REPORTID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Alarmreport original; 

  public Alarmreport(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Alarmreport(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.INTERFACEID = null;
         this.REPORTID = null;
         this.REPORTNAME = null;
         this.URL = null;
         this.STATUS = null;
         this.SIMULTANEOUS = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Alarmreport(final RockFactory rockFact   ,final String REPORTID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.REPORTID = REPORTID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Alarmreport> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Alarmreport o = it.next();

              this.INTERFACEID = o.getInterfaceid();
              this.REPORTID = o.getReportid();
              this.REPORTNAME = o.getReportname();
              this.URL = o.getUrl();
              this.STATUS = o.getStatus();
              this.SIMULTANEOUS = o.getSimultaneous();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Alarmreport");
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
  public Alarmreport(final RockFactory rockFact, final Alarmreport whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Alarmreport> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Alarmreport o = it.next();
                this.INTERFACEID = o.getInterfaceid();
                this.REPORTID = o.getReportid();
                this.REPORTNAME = o.getReportname();
                this.URL = o.getUrl();
                this.STATUS = o.getStatus();
                this.SIMULTANEOUS = o.getSimultaneous();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Alarmreport");
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
    return "Alarmreport";
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
  public int updateDB(final boolean useTimestamp, final Alarmreport whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Alarmreport whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Alarmreport.saveDB(), no primary key defined");
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
    sbuff.append("<Alarmreport ");
        sbuff.append("INTERFACEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEID),12, true)+"\" ");
        sbuff.append("REPORTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REPORTID),12, true)+"\" ");
        sbuff.append("REPORTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REPORTNAME),12, true)+"\" ");
        sbuff.append("URL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.URL),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("SIMULTANEOUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SIMULTANEOUS),4, true)+"\" ");
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
    sbuff.append("<Alarmreport ");
        sbuff.append("INTERFACEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEID),12, true)+"\" ");
        sbuff.append("REPORTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REPORTID),12, true)+"\" ");
        sbuff.append("REPORTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REPORTNAME),12, true)+"\" ");
        sbuff.append("URL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.URL),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),12, true)+"\" ");
        sbuff.append("SIMULTANEOUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SIMULTANEOUS),4, true)+"\" ");
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
    sbuff.append("</Alarmreport>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Alarmreport ( ");
	    		sbuff.append("INTERFACEID");
		    		sbuff.append(", REPORTID");
	    		sbuff.append(", REPORTNAME");
	    		sbuff.append(", URL");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", SIMULTANEOUS");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.INTERFACEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.REPORTID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.REPORTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.URL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SIMULTANEOUS,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getInterfaceid() { 
    return this.INTERFACEID;
  }
   public String getReportid() { 
    return this.REPORTID;
  }
   public String getReportname() { 
    return this.REPORTNAME;
  }
   public String getUrl() { 
    return this.URL;
  }
   public String getStatus() { 
    return this.STATUS;
  }
   public Integer getSimultaneous() { 
    return this.SIMULTANEOUS;
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
     if (REPORTID == null) {
      REPORTID = "";
    }
     if (REPORTNAME == null) {
      REPORTNAME = "";
    }
     if (URL == null) {
      URL = "";
    }
     if (STATUS == null) {
      STATUS = "";
    }
     if (SIMULTANEOUS == null) {
      SIMULTANEOUS = new Integer (0);
    }
   }

   public void setInterfaceid(final String INTERFACEID) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACEID,"INTERFACEID",12,50,0);
    }
    modifiedColumns.add("INTERFACEID");
    this.INTERFACEID = INTERFACEID;
  }
   public void setReportid(final String REPORTID) {
    if (validateData){
      DataValidator.validateData((Object)REPORTID,"REPORTID",12,255,0);
    }
    modifiedColumns.add("REPORTID");
    this.REPORTID = REPORTID;
  }
   public void setReportname(final String REPORTNAME) {
    if (validateData){
      DataValidator.validateData((Object)REPORTNAME,"REPORTNAME",12,255,0);
    }
    modifiedColumns.add("REPORTNAME");
    this.REPORTNAME = REPORTNAME;
  }
   public void setUrl(final String URL) {
    if (validateData){
      DataValidator.validateData((Object)URL,"URL",12,32000,0);
    }
    modifiedColumns.add("URL");
    this.URL = URL;
  }
   public void setStatus(final String STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",12,10,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setSimultaneous(final Integer SIMULTANEOUS) {
    if (validateData){
      DataValidator.validateData((Object)SIMULTANEOUS,"SIMULTANEOUS",4,10,0);
    }
    modifiedColumns.add("SIMULTANEOUS");
    this.SIMULTANEOUS = SIMULTANEOUS;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Alarmreport o) {

         if ((((this.REPORTID == null) || (o.REPORTID == null)) && (this.REPORTID != o.REPORTID))
          ){
    return false;
    } else
         if ((((this.REPORTID != null) && (o.REPORTID != null)) && (this.REPORTID.equals(o.REPORTID) == false))
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
  public boolean equals(final Alarmreport o) {

         if ((((this.INTERFACEID == null) || (o.INTERFACEID == null)) && (this.INTERFACEID != o.INTERFACEID))
            || (((this.REPORTID == null) || (o.REPORTID == null)) && (this.REPORTID != o.REPORTID))
            || (((this.REPORTNAME == null) || (o.REPORTNAME == null)) && (this.REPORTNAME != o.REPORTNAME))
            || (((this.URL == null) || (o.URL == null)) && (this.URL != o.URL))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.SIMULTANEOUS == null) || (o.SIMULTANEOUS == null)) && (this.SIMULTANEOUS != o.SIMULTANEOUS))
          ){
    return false;
    } else
         if ((((this.INTERFACEID != null) && (o.INTERFACEID != null)) && (this.INTERFACEID.equals(o.INTERFACEID) == false))
            || (((this.REPORTID != null) && (o.REPORTID != null)) && (this.REPORTID.equals(o.REPORTID) == false))
            || (((this.REPORTNAME != null) && (o.REPORTNAME != null)) && (this.REPORTNAME.equals(o.REPORTNAME) == false))
            || (((this.URL != null) && (o.URL != null)) && (this.URL.equals(o.URL) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.SIMULTANEOUS != null) && (o.SIMULTANEOUS != null)) && (this.SIMULTANEOUS.equals(o.SIMULTANEOUS) == false))
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
  * return 255
  */
  public static int getReportidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getReportidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getReportidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getReportnameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getReportnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getReportnameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getUrlColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUrlDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUrlSQLType() {
    
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
  * return 10
  */
  public static int getSimultaneousColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSimultaneousDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getSimultaneousSQLType() {
    
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

  public Alarmreport getOriginal() {
    return original;
  }
   
  public void setOriginal(final Alarmreport original) {
    this.original = (Alarmreport) original.clone();
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
