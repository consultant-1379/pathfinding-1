package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.sql.Timestamp; // NOPMD : eeipca : may not be used
import java.util.Base64;
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
public class Meta_databases implements Cloneable,RockDBObject  {

    private String USERNAME;
    private String VERSION_NUMBER;
    private String TYPE_NAME;
    private Long CONNECTION_ID;
    private String CONNECTION_NAME;
    private String CONNECTION_STRING;
    private String PASSWORD;
    private String DESCRIPTION;
    private String DRIVER_NAME;
    private String DB_LINK_NAME;
    private String ENCRYPTION_FLAG;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSION_NUMBER"    ,"CONNECTION_ID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private boolean isDecryptionRequired = true;
  
  private Meta_databases original; 

  public Meta_databases(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Meta_databases(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.USERNAME = null;
         this.VERSION_NUMBER = null;
         this.TYPE_NAME = null;
         this.CONNECTION_ID = null;
         this.CONNECTION_NAME = null;
         this.CONNECTION_STRING = null;
         this.PASSWORD = null;
         this.DESCRIPTION = null;
         this.DRIVER_NAME = null;
         this.DB_LINK_NAME = null;
         this.ENCRYPTION_FLAG = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Meta_databases(final RockFactory rockFact   ,final String VERSION_NUMBER ,final Long CONNECTION_ID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSION_NUMBER = VERSION_NUMBER;
            this.CONNECTION_ID = CONNECTION_ID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Meta_databases> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Meta_databases o = it.next();

              this.USERNAME = o.getUsername();
              this.VERSION_NUMBER = o.getVersion_number();
              this.TYPE_NAME = o.getType_name();
              this.CONNECTION_ID = o.getConnection_id();
              this.CONNECTION_NAME = o.getConnection_name();
              this.CONNECTION_STRING = o.getConnection_string();
              this.PASSWORD = o.getPassword();
              this.DESCRIPTION = o.getDescription();
              this.DRIVER_NAME = o.getDriver_name();
              this.DB_LINK_NAME = o.getDb_link_name();
              // Calling the get method for the password on an object retrieved from DB will decrypt the password if it is encrypted.
              // Thus setting the Encryption Flag to "N"
              this.ENCRYPTION_FLAG = "N"; 
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_databases");
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
  public Meta_databases(final RockFactory rockFact, final Meta_databases whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Meta_databases> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Meta_databases o = it.next();
                this.USERNAME = o.getUsername();
                this.VERSION_NUMBER = o.getVersion_number();
                this.TYPE_NAME = o.getType_name();
                this.CONNECTION_ID = o.getConnection_id();
                this.CONNECTION_NAME = o.getConnection_name();
                this.CONNECTION_STRING = o.getConnection_string();
                this.PASSWORD = o.getPassword();
                this.DESCRIPTION = o.getDescription();
                this.DRIVER_NAME = o.getDriver_name();
                this.DB_LINK_NAME = o.getDb_link_name();
                // Calling the get method for the password on an object retrieved from DB will decrypt the password if it is encrypted.
                // Thus setting the Encryption Flag to "N"
                this.ENCRYPTION_FLAG = "N";
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Meta_databases");
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
    return "Meta_databases";
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
  public int updateDB(final boolean useTimestamp, final Meta_databases whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Meta_databases whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Meta_databases.saveDB(), no primary key defined");
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

  public boolean isTypeAndConnection(final String type, final String connection) {
      return (this.TYPE_NAME != null && this.TYPE_NAME.equalsIgnoreCase(type) && this.CONNECTION_NAME != null &&
        this.CONNECTION_NAME.equalsIgnoreCase(connection));
  }


  /**
   * Prints the object out as XML
   * 
   * @exception SQLException
   */
    public String toXML_tag() throws SQLException, RockException {
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("<Meta_databases ");
        sbuff.append("USERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USERNAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TYPE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE_NAME),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("CONNECTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_NAME),12, true)+"\" ");
        sbuff.append("CONNECTION_STRING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_STRING),12, true)+"\" ");
        sbuff.append("PASSWORD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PASSWORD),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("DRIVER_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DRIVER_NAME),12, true)+"\" ");
        sbuff.append("DB_LINK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DB_LINK_NAME),12, true)+"\" ");
        sbuff.append("ENCRYPTION_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENCRYPTION_FLAG),12, true)+"\" ");
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
    sbuff.append("<Meta_databases ");
        sbuff.append("USERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.USERNAME),12, true)+"\" ");
        sbuff.append("VERSION_NUMBER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSION_NUMBER),12, true)+"\" ");
        sbuff.append("TYPE_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE_NAME),12, true)+"\" ");
        sbuff.append("CONNECTION_ID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_ID),2, true)+"\" ");
        sbuff.append("CONNECTION_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_NAME),12, true)+"\" ");
        sbuff.append("CONNECTION_STRING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONNECTION_STRING),12, true)+"\" ");
        sbuff.append("PASSWORD=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PASSWORD),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("DRIVER_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DRIVER_NAME),12, true)+"\" ");
        sbuff.append("DB_LINK_NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DB_LINK_NAME),12, true)+"\" ");
        sbuff.append("ENCRYPTION_FLAG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ENCRYPTION_FLAG),12, true)+"\" ");
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
    sbuff.append("</Meta_databases>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Meta_databases ( ");
	    		sbuff.append("USERNAME");
		    		sbuff.append(", VERSION_NUMBER");
	    		sbuff.append(", TYPE_NAME");
	    		sbuff.append(", CONNECTION_ID");
	    		sbuff.append(", CONNECTION_NAME");
	    		sbuff.append(", CONNECTION_STRING");
	    		sbuff.append(", PASSWORD");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", DRIVER_NAME");
	    		sbuff.append(", DB_LINK_NAME");
	    		sbuff.append(", ENCRYPTION_FLAG");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.USERNAME,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VERSION_NUMBER,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPE_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_ID,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONNECTION_STRING,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PASSWORD,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DRIVER_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DB_LINK_NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ENCRYPTION_FLAG,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getUsername() { 
    return this.USERNAME;
  }
   public String getVersion_number() { 
    return this.VERSION_NUMBER;
  }
   public String getType_name() { 
    return this.TYPE_NAME;
  }
   public Long getConnection_id() { 
    return this.CONNECTION_ID;
  }
   public String getConnection_name() { 
    return this.CONNECTION_NAME;
  }
   public String getConnection_string() { 
    return this.CONNECTION_STRING;
  }
   public String getPassword() { 
	String password = this.PASSWORD;
	if (password != null) {
	  if (this.ENCRYPTION_FLAG != null && this.ENCRYPTION_FLAG.equalsIgnoreCase("Y") 
			  && isDecryptionRequired) {
		  password = new String(Base64.getDecoder().decode(password.trim()));
	  }
	}
	
    return password;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getDriver_name() { 
    return this.DRIVER_NAME;
  }
   public String getDb_link_name() { 
    return this.DB_LINK_NAME;
  }
  public String getEncryption_flag() { 
	return this.ENCRYPTION_FLAG;
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
     if (USERNAME == null) {
      USERNAME = "";
    }
     if (VERSION_NUMBER == null) {
      VERSION_NUMBER = "";
    }
     if (TYPE_NAME == null) {
      TYPE_NAME = "";
    }
     if (CONNECTION_ID == null) {
      CONNECTION_ID = new Long (0);
    }
     if (CONNECTION_NAME == null) {
      CONNECTION_NAME = "";
    }
     if (CONNECTION_STRING == null) {
      CONNECTION_STRING = "";
    }
     if (PASSWORD == null) {
      PASSWORD = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (DRIVER_NAME == null) {
      DRIVER_NAME = "";
    }
     if (DB_LINK_NAME == null) {
      DB_LINK_NAME = "";
    }
     if (ENCRYPTION_FLAG == null) {
      ENCRYPTION_FLAG = "";
    }
   }

   public void setUsername(final String USERNAME) {
    if (validateData){
      DataValidator.validateData((Object)USERNAME,"USERNAME",12,30,0);
    }
    modifiedColumns.add("USERNAME");
    this.USERNAME = USERNAME;
  }
   public void setVersion_number(final String VERSION_NUMBER) {
    if (validateData){
      DataValidator.validateData((Object)VERSION_NUMBER,"VERSION_NUMBER",12,32,0);
    }
    modifiedColumns.add("VERSION_NUMBER");
    this.VERSION_NUMBER = VERSION_NUMBER;
  }
   public void setType_name(final String TYPE_NAME) {
    if (validateData){
      DataValidator.validateData((Object)TYPE_NAME,"TYPE_NAME",12,15,0);
    }
    modifiedColumns.add("TYPE_NAME");
    this.TYPE_NAME = TYPE_NAME;
  }
   public void setConnection_id(final Long CONNECTION_ID) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_ID,"CONNECTION_ID",2,38,0);
    }
    modifiedColumns.add("CONNECTION_ID");
    this.CONNECTION_ID = CONNECTION_ID;
  }
   public void setConnection_name(final String CONNECTION_NAME) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_NAME,"CONNECTION_NAME",12,30,0);
    }
    modifiedColumns.add("CONNECTION_NAME");
    this.CONNECTION_NAME = CONNECTION_NAME;
  }
   public void setConnection_string(final String CONNECTION_STRING) {
    if (validateData){
      DataValidator.validateData((Object)CONNECTION_STRING,"CONNECTION_STRING",12,400,0);
    }
    modifiedColumns.add("CONNECTION_STRING");
    this.CONNECTION_STRING = CONNECTION_STRING;
  }
   
   /**
    * Always set an encrypted string as the password.
    * While creating the whereObject to retrieve data from the Meta_Databases table, always set the isDecryptionRequired flag to false because
    * it will decrypt the password while generating the SQL statement and thus the password will not match with the ones in the Table.
    * 
    * While retrieving the data from the Meta_Databases Table, Rock Factory will automatically set the ENCRYPTION_FLAG and thus
    * the password will be decrypted accordingly while retrieval. 
    * */
   public void setPassword(final String PASSWORD) {
    if (validateData){
      DataValidator.validateData((Object)PASSWORD,"PASSWORD",12,128,0);
    }
    modifiedColumns.add("PASSWORD");
    this.PASSWORD = PASSWORD;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setDriver_name(final String DRIVER_NAME) {
    if (validateData){
      DataValidator.validateData((Object)DRIVER_NAME,"DRIVER_NAME",12,100,0);
    }
    modifiedColumns.add("DRIVER_NAME");
    this.DRIVER_NAME = DRIVER_NAME;
  }
   public void setDb_link_name(final String DB_LINK_NAME) {
    if (validateData){
      DataValidator.validateData((Object)DB_LINK_NAME,"DB_LINK_NAME",12,128,0);
    }
    modifiedColumns.add("DB_LINK_NAME");
    this.DB_LINK_NAME = DB_LINK_NAME;
  }
   public void setEncryption_flag(final String ENCRYPTION_FLAG) {
    if (validateData){
      DataValidator.validateData((Object)ENCRYPTION_FLAG,"ENCRYPTION_FLAG",12,2,0);
    }
    modifiedColumns.add("ENCRYPTION_FLAG");
    this.ENCRYPTION_FLAG = ENCRYPTION_FLAG;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Meta_databases o) {

         if ((((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
          ){
    return false;
    } else
         if ((((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
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
  public boolean equals(final Meta_databases o) {

         if ((((this.USERNAME == null) || (o.USERNAME == null)) && (this.USERNAME != o.USERNAME))
            || (((this.VERSION_NUMBER == null) || (o.VERSION_NUMBER == null)) && (this.VERSION_NUMBER != o.VERSION_NUMBER))
            || (((this.TYPE_NAME == null) || (o.TYPE_NAME == null)) && (this.TYPE_NAME != o.TYPE_NAME))
            || (((this.CONNECTION_ID == null) || (o.CONNECTION_ID == null)) && (this.CONNECTION_ID != o.CONNECTION_ID))
            || (((this.CONNECTION_NAME == null) || (o.CONNECTION_NAME == null)) && (this.CONNECTION_NAME != o.CONNECTION_NAME))
            || (((this.CONNECTION_STRING == null) || (o.CONNECTION_STRING == null)) && (this.CONNECTION_STRING != o.CONNECTION_STRING))
            || (((this.PASSWORD == null) || (o.PASSWORD == null)) && (this.PASSWORD != o.PASSWORD))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.DRIVER_NAME == null) || (o.DRIVER_NAME == null)) && (this.DRIVER_NAME != o.DRIVER_NAME))
            || (((this.DB_LINK_NAME == null) || (o.DB_LINK_NAME == null)) && (this.DB_LINK_NAME != o.DB_LINK_NAME))
            || (((this.ENCRYPTION_FLAG == null) || (o.ENCRYPTION_FLAG == null)) && (this.ENCRYPTION_FLAG != o.ENCRYPTION_FLAG))
          ){
    return false;
    } else
         if ((((this.USERNAME != null) && (o.USERNAME != null)) && (this.USERNAME.equals(o.USERNAME) == false))
            || (((this.VERSION_NUMBER != null) && (o.VERSION_NUMBER != null)) && (this.VERSION_NUMBER.equals(o.VERSION_NUMBER) == false))
            || (((this.TYPE_NAME != null) && (o.TYPE_NAME != null)) && (this.TYPE_NAME.equals(o.TYPE_NAME) == false))
            || (((this.CONNECTION_ID != null) && (o.CONNECTION_ID != null)) && (this.CONNECTION_ID.equals(o.CONNECTION_ID) == false))
            || (((this.CONNECTION_NAME != null) && (o.CONNECTION_NAME != null)) && (this.CONNECTION_NAME.equals(o.CONNECTION_NAME) == false))
            || (((this.CONNECTION_STRING != null) && (o.CONNECTION_STRING != null)) && (this.CONNECTION_STRING.equals(o.CONNECTION_STRING) == false))
            || (((this.PASSWORD != null) && (o.PASSWORD != null)) && (this.PASSWORD.equals(o.PASSWORD) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.DRIVER_NAME != null) && (o.DRIVER_NAME != null)) && (this.DRIVER_NAME.equals(o.DRIVER_NAME) == false))
            || (((this.DB_LINK_NAME != null) && (o.DB_LINK_NAME != null)) && (this.DB_LINK_NAME.equals(o.DB_LINK_NAME) == false))
            || (((this.ENCRYPTION_FLAG != null) && (o.ENCRYPTION_FLAG != null)) && (this.ENCRYPTION_FLAG.equals(o.ENCRYPTION_FLAG) == false))
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
  public static int getUsernameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUsernameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUsernameSQLType() {
    
    return 12;   
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
  * return 15
  */
  public static int getType_nameColumnSize() {
    
     return 15;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getType_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getType_nameSQLType() {
    
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
  * return 30
  */
  public static int getConnection_nameColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConnection_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConnection_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 400
  */
  public static int getConnection_stringColumnSize() {
    
     return 400;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConnection_stringDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConnection_stringSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPasswordColumnSize() {
    
     return 128;   
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
  * return 100
  */
  public static int getDriver_nameColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDriver_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDriver_nameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getDb_link_nameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDb_link_nameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDb_link_nameSQLType() {
    
    return 12;   
  }
  
  /**
   * get columnSize
   * return 2
   */
   public static int getEncryption_flagColumnSize() {
     
      return 2;   
   }
   
  /**
   * get DecimalDigits
   * return 0
   */
  public static int getEncryption_flagDecimalDigits() {
      
     return 0;   
  }
    
  /**
   * get SQLType
   * return 12
   */
  public static int getEncryption_flagSQLType() {
      
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
  
  public boolean decryptionRequired() {
    return isDecryptionRequired;
  }

  public void setDecryptionRequired(final boolean isDecryptionRequired) {
    this.isDecryptionRequired = isDecryptionRequired;
  }

  public Meta_databases getOriginal() {
    return original;
  }
   
  public void setOriginal(final Meta_databases original) {
    this.original = (Meta_databases) original.clone();
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
