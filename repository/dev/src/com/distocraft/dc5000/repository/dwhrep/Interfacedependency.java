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
public class Interfacedependency implements Cloneable,RockDBObject  {

    private String INTERFACEVERSION;
    private String INTERFACENAME;
    private String TECHPACKNAME;
    private String TECHPACKVERSION;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "INTERFACEVERSION"    ,"INTERFACENAME"    ,"TECHPACKNAME"    ,"TECHPACKVERSION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Interfacedependency original; 

  public Interfacedependency(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Interfacedependency(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.INTERFACEVERSION = null;
         this.INTERFACENAME = null;
         this.TECHPACKNAME = null;
         this.TECHPACKVERSION = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Interfacedependency(final RockFactory rockFact   ,final String INTERFACEVERSION ,final String INTERFACENAME ,final String TECHPACKNAME ,final String TECHPACKVERSION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.INTERFACEVERSION = INTERFACEVERSION;
            this.INTERFACENAME = INTERFACENAME;
            this.TECHPACKNAME = TECHPACKNAME;
            this.TECHPACKVERSION = TECHPACKVERSION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Interfacedependency> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Interfacedependency o = it.next();

              this.INTERFACEVERSION = o.getInterfaceversion();
              this.INTERFACENAME = o.getInterfacename();
              this.TECHPACKNAME = o.getTechpackname();
              this.TECHPACKVERSION = o.getTechpackversion();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Interfacedependency");
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
  public Interfacedependency(final RockFactory rockFact, final Interfacedependency whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Interfacedependency> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Interfacedependency o = it.next();
                this.INTERFACEVERSION = o.getInterfaceversion();
                this.INTERFACENAME = o.getInterfacename();
                this.TECHPACKNAME = o.getTechpackname();
                this.TECHPACKVERSION = o.getTechpackversion();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Interfacedependency");
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
    return "Interfacedependency";
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
  public int updateDB(final boolean useTimestamp, final Interfacedependency whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Interfacedependency whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Interfacedependency.saveDB(), no primary key defined");
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
    sbuff.append("<Interfacedependency ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("TECHPACKNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKNAME),12, true)+"\" ");
        sbuff.append("TECHPACKVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKVERSION),12, true)+"\" ");
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
    sbuff.append("<Interfacedependency ");
        sbuff.append("INTERFACEVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACEVERSION),12, true)+"\" ");
        sbuff.append("INTERFACENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERFACENAME),12, true)+"\" ");
        sbuff.append("TECHPACKNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKNAME),12, true)+"\" ");
        sbuff.append("TECHPACKVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACKVERSION),12, true)+"\" ");
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
    sbuff.append("</Interfacedependency>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Interfacedependency ( ");
	    		sbuff.append("INTERFACEVERSION");
		    		sbuff.append(", INTERFACENAME");
	    		sbuff.append(", TECHPACKNAME");
	    		sbuff.append(", TECHPACKVERSION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.INTERFACEVERSION,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.INTERFACENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACKNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACKVERSION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getInterfaceversion() { 
    return this.INTERFACEVERSION;
  }
   public String getInterfacename() { 
    return this.INTERFACENAME;
  }
   public String getTechpackname() { 
    return this.TECHPACKNAME;
  }
   public String getTechpackversion() { 
    return this.TECHPACKVERSION;
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
     if (INTERFACEVERSION == null) {
      INTERFACEVERSION = "";
    }
     if (INTERFACENAME == null) {
      INTERFACENAME = "";
    }
     if (TECHPACKNAME == null) {
      TECHPACKNAME = "";
    }
     if (TECHPACKVERSION == null) {
      TECHPACKVERSION = "";
    }
   }

   public void setInterfaceversion(final String INTERFACEVERSION) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACEVERSION,"INTERFACEVERSION",12,32,0);
    }
    modifiedColumns.add("INTERFACEVERSION");
    this.INTERFACEVERSION = INTERFACEVERSION;
  }
   public void setInterfacename(final String INTERFACENAME) {
    if (validateData){
      DataValidator.validateData((Object)INTERFACENAME,"INTERFACENAME",12,255,0);
    }
    modifiedColumns.add("INTERFACENAME");
    this.INTERFACENAME = INTERFACENAME;
  }
   public void setTechpackname(final String TECHPACKNAME) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACKNAME,"TECHPACKNAME",12,255,0);
    }
    modifiedColumns.add("TECHPACKNAME");
    this.TECHPACKNAME = TECHPACKNAME;
  }
   public void setTechpackversion(final String TECHPACKVERSION) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACKVERSION,"TECHPACKVERSION",12,128,0);
    }
    modifiedColumns.add("TECHPACKVERSION");
    this.TECHPACKVERSION = TECHPACKVERSION;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Interfacedependency o) {

         if ((((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
            || (((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.TECHPACKNAME == null) || (o.TECHPACKNAME == null)) && (this.TECHPACKNAME != o.TECHPACKNAME))
            || (((this.TECHPACKVERSION == null) || (o.TECHPACKVERSION == null)) && (this.TECHPACKVERSION != o.TECHPACKVERSION))
          ){
    return false;
    } else
         if ((((this.INTERFACEVERSION != null) && (o.INTERFACEVERSION != null)) && (this.INTERFACEVERSION.equals(o.INTERFACEVERSION) == false))
            || (((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
            || (((this.TECHPACKNAME != null) && (o.TECHPACKNAME != null)) && (this.TECHPACKNAME.equals(o.TECHPACKNAME) == false))
            || (((this.TECHPACKVERSION != null) && (o.TECHPACKVERSION != null)) && (this.TECHPACKVERSION.equals(o.TECHPACKVERSION) == false))
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
  public boolean equals(final Interfacedependency o) {

         if ((((this.INTERFACEVERSION == null) || (o.INTERFACEVERSION == null)) && (this.INTERFACEVERSION != o.INTERFACEVERSION))
            || (((this.INTERFACENAME == null) || (o.INTERFACENAME == null)) && (this.INTERFACENAME != o.INTERFACENAME))
            || (((this.TECHPACKNAME == null) || (o.TECHPACKNAME == null)) && (this.TECHPACKNAME != o.TECHPACKNAME))
            || (((this.TECHPACKVERSION == null) || (o.TECHPACKVERSION == null)) && (this.TECHPACKVERSION != o.TECHPACKVERSION))
          ){
    return false;
    } else
         if ((((this.INTERFACEVERSION != null) && (o.INTERFACEVERSION != null)) && (this.INTERFACEVERSION.equals(o.INTERFACEVERSION) == false))
            || (((this.INTERFACENAME != null) && (o.INTERFACENAME != null)) && (this.INTERFACENAME.equals(o.INTERFACENAME) == false))
            || (((this.TECHPACKNAME != null) && (o.TECHPACKNAME != null)) && (this.TECHPACKNAME.equals(o.TECHPACKNAME) == false))
            || (((this.TECHPACKVERSION != null) && (o.TECHPACKVERSION != null)) && (this.TECHPACKVERSION.equals(o.TECHPACKVERSION) == false))
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
  public static int getInterfaceversionColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfaceversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfaceversionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getInterfacenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInterfacenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getInterfacenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getTechpacknameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpacknameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpacknameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getTechpackversionColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpackversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpackversionSQLType() {
    
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

  public Interfacedependency getOriginal() {
    return original;
  }
   
  public void setOriginal(final Interfacedependency original) {
    this.original = (Interfacedependency) original.clone();
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
