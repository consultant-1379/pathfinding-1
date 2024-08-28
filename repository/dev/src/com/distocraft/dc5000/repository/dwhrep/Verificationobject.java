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
public class Verificationobject implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String MEASTYPE;
    private String MEASLEVEL;
    private String OBJECTCLASS;
    private String OBJECTNAME;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"MEASLEVEL"    ,"OBJECTCLASS"    ,"OBJECTNAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Verificationobject original; 

  public Verificationobject(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Verificationobject(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.MEASTYPE = null;
         this.MEASLEVEL = null;
         this.OBJECTCLASS = null;
         this.OBJECTNAME = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Verificationobject(final RockFactory rockFact   ,final String VERSIONID ,final String MEASLEVEL ,final String OBJECTCLASS ,final String OBJECTNAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.MEASLEVEL = MEASLEVEL;
            this.OBJECTCLASS = OBJECTCLASS;
            this.OBJECTNAME = OBJECTNAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Verificationobject> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Verificationobject o = it.next();

              this.VERSIONID = o.getVersionid();
              this.MEASTYPE = o.getMeastype();
              this.MEASLEVEL = o.getMeaslevel();
              this.OBJECTCLASS = o.getObjectclass();
              this.OBJECTNAME = o.getObjectname();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Verificationobject");
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
  public Verificationobject(final RockFactory rockFact, final Verificationobject whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Verificationobject> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Verificationobject o = it.next();
                this.VERSIONID = o.getVersionid();
                this.MEASTYPE = o.getMeastype();
                this.MEASLEVEL = o.getMeaslevel();
                this.OBJECTCLASS = o.getObjectclass();
                this.OBJECTNAME = o.getObjectname();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Verificationobject");
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
    return "Verificationobject";
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
  public int updateDB(final boolean useTimestamp, final Verificationobject whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Verificationobject whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Verificationobject.saveDB(), no primary key defined");
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
    sbuff.append("<Verificationobject ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("MEASTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASTYPE),12, true)+"\" ");
        sbuff.append("MEASLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASLEVEL),12, true)+"\" ");
        sbuff.append("OBJECTCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTCLASS),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
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
    sbuff.append("<Verificationobject ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("MEASTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASTYPE),12, true)+"\" ");
        sbuff.append("MEASLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASLEVEL),12, true)+"\" ");
        sbuff.append("OBJECTCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTCLASS),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
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
    sbuff.append("</Verificationobject>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Verificationobject ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", MEASTYPE");
	    		sbuff.append(", MEASLEVEL");
	    		sbuff.append(", OBJECTCLASS");
	    		sbuff.append(", OBJECTNAME");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.MEASTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MEASLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTCLASS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTNAME,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getMeastype() { 
    return this.MEASTYPE;
  }
   public String getMeaslevel() { 
    return this.MEASLEVEL;
  }
   public String getObjectclass() { 
    return this.OBJECTCLASS;
  }
   public String getObjectname() { 
    return this.OBJECTNAME;
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
     if (MEASTYPE == null) {
      MEASTYPE = "";
    }
     if (MEASLEVEL == null) {
      MEASLEVEL = "";
    }
     if (OBJECTCLASS == null) {
      OBJECTCLASS = "";
    }
     if (OBJECTNAME == null) {
      OBJECTNAME = "";
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setMeastype(final String MEASTYPE) {
    if (validateData){
      DataValidator.validateData((Object)MEASTYPE,"MEASTYPE",12,128,0);
    }
    modifiedColumns.add("MEASTYPE");
    this.MEASTYPE = MEASTYPE;
  }
   public void setMeaslevel(final String MEASLEVEL) {
    if (validateData){
      DataValidator.validateData((Object)MEASLEVEL,"MEASLEVEL",12,32,0);
    }
    modifiedColumns.add("MEASLEVEL");
    this.MEASLEVEL = MEASLEVEL;
  }
   public void setObjectclass(final String OBJECTCLASS) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTCLASS,"OBJECTCLASS",12,32,0);
    }
    modifiedColumns.add("OBJECTCLASS");
    this.OBJECTCLASS = OBJECTCLASS;
  }
   public void setObjectname(final String OBJECTNAME) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTNAME,"OBJECTNAME",12,32,0);
    }
    modifiedColumns.add("OBJECTNAME");
    this.OBJECTNAME = OBJECTNAME;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Verificationobject o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.MEASLEVEL == null) || (o.MEASLEVEL == null)) && (this.MEASLEVEL != o.MEASLEVEL))
            || (((this.OBJECTCLASS == null) || (o.OBJECTCLASS == null)) && (this.OBJECTCLASS != o.OBJECTCLASS))
            || (((this.OBJECTNAME == null) || (o.OBJECTNAME == null)) && (this.OBJECTNAME != o.OBJECTNAME))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.MEASLEVEL != null) && (o.MEASLEVEL != null)) && (this.MEASLEVEL.equals(o.MEASLEVEL) == false))
            || (((this.OBJECTCLASS != null) && (o.OBJECTCLASS != null)) && (this.OBJECTCLASS.equals(o.OBJECTCLASS) == false))
            || (((this.OBJECTNAME != null) && (o.OBJECTNAME != null)) && (this.OBJECTNAME.equals(o.OBJECTNAME) == false))
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
  public boolean equals(final Verificationobject o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.MEASTYPE == null) || (o.MEASTYPE == null)) && (this.MEASTYPE != o.MEASTYPE))
            || (((this.MEASLEVEL == null) || (o.MEASLEVEL == null)) && (this.MEASLEVEL != o.MEASLEVEL))
            || (((this.OBJECTCLASS == null) || (o.OBJECTCLASS == null)) && (this.OBJECTCLASS != o.OBJECTCLASS))
            || (((this.OBJECTNAME == null) || (o.OBJECTNAME == null)) && (this.OBJECTNAME != o.OBJECTNAME))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.MEASTYPE != null) && (o.MEASTYPE != null)) && (this.MEASTYPE.equals(o.MEASTYPE) == false))
            || (((this.MEASLEVEL != null) && (o.MEASLEVEL != null)) && (this.MEASLEVEL.equals(o.MEASLEVEL) == false))
            || (((this.OBJECTCLASS != null) && (o.OBJECTCLASS != null)) && (this.OBJECTCLASS.equals(o.OBJECTCLASS) == false))
            || (((this.OBJECTNAME != null) && (o.OBJECTNAME != null)) && (this.OBJECTNAME.equals(o.OBJECTNAME) == false))
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
  * return 128
  */
  public static int getMeastypeColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMeastypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMeastypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getMeaslevelColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMeaslevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMeaslevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getObjectclassColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjectclassDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjectclassSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getObjectnameColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjectnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjectnameSQLType() {
    
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

  public Verificationobject getOriginal() {
    return original;
  }
   
  public void setOriginal(final Verificationobject original) {
    this.original = (Verificationobject) original.clone();
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
