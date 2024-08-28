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
public class Measurementvector implements Cloneable,RockDBObject  {

    private String TYPEID;
    private String DATANAME;
    private String VENDORRELEASE;
    private Long VINDEX;
    private String VFROM;
    private String VTO;
    private String MEASURE;
    private Long QUANTITY;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPEID"    ,"DATANAME"    ,"VENDORRELEASE"    ,"VINDEX"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Measurementvector original; 

  public Measurementvector(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Measurementvector(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPEID = null;
         this.DATANAME = null;
         this.VENDORRELEASE = null;
         this.VINDEX = null;
         this.VFROM = null;
         this.VTO = null;
         this.MEASURE = null;
         this.QUANTITY = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Measurementvector(final RockFactory rockFact   ,final String TYPEID ,final String DATANAME ,final String VENDORRELEASE ,final Long VINDEX ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPEID = TYPEID;
            this.DATANAME = DATANAME;
            this.VENDORRELEASE = VENDORRELEASE;
            this.VINDEX = VINDEX;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Measurementvector> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Measurementvector o = it.next();

              this.TYPEID = o.getTypeid();
              this.DATANAME = o.getDataname();
              this.VENDORRELEASE = o.getVendorrelease();
              this.VINDEX = o.getVindex();
              this.VFROM = o.getVfrom();
              this.VTO = o.getVto();
              this.MEASURE = o.getMeasure();
              this.QUANTITY = o.getQuantity();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementvector");
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
  public Measurementvector(final RockFactory rockFact, final Measurementvector whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Measurementvector> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Measurementvector o = it.next();
                this.TYPEID = o.getTypeid();
                this.DATANAME = o.getDataname();
                this.VENDORRELEASE = o.getVendorrelease();
                this.VINDEX = o.getVindex();
                this.VFROM = o.getVfrom();
                this.VTO = o.getVto();
                this.MEASURE = o.getMeasure();
                this.QUANTITY = o.getQuantity();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementvector");
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
    return "Measurementvector";
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
  public int updateDB(final boolean useTimestamp, final Measurementvector whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Measurementvector whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Measurementvector.saveDB(), no primary key defined");
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
    sbuff.append("<Measurementvector ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("DATANAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATANAME),12, true)+"\" ");
        sbuff.append("VENDORRELEASE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORRELEASE),12, true)+"\" ");
        sbuff.append("VINDEX=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VINDEX),2, true)+"\" ");
        sbuff.append("VFROM=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VFROM),12, true)+"\" ");
        sbuff.append("VTO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VTO),12, true)+"\" ");
        sbuff.append("MEASURE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASURE),12, true)+"\" ");
        sbuff.append("QUANTITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUANTITY),2, true)+"\" ");
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
    sbuff.append("<Measurementvector ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("DATANAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATANAME),12, true)+"\" ");
        sbuff.append("VENDORRELEASE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORRELEASE),12, true)+"\" ");
        sbuff.append("VINDEX=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VINDEX),2, true)+"\" ");
        sbuff.append("VFROM=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VFROM),12, true)+"\" ");
        sbuff.append("VTO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VTO),12, true)+"\" ");
        sbuff.append("MEASURE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MEASURE),12, true)+"\" ");
        sbuff.append("QUANTITY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUANTITY),2, true)+"\" ");
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
    sbuff.append("</Measurementvector>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Measurementvector ( ");
	    		sbuff.append("TYPEID");
		    		sbuff.append(", DATANAME");
	    		sbuff.append(", VENDORRELEASE");
	    		sbuff.append(", VINDEX");
	    		sbuff.append(", VFROM");
	    		sbuff.append(", VTO");
	    		sbuff.append(", MEASURE");
	    		sbuff.append(", QUANTITY");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.DATANAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VENDORRELEASE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VINDEX,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VFROM,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VTO,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MEASURE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.QUANTITY,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getDataname() { 
    return this.DATANAME;
  }
   public String getVendorrelease() { 
    return this.VENDORRELEASE;
  }
   public Long getVindex() { 
    return this.VINDEX;
  }
   public String getVfrom() { 
    return this.VFROM;
  }
   public String getVto() { 
    return this.VTO;
  }
   public String getMeasure() { 
    return this.MEASURE;
  }
   public Long getQuantity() { 
    return this.QUANTITY;
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
     if (TYPEID == null) {
      TYPEID = "";
    }
     if (DATANAME == null) {
      DATANAME = "";
    }
     if (VENDORRELEASE == null) {
      VENDORRELEASE = "";
    }
     if (VINDEX == null) {
      VINDEX = new Long (0);
    }
     if (VFROM == null) {
      VFROM = "";
    }
     if (VTO == null) {
      VTO = "";
    }
     if (MEASURE == null) {
      MEASURE = "";
    }
     if (QUANTITY == null) {
      QUANTITY = new Long (0);
    }
   }

   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setDataname(final String DATANAME) {
    if (validateData){
      DataValidator.validateData((Object)DATANAME,"DATANAME",12,128,0);
    }
    modifiedColumns.add("DATANAME");
    this.DATANAME = DATANAME;
  }
   public void setVendorrelease(final String VENDORRELEASE) {
    if (validateData){
      DataValidator.validateData((Object)VENDORRELEASE,"VENDORRELEASE",12,16,0);
    }
    modifiedColumns.add("VENDORRELEASE");
    this.VENDORRELEASE = VENDORRELEASE;
  }
   public void setVindex(final Long VINDEX) {
    if (validateData){
      DataValidator.validateData((Object)VINDEX,"VINDEX",2,30,6);
    }
    modifiedColumns.add("VINDEX");
    this.VINDEX = VINDEX;
  }
   public void setVfrom(final String VFROM) {
    if (validateData){
      DataValidator.validateData((Object)VFROM,"VFROM",12,255,0);
    }
    modifiedColumns.add("VFROM");
    this.VFROM = VFROM;
  }
   public void setVto(final String VTO) {
    if (validateData){
      DataValidator.validateData((Object)VTO,"VTO",12,255,0);
    }
    modifiedColumns.add("VTO");
    this.VTO = VTO;
  }
   public void setMeasure(final String MEASURE) {
    if (validateData){
      DataValidator.validateData((Object)MEASURE,"MEASURE",12,255,0);
    }
    modifiedColumns.add("MEASURE");
    this.MEASURE = MEASURE;
  }
   public void setQuantity(final Long QUANTITY) {
    if (validateData){
      DataValidator.validateData((Object)QUANTITY,"QUANTITY",2,30,6);
    }
    modifiedColumns.add("QUANTITY");
    this.QUANTITY = QUANTITY;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Measurementvector o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.DATANAME == null) || (o.DATANAME == null)) && (this.DATANAME != o.DATANAME))
            || (((this.VENDORRELEASE == null) || (o.VENDORRELEASE == null)) && (this.VENDORRELEASE != o.VENDORRELEASE))
            || (((this.VINDEX == null) || (o.VINDEX == null)) && (this.VINDEX != o.VINDEX))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.DATANAME != null) && (o.DATANAME != null)) && (this.DATANAME.equals(o.DATANAME) == false))
            || (((this.VENDORRELEASE != null) && (o.VENDORRELEASE != null)) && (this.VENDORRELEASE.equals(o.VENDORRELEASE) == false))
            || (((this.VINDEX != null) && (o.VINDEX != null)) && (this.VINDEX.equals(o.VINDEX) == false))
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
  public boolean equals(final Measurementvector o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.DATANAME == null) || (o.DATANAME == null)) && (this.DATANAME != o.DATANAME))
            || (((this.VENDORRELEASE == null) || (o.VENDORRELEASE == null)) && (this.VENDORRELEASE != o.VENDORRELEASE))
            || (((this.VINDEX == null) || (o.VINDEX == null)) && (this.VINDEX != o.VINDEX))
            || (((this.VFROM == null) || (o.VFROM == null)) && (this.VFROM != o.VFROM))
            || (((this.VTO == null) || (o.VTO == null)) && (this.VTO != o.VTO))
            || (((this.MEASURE == null) || (o.MEASURE == null)) && (this.MEASURE != o.MEASURE))
            || (((this.QUANTITY == null) || (o.QUANTITY == null)) && (this.QUANTITY != o.QUANTITY))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.DATANAME != null) && (o.DATANAME != null)) && (this.DATANAME.equals(o.DATANAME) == false))
            || (((this.VENDORRELEASE != null) && (o.VENDORRELEASE != null)) && (this.VENDORRELEASE.equals(o.VENDORRELEASE) == false))
            || (((this.VINDEX != null) && (o.VINDEX != null)) && (this.VINDEX.equals(o.VINDEX) == false))
            || (((this.VFROM != null) && (o.VFROM != null)) && (this.VFROM.equals(o.VFROM) == false))
            || (((this.VTO != null) && (o.VTO != null)) && (this.VTO.equals(o.VTO) == false))
            || (((this.MEASURE != null) && (o.MEASURE != null)) && (this.MEASURE.equals(o.MEASURE) == false))
            || (((this.QUANTITY != null) && (o.QUANTITY != null)) && (this.QUANTITY.equals(o.QUANTITY) == false))
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
  public static int getTypeidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getDatanameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDatanameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDatanameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getVendorreleaseColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVendorreleaseDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVendorreleaseSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getVindexColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 6
  */
  public static int getVindexDecimalDigits() {
    
     return 6;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getVindexSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getVfromColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVfromDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVfromSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getVtoColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVtoDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVtoSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getMeasureColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMeasureDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMeasureSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getQuantityColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 6
  */
  public static int getQuantityDecimalDigits() {
    
     return 6;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getQuantitySQLType() {
    
    return 2;   
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

  public Measurementvector getOriginal() {
    return original;
  }
   
  public void setOriginal(final Measurementvector original) {
    this.original = (Measurementvector) original.clone();
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
