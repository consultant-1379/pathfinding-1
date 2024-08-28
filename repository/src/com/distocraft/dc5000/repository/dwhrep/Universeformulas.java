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
public class Universeformulas implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String TECHPACK_TYPE;
    private String NAME;
    private String FORMULA;
    private String OBJECTTYPE;
    private String QUALIFICATION;
    private String AGGREGATION;
    private Long ORDERNRO;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"NAME"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Universeformulas original; 

  public Universeformulas(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Universeformulas(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.TECHPACK_TYPE = null;
         this.NAME = null;
         this.FORMULA = null;
         this.OBJECTTYPE = null;
         this.QUALIFICATION = null;
         this.AGGREGATION = null;
         this.ORDERNRO = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Universeformulas(final RockFactory rockFact   ,final String VERSIONID ,final String NAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.NAME = NAME;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Universeformulas> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Universeformulas o = it.next();

              this.VERSIONID = o.getVersionid();
              this.TECHPACK_TYPE = o.getTechpack_type();
              this.NAME = o.getName();
              this.FORMULA = o.getFormula();
              this.OBJECTTYPE = o.getObjecttype();
              this.QUALIFICATION = o.getQualification();
              this.AGGREGATION = o.getAggregation();
              this.ORDERNRO = o.getOrdernro();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universeformulas");
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
  public Universeformulas(final RockFactory rockFact, final Universeformulas whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Universeformulas> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Universeformulas o = it.next();
                this.VERSIONID = o.getVersionid();
                this.TECHPACK_TYPE = o.getTechpack_type();
                this.NAME = o.getName();
                this.FORMULA = o.getFormula();
                this.OBJECTTYPE = o.getObjecttype();
                this.QUALIFICATION = o.getQualification();
                this.AGGREGATION = o.getAggregation();
                this.ORDERNRO = o.getOrdernro();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universeformulas");
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
    return "Universeformulas";
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
  public int updateDB(final boolean useTimestamp, final Universeformulas whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Universeformulas whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Universeformulas.saveDB(), no primary key defined");
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
    sbuff.append("<Universeformulas ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("TECHPACK_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_TYPE),12, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("FORMULA=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FORMULA),12, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("QUALIFICATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUALIFICATION),12, true)+"\" ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
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
    sbuff.append("<Universeformulas ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("TECHPACK_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TECHPACK_TYPE),12, true)+"\" ");
        sbuff.append("NAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.NAME),12, true)+"\" ");
        sbuff.append("FORMULA=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FORMULA),12, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("QUALIFICATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.QUALIFICATION),12, true)+"\" ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("ORDERNRO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNRO),2, true)+"\" ");
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
    sbuff.append("</Universeformulas>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Universeformulas ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", TECHPACK_TYPE");
	    		sbuff.append(", NAME");
	    		sbuff.append(", FORMULA");
	    		sbuff.append(", OBJECTTYPE");
	    		sbuff.append(", QUALIFICATION");
	    		sbuff.append(", AGGREGATION");
	    		sbuff.append(", ORDERNRO");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TECHPACK_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.NAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FORMULA,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.QUALIFICATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ORDERNRO,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getTechpack_type() { 
    return this.TECHPACK_TYPE;
  }
   public String getName() { 
    return this.NAME;
  }
   public String getFormula() { 
    return this.FORMULA;
  }
   public String getObjecttype() { 
    return this.OBJECTTYPE;
  }
   public String getQualification() { 
    return this.QUALIFICATION;
  }
   public String getAggregation() { 
    return this.AGGREGATION;
  }
   public Long getOrdernro() { 
    return this.ORDERNRO;
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
     if (TECHPACK_TYPE == null) {
      TECHPACK_TYPE = "";
    }
     if (NAME == null) {
      NAME = "";
    }
     if (FORMULA == null) {
      FORMULA = "";
    }
     if (OBJECTTYPE == null) {
      OBJECTTYPE = "";
    }
     if (QUALIFICATION == null) {
      QUALIFICATION = "";
    }
     if (AGGREGATION == null) {
      AGGREGATION = "";
    }
     if (ORDERNRO == null) {
      ORDERNRO = new Long (0);
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setTechpack_type(final String TECHPACK_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TECHPACK_TYPE,"TECHPACK_TYPE",12,32,0);
    }
    modifiedColumns.add("TECHPACK_TYPE");
    this.TECHPACK_TYPE = TECHPACK_TYPE;
  }
   public void setName(final String NAME) {
    if (validateData){
      DataValidator.validateData((Object)NAME,"NAME",12,255,0);
    }
    modifiedColumns.add("NAME");
    this.NAME = NAME;
  }
   public void setFormula(final String FORMULA) {
    if (validateData){
      DataValidator.validateData((Object)FORMULA,"FORMULA",12,32000,0);
    }
    modifiedColumns.add("FORMULA");
    this.FORMULA = FORMULA;
  }
   public void setObjecttype(final String OBJECTTYPE) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTTYPE,"OBJECTTYPE",12,16,0);
    }
    modifiedColumns.add("OBJECTTYPE");
    this.OBJECTTYPE = OBJECTTYPE;
  }
   public void setQualification(final String QUALIFICATION) {
    if (validateData){
      DataValidator.validateData((Object)QUALIFICATION,"QUALIFICATION",12,16,0);
    }
    modifiedColumns.add("QUALIFICATION");
    this.QUALIFICATION = QUALIFICATION;
  }
   public void setAggregation(final String AGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATION,"AGGREGATION",12,16,0);
    }
    modifiedColumns.add("AGGREGATION");
    this.AGGREGATION = AGGREGATION;
  }
   public void setOrdernro(final Long ORDERNRO) {
    if (validateData){
      DataValidator.validateData((Object)ORDERNRO,"ORDERNRO",2,30,6);
    }
    modifiedColumns.add("ORDERNRO");
    this.ORDERNRO = ORDERNRO;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Universeformulas o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
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
  public boolean equals(final Universeformulas o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.TECHPACK_TYPE == null) || (o.TECHPACK_TYPE == null)) && (this.TECHPACK_TYPE != o.TECHPACK_TYPE))
            || (((this.NAME == null) || (o.NAME == null)) && (this.NAME != o.NAME))
            || (((this.FORMULA == null) || (o.FORMULA == null)) && (this.FORMULA != o.FORMULA))
            || (((this.OBJECTTYPE == null) || (o.OBJECTTYPE == null)) && (this.OBJECTTYPE != o.OBJECTTYPE))
            || (((this.QUALIFICATION == null) || (o.QUALIFICATION == null)) && (this.QUALIFICATION != o.QUALIFICATION))
            || (((this.AGGREGATION == null) || (o.AGGREGATION == null)) && (this.AGGREGATION != o.AGGREGATION))
            || (((this.ORDERNRO == null) || (o.ORDERNRO == null)) && (this.ORDERNRO != o.ORDERNRO))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.TECHPACK_TYPE != null) && (o.TECHPACK_TYPE != null)) && (this.TECHPACK_TYPE.equals(o.TECHPACK_TYPE) == false))
            || (((this.NAME != null) && (o.NAME != null)) && (this.NAME.equals(o.NAME) == false))
            || (((this.FORMULA != null) && (o.FORMULA != null)) && (this.FORMULA.equals(o.FORMULA) == false))
            || (((this.OBJECTTYPE != null) && (o.OBJECTTYPE != null)) && (this.OBJECTTYPE.equals(o.OBJECTTYPE) == false))
            || (((this.QUALIFICATION != null) && (o.QUALIFICATION != null)) && (this.QUALIFICATION.equals(o.QUALIFICATION) == false))
            || (((this.AGGREGATION != null) && (o.AGGREGATION != null)) && (this.AGGREGATION.equals(o.AGGREGATION) == false))
            || (((this.ORDERNRO != null) && (o.ORDERNRO != null)) && (this.ORDERNRO.equals(o.ORDERNRO) == false))
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
  * return 32
  */
  public static int getTechpack_typeColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTechpack_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTechpack_typeSQLType() {
    
    return 12;   
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
  * return 32000
  */
  public static int getFormulaColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFormulaDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFormulaSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getObjecttypeColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjecttypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjecttypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getQualificationColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getQualificationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getQualificationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getAggregationColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 30
  */
  public static int getOrdernroColumnSize() {
    
     return 30;   
  }

 /**
  * get DecimalDigits
  * return 6
  */
  public static int getOrdernroDecimalDigits() {
    
     return 6;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getOrdernroSQLType() {
    
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

  public Universeformulas getOriginal() {
    return original;
  }
   
  public void setOriginal(final Universeformulas original) {
    this.original = (Universeformulas) original.clone();
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
