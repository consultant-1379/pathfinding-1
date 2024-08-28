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
public class Universeclass implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String CLASSNAME;
    private String UNIVERSEEXTENSION;
    private String DESCRIPTION;
    private String PARENT;
    private Integer OBJ_BH_REL;
    private Integer ELEM_BH_REL;
    private Integer INHERITANCE;
    private Long ORDERNRO;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"CLASSNAME"    ,"UNIVERSEEXTENSION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Universeclass original; 

  public Universeclass(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Universeclass(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.CLASSNAME = null;
         this.UNIVERSEEXTENSION = null;
         this.DESCRIPTION = null;
         this.PARENT = null;
         this.OBJ_BH_REL = null;
         this.ELEM_BH_REL = null;
         this.INHERITANCE = null;
         this.ORDERNRO = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Universeclass(final RockFactory rockFact   ,final String VERSIONID ,final String CLASSNAME ,final String UNIVERSEEXTENSION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.CLASSNAME = CLASSNAME;
            this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Universeclass> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Universeclass o = it.next();

              this.VERSIONID = o.getVersionid();
              this.CLASSNAME = o.getClassname();
              this.UNIVERSEEXTENSION = o.getUniverseextension();
              this.DESCRIPTION = o.getDescription();
              this.PARENT = o.getParent();
              this.OBJ_BH_REL = o.getObj_bh_rel();
              this.ELEM_BH_REL = o.getElem_bh_rel();
              this.INHERITANCE = o.getInheritance();
              this.ORDERNRO = o.getOrdernro();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universeclass");
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
  public Universeclass(final RockFactory rockFact, final Universeclass whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Universeclass> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Universeclass o = it.next();
                this.VERSIONID = o.getVersionid();
                this.CLASSNAME = o.getClassname();
                this.UNIVERSEEXTENSION = o.getUniverseextension();
                this.DESCRIPTION = o.getDescription();
                this.PARENT = o.getParent();
                this.OBJ_BH_REL = o.getObj_bh_rel();
                this.ELEM_BH_REL = o.getElem_bh_rel();
                this.INHERITANCE = o.getInheritance();
                this.ORDERNRO = o.getOrdernro();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Universeclass");
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
    return "Universeclass";
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
  public int updateDB(final boolean useTimestamp, final Universeclass whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Universeclass whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Universeclass.saveDB(), no primary key defined");
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
    sbuff.append("<Universeclass ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("CLASSNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CLASSNAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("PARENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARENT),12, true)+"\" ");
        sbuff.append("OBJ_BH_REL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJ_BH_REL),4, true)+"\" ");
        sbuff.append("ELEM_BH_REL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ELEM_BH_REL),4, true)+"\" ");
        sbuff.append("INHERITANCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INHERITANCE),4, true)+"\" ");
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
    sbuff.append("<Universeclass ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("CLASSNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CLASSNAME),12, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("PARENT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARENT),12, true)+"\" ");
        sbuff.append("OBJ_BH_REL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJ_BH_REL),4, true)+"\" ");
        sbuff.append("ELEM_BH_REL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ELEM_BH_REL),4, true)+"\" ");
        sbuff.append("INHERITANCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INHERITANCE),4, true)+"\" ");
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
    sbuff.append("</Universeclass>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Universeclass ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", CLASSNAME");
	    		sbuff.append(", UNIVERSEEXTENSION");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", PARENT");
	    		sbuff.append(", OBJ_BH_REL");
	    		sbuff.append(", ELEM_BH_REL");
	    		sbuff.append(", INHERITANCE");
	    		sbuff.append(", ORDERNRO");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.CLASSNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARENT,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJ_BH_REL,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ELEM_BH_REL,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INHERITANCE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ORDERNRO,2)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getClassname() { 
    return this.CLASSNAME;
  }
   public String getUniverseextension() { 
    return this.UNIVERSEEXTENSION;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public String getParent() { 
    return this.PARENT;
  }
   public Integer getObj_bh_rel() { 
    return this.OBJ_BH_REL;
  }
   public Integer getElem_bh_rel() { 
    return this.ELEM_BH_REL;
  }
   public Integer getInheritance() { 
    return this.INHERITANCE;
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
     if (CLASSNAME == null) {
      CLASSNAME = "";
    }
     if (UNIVERSEEXTENSION == null) {
      UNIVERSEEXTENSION = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (PARENT == null) {
      PARENT = "";
    }
     if (OBJ_BH_REL == null) {
      OBJ_BH_REL = new Integer (0);
    }
     if (ELEM_BH_REL == null) {
      ELEM_BH_REL = new Integer (0);
    }
     if (INHERITANCE == null) {
      INHERITANCE = new Integer (0);
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
   public void setClassname(final String CLASSNAME) {
    if (validateData){
      DataValidator.validateData((Object)CLASSNAME,"CLASSNAME",12,128,0);
    }
    modifiedColumns.add("CLASSNAME");
    this.CLASSNAME = CLASSNAME;
  }
   public void setUniverseextension(final String UNIVERSEEXTENSION) {
    if (validateData){
      DataValidator.validateData((Object)UNIVERSEEXTENSION,"UNIVERSEEXTENSION",12,12,0);
    }
    modifiedColumns.add("UNIVERSEEXTENSION");
    this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setParent(final String PARENT) {
    if (validateData){
      DataValidator.validateData((Object)PARENT,"PARENT",12,128,0);
    }
    modifiedColumns.add("PARENT");
    this.PARENT = PARENT;
  }
   public void setObj_bh_rel(final Integer OBJ_BH_REL) {
    if (validateData){
      DataValidator.validateData((Object)OBJ_BH_REL,"OBJ_BH_REL",4,10,0);
    }
    modifiedColumns.add("OBJ_BH_REL");
    this.OBJ_BH_REL = OBJ_BH_REL;
  }
   public void setElem_bh_rel(final Integer ELEM_BH_REL) {
    if (validateData){
      DataValidator.validateData((Object)ELEM_BH_REL,"ELEM_BH_REL",4,10,0);
    }
    modifiedColumns.add("ELEM_BH_REL");
    this.ELEM_BH_REL = ELEM_BH_REL;
  }
   public void setInheritance(final Integer INHERITANCE) {
    if (validateData){
      DataValidator.validateData((Object)INHERITANCE,"INHERITANCE",4,10,0);
    }
    modifiedColumns.add("INHERITANCE");
    this.INHERITANCE = INHERITANCE;
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
  public boolean dbEquals(final Universeclass o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.CLASSNAME == null) || (o.CLASSNAME == null)) && (this.CLASSNAME != o.CLASSNAME))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.CLASSNAME != null) && (o.CLASSNAME != null)) && (this.CLASSNAME.equals(o.CLASSNAME) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
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
  public boolean equals(final Universeclass o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.CLASSNAME == null) || (o.CLASSNAME == null)) && (this.CLASSNAME != o.CLASSNAME))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.PARENT == null) || (o.PARENT == null)) && (this.PARENT != o.PARENT))
            || (((this.OBJ_BH_REL == null) || (o.OBJ_BH_REL == null)) && (this.OBJ_BH_REL != o.OBJ_BH_REL))
            || (((this.ELEM_BH_REL == null) || (o.ELEM_BH_REL == null)) && (this.ELEM_BH_REL != o.ELEM_BH_REL))
            || (((this.INHERITANCE == null) || (o.INHERITANCE == null)) && (this.INHERITANCE != o.INHERITANCE))
            || (((this.ORDERNRO == null) || (o.ORDERNRO == null)) && (this.ORDERNRO != o.ORDERNRO))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.CLASSNAME != null) && (o.CLASSNAME != null)) && (this.CLASSNAME.equals(o.CLASSNAME) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.PARENT != null) && (o.PARENT != null)) && (this.PARENT.equals(o.PARENT) == false))
            || (((this.OBJ_BH_REL != null) && (o.OBJ_BH_REL != null)) && (this.OBJ_BH_REL.equals(o.OBJ_BH_REL) == false))
            || (((this.ELEM_BH_REL != null) && (o.ELEM_BH_REL != null)) && (this.ELEM_BH_REL.equals(o.ELEM_BH_REL) == false))
            || (((this.INHERITANCE != null) && (o.INHERITANCE != null)) && (this.INHERITANCE.equals(o.INHERITANCE) == false))
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
  * return 128
  */
  public static int getClassnameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getClassnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getClassnameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 12
  */
  public static int getUniverseextensionColumnSize() {
    
     return 12;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUniverseextensionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getUniverseextensionSQLType() {
    
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
  * return 128
  */
  public static int getParentColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getParentDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getParentSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getObj_bh_relColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObj_bh_relDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getObj_bh_relSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getElem_bh_relColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getElem_bh_relDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getElem_bh_relSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getInheritanceColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getInheritanceDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getInheritanceSQLType() {
    
    return 4;   
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

  public Universeclass getOriginal() {
    return original;
  }
   
  public void setOriginal(final Universeclass original) {
    this.original = (Universeclass) original.clone();
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
