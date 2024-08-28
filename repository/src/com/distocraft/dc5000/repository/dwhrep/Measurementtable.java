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
public class Measurementtable implements Cloneable,RockDBObject  {

    private String MTABLEID;
    private String TABLELEVEL;
    private String TYPEID;
    private String BASETABLENAME;
    private String DEFAULT_TEMPLATE;
    private String PARTITIONPLAN;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "MTABLEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Measurementtable original; 

  public Measurementtable(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Measurementtable(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.MTABLEID = null;
         this.TABLELEVEL = null;
         this.TYPEID = null;
         this.BASETABLENAME = null;
         this.DEFAULT_TEMPLATE = null;
         this.PARTITIONPLAN = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Measurementtable(final RockFactory rockFact   ,final String MTABLEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.MTABLEID = MTABLEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Measurementtable> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Measurementtable o = it.next();

              this.MTABLEID = o.getMtableid();
              this.TABLELEVEL = o.getTablelevel();
              this.TYPEID = o.getTypeid();
              this.BASETABLENAME = o.getBasetablename();
              this.DEFAULT_TEMPLATE = o.getDefault_template();
              this.PARTITIONPLAN = o.getPartitionplan();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementtable");
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
  public Measurementtable(final RockFactory rockFact, final Measurementtable whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Measurementtable> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Measurementtable o = it.next();
                this.MTABLEID = o.getMtableid();
                this.TABLELEVEL = o.getTablelevel();
                this.TYPEID = o.getTypeid();
                this.BASETABLENAME = o.getBasetablename();
                this.DEFAULT_TEMPLATE = o.getDefault_template();
                this.PARTITIONPLAN = o.getPartitionplan();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementtable");
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
    return "Measurementtable";
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
  public int updateDB(final boolean useTimestamp, final Measurementtable whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Measurementtable whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Measurementtable.saveDB(), no primary key defined");
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
    sbuff.append("<Measurementtable ");
        sbuff.append("MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MTABLEID),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("BASETABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASETABLENAME),12, true)+"\" ");
        sbuff.append("DEFAULT_TEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_TEMPLATE),12, true)+"\" ");
        sbuff.append("PARTITIONPLAN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONPLAN),12, true)+"\" ");
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
    sbuff.append("<Measurementtable ");
        sbuff.append("MTABLEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MTABLEID),12, true)+"\" ");
        sbuff.append("TABLELEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLELEVEL),12, true)+"\" ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("BASETABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASETABLENAME),12, true)+"\" ");
        sbuff.append("DEFAULT_TEMPLATE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DEFAULT_TEMPLATE),12, true)+"\" ");
        sbuff.append("PARTITIONPLAN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PARTITIONPLAN),12, true)+"\" ");
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
    sbuff.append("</Measurementtable>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Measurementtable ( ");
	    		sbuff.append("MTABLEID");
		    		sbuff.append(", TABLELEVEL");
	    		sbuff.append(", TYPEID");
	    		sbuff.append(", BASETABLENAME");
	    		sbuff.append(", DEFAULT_TEMPLATE");
	    		sbuff.append(", PARTITIONPLAN");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.MTABLEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TABLELEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPEID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASETABLENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DEFAULT_TEMPLATE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PARTITIONPLAN,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getMtableid() { 
    return this.MTABLEID;
  }
   public String getTablelevel() { 
    return this.TABLELEVEL;
  }
   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getBasetablename() { 
    return this.BASETABLENAME;
  }
   public String getDefault_template() { 
    return this.DEFAULT_TEMPLATE;
  }
   public String getPartitionplan() { 
    return this.PARTITIONPLAN;
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
     if (MTABLEID == null) {
      MTABLEID = "";
    }
     if (TABLELEVEL == null) {
      TABLELEVEL = "";
    }
     if (TYPEID == null) {
      TYPEID = "";
    }
     if (BASETABLENAME == null) {
      BASETABLENAME = "";
    }
     if (DEFAULT_TEMPLATE == null) {
      DEFAULT_TEMPLATE = "";
    }
     if (PARTITIONPLAN == null) {
      PARTITIONPLAN = "";
    }
   }

   public void setMtableid(final String MTABLEID) {
    if (validateData){
      DataValidator.validateData((Object)MTABLEID,"MTABLEID",12,255,0);
    }
    modifiedColumns.add("MTABLEID");
    this.MTABLEID = MTABLEID;
  }
   public void setTablelevel(final String TABLELEVEL) {
    if (validateData){
      DataValidator.validateData((Object)TABLELEVEL,"TABLELEVEL",12,50,0);
    }
    modifiedColumns.add("TABLELEVEL");
    this.TABLELEVEL = TABLELEVEL;
  }
   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setBasetablename(final String BASETABLENAME) {
    if (validateData){
      DataValidator.validateData((Object)BASETABLENAME,"BASETABLENAME",12,255,0);
    }
    modifiedColumns.add("BASETABLENAME");
    this.BASETABLENAME = BASETABLENAME;
  }
   public void setDefault_template(final String DEFAULT_TEMPLATE) {
    if (validateData){
      DataValidator.validateData((Object)DEFAULT_TEMPLATE,"DEFAULT_TEMPLATE",12,50,0);
    }
    modifiedColumns.add("DEFAULT_TEMPLATE");
    this.DEFAULT_TEMPLATE = DEFAULT_TEMPLATE;
  }
   public void setPartitionplan(final String PARTITIONPLAN) {
    if (validateData){
      DataValidator.validateData((Object)PARTITIONPLAN,"PARTITIONPLAN",12,128,0);
    }
    modifiedColumns.add("PARTITIONPLAN");
    this.PARTITIONPLAN = PARTITIONPLAN;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Measurementtable o) {

         if ((((this.MTABLEID == null) || (o.MTABLEID == null)) && (this.MTABLEID != o.MTABLEID))
          ){
    return false;
    } else
         if ((((this.MTABLEID != null) && (o.MTABLEID != null)) && (this.MTABLEID.equals(o.MTABLEID) == false))
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
  public boolean equals(final Measurementtable o) {

         if ((((this.MTABLEID == null) || (o.MTABLEID == null)) && (this.MTABLEID != o.MTABLEID))
            || (((this.TABLELEVEL == null) || (o.TABLELEVEL == null)) && (this.TABLELEVEL != o.TABLELEVEL))
            || (((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.BASETABLENAME == null) || (o.BASETABLENAME == null)) && (this.BASETABLENAME != o.BASETABLENAME))
            || (((this.DEFAULT_TEMPLATE == null) || (o.DEFAULT_TEMPLATE == null)) && (this.DEFAULT_TEMPLATE != o.DEFAULT_TEMPLATE))
            || (((this.PARTITIONPLAN == null) || (o.PARTITIONPLAN == null)) && (this.PARTITIONPLAN != o.PARTITIONPLAN))
          ){
    return false;
    } else
         if ((((this.MTABLEID != null) && (o.MTABLEID != null)) && (this.MTABLEID.equals(o.MTABLEID) == false))
            || (((this.TABLELEVEL != null) && (o.TABLELEVEL != null)) && (this.TABLELEVEL.equals(o.TABLELEVEL) == false))
            || (((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.BASETABLENAME != null) && (o.BASETABLENAME != null)) && (this.BASETABLENAME.equals(o.BASETABLENAME) == false))
            || (((this.DEFAULT_TEMPLATE != null) && (o.DEFAULT_TEMPLATE != null)) && (this.DEFAULT_TEMPLATE.equals(o.DEFAULT_TEMPLATE) == false))
            || (((this.PARTITIONPLAN != null) && (o.PARTITIONPLAN != null)) && (this.PARTITIONPLAN.equals(o.PARTITIONPLAN) == false))
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
  public static int getMtableidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMtableidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getMtableidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getTablelevelColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTablelevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTablelevelSQLType() {
    
    return 12;   
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
  * return 255
  */
  public static int getBasetablenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBasetablenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getBasetablenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getDefault_templateColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDefault_templateDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getDefault_templateSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPartitionplanColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPartitionplanDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPartitionplanSQLType() {
    
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

  public Measurementtable getOriginal() {
    return original;
  }
   
  public void setOriginal(final Measurementtable original) {
    this.original = (Measurementtable) original.clone();
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
