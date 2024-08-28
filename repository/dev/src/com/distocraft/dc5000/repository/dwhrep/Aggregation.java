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
public class Aggregation implements Cloneable,RockDBObject  {

    private String AGGREGATION;
    private String VERSIONID;
    private String AGGREGATIONSET;
    private String AGGREGATIONGROUP;
    private String REAGGREGATIONSET;
    private String REAGGREGATIONGROUP;
    private Integer GROUPORDER;
    private Integer AGGREGATIONORDER;
    private String AGGREGATIONTYPE;
    private String AGGREGATIONSCOPE;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "AGGREGATION"    ,"VERSIONID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Aggregation original; 

  public Aggregation(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Aggregation(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.AGGREGATION = null;
         this.VERSIONID = null;
         this.AGGREGATIONSET = null;
         this.AGGREGATIONGROUP = null;
         this.REAGGREGATIONSET = null;
         this.REAGGREGATIONGROUP = null;
         this.GROUPORDER = null;
         this.AGGREGATIONORDER = null;
         this.AGGREGATIONTYPE = null;
         this.AGGREGATIONSCOPE = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Aggregation(final RockFactory rockFact   ,final String AGGREGATION ,final String VERSIONID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.AGGREGATION = AGGREGATION;
            this.VERSIONID = VERSIONID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Aggregation> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Aggregation o = it.next();

              this.AGGREGATION = o.getAggregation();
              this.VERSIONID = o.getVersionid();
              this.AGGREGATIONSET = o.getAggregationset();
              this.AGGREGATIONGROUP = o.getAggregationgroup();
              this.REAGGREGATIONSET = o.getReaggregationset();
              this.REAGGREGATIONGROUP = o.getReaggregationgroup();
              this.GROUPORDER = o.getGrouporder();
              this.AGGREGATIONORDER = o.getAggregationorder();
              this.AGGREGATIONTYPE = o.getAggregationtype();
              this.AGGREGATIONSCOPE = o.getAggregationscope();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Aggregation");
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
  public Aggregation(final RockFactory rockFact, final Aggregation whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Aggregation> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Aggregation o = it.next();
                this.AGGREGATION = o.getAggregation();
                this.VERSIONID = o.getVersionid();
                this.AGGREGATIONSET = o.getAggregationset();
                this.AGGREGATIONGROUP = o.getAggregationgroup();
                this.REAGGREGATIONSET = o.getReaggregationset();
                this.REAGGREGATIONGROUP = o.getReaggregationgroup();
                this.GROUPORDER = o.getGrouporder();
                this.AGGREGATIONORDER = o.getAggregationorder();
                this.AGGREGATIONTYPE = o.getAggregationtype();
                this.AGGREGATIONSCOPE = o.getAggregationscope();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Aggregation");
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
    return "Aggregation";
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
  public int updateDB(final boolean useTimestamp, final Aggregation whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Aggregation whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Aggregation.saveDB(), no primary key defined");
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
    sbuff.append("<Aggregation ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("AGGREGATIONSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSET),12, true)+"\" ");
        sbuff.append("AGGREGATIONGROUP=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONGROUP),12, true)+"\" ");
        sbuff.append("REAGGREGATIONSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REAGGREGATIONSET),12, true)+"\" ");
        sbuff.append("REAGGREGATIONGROUP=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REAGGREGATIONGROUP),12, true)+"\" ");
        sbuff.append("GROUPORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPORDER),4, true)+"\" ");
        sbuff.append("AGGREGATIONORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONORDER),4, true)+"\" ");
        sbuff.append("AGGREGATIONTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONTYPE),12, true)+"\" ");
        sbuff.append("AGGREGATIONSCOPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSCOPE),12, true)+"\" ");
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
    sbuff.append("<Aggregation ");
        sbuff.append("AGGREGATION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATION),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("AGGREGATIONSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSET),12, true)+"\" ");
        sbuff.append("AGGREGATIONGROUP=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONGROUP),12, true)+"\" ");
        sbuff.append("REAGGREGATIONSET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REAGGREGATIONSET),12, true)+"\" ");
        sbuff.append("REAGGREGATIONGROUP=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.REAGGREGATIONGROUP),12, true)+"\" ");
        sbuff.append("GROUPORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.GROUPORDER),4, true)+"\" ");
        sbuff.append("AGGREGATIONORDER=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONORDER),4, true)+"\" ");
        sbuff.append("AGGREGATIONTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONTYPE),12, true)+"\" ");
        sbuff.append("AGGREGATIONSCOPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.AGGREGATIONSCOPE),12, true)+"\" ");
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
    sbuff.append("</Aggregation>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Aggregation ( ");
	    		sbuff.append("AGGREGATION");
		    		sbuff.append(", VERSIONID");
	    		sbuff.append(", AGGREGATIONSET");
	    		sbuff.append(", AGGREGATIONGROUP");
	    		sbuff.append(", REAGGREGATIONSET");
	    		sbuff.append(", REAGGREGATIONGROUP");
	    		sbuff.append(", GROUPORDER");
	    		sbuff.append(", AGGREGATIONORDER");
	    		sbuff.append(", AGGREGATIONTYPE");
	    		sbuff.append(", AGGREGATIONSCOPE");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.AGGREGATION,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONSET,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONGROUP,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.REAGGREGATIONSET,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.REAGGREGATIONGROUP,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.GROUPORDER,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONORDER,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.AGGREGATIONSCOPE,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getAggregation() { 
    return this.AGGREGATION;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getAggregationset() { 
    return this.AGGREGATIONSET;
  }
   public String getAggregationgroup() { 
    return this.AGGREGATIONGROUP;
  }
   public String getReaggregationset() { 
    return this.REAGGREGATIONSET;
  }
   public String getReaggregationgroup() { 
    return this.REAGGREGATIONGROUP;
  }
   public Integer getGrouporder() { 
    return this.GROUPORDER;
  }
   public Integer getAggregationorder() { 
    return this.AGGREGATIONORDER;
  }
   public String getAggregationtype() { 
    return this.AGGREGATIONTYPE;
  }
   public String getAggregationscope() { 
    return this.AGGREGATIONSCOPE;
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
     if (AGGREGATION == null) {
      AGGREGATION = "";
    }
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (AGGREGATIONSET == null) {
      AGGREGATIONSET = "";
    }
     if (AGGREGATIONGROUP == null) {
      AGGREGATIONGROUP = "";
    }
     if (REAGGREGATIONSET == null) {
      REAGGREGATIONSET = "";
    }
     if (REAGGREGATIONGROUP == null) {
      REAGGREGATIONGROUP = "";
    }
     if (GROUPORDER == null) {
      GROUPORDER = new Integer (0);
    }
     if (AGGREGATIONORDER == null) {
      AGGREGATIONORDER = new Integer (0);
    }
     if (AGGREGATIONTYPE == null) {
      AGGREGATIONTYPE = "";
    }
     if (AGGREGATIONSCOPE == null) {
      AGGREGATIONSCOPE = "";
    }
   }

   public void setAggregation(final String AGGREGATION) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATION,"AGGREGATION",12,255,0);
    }
    modifiedColumns.add("AGGREGATION");
    this.AGGREGATION = AGGREGATION;
  }
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setAggregationset(final String AGGREGATIONSET) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONSET,"AGGREGATIONSET",12,100,0);
    }
    modifiedColumns.add("AGGREGATIONSET");
    this.AGGREGATIONSET = AGGREGATIONSET;
  }
   public void setAggregationgroup(final String AGGREGATIONGROUP) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONGROUP,"AGGREGATIONGROUP",12,100,0);
    }
    modifiedColumns.add("AGGREGATIONGROUP");
    this.AGGREGATIONGROUP = AGGREGATIONGROUP;
  }
   public void setReaggregationset(final String REAGGREGATIONSET) {
    if (validateData){
      DataValidator.validateData((Object)REAGGREGATIONSET,"REAGGREGATIONSET",12,100,0);
    }
    modifiedColumns.add("REAGGREGATIONSET");
    this.REAGGREGATIONSET = REAGGREGATIONSET;
  }
   public void setReaggregationgroup(final String REAGGREGATIONGROUP) {
    if (validateData){
      DataValidator.validateData((Object)REAGGREGATIONGROUP,"REAGGREGATIONGROUP",12,100,0);
    }
    modifiedColumns.add("REAGGREGATIONGROUP");
    this.REAGGREGATIONGROUP = REAGGREGATIONGROUP;
  }
   public void setGrouporder(final Integer GROUPORDER) {
    if (validateData){
      DataValidator.validateData((Object)GROUPORDER,"GROUPORDER",4,10,0);
    }
    modifiedColumns.add("GROUPORDER");
    this.GROUPORDER = GROUPORDER;
  }
   public void setAggregationorder(final Integer AGGREGATIONORDER) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONORDER,"AGGREGATIONORDER",4,10,0);
    }
    modifiedColumns.add("AGGREGATIONORDER");
    this.AGGREGATIONORDER = AGGREGATIONORDER;
  }
   public void setAggregationtype(final String AGGREGATIONTYPE) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONTYPE,"AGGREGATIONTYPE",12,50,0);
    }
    modifiedColumns.add("AGGREGATIONTYPE");
    this.AGGREGATIONTYPE = AGGREGATIONTYPE;
  }
   public void setAggregationscope(final String AGGREGATIONSCOPE) {
    if (validateData){
      DataValidator.validateData((Object)AGGREGATIONSCOPE,"AGGREGATIONSCOPE",12,50,0);
    }
    modifiedColumns.add("AGGREGATIONSCOPE");
    this.AGGREGATIONSCOPE = AGGREGATIONSCOPE;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Aggregation o) {

         if ((((this.AGGREGATION == null) || (o.AGGREGATION == null)) && (this.AGGREGATION != o.AGGREGATION))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
          ){
    return false;
    } else
         if ((((this.AGGREGATION != null) && (o.AGGREGATION != null)) && (this.AGGREGATION.equals(o.AGGREGATION) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
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
  public boolean equals(final Aggregation o) {

         if ((((this.AGGREGATION == null) || (o.AGGREGATION == null)) && (this.AGGREGATION != o.AGGREGATION))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.AGGREGATIONSET == null) || (o.AGGREGATIONSET == null)) && (this.AGGREGATIONSET != o.AGGREGATIONSET))
            || (((this.AGGREGATIONGROUP == null) || (o.AGGREGATIONGROUP == null)) && (this.AGGREGATIONGROUP != o.AGGREGATIONGROUP))
            || (((this.REAGGREGATIONSET == null) || (o.REAGGREGATIONSET == null)) && (this.REAGGREGATIONSET != o.REAGGREGATIONSET))
            || (((this.REAGGREGATIONGROUP == null) || (o.REAGGREGATIONGROUP == null)) && (this.REAGGREGATIONGROUP != o.REAGGREGATIONGROUP))
            || (((this.GROUPORDER == null) || (o.GROUPORDER == null)) && (this.GROUPORDER != o.GROUPORDER))
            || (((this.AGGREGATIONORDER == null) || (o.AGGREGATIONORDER == null)) && (this.AGGREGATIONORDER != o.AGGREGATIONORDER))
            || (((this.AGGREGATIONTYPE == null) || (o.AGGREGATIONTYPE == null)) && (this.AGGREGATIONTYPE != o.AGGREGATIONTYPE))
            || (((this.AGGREGATIONSCOPE == null) || (o.AGGREGATIONSCOPE == null)) && (this.AGGREGATIONSCOPE != o.AGGREGATIONSCOPE))
          ){
    return false;
    } else
         if ((((this.AGGREGATION != null) && (o.AGGREGATION != null)) && (this.AGGREGATION.equals(o.AGGREGATION) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.AGGREGATIONSET != null) && (o.AGGREGATIONSET != null)) && (this.AGGREGATIONSET.equals(o.AGGREGATIONSET) == false))
            || (((this.AGGREGATIONGROUP != null) && (o.AGGREGATIONGROUP != null)) && (this.AGGREGATIONGROUP.equals(o.AGGREGATIONGROUP) == false))
            || (((this.REAGGREGATIONSET != null) && (o.REAGGREGATIONSET != null)) && (this.REAGGREGATIONSET.equals(o.REAGGREGATIONSET) == false))
            || (((this.REAGGREGATIONGROUP != null) && (o.REAGGREGATIONGROUP != null)) && (this.REAGGREGATIONGROUP.equals(o.REAGGREGATIONGROUP) == false))
            || (((this.GROUPORDER != null) && (o.GROUPORDER != null)) && (this.GROUPORDER.equals(o.GROUPORDER) == false))
            || (((this.AGGREGATIONORDER != null) && (o.AGGREGATIONORDER != null)) && (this.AGGREGATIONORDER.equals(o.AGGREGATIONORDER) == false))
            || (((this.AGGREGATIONTYPE != null) && (o.AGGREGATIONTYPE != null)) && (this.AGGREGATIONTYPE.equals(o.AGGREGATIONTYPE) == false))
            || (((this.AGGREGATIONSCOPE != null) && (o.AGGREGATIONSCOPE != null)) && (this.AGGREGATIONSCOPE.equals(o.AGGREGATIONSCOPE) == false))
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
  public static int getAggregationColumnSize() {
    
     return 255;   
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
  * return 100
  */
  public static int getAggregationsetColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationsetDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationsetSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getAggregationgroupColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationgroupDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationgroupSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getReaggregationsetColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getReaggregationsetDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getReaggregationsetSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 100
  */
  public static int getReaggregationgroupColumnSize() {
    
     return 100;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getReaggregationgroupDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getReaggregationgroupSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getGrouporderColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getGrouporderDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getGrouporderSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getAggregationorderColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationorderDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getAggregationorderSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getAggregationtypeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationtypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationtypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getAggregationscopeColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getAggregationscopeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getAggregationscopeSQLType() {
    
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

  public Aggregation getOriginal() {
    return original;
  }
   
  public void setOriginal(final Aggregation original) {
    this.original = (Aggregation) original.clone();
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
