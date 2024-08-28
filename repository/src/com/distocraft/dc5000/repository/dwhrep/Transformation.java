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
public class Transformation implements Cloneable,RockDBObject  {

    private String TRANSFORMERID;
    private Long ORDERNO;
    private String TYPE;
    private String SOURCE;
    private String TARGET;
    private String CONFIG;
    private String DESCRIPTION;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TRANSFORMERID"    ,"ORDERNO"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Transformation original; 
  private StringBuffer currentTrans = new StringBuffer();
  private StringBuffer otherTrans = new StringBuffer();
  
  public Transformation() {
	  // for testing only.
  }

  public Transformation(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Transformation(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TRANSFORMERID = null;
         this.ORDERNO = null;
         this.TYPE = null;
         this.SOURCE = null;
         this.TARGET = null;
         this.CONFIG = null;
         this.DESCRIPTION = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Transformation(final RockFactory rockFact   ,final String TRANSFORMERID ,final Long ORDERNO ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TRANSFORMERID = TRANSFORMERID;
            this.ORDERNO = ORDERNO;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Transformation> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Transformation o = it.next();

              this.TRANSFORMERID = o.getTransformerid();
              this.ORDERNO = o.getOrderno();
              this.TYPE = o.getType();
              this.SOURCE = o.getSource();
              this.TARGET = o.getTarget();
              this.CONFIG = o.getConfig();
              this.DESCRIPTION = o.getDescription();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Transformation");
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
  public Transformation(final RockFactory rockFact, final Transformation whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Transformation> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Transformation o = it.next();
                this.TRANSFORMERID = o.getTransformerid();
                this.ORDERNO = o.getOrderno();
                this.TYPE = o.getType();
                this.SOURCE = o.getSource();
                this.TARGET = o.getTarget();
                this.CONFIG = o.getConfig();
                this.DESCRIPTION = o.getDescription();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Transformation");
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
    return "Transformation";
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
  public int updateDB(final boolean useTimestamp, final Transformation whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Transformation whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Transformation.saveDB(), no primary key defined");
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
    sbuff.append("<Transformation ");
        sbuff.append("TRANSFORMERID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMERID),12, true)+"\" ");
        sbuff.append("ORDERNO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNO),2, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("SOURCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE),12, true)+"\" ");
        sbuff.append("TARGET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET),12, true)+"\" ");
        sbuff.append("CONFIG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONFIG),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
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
    sbuff.append("<Transformation ");
        sbuff.append("TRANSFORMERID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TRANSFORMERID),12, true)+"\" ");
        sbuff.append("ORDERNO=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ORDERNO),2, true)+"\" ");
        sbuff.append("TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPE),12, true)+"\" ");
        sbuff.append("SOURCE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SOURCE),12, true)+"\" ");
        sbuff.append("TARGET=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TARGET),12, true)+"\" ");
        sbuff.append("CONFIG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONFIG),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
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
    sbuff.append("</Transformation>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Transformation ( ");
	    		sbuff.append("TRANSFORMERID");
		    		sbuff.append(", ORDERNO");
	    		sbuff.append(", TYPE");
	    		sbuff.append(", SOURCE");
	    		sbuff.append(", TARGET");
	    		sbuff.append(", CONFIG");
	    		sbuff.append(", DESCRIPTION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TRANSFORMERID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.ORDERNO,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SOURCE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TARGET,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONFIG,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTransformerid() { 
    return this.TRANSFORMERID;
  }
   public Long getOrderno() { 
    return this.ORDERNO;
  }
   public String getType() { 
    return this.TYPE;
  }
   public String getSource() { 
    return this.SOURCE;
  }
   public String getTarget() { 
    return this.TARGET;
  }
   public String getConfig() { 
    return this.CONFIG;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
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
     if (TRANSFORMERID == null) {
      TRANSFORMERID = "";
    }
     if (ORDERNO == null) {
      ORDERNO = new Long (0);
    }
     if (TYPE == null) {
      TYPE = "";
    }
     if (SOURCE == null) {
      SOURCE = "";
    }
     if (TARGET == null) {
      TARGET = "";
    }
     if (CONFIG == null) {
      CONFIG = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
   }

   public void setTransformerid(final String TRANSFORMERID) {
    if (validateData){
      DataValidator.validateData((Object)TRANSFORMERID,"TRANSFORMERID",12,255,0);
    }
    modifiedColumns.add("TRANSFORMERID");
    this.TRANSFORMERID = TRANSFORMERID;
  }
   public void setOrderno(final Long ORDERNO) {
    if (validateData){
      DataValidator.validateData((Object)ORDERNO,"ORDERNO",2,9,0);
    }
    modifiedColumns.add("ORDERNO");
    this.ORDERNO = ORDERNO;
  }
   public void setType(final String TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TYPE,"TYPE",12,128,0);
    }
    modifiedColumns.add("TYPE");
    this.TYPE = TYPE;
  }
   public void setSource(final String SOURCE) {
    if (validateData){
      DataValidator.validateData((Object)SOURCE,"SOURCE",12,128,0);
    }
    modifiedColumns.add("SOURCE");
    this.SOURCE = SOURCE;
  }
   public void setTarget(final String TARGET) {
    if (validateData){
      DataValidator.validateData((Object)TARGET,"TARGET",12,128,0);
    }
    modifiedColumns.add("TARGET");
    this.TARGET = TARGET;
  }
   public void setConfig(final String CONFIG) {
    if (validateData){
      DataValidator.validateData((Object)CONFIG,"CONFIG",12,32000,0);
    }
    modifiedColumns.add("CONFIG");
    this.CONFIG = CONFIG;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Transformation o) {

         if ((((this.TRANSFORMERID == null) || (o.TRANSFORMERID == null)) && (this.TRANSFORMERID != o.TRANSFORMERID))
            || (((this.ORDERNO == null) || (o.ORDERNO == null)) && (this.ORDERNO != o.ORDERNO))
          ){
    return false;
    } else
         if ((((this.TRANSFORMERID != null) && (o.TRANSFORMERID != null)) && (this.TRANSFORMERID.equals(o.TRANSFORMERID) == false))
            || (((this.ORDERNO != null) && (o.ORDERNO != null)) && (this.ORDERNO.equals(o.ORDERNO) == false))
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
  public boolean equals(final Transformation o) {

         if ((((this.TRANSFORMERID == null) || (o.TRANSFORMERID == null)) && (this.TRANSFORMERID != o.TRANSFORMERID))
            || (((this.ORDERNO == null) || (o.ORDERNO == null)) && (this.ORDERNO != o.ORDERNO))
            || (((this.TYPE == null) || (o.TYPE == null)) && (this.TYPE != o.TYPE))
            || (((this.SOURCE == null) || (o.SOURCE == null)) && (this.SOURCE != o.SOURCE))
            || (((this.TARGET == null) || (o.TARGET == null)) && (this.TARGET != o.TARGET))
            || (((this.CONFIG == null) || (o.CONFIG == null)) && (this.CONFIG != o.CONFIG))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
          ){
    return false;
    } else
         if ((((this.TRANSFORMERID != null) && (o.TRANSFORMERID != null)) && (this.TRANSFORMERID.equals(o.TRANSFORMERID) == false))
            || (((this.ORDERNO != null) && (o.ORDERNO != null)) && (this.ORDERNO.equals(o.ORDERNO) == false))
            || (((this.TYPE != null) && (o.TYPE != null)) && (this.TYPE.equals(o.TYPE) == false))
            || (((this.SOURCE != null) && (o.SOURCE != null)) && (this.SOURCE.equals(o.SOURCE) == false))
            || (((this.TARGET != null) && (o.TARGET != null)) && (this.TARGET.equals(o.TARGET) == false))
            || (((this.CONFIG != null) && (o.CONFIG != null)) && (this.CONFIG.equals(o.CONFIG) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
          ){
    return false;
    } else {
      return true;
    }
  }
  
//  @Override
//  public int hashCode() {
//    final int prime = 31;
//    int result = 1;
//    result = prime * result + ((CONFIG == null) ? 0 : CONFIG.hashCode());
//    result = prime * result + ((SOURCE == null) ? 0 : SOURCE.hashCode());
//    result = prime * result + ((TARGET == null) ? 0 : TARGET.hashCode());
//    result = prime * result + ((TYPE == null) ? 0 : TYPE.hashCode());
//    return result;
//  }
  
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
  public static int getTransformeridColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTransformeridDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTransformeridSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getOrdernoColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOrdernoDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getOrdernoSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getTypeColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getSourceColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSourceDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSourceSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getTargetColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTargetDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTargetSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32000
  */
  public static int getConfigColumnSize() {
    
     return 32000;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConfigDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConfigSQLType() {
    
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

  public Transformation getOriginal() {
    return original;
  }
   
  public void setOriginal(final Transformation original) {
    this.original = (Transformation) original.clone();
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
