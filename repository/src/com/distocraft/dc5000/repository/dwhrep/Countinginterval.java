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
public class Countinginterval implements Cloneable,RockDBObject  {

    private Integer INTERVAL;
    private String INTERVALNAME;
    private String INTERVALDESCRIPTION;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "INTERVAL"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Countinginterval original; 

  public Countinginterval(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Countinginterval(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.INTERVAL = null;
         this.INTERVALNAME = null;
         this.INTERVALDESCRIPTION = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Countinginterval(final RockFactory rockFact   ,final Integer INTERVAL ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.INTERVAL = INTERVAL;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Countinginterval> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Countinginterval o = it.next();

              this.INTERVAL = o.getInterval();
              this.INTERVALNAME = o.getIntervalname();
              this.INTERVALDESCRIPTION = o.getIntervaldescription();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Countinginterval");
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
  public Countinginterval(final RockFactory rockFact, final Countinginterval whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Countinginterval> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Countinginterval o = it.next();
                this.INTERVAL = o.getInterval();
                this.INTERVALNAME = o.getIntervalname();
                this.INTERVALDESCRIPTION = o.getIntervaldescription();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Countinginterval");
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
    return "Countinginterval";
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
  public int updateDB(final boolean useTimestamp, final Countinginterval whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Countinginterval whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Countinginterval.saveDB(), no primary key defined");
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
    sbuff.append("<Countinginterval ");
        sbuff.append("INTERVAL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL),4, true)+"\" ");
        sbuff.append("INTERVALNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVALNAME),12, true)+"\" ");
        sbuff.append("INTERVALDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVALDESCRIPTION),12, true)+"\" ");
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
    sbuff.append("<Countinginterval ");
        sbuff.append("INTERVAL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVAL),4, true)+"\" ");
        sbuff.append("INTERVALNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVALNAME),12, true)+"\" ");
        sbuff.append("INTERVALDESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.INTERVALDESCRIPTION),12, true)+"\" ");
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
    sbuff.append("</Countinginterval>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Countinginterval ( ");
	    		sbuff.append("INTERVAL");
		    		sbuff.append(", INTERVALNAME");
	    		sbuff.append(", INTERVALDESCRIPTION");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.INTERVAL,4)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.INTERVALNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.INTERVALDESCRIPTION,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public Integer getInterval() { 
    return this.INTERVAL;
  }
   public String getIntervalname() { 
    return this.INTERVALNAME;
  }
   public String getIntervaldescription() { 
    return this.INTERVALDESCRIPTION;
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
     if (INTERVAL == null) {
      INTERVAL = new Integer (0);
    }
     if (INTERVALNAME == null) {
      INTERVALNAME = "";
    }
     if (INTERVALDESCRIPTION == null) {
      INTERVALDESCRIPTION = "";
    }
   }

   public void setInterval(final Integer INTERVAL) {
    if (validateData){
      DataValidator.validateData((Object)INTERVAL,"INTERVAL",4,10,0);
    }
    modifiedColumns.add("INTERVAL");
    this.INTERVAL = INTERVAL;
  }
   public void setIntervalname(final String INTERVALNAME) {
    if (validateData){
      DataValidator.validateData((Object)INTERVALNAME,"INTERVALNAME",12,16,0);
    }
    modifiedColumns.add("INTERVALNAME");
    this.INTERVALNAME = INTERVALNAME;
  }
   public void setIntervaldescription(final String INTERVALDESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)INTERVALDESCRIPTION,"INTERVALDESCRIPTION",12,255,0);
    }
    modifiedColumns.add("INTERVALDESCRIPTION");
    this.INTERVALDESCRIPTION = INTERVALDESCRIPTION;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Countinginterval o) {

         if ((((this.INTERVAL == null) || (o.INTERVAL == null)) && (this.INTERVAL != o.INTERVAL))
          ){
    return false;
    } else
         if ((((this.INTERVAL != null) && (o.INTERVAL != null)) && (this.INTERVAL.equals(o.INTERVAL) == false))
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
  public boolean equals(final Countinginterval o) {

         if ((((this.INTERVAL == null) || (o.INTERVAL == null)) && (this.INTERVAL != o.INTERVAL))
            || (((this.INTERVALNAME == null) || (o.INTERVALNAME == null)) && (this.INTERVALNAME != o.INTERVALNAME))
            || (((this.INTERVALDESCRIPTION == null) || (o.INTERVALDESCRIPTION == null)) && (this.INTERVALDESCRIPTION != o.INTERVALDESCRIPTION))
          ){
    return false;
    } else
         if ((((this.INTERVAL != null) && (o.INTERVAL != null)) && (this.INTERVAL.equals(o.INTERVAL) == false))
            || (((this.INTERVALNAME != null) && (o.INTERVALNAME != null)) && (this.INTERVALNAME.equals(o.INTERVALNAME) == false))
            || (((this.INTERVALDESCRIPTION != null) && (o.INTERVALDESCRIPTION != null)) && (this.INTERVALDESCRIPTION.equals(o.INTERVALDESCRIPTION) == false))
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
  * return 10
  */
  public static int getIntervalColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIntervalDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getIntervalSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getIntervalnameColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIntervalnameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIntervalnameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getIntervaldescriptionColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getIntervaldescriptionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getIntervaldescriptionSQLType() {
    
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

  public Countinginterval getOriginal() {
    return original;
  }
   
  public void setOriginal(final Countinginterval original) {
    this.original = (Countinginterval) original.clone();
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
