package com.distocraft.dc5000.repository.dwhrep;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import ssc.rockfactory.DataValidator;
import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockDBObject;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

@SuppressWarnings({"PMD.SuspiciousConstantFieldName"}) // eeipca : columns are in UPPERCASE
public class Countingmanagement implements Cloneable,RockDBObject  {

    private String STORAGEID;
    private String TABLENAME;
    private Long LASTAGGREGATEDROW;

  private final String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TABLENAME"   };

  private RockFactory rockFact;

  private boolean newItem;

  private Set<String> modifiedColumns = new HashSet<String>();

  private boolean validateData = false;

  private Countingmanagement original;

  public Countingmanagement(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null;
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Countingmanagement(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;

         this.STORAGEID = null;
         this.TABLENAME = null;
         this.LASTAGGREGATEDROW = null;
      	original = null;

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   *
   * @params primarykeys
   * @exception SQLException
   */
  public Countingmanagement(final RockFactory rockFact   ,final String TABLENAME ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TABLENAME = TABLENAME;

      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Countingmanagement> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Countingmanagement o = it.next();

              this.STORAGEID = o.getStorageid();
              this.TABLENAME = o.getTablename();
              this.LASTAGGREGATEDROW = o.getLastaggregatedrow();

        results.close();
        this.newItem = false;
  	    this.original = this;
      } else {
        results.close();
  	    this.original = this;
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Countingmanagement");
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
  public Countingmanagement(final RockFactory rockFact, final Countingmanagement whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Countingmanagement> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Countingmanagement o = it.next();
                this.STORAGEID = o.getStorageid();
                this.TABLENAME = o.getTablename();
                this.LASTAGGREGATEDROW = o.getLastaggregatedrow();
                results.close();
        this.newItem = false;
  	    this.original = this;
      } else {
        results.close();
  	    this.original = this;
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Countingmanagement");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  @Override
  public Set<String> gimmeModifiedColumns(){
    return modifiedColumns;
  }

  public void setModifiedColumns(final Set<String> modifiedColumns){
    this.modifiedColumns = modifiedColumns;
  }

  @Override
  public void cleanModifiedColumns(){
    modifiedColumns.clear();
  }

  /**
   * Method for getting the table name
   *
   * @return String name of the corresponding table
   */
  public String getTableName() {
    return "Countingmanagement";
  }

  /**
   * Update object contents into database PRIMARY KEY MUST BE DEFINED
   *
   * @exception SQLException
   */
  @Override
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
  public int updateDB(final boolean useTimestamp, final Countingmanagement whereObject) throws SQLException, RockException {
    this.newItem = false;
    return rockFact.updateData(this, false, whereObject, useTimestamp);
  }

  /**
   * Insert object contents into database
   *
   * @exception SQLException
   */
  @Override
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
  @Override
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
  public int deleteDB(final Countingmanagement whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Countingmanagement.saveDB(), no primary key defined");
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
    sbuff.append("<Countingmanagement ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("TABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLENAME),12, true)+"\" ");
        sbuff.append("LASTAGGREGATEDROW=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LASTAGGREGATEDROW),-5, true)+"\" ");
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
    sbuff.append("<Countingmanagement ");
        sbuff.append("STORAGEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STORAGEID),12, true)+"\" ");
        sbuff.append("TABLENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLENAME),12, true)+"\" ");
        sbuff.append("LASTAGGREGATEDROW=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LASTAGGREGATEDROW),-5, true)+"\" ");
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
    sbuff.append("</Countingmanagement>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   *
   * @exception SQLException
   */

  public String toSQLInsert(){

    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Countingmanagement ( ");
	    		sbuff.append("STORAGEID");
		    		sbuff.append(", TABLENAME");
	    		sbuff.append(", LASTAGGREGATEDROW");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.STORAGEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TABLENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LASTAGGREGATEDROW,-5)+"");
    	    sbuff.append(" );\n");
    return sbuff.toString();
  }


   public String getStorageid() {
    return this.STORAGEID;
  }
   public String getTablename() {
    return this.TABLENAME;
  }
   public Long getLastaggregatedrow() {
    return this.LASTAGGREGATEDROW;
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
     if (STORAGEID == null) {
      STORAGEID = "";
    }
     if (TABLENAME == null) {
      TABLENAME = "";
    }
     if (LASTAGGREGATEDROW == null) {
      LASTAGGREGATEDROW = new Long (0);
    }
   }

   public void setStorageid(final String STORAGEID) {
    if (validateData){
      DataValidator.validateData(STORAGEID,"STORAGEID",12,255,0);
    }
    modifiedColumns.add("STORAGEID");
    this.STORAGEID = STORAGEID;
  }
   public void setTablename(final String TABLENAME) {
    if (validateData){
      DataValidator.validateData(TABLENAME,"TABLENAME",12,255,0);
    }
    modifiedColumns.add("TABLENAME");
    this.TABLENAME = TABLENAME;
  }
   public void setLastaggregatedrow(final Long LASTAGGREGATEDROW) {
    if (validateData){
      DataValidator.validateData(LASTAGGREGATEDROW,"LASTAGGREGATEDROW",-5,19,0);
    }
    modifiedColumns.add("LASTAGGREGATEDROW");
    this.LASTAGGREGATEDROW = LASTAGGREGATEDROW;
  }




  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Countingmanagement o) {

         if ((((this.TABLENAME == null) || (o.TABLENAME == null)) && (this.TABLENAME != o.TABLENAME))
          ){
    return false;
    } else
         if ((((this.TABLENAME != null) && (o.TABLENAME != null)) && (this.TABLENAME.equals(o.TABLENAME) == false))
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
  public boolean equals(final Countingmanagement o) {

         if ((((this.STORAGEID == null) || (o.STORAGEID == null)) && (this.STORAGEID != o.STORAGEID))
            || (((this.TABLENAME == null) || (o.TABLENAME == null)) && (this.TABLENAME != o.TABLENAME))
            || (((this.LASTAGGREGATEDROW == null) || (o.LASTAGGREGATEDROW == null)) && (this.LASTAGGREGATEDROW != o.LASTAGGREGATEDROW))
          ){
    return false;
    } else
         if ((((this.STORAGEID != null) && (o.STORAGEID != null)) && (this.STORAGEID.equals(o.STORAGEID) == false))
            || (((this.TABLENAME != null) && (o.TABLENAME != null)) && (this.TABLENAME.equals(o.TABLENAME) == false))
            || (((this.LASTAGGREGATEDROW != null) && (o.LASTAGGREGATEDROW != null)) && (this.LASTAGGREGATEDROW.equals(o.LASTAGGREGATEDROW) == false))
          ){
    return false;
    } else {
      return true;
    }
  }

  /**
   * to enable a public clone method inherited from Object class (private method)
   */
  @Override
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
  public static int getStorageidColumnSize() {

     return 255;
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStorageidDecimalDigits() {

     return 0;
  }

 /**
  * get SQLType
  * return 12
  */
  public static int getStorageidSQLType() {

    return 12;
  }


  /**
  * get columnSize
  * return 255
  */
  public static int getTablenameColumnSize() {

     return 255;
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTablenameDecimalDigits() {

     return 0;
  }

 /**
  * get SQLType
  * return 12
  */
  public static int getTablenameSQLType() {

    return 12;
  }


  /**
  * get columnSize
  * return 19
  */
  public static int getLastaggregatedrowColumnSize() {

     return 19;
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLastaggregatedrowDecimalDigits() {

     return 0;
  }

 /**
  * get SQLType
  * return -5
  */
  public static int getLastaggregatedrowSQLType() {

    return -5;
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

  public Countingmanagement getOriginal() {
    return original;
  }

  public void setOriginal(final Countingmanagement original) {
    this.original = (Countingmanagement) original.clone();
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

  /**
   * Method to set a new RockFactory object.
   */
  public void setRockFactory(RockFactory rockFactoryObject) {
    this.rockFact = rockFactoryObject;
  }


}
