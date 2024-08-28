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
public class Verificationcondition implements Cloneable,RockDBObject  {

    private String VERSIONID;
    private String FACTTABLE;
    private String VERLEVEL;
    private String CONDITIONCLASS;
    private String VERCONDITION;
    private String PROMPTNAME1;
    private String PROMPTVALUE1;
    private String PROMPTNAME2;
    private String PROMPTVALUE2;
    private String OBJECTCONDITION;
    private String PROMPTNAME3;
    private String PROMPTVALUE3;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "VERSIONID"    ,"VERLEVEL"    ,"CONDITIONCLASS"    ,"VERCONDITION"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Verificationcondition original; 

  public Verificationcondition(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Verificationcondition(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.VERSIONID = null;
         this.FACTTABLE = null;
         this.VERLEVEL = null;
         this.CONDITIONCLASS = null;
         this.VERCONDITION = null;
         this.PROMPTNAME1 = null;
         this.PROMPTVALUE1 = null;
         this.PROMPTNAME2 = null;
         this.PROMPTVALUE2 = null;
         this.OBJECTCONDITION = null;
         this.PROMPTNAME3 = null;
         this.PROMPTVALUE3 = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Verificationcondition(final RockFactory rockFact   ,final String VERSIONID ,final String VERLEVEL ,final String CONDITIONCLASS ,final String VERCONDITION ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.VERSIONID = VERSIONID;
            this.VERLEVEL = VERLEVEL;
            this.CONDITIONCLASS = CONDITIONCLASS;
            this.VERCONDITION = VERCONDITION;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Verificationcondition> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Verificationcondition o = it.next();

              this.VERSIONID = o.getVersionid();
              this.FACTTABLE = o.getFacttable();
              this.VERLEVEL = o.getVerlevel();
              this.CONDITIONCLASS = o.getConditionclass();
              this.VERCONDITION = o.getVercondition();
              this.PROMPTNAME1 = o.getPromptname1();
              this.PROMPTVALUE1 = o.getPromptvalue1();
              this.PROMPTNAME2 = o.getPromptname2();
              this.PROMPTVALUE2 = o.getPromptvalue2();
              this.OBJECTCONDITION = o.getObjectcondition();
              this.PROMPTNAME3 = o.getPromptname3();
              this.PROMPTVALUE3 = o.getPromptvalue3();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Verificationcondition");
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
  public Verificationcondition(final RockFactory rockFact, final Verificationcondition whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Verificationcondition> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Verificationcondition o = it.next();
                this.VERSIONID = o.getVersionid();
                this.FACTTABLE = o.getFacttable();
                this.VERLEVEL = o.getVerlevel();
                this.CONDITIONCLASS = o.getConditionclass();
                this.VERCONDITION = o.getVercondition();
                this.PROMPTNAME1 = o.getPromptname1();
                this.PROMPTVALUE1 = o.getPromptvalue1();
                this.PROMPTNAME2 = o.getPromptname2();
                this.PROMPTVALUE2 = o.getPromptvalue2();
                this.OBJECTCONDITION = o.getObjectcondition();
                this.PROMPTNAME3 = o.getPromptname3();
                this.PROMPTVALUE3 = o.getPromptvalue3();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Verificationcondition");
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
    return "Verificationcondition";
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
  public int updateDB(final boolean useTimestamp, final Verificationcondition whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Verificationcondition whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Verificationcondition.saveDB(), no primary key defined");
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
    sbuff.append("<Verificationcondition ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("FACTTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FACTTABLE),12, true)+"\" ");
        sbuff.append("VERLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERLEVEL),12, true)+"\" ");
        sbuff.append("CONDITIONCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONDITIONCLASS),12, true)+"\" ");
        sbuff.append("VERCONDITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERCONDITION),12, true)+"\" ");
        sbuff.append("PROMPTNAME1=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME1),12, true)+"\" ");
        sbuff.append("PROMPTVALUE1=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE1),12, true)+"\" ");
        sbuff.append("PROMPTNAME2=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME2),12, true)+"\" ");
        sbuff.append("PROMPTVALUE2=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE2),12, true)+"\" ");
        sbuff.append("OBJECTCONDITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTCONDITION),12, true)+"\" ");
        sbuff.append("PROMPTNAME3=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME3),12, true)+"\" ");
        sbuff.append("PROMPTVALUE3=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE3),12, true)+"\" ");
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
    sbuff.append("<Verificationcondition ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("FACTTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FACTTABLE),12, true)+"\" ");
        sbuff.append("VERLEVEL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERLEVEL),12, true)+"\" ");
        sbuff.append("CONDITIONCLASS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.CONDITIONCLASS),12, true)+"\" ");
        sbuff.append("VERCONDITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERCONDITION),12, true)+"\" ");
        sbuff.append("PROMPTNAME1=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME1),12, true)+"\" ");
        sbuff.append("PROMPTVALUE1=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE1),12, true)+"\" ");
        sbuff.append("PROMPTNAME2=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME2),12, true)+"\" ");
        sbuff.append("PROMPTVALUE2=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE2),12, true)+"\" ");
        sbuff.append("OBJECTCONDITION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTCONDITION),12, true)+"\" ");
        sbuff.append("PROMPTNAME3=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTNAME3),12, true)+"\" ");
        sbuff.append("PROMPTVALUE3=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PROMPTVALUE3),12, true)+"\" ");
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
    sbuff.append("</Verificationcondition>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Verificationcondition ( ");
	    		sbuff.append("VERSIONID");
		    		sbuff.append(", FACTTABLE");
	    		sbuff.append(", VERLEVEL");
	    		sbuff.append(", CONDITIONCLASS");
	    		sbuff.append(", VERCONDITION");
	    		sbuff.append(", PROMPTNAME1");
	    		sbuff.append(", PROMPTVALUE1");
	    		sbuff.append(", PROMPTNAME2");
	    		sbuff.append(", PROMPTVALUE2");
	    		sbuff.append(", OBJECTCONDITION");
	    		sbuff.append(", PROMPTNAME3");
	    		sbuff.append(", PROMPTVALUE3");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.VERSIONID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.FACTTABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERLEVEL,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.CONDITIONCLASS,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERCONDITION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTNAME1,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTVALUE1,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTNAME2,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTVALUE2,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTCONDITION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTNAME3,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PROMPTVALUE3,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getFacttable() { 
    return this.FACTTABLE;
  }
   public String getVerlevel() { 
    return this.VERLEVEL;
  }
   public String getConditionclass() { 
    return this.CONDITIONCLASS;
  }
   public String getVercondition() { 
    return this.VERCONDITION;
  }
   public String getPromptname1() { 
    return this.PROMPTNAME1;
  }
   public String getPromptvalue1() { 
    return this.PROMPTVALUE1;
  }
   public String getPromptname2() { 
    return this.PROMPTNAME2;
  }
   public String getPromptvalue2() { 
    return this.PROMPTVALUE2;
  }
   public String getObjectcondition() { 
    return this.OBJECTCONDITION;
  }
   public String getPromptname3() { 
    return this.PROMPTNAME3;
  }
   public String getPromptvalue3() { 
    return this.PROMPTVALUE3;
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
     if (FACTTABLE == null) {
      FACTTABLE = "";
    }
     if (VERLEVEL == null) {
      VERLEVEL = "";
    }
     if (CONDITIONCLASS == null) {
      CONDITIONCLASS = "";
    }
     if (VERCONDITION == null) {
      VERCONDITION = "";
    }
     if (PROMPTNAME1 == null) {
      PROMPTNAME1 = "";
    }
     if (PROMPTVALUE1 == null) {
      PROMPTVALUE1 = "";
    }
     if (PROMPTNAME2 == null) {
      PROMPTNAME2 = "";
    }
     if (PROMPTVALUE2 == null) {
      PROMPTVALUE2 = "";
    }
     if (OBJECTCONDITION == null) {
      OBJECTCONDITION = "";
    }
     if (PROMPTNAME3 == null) {
      PROMPTNAME3 = "";
    }
     if (PROMPTVALUE3 == null) {
      PROMPTVALUE3 = "";
    }
   }

   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setFacttable(final String FACTTABLE) {
    if (validateData){
      DataValidator.validateData((Object)FACTTABLE,"FACTTABLE",12,2560,0);
    }
    modifiedColumns.add("FACTTABLE");
    this.FACTTABLE = FACTTABLE;
  }
   public void setVerlevel(final String VERLEVEL) {
    if (validateData){
      DataValidator.validateData((Object)VERLEVEL,"VERLEVEL",12,32,0);
    }
    modifiedColumns.add("VERLEVEL");
    this.VERLEVEL = VERLEVEL;
  }
   public void setConditionclass(final String CONDITIONCLASS) {
    if (validateData){
      DataValidator.validateData((Object)CONDITIONCLASS,"CONDITIONCLASS",12,32,0);
    }
    modifiedColumns.add("CONDITIONCLASS");
    this.CONDITIONCLASS = CONDITIONCLASS;
  }
   public void setVercondition(final String VERCONDITION) {
    if (validateData){
      DataValidator.validateData((Object)VERCONDITION,"VERCONDITION",12,128,0);
    }
    modifiedColumns.add("VERCONDITION");
    this.VERCONDITION = VERCONDITION;
  }
   public void setPromptname1(final String PROMPTNAME1) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTNAME1,"PROMPTNAME1",12,255,0);
    }
    modifiedColumns.add("PROMPTNAME1");
    this.PROMPTNAME1 = PROMPTNAME1;
  }
   public void setPromptvalue1(final String PROMPTVALUE1) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTVALUE1,"PROMPTVALUE1",12,128,0);
    }
    modifiedColumns.add("PROMPTVALUE1");
    this.PROMPTVALUE1 = PROMPTVALUE1;
  }
   public void setPromptname2(final String PROMPTNAME2) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTNAME2,"PROMPTNAME2",12,255,0);
    }
    modifiedColumns.add("PROMPTNAME2");
    this.PROMPTNAME2 = PROMPTNAME2;
  }
   public void setPromptvalue2(final String PROMPTVALUE2) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTVALUE2,"PROMPTVALUE2",12,128,0);
    }
    modifiedColumns.add("PROMPTVALUE2");
    this.PROMPTVALUE2 = PROMPTVALUE2;
  }
   public void setObjectcondition(final String OBJECTCONDITION) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTCONDITION,"OBJECTCONDITION",12,255,0);
    }
    modifiedColumns.add("OBJECTCONDITION");
    this.OBJECTCONDITION = OBJECTCONDITION;
  }
   public void setPromptname3(final String PROMPTNAME3) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTNAME3,"PROMPTNAME3",12,255,0);
    }
    modifiedColumns.add("PROMPTNAME3");
    this.PROMPTNAME3 = PROMPTNAME3;
  }
   public void setPromptvalue3(final String PROMPTVALUE3) {
    if (validateData){
      DataValidator.validateData((Object)PROMPTVALUE3,"PROMPTVALUE3",12,128,0);
    }
    modifiedColumns.add("PROMPTVALUE3");
    this.PROMPTVALUE3 = PROMPTVALUE3;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Verificationcondition o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.VERLEVEL == null) || (o.VERLEVEL == null)) && (this.VERLEVEL != o.VERLEVEL))
            || (((this.CONDITIONCLASS == null) || (o.CONDITIONCLASS == null)) && (this.CONDITIONCLASS != o.CONDITIONCLASS))
            || (((this.VERCONDITION == null) || (o.VERCONDITION == null)) && (this.VERCONDITION != o.VERCONDITION))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.VERLEVEL != null) && (o.VERLEVEL != null)) && (this.VERLEVEL.equals(o.VERLEVEL) == false))
            || (((this.CONDITIONCLASS != null) && (o.CONDITIONCLASS != null)) && (this.CONDITIONCLASS.equals(o.CONDITIONCLASS) == false))
            || (((this.VERCONDITION != null) && (o.VERCONDITION != null)) && (this.VERCONDITION.equals(o.VERCONDITION) == false))
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
  public boolean equals(final Verificationcondition o) {

         if ((((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.FACTTABLE == null) || (o.FACTTABLE == null)) && (this.FACTTABLE != o.FACTTABLE))
            || (((this.VERLEVEL == null) || (o.VERLEVEL == null)) && (this.VERLEVEL != o.VERLEVEL))
            || (((this.CONDITIONCLASS == null) || (o.CONDITIONCLASS == null)) && (this.CONDITIONCLASS != o.CONDITIONCLASS))
            || (((this.VERCONDITION == null) || (o.VERCONDITION == null)) && (this.VERCONDITION != o.VERCONDITION))
            || (((this.PROMPTNAME1 == null) || (o.PROMPTNAME1 == null)) && (this.PROMPTNAME1 != o.PROMPTNAME1))
            || (((this.PROMPTVALUE1 == null) || (o.PROMPTVALUE1 == null)) && (this.PROMPTVALUE1 != o.PROMPTVALUE1))
            || (((this.PROMPTNAME2 == null) || (o.PROMPTNAME2 == null)) && (this.PROMPTNAME2 != o.PROMPTNAME2))
            || (((this.PROMPTVALUE2 == null) || (o.PROMPTVALUE2 == null)) && (this.PROMPTVALUE2 != o.PROMPTVALUE2))
            || (((this.OBJECTCONDITION == null) || (o.OBJECTCONDITION == null)) && (this.OBJECTCONDITION != o.OBJECTCONDITION))
            || (((this.PROMPTNAME3 == null) || (o.PROMPTNAME3 == null)) && (this.PROMPTNAME3 != o.PROMPTNAME3))
            || (((this.PROMPTVALUE3 == null) || (o.PROMPTVALUE3 == null)) && (this.PROMPTVALUE3 != o.PROMPTVALUE3))
          ){
    return false;
    } else
         if ((((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.FACTTABLE != null) && (o.FACTTABLE != null)) && (this.FACTTABLE.equals(o.FACTTABLE) == false))
            || (((this.VERLEVEL != null) && (o.VERLEVEL != null)) && (this.VERLEVEL.equals(o.VERLEVEL) == false))
            || (((this.CONDITIONCLASS != null) && (o.CONDITIONCLASS != null)) && (this.CONDITIONCLASS.equals(o.CONDITIONCLASS) == false))
            || (((this.VERCONDITION != null) && (o.VERCONDITION != null)) && (this.VERCONDITION.equals(o.VERCONDITION) == false))
            || (((this.PROMPTNAME1 != null) && (o.PROMPTNAME1 != null)) && (this.PROMPTNAME1.equals(o.PROMPTNAME1) == false))
            || (((this.PROMPTVALUE1 != null) && (o.PROMPTVALUE1 != null)) && (this.PROMPTVALUE1.equals(o.PROMPTVALUE1) == false))
            || (((this.PROMPTNAME2 != null) && (o.PROMPTNAME2 != null)) && (this.PROMPTNAME2.equals(o.PROMPTNAME2) == false))
            || (((this.PROMPTVALUE2 != null) && (o.PROMPTVALUE2 != null)) && (this.PROMPTVALUE2.equals(o.PROMPTVALUE2) == false))
            || (((this.OBJECTCONDITION != null) && (o.OBJECTCONDITION != null)) && (this.OBJECTCONDITION.equals(o.OBJECTCONDITION) == false))
            || (((this.PROMPTNAME3 != null) && (o.PROMPTNAME3 != null)) && (this.PROMPTNAME3.equals(o.PROMPTNAME3) == false))
            || (((this.PROMPTVALUE3 != null) && (o.PROMPTVALUE3 != null)) && (this.PROMPTVALUE3.equals(o.PROMPTVALUE3) == false))
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
  * return 2560
  */
  public static int getFacttableColumnSize() {
    
     return 2560;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFacttableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFacttableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getVerlevelColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVerlevelDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVerlevelSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 32
  */
  public static int getConditionclassColumnSize() {
    
     return 32;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getConditionclassDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getConditionclassSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getVerconditionColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVerconditionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVerconditionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getPromptname1ColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptname1DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptname1SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPromptvalue1ColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptvalue1DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptvalue1SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getPromptname2ColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptname2DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptname2SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPromptvalue2ColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptvalue2DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptvalue2SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getObjectconditionColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjectconditionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjectconditionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getPromptname3ColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptname3DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptname3SQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getPromptvalue3ColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPromptvalue3DecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getPromptvalue3SQLType() {
    
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

  public Verificationcondition getOriginal() {
    return original;
  }
   
  public void setOriginal(final Verificationcondition original) {
    this.original = (Verificationcondition) original.clone();
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
