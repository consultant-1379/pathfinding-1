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
public class Referencetable implements Cloneable,RockDBObject  {

    private String TYPEID;
    private String VERSIONID;
    private String TYPENAME;
    private String OBJECTID;
    private String OBJECTNAME;
    private String OBJECTVERSION;
    private String OBJECTTYPE;
    private String DESCRIPTION;
    private Long STATUS;
    private Long UPDATE_POLICY;
    private String TABLE_TYPE;
    private Integer DATAFORMATSUPPORT;
    private Integer BASEDEF;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Referencetable original; 

  public Referencetable(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Referencetable(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPEID = null;
         this.VERSIONID = null;
         this.TYPENAME = null;
         this.OBJECTID = null;
         this.OBJECTNAME = null;
         this.OBJECTVERSION = null;
         this.OBJECTTYPE = null;
         this.DESCRIPTION = null;
         this.STATUS = null;
         this.UPDATE_POLICY = null;
         this.TABLE_TYPE = null;
         this.DATAFORMATSUPPORT = null;
         this.BASEDEF = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Referencetable(final RockFactory rockFact   ,final String TYPEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPEID = TYPEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Referencetable> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Referencetable o = it.next();

              this.TYPEID = o.getTypeid();
              this.VERSIONID = o.getVersionid();
              this.TYPENAME = o.getTypename();
              this.OBJECTID = o.getObjectid();
              this.OBJECTNAME = o.getObjectname();
              this.OBJECTVERSION = o.getObjectversion();
              this.OBJECTTYPE = o.getObjecttype();
              this.DESCRIPTION = o.getDescription();
              this.STATUS = o.getStatus();
              this.UPDATE_POLICY = o.getUpdate_policy();
              this.TABLE_TYPE = o.getTable_type();
              this.DATAFORMATSUPPORT = o.getDataformatsupport();
              this.BASEDEF = o.getBasedef();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Referencetable");
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
  public Referencetable(final RockFactory rockFact, final Referencetable whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Referencetable> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Referencetable o = it.next();
                this.TYPEID = o.getTypeid();
                this.VERSIONID = o.getVersionid();
                this.TYPENAME = o.getTypename();
                this.OBJECTID = o.getObjectid();
                this.OBJECTNAME = o.getObjectname();
                this.OBJECTVERSION = o.getObjectversion();
                this.OBJECTTYPE = o.getObjecttype();
                this.DESCRIPTION = o.getDescription();
                this.STATUS = o.getStatus();
                this.UPDATE_POLICY = o.getUpdate_policy();
                this.TABLE_TYPE = o.getTable_type();
                this.DATAFORMATSUPPORT = o.getDataformatsupport();
                this.BASEDEF = o.getBasedef();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Referencetable");
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
    return "Referencetable";
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
  public int updateDB(final boolean useTimestamp, final Referencetable whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Referencetable whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Referencetable.saveDB(), no primary key defined");
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
    sbuff.append("<Referencetable ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("OBJECTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTID),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
        sbuff.append("OBJECTVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTVERSION),12, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("UPDATE_POLICY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UPDATE_POLICY),2, true)+"\" ");
        sbuff.append("TABLE_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_TYPE),12, true)+"\" ");
        sbuff.append("DATAFORMATSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATSUPPORT),4, true)+"\" ");
        sbuff.append("BASEDEF=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEF),4, true)+"\" ");
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
    sbuff.append("<Referencetable ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("OBJECTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTID),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
        sbuff.append("OBJECTVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTVERSION),12, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("UPDATE_POLICY=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UPDATE_POLICY),2, true)+"\" ");
        sbuff.append("TABLE_TYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TABLE_TYPE),12, true)+"\" ");
        sbuff.append("DATAFORMATSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATSUPPORT),4, true)+"\" ");
        sbuff.append("BASEDEF=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.BASEDEF),4, true)+"\" ");
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
    sbuff.append("</Referencetable>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Referencetable ( ");
	    		sbuff.append("TYPEID");
		    		sbuff.append(", VERSIONID");
	    		sbuff.append(", TYPENAME");
	    		sbuff.append(", OBJECTID");
	    		sbuff.append(", OBJECTNAME");
	    		sbuff.append(", OBJECTVERSION");
	    		sbuff.append(", OBJECTTYPE");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", UPDATE_POLICY");
	    		sbuff.append(", TABLE_TYPE");
	    		sbuff.append(", DATAFORMATSUPPORT");
	    		sbuff.append(", BASEDEF");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTVERSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UPDATE_POLICY,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TABLE_TYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATAFORMATSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.BASEDEF,4)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getTypename() { 
    return this.TYPENAME;
  }
   public String getObjectid() { 
    return this.OBJECTID;
  }
   public String getObjectname() { 
    return this.OBJECTNAME;
  }
   public String getObjectversion() { 
    return this.OBJECTVERSION;
  }
   public String getObjecttype() { 
    return this.OBJECTTYPE;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public Long getStatus() { 
    return this.STATUS;
  }
   public Long getUpdate_policy() { 
    return this.UPDATE_POLICY;
  }
   public String getTable_type() { 
    return this.TABLE_TYPE;
  }
   public Integer getDataformatsupport() { 
    return this.DATAFORMATSUPPORT;
  }
   public Integer getBasedef() { 
    return this.BASEDEF;
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
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (TYPENAME == null) {
      TYPENAME = "";
    }
     if (OBJECTID == null) {
      OBJECTID = "";
    }
     if (OBJECTNAME == null) {
      OBJECTNAME = "";
    }
     if (OBJECTVERSION == null) {
      OBJECTVERSION = "";
    }
     if (OBJECTTYPE == null) {
      OBJECTTYPE = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (STATUS == null) {
      STATUS = new Long (0);
    }
     if (UPDATE_POLICY == null) {
      UPDATE_POLICY = new Long (0);
    }
     if (TABLE_TYPE == null) {
      TABLE_TYPE = "";
    }
     if (DATAFORMATSUPPORT == null) {
      DATAFORMATSUPPORT = new Integer (0);
    }
     if (BASEDEF == null) {
      BASEDEF = new Integer (0);
    }
   }

   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
  }
   public void setTypename(final String TYPENAME) {
    if (validateData){
      DataValidator.validateData((Object)TYPENAME,"TYPENAME",12,255,0);
    }
    modifiedColumns.add("TYPENAME");
    this.TYPENAME = TYPENAME;
  }
   public void setObjectid(final String OBJECTID) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTID,"OBJECTID",12,255,0);
    }
    modifiedColumns.add("OBJECTID");
    this.OBJECTID = OBJECTID;
  }
   public void setObjectname(final String OBJECTNAME) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTNAME,"OBJECTNAME",12,255,0);
    }
    modifiedColumns.add("OBJECTNAME");
    this.OBJECTNAME = OBJECTNAME;
  }
   public void setObjectversion(final String OBJECTVERSION) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTVERSION,"OBJECTVERSION",12,50,0);
    }
    modifiedColumns.add("OBJECTVERSION");
    this.OBJECTVERSION = OBJECTVERSION;
  }
   public void setObjecttype(final String OBJECTTYPE) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTTYPE,"OBJECTTYPE",12,255,0);
    }
    modifiedColumns.add("OBJECTTYPE");
    this.OBJECTTYPE = OBJECTTYPE;
  }
   public void setDescription(final String DESCRIPTION) {
    if (validateData){
      DataValidator.validateData((Object)DESCRIPTION,"DESCRIPTION",12,32000,0);
    }
    modifiedColumns.add("DESCRIPTION");
    this.DESCRIPTION = DESCRIPTION;
  }
   public void setStatus(final Long STATUS) {
    if (validateData){
      DataValidator.validateData((Object)STATUS,"STATUS",2,9,0);
    }
    modifiedColumns.add("STATUS");
    this.STATUS = STATUS;
  }
   public void setUpdate_policy(final Long UPDATE_POLICY) {
    if (validateData){
      DataValidator.validateData((Object)UPDATE_POLICY,"UPDATE_POLICY",2,9,0);
    }
    modifiedColumns.add("UPDATE_POLICY");
    this.UPDATE_POLICY = UPDATE_POLICY;
  }
   public void setTable_type(final String TABLE_TYPE) {
    if (validateData){
      DataValidator.validateData((Object)TABLE_TYPE,"TABLE_TYPE",12,12,0);
    }
    modifiedColumns.add("TABLE_TYPE");
    this.TABLE_TYPE = TABLE_TYPE;
  }
   public void setDataformatsupport(final Integer DATAFORMATSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)DATAFORMATSUPPORT,"DATAFORMATSUPPORT",4,10,0);
    }
    modifiedColumns.add("DATAFORMATSUPPORT");
    this.DATAFORMATSUPPORT = DATAFORMATSUPPORT;
  }
   public void setBasedef(final Integer BASEDEF) {
    if (validateData){
      DataValidator.validateData((Object)BASEDEF,"BASEDEF",4,10,0);
    }
    modifiedColumns.add("BASEDEF");
    this.BASEDEF = BASEDEF;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Referencetable o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
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
  public boolean equals(final Referencetable o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.TYPENAME == null) || (o.TYPENAME == null)) && (this.TYPENAME != o.TYPENAME))
            || (((this.OBJECTID == null) || (o.OBJECTID == null)) && (this.OBJECTID != o.OBJECTID))
            || (((this.OBJECTNAME == null) || (o.OBJECTNAME == null)) && (this.OBJECTNAME != o.OBJECTNAME))
            || (((this.OBJECTVERSION == null) || (o.OBJECTVERSION == null)) && (this.OBJECTVERSION != o.OBJECTVERSION))
            || (((this.OBJECTTYPE == null) || (o.OBJECTTYPE == null)) && (this.OBJECTTYPE != o.OBJECTTYPE))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.UPDATE_POLICY == null) || (o.UPDATE_POLICY == null)) && (this.UPDATE_POLICY != o.UPDATE_POLICY))
            || (((this.TABLE_TYPE == null) || (o.TABLE_TYPE == null)) && (this.TABLE_TYPE != o.TABLE_TYPE))
            || (((this.DATAFORMATSUPPORT == null) || (o.DATAFORMATSUPPORT == null)) && (this.DATAFORMATSUPPORT != o.DATAFORMATSUPPORT))
            || (((this.BASEDEF == null) || (o.BASEDEF == null)) && (this.BASEDEF != o.BASEDEF))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.TYPENAME != null) && (o.TYPENAME != null)) && (this.TYPENAME.equals(o.TYPENAME) == false))
            || (((this.OBJECTID != null) && (o.OBJECTID != null)) && (this.OBJECTID.equals(o.OBJECTID) == false))
            || (((this.OBJECTNAME != null) && (o.OBJECTNAME != null)) && (this.OBJECTNAME.equals(o.OBJECTNAME) == false))
            || (((this.OBJECTVERSION != null) && (o.OBJECTVERSION != null)) && (this.OBJECTVERSION.equals(o.OBJECTVERSION) == false))
            || (((this.OBJECTTYPE != null) && (o.OBJECTTYPE != null)) && (this.OBJECTTYPE.equals(o.OBJECTTYPE) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.UPDATE_POLICY != null) && (o.UPDATE_POLICY != null)) && (this.UPDATE_POLICY.equals(o.UPDATE_POLICY) == false))
            || (((this.TABLE_TYPE != null) && (o.TABLE_TYPE != null)) && (this.TABLE_TYPE.equals(o.TABLE_TYPE) == false))
            || (((this.DATAFORMATSUPPORT != null) && (o.DATAFORMATSUPPORT != null)) && (this.DATAFORMATSUPPORT.equals(o.DATAFORMATSUPPORT) == false))
            || (((this.BASEDEF != null) && (o.BASEDEF != null)) && (this.BASEDEF.equals(o.BASEDEF) == false))
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
  * return 255
  */
  public static int getTypenameColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypenameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypenameSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getObjectidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjectidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjectidSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getObjectnameColumnSize() {
    
     return 255;   
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
    
 
  /**
  * get columnSize
  * return 50
  */
  public static int getObjectversionColumnSize() {
    
     return 50;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getObjectversionDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getObjectversionSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getObjecttypeColumnSize() {
    
     return 255;   
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
  * return 9
  */
  public static int getStatusColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getStatusDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getStatusSQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 9
  */
  public static int getUpdate_policyColumnSize() {
    
     return 9;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getUpdate_policyDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 2
  */
  public static int getUpdate_policySQLType() {
    
    return 2;   
  }
    
 
  /**
  * get columnSize
  * return 12
  */
  public static int getTable_typeColumnSize() {
    
     return 12;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTable_typeDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTable_typeSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getDataformatsupportColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDataformatsupportDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getDataformatsupportSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getBasedefColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getBasedefDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getBasedefSQLType() {
    
    return 4;   
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

  public Referencetable getOriginal() {
    return original;
  }
   
  public void setOriginal(final Referencetable original) {
    this.original = (Referencetable) original.clone();
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
