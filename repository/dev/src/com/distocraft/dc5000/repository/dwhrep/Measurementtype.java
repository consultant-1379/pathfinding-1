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
public class Measurementtype implements Cloneable,RockDBObject  {

    private String TYPEID;
    private String TYPECLASSID;
    private String TYPENAME;
    private String VENDORID;
    private String FOLDERNAME;
    private String DESCRIPTION;
    private Long STATUS;
    private String VERSIONID;
    private String OBJECTID;
    private String OBJECTNAME;
    private Integer OBJECTVERSION;
    private String OBJECTTYPE;
    private String JOINABLE;
    private String SIZING;
    private Integer TOTALAGG;
    private Integer ELEMENTBHSUPPORT;
    private Integer RANKINGTABLE;
    private Integer DELTACALCSUPPORT;
    private Integer PLAINTABLE;
    private String UNIVERSEEXTENSION;
    private Integer VECTORSUPPORT;
    private Integer DATAFORMATSUPPORT;
    private Integer FOLLOWJOHN;
    private Integer ONEMINAGG;
    private Integer FIFTEENMINAGG;
    private Integer EVENTSCALCTABLE;
    private Integer MIXEDPARTITIONSTABLE;
    private Integer LOADFILE_DUP_CHECK;
    private Integer SONAGG;
    private Integer SONFIFTEENMINAGG;
    private String ROPGRPCELL;
   
  private String timeStampName = "LAST_UPDATED"; // NOPMD : Dont set as static or final, unit tests change this via reflection

  private String[] columnsAndSequences = {  };

  private final String[] primaryKeyNames = {    "TYPEID"   };

  private RockFactory rockFact;

  private boolean newItem;
  
  private Set<String> modifiedColumns = new HashSet<String>();
  
  private boolean validateData = false;
  
  private Measurementtype original; 

  public Measurementtype(final RockFactory rockFact) {
  	this(rockFact, false);
  	original = null; 
  }

  /**
   * Constructor to initialize all objects to null
   */
  public Measurementtype(final RockFactory rockFact, final boolean validate) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.validateData = validate;
    
         this.TYPEID = null;
         this.TYPECLASSID = null;
         this.TYPENAME = null;
         this.VENDORID = null;
         this.FOLDERNAME = null;
         this.DESCRIPTION = null;
         this.STATUS = null;
         this.VERSIONID = null;
         this.OBJECTID = null;
         this.OBJECTNAME = null;
         this.OBJECTVERSION = null;
         this.OBJECTTYPE = null;
         this.JOINABLE = null;
         this.SIZING = null;
         this.TOTALAGG = null;
         this.ELEMENTBHSUPPORT = null;
         this.RANKINGTABLE = null;
         this.DELTACALCSUPPORT = null;
         this.PLAINTABLE = null;
         this.UNIVERSEEXTENSION = null;
         this.VECTORSUPPORT = null;
         this.DATAFORMATSUPPORT = null;
         this.FOLLOWJOHN = null;
         this.ONEMINAGG = null;
         this.FIFTEENMINAGG = null;
         this.EVENTSCALCTABLE = null;
         this.MIXEDPARTITIONSTABLE = null;
         this.LOADFILE_DUP_CHECK = null;
         this.SONAGG = null;
         this.SONFIFTEENMINAGG = null;
         this.ROPGRPCELL = null;
      	original = null; 

  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public Measurementtype(final RockFactory rockFact   ,final String TYPEID ) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;

            this.TYPEID = TYPEID;
      
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator<Measurementtype> it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final Measurementtype o = it.next();

              this.TYPEID = o.getTypeid();
              this.TYPECLASSID = o.getTypeclassid();
              this.TYPENAME = o.getTypename();
              this.VENDORID = o.getVendorid();
              this.FOLDERNAME = o.getFoldername();
              this.DESCRIPTION = o.getDescription();
              this.STATUS = o.getStatus();
              this.VERSIONID = o.getVersionid();
              this.OBJECTID = o.getObjectid();
              this.OBJECTNAME = o.getObjectname();
              this.OBJECTVERSION = o.getObjectversion();
              this.OBJECTTYPE = o.getObjecttype();
              this.JOINABLE = o.getJoinable();
              this.SIZING = o.getSizing();
              this.TOTALAGG = o.getTotalagg();
              this.ELEMENTBHSUPPORT = o.getElementbhsupport();
              this.RANKINGTABLE = o.getRankingtable();
              this.DELTACALCSUPPORT = o.getDeltacalcsupport();
              this.PLAINTABLE = o.getPlaintable();
              this.UNIVERSEEXTENSION = o.getUniverseextension();
              this.VECTORSUPPORT = o.getVectorsupport();
              this.DATAFORMATSUPPORT = o.getDataformatsupport();
              this.FOLLOWJOHN = o.getFollowjohn();
              this.ONEMINAGG = o.getOneminagg();
              this.FIFTEENMINAGG = o.getFifteenminagg();
              this.EVENTSCALCTABLE = o.getEventscalctable();
              this.MIXEDPARTITIONSTABLE = o.getMixedpartitionstable();
              this.LOADFILE_DUP_CHECK = o.getLoadfile_dup_check();
              this.SONAGG = o.getSonagg();
              this.SONFIFTEENMINAGG = o.getSonfifteenminagg();
              this.ROPGRPCELL = o.getRopgrpcell();
       
        results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementtype");
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
  public Measurementtype(final RockFactory rockFact, final Measurementtype whereObject) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator<Measurementtype> it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final Measurementtype o = it.next();
                this.TYPEID = o.getTypeid();
                this.TYPECLASSID = o.getTypeclassid();
                this.TYPENAME = o.getTypename();
                this.VENDORID = o.getVendorid();
                this.FOLDERNAME = o.getFoldername();
                this.DESCRIPTION = o.getDescription();
                this.STATUS = o.getStatus();
                this.VERSIONID = o.getVersionid();
                this.OBJECTID = o.getObjectid();
                this.OBJECTNAME = o.getObjectname();
                this.OBJECTVERSION = o.getObjectversion();
                this.OBJECTTYPE = o.getObjecttype();
                this.JOINABLE = o.getJoinable();
                this.SIZING = o.getSizing();
                this.TOTALAGG = o.getTotalagg();
                this.ELEMENTBHSUPPORT = o.getElementbhsupport();
                this.RANKINGTABLE = o.getRankingtable();
                this.DELTACALCSUPPORT = o.getDeltacalcsupport();
                this.PLAINTABLE = o.getPlaintable();
                this.UNIVERSEEXTENSION = o.getUniverseextension();
                this.VECTORSUPPORT = o.getVectorsupport();
                this.DATAFORMATSUPPORT = o.getDataformatsupport();
                this.FOLLOWJOHN = o.getFollowjohn();
                this.ONEMINAGG = o.getOneminagg();
                this.FIFTEENMINAGG = o.getFifteenminagg();
                this.EVENTSCALCTABLE = o.getEventscalctable();
                this.MIXEDPARTITIONSTABLE = o.getMixedpartitionstable();
                this.LOADFILE_DUP_CHECK = o.getLoadfile_dup_check();
                this.SONAGG = o.getSonagg();
                this.SONFIFTEENMINAGG = o.getSonfifteenminagg();
                this.ROPGRPCELL = o.getRopgrpcell();
                results.close();
        this.newItem = false;
  	    this.original = this; 
      } else {
        results.close();
  	    this.original = this; 
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "Measurementtype");
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
    return "Measurementtype";
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
  public int updateDB(final boolean useTimestamp, final Measurementtype whereObject) throws SQLException, RockException {
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
  public int deleteDB(final Measurementtype whereObject) throws SQLException, RockException {
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
        throw new RockException("Cannot use rock.Measurementtype.saveDB(), no primary key defined");
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
    sbuff.append("<Measurementtype ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("TYPECLASSID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPECLASSID),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("VENDORID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORID),12, true)+"\" ");
        sbuff.append("FOLDERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLDERNAME),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("OBJECTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTID),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
        sbuff.append("OBJECTVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTVERSION),4, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("JOINABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.JOINABLE),12, true)+"\" ");
        sbuff.append("SIZING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SIZING),12, true)+"\" ");
        sbuff.append("TOTALAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TOTALAGG),4, true)+"\" ");
        sbuff.append("ELEMENTBHSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ELEMENTBHSUPPORT),4, true)+"\" ");
        sbuff.append("RANKINGTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RANKINGTABLE),4, true)+"\" ");
        sbuff.append("DELTACALCSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DELTACALCSUPPORT),4, true)+"\" ");
        sbuff.append("PLAINTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLAINTABLE),4, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("VECTORSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VECTORSUPPORT),4, true)+"\" ");
        sbuff.append("DATAFORMATSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATSUPPORT),4, true)+"\" ");
        sbuff.append("FOLLOWJOHN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLLOWJOHN),4, true)+"\" ");
        sbuff.append("ONEMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ONEMINAGG),4, true)+"\" ");
        sbuff.append("FIFTEENMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FIFTEENMINAGG),4, true)+"\" ");
        sbuff.append("EVENTSCALCTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EVENTSCALCTABLE),4, true)+"\" ");
        sbuff.append("MIXEDPARTITIONSTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MIXEDPARTITIONSTABLE),4, true)+"\" ");
        sbuff.append("LOADFILE_DUP_CHECK=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOADFILE_DUP_CHECK),4, true)+"\" ");
        sbuff.append("SONAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SONAGG),4, true)+"\" ");
        sbuff.append("SONFIFTEENMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SONFIFTEENMINAGG),4, true)+"\" ");
        sbuff.append("ROPGRPCELL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ROPGRPCELL),12, true)+"\" ");
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
    sbuff.append("<Measurementtype ");
        sbuff.append("TYPEID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPEID),12, true)+"\" ");
        sbuff.append("TYPECLASSID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPECLASSID),12, true)+"\" ");
        sbuff.append("TYPENAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TYPENAME),12, true)+"\" ");
        sbuff.append("VENDORID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VENDORID),12, true)+"\" ");
        sbuff.append("FOLDERNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLDERNAME),12, true)+"\" ");
        sbuff.append("DESCRIPTION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DESCRIPTION),12, true)+"\" ");
        sbuff.append("STATUS=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.STATUS),2, true)+"\" ");
        sbuff.append("VERSIONID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VERSIONID),12, true)+"\" ");
        sbuff.append("OBJECTID=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTID),12, true)+"\" ");
        sbuff.append("OBJECTNAME=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTNAME),12, true)+"\" ");
        sbuff.append("OBJECTVERSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTVERSION),4, true)+"\" ");
        sbuff.append("OBJECTTYPE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.OBJECTTYPE),12, true)+"\" ");
        sbuff.append("JOINABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.JOINABLE),12, true)+"\" ");
        sbuff.append("SIZING=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SIZING),12, true)+"\" ");
        sbuff.append("TOTALAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.TOTALAGG),4, true)+"\" ");
        sbuff.append("ELEMENTBHSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ELEMENTBHSUPPORT),4, true)+"\" ");
        sbuff.append("RANKINGTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.RANKINGTABLE),4, true)+"\" ");
        sbuff.append("DELTACALCSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DELTACALCSUPPORT),4, true)+"\" ");
        sbuff.append("PLAINTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.PLAINTABLE),4, true)+"\" ");
        sbuff.append("UNIVERSEEXTENSION=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.UNIVERSEEXTENSION),12, true)+"\" ");
        sbuff.append("VECTORSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.VECTORSUPPORT),4, true)+"\" ");
        sbuff.append("DATAFORMATSUPPORT=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.DATAFORMATSUPPORT),4, true)+"\" ");
        sbuff.append("FOLLOWJOHN=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FOLLOWJOHN),4, true)+"\" ");
        sbuff.append("ONEMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ONEMINAGG),4, true)+"\" ");
        sbuff.append("FIFTEENMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.FIFTEENMINAGG),4, true)+"\" ");
        sbuff.append("EVENTSCALCTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.EVENTSCALCTABLE),4, true)+"\" ");
        sbuff.append("MIXEDPARTITIONSTABLE=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.MIXEDPARTITIONSTABLE),4, true)+"\" ");
        sbuff.append("LOADFILE_DUP_CHECK=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.LOADFILE_DUP_CHECK),4, true)+"\" ");
        sbuff.append("SONAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SONAGG),4, true)+"\" ");
        sbuff.append("SONFIFTEENMINAGG=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.SONFIFTEENMINAGG),4, true)+"\" ");
        sbuff.append("ROPGRPCELL=\""+DataValidator.wrap(DataValidator.escapeXML(""+this.ROPGRPCELL),12, true)+"\" ");
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
    sbuff.append("</Measurementtype>\n");
    return sbuff.toString();
  }

  /**
   * Prints the object out as a sql Insert clause
   * 
   * @exception SQLException
   */
   
  public String toSQLInsert(){
       
    final StringBuffer sbuff = new StringBuffer();
    sbuff.append("insert into Measurementtype ( ");
	    		sbuff.append("TYPEID");
		    		sbuff.append(", TYPECLASSID");
	    		sbuff.append(", TYPENAME");
	    		sbuff.append(", VENDORID");
	    		sbuff.append(", FOLDERNAME");
	    		sbuff.append(", DESCRIPTION");
	    		sbuff.append(", STATUS");
	    		sbuff.append(", VERSIONID");
	    		sbuff.append(", OBJECTID");
	    		sbuff.append(", OBJECTNAME");
	    		sbuff.append(", OBJECTVERSION");
	    		sbuff.append(", OBJECTTYPE");
	    		sbuff.append(", JOINABLE");
	    		sbuff.append(", SIZING");
	    		sbuff.append(", TOTALAGG");
	    		sbuff.append(", ELEMENTBHSUPPORT");
	    		sbuff.append(", RANKINGTABLE");
	    		sbuff.append(", DELTACALCSUPPORT");
	    		sbuff.append(", PLAINTABLE");
	    		sbuff.append(", UNIVERSEEXTENSION");
	    		sbuff.append(", VECTORSUPPORT");
	    		sbuff.append(", DATAFORMATSUPPORT");
	    		sbuff.append(", FOLLOWJOHN");
	    		sbuff.append(", ONEMINAGG");
	    		sbuff.append(", FIFTEENMINAGG");
	    		sbuff.append(", EVENTSCALCTABLE");
	    		sbuff.append(", MIXEDPARTITIONSTABLE");
	    		sbuff.append(", LOADFILE_DUP_CHECK");
	    		sbuff.append(", SONAGG");
	    		sbuff.append(", SONFIFTEENMINAGG");
	    		sbuff.append(", ROPGRPCELL");
	        sbuff.append(" ) values ( ");
    	        sbuff.append(""+DataValidator.wrap(""+this.TYPEID,12)+"");
        	        sbuff.append(", "+DataValidator.wrap(""+this.TYPECLASSID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TYPENAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VENDORID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FOLDERNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DESCRIPTION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.STATUS,2)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VERSIONID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTID,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTNAME,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTVERSION,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.OBJECTTYPE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.JOINABLE,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SIZING,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.TOTALAGG,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ELEMENTBHSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.RANKINGTABLE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DELTACALCSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.PLAINTABLE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.UNIVERSEEXTENSION,12)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.VECTORSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.DATAFORMATSUPPORT,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FOLLOWJOHN,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ONEMINAGG,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.FIFTEENMINAGG,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.EVENTSCALCTABLE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.MIXEDPARTITIONSTABLE,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.LOADFILE_DUP_CHECK,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SONAGG,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.SONFIFTEENMINAGG,4)+"");
    	        sbuff.append(", "+DataValidator.wrap(""+this.ROPGRPCELL,12)+"");
    	    sbuff.append(" );\n");   
    return sbuff.toString();
  }
  

   public String getTypeid() { 
    return this.TYPEID;
  }
   public String getTypeclassid() { 
    return this.TYPECLASSID;
  }
   public String getTypename() { 
    return this.TYPENAME;
  }
   public String getVendorid() { 
    return this.VENDORID;
  }
   public String getFoldername() { 
    return this.FOLDERNAME;
  }
   public String getDescription() { 
    return this.DESCRIPTION;
  }
   public Long getStatus() { 
    return this.STATUS;
  }
   public String getVersionid() { 
    return this.VERSIONID;
  }
   public String getObjectid() { 
    return this.OBJECTID;
  }
   public String getObjectname() { 
    return this.OBJECTNAME;
  }
   public Integer getObjectversion() { 
    return this.OBJECTVERSION;
  }
   public String getObjecttype() { 
    return this.OBJECTTYPE;
  }
   public String getJoinable() { 
    return this.JOINABLE;
  }
   public String getSizing() { 
    return this.SIZING;
  }
   public Integer getTotalagg() { 
    return this.TOTALAGG;
  }
   public Integer getElementbhsupport() { 
    return this.ELEMENTBHSUPPORT;
  }
   public Integer getRankingtable() { 
    return this.RANKINGTABLE;
  }
   public Integer getDeltacalcsupport() { 
    return this.DELTACALCSUPPORT;
  }
   public Integer getPlaintable() { 
    return this.PLAINTABLE;
  }
   public String getUniverseextension() { 
    return this.UNIVERSEEXTENSION;
  }
   public Integer getVectorsupport() { 
    return this.VECTORSUPPORT;
  }
   public Integer getDataformatsupport() { 
    return this.DATAFORMATSUPPORT;
  }
   public Integer getFollowjohn() { 
    return this.FOLLOWJOHN;
  }
   public Integer getOneminagg() { 
    return this.ONEMINAGG;
  }
   public Integer getFifteenminagg() { 
    return this.FIFTEENMINAGG;
  }
   public Integer getEventscalctable() { 
    return this.EVENTSCALCTABLE;
  }
   public Integer getMixedpartitionstable() { 
    return this.MIXEDPARTITIONSTABLE;
  }
   public Integer getLoadfile_dup_check() { 
    return this.LOADFILE_DUP_CHECK;
  }
   public Integer getSonagg() { 
    return this.SONAGG;
  }
   public Integer getSonfifteenminagg() { 
    return this.SONFIFTEENMINAGG;
  }
   public String getRopgrpcell() { 
    return this.ROPGRPCELL;
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
     if (TYPECLASSID == null) {
      TYPECLASSID = "";
    }
     if (TYPENAME == null) {
      TYPENAME = "";
    }
     if (VENDORID == null) {
      VENDORID = "";
    }
     if (FOLDERNAME == null) {
      FOLDERNAME = "";
    }
     if (DESCRIPTION == null) {
      DESCRIPTION = "";
    }
     if (STATUS == null) {
      STATUS = new Long (0);
    }
     if (VERSIONID == null) {
      VERSIONID = "";
    }
     if (OBJECTID == null) {
      OBJECTID = "";
    }
     if (OBJECTNAME == null) {
      OBJECTNAME = "";
    }
     if (OBJECTVERSION == null) {
      OBJECTVERSION = new Integer (0);
    }
     if (OBJECTTYPE == null) {
      OBJECTTYPE = "";
    }
     if (JOINABLE == null) {
      JOINABLE = "";
    }
     if (SIZING == null) {
      SIZING = "";
    }
     if (TOTALAGG == null) {
      TOTALAGG = new Integer (0);
    }
     if (ELEMENTBHSUPPORT == null) {
      ELEMENTBHSUPPORT = new Integer (0);
    }
     if (RANKINGTABLE == null) {
      RANKINGTABLE = new Integer (0);
    }
     if (DELTACALCSUPPORT == null) {
      DELTACALCSUPPORT = new Integer (0);
    }
     if (PLAINTABLE == null) {
      PLAINTABLE = new Integer (0);
    }
     if (UNIVERSEEXTENSION == null) {
      UNIVERSEEXTENSION = "";
    }
     if (VECTORSUPPORT == null) {
      VECTORSUPPORT = new Integer (0);
    }
     if (DATAFORMATSUPPORT == null) {
      DATAFORMATSUPPORT = new Integer (0);
    }
     if (FOLLOWJOHN == null) {
      FOLLOWJOHN = new Integer (0);
    }
     if (ONEMINAGG == null) {
      ONEMINAGG = new Integer (0);
    }
     if (FIFTEENMINAGG == null) {
      FIFTEENMINAGG = new Integer (0);
    }
     if (EVENTSCALCTABLE == null) {
      EVENTSCALCTABLE = new Integer (0);
    }
     if (MIXEDPARTITIONSTABLE == null) {
      MIXEDPARTITIONSTABLE = new Integer (0);
    }
     if (LOADFILE_DUP_CHECK == null) {
      LOADFILE_DUP_CHECK = new Integer (0);
    }
     if (SONAGG == null) {
      SONAGG = new Integer (0);
    }
     if (SONFIFTEENMINAGG == null) {
      SONFIFTEENMINAGG = new Integer (0);
    }
     if (ROPGRPCELL == null) {
      ROPGRPCELL = "";
    }
   }

   public void setTypeid(final String TYPEID) {
    if (validateData){
      DataValidator.validateData((Object)TYPEID,"TYPEID",12,255,0);
    }
    modifiedColumns.add("TYPEID");
    this.TYPEID = TYPEID;
  }
   public void setTypeclassid(final String TYPECLASSID) {
    if (validateData){
      DataValidator.validateData((Object)TYPECLASSID,"TYPECLASSID",12,255,0);
    }
    modifiedColumns.add("TYPECLASSID");
    this.TYPECLASSID = TYPECLASSID;
  }
   public void setTypename(final String TYPENAME) {
    if (validateData){
      DataValidator.validateData((Object)TYPENAME,"TYPENAME",12,255,0);
    }
    modifiedColumns.add("TYPENAME");
    this.TYPENAME = TYPENAME;
  }
   public void setVendorid(final String VENDORID) {
    if (validateData){
      DataValidator.validateData((Object)VENDORID,"VENDORID",12,128,0);
    }
    modifiedColumns.add("VENDORID");
    this.VENDORID = VENDORID;
  }
   public void setFoldername(final String FOLDERNAME) {
    if (validateData){
      DataValidator.validateData((Object)FOLDERNAME,"FOLDERNAME",12,128,0);
    }
    modifiedColumns.add("FOLDERNAME");
    this.FOLDERNAME = FOLDERNAME;
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
   public void setVersionid(final String VERSIONID) {
    if (validateData){
      DataValidator.validateData((Object)VERSIONID,"VERSIONID",12,128,0);
    }
    modifiedColumns.add("VERSIONID");
    this.VERSIONID = VERSIONID;
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
   public void setObjectversion(final Integer OBJECTVERSION) {
    if (validateData){
      DataValidator.validateData((Object)OBJECTVERSION,"OBJECTVERSION",4,10,0);
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
   public void setJoinable(final String JOINABLE) {
    if (validateData){
      DataValidator.validateData((Object)JOINABLE,"JOINABLE",12,255,0);
    }
    modifiedColumns.add("JOINABLE");
    this.JOINABLE = JOINABLE;
  }
   public void setSizing(final String SIZING) {
    if (validateData){
      DataValidator.validateData((Object)SIZING,"SIZING",12,16,0);
    }
    modifiedColumns.add("SIZING");
    this.SIZING = SIZING;
  }
   public void setTotalagg(final Integer TOTALAGG) {
    if (validateData){
      DataValidator.validateData((Object)TOTALAGG,"TOTALAGG",4,10,0);
    }
    modifiedColumns.add("TOTALAGG");
    this.TOTALAGG = TOTALAGG;
  }
   public void setElementbhsupport(final Integer ELEMENTBHSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)ELEMENTBHSUPPORT,"ELEMENTBHSUPPORT",4,10,0);
    }
    modifiedColumns.add("ELEMENTBHSUPPORT");
    this.ELEMENTBHSUPPORT = ELEMENTBHSUPPORT;
  }
   public void setRankingtable(final Integer RANKINGTABLE) {
    if (validateData){
      DataValidator.validateData((Object)RANKINGTABLE,"RANKINGTABLE",4,10,0);
    }
    modifiedColumns.add("RANKINGTABLE");
    this.RANKINGTABLE = RANKINGTABLE;
  }
   public void setDeltacalcsupport(final Integer DELTACALCSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)DELTACALCSUPPORT,"DELTACALCSUPPORT",4,10,0);
    }
    modifiedColumns.add("DELTACALCSUPPORT");
    this.DELTACALCSUPPORT = DELTACALCSUPPORT;
  }
   public void setPlaintable(final Integer PLAINTABLE) {
    if (validateData){
      DataValidator.validateData((Object)PLAINTABLE,"PLAINTABLE",4,10,0);
    }
    modifiedColumns.add("PLAINTABLE");
    this.PLAINTABLE = PLAINTABLE;
  }
   public void setUniverseextension(final String UNIVERSEEXTENSION) {
    if (validateData){
      DataValidator.validateData((Object)UNIVERSEEXTENSION,"UNIVERSEEXTENSION",12,12,0);
    }
    modifiedColumns.add("UNIVERSEEXTENSION");
    this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
  }
   public void setVectorsupport(final Integer VECTORSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)VECTORSUPPORT,"VECTORSUPPORT",4,10,0);
    }
    modifiedColumns.add("VECTORSUPPORT");
    this.VECTORSUPPORT = VECTORSUPPORT;
  }
   public void setDataformatsupport(final Integer DATAFORMATSUPPORT) {
    if (validateData){
      DataValidator.validateData((Object)DATAFORMATSUPPORT,"DATAFORMATSUPPORT",4,10,0);
    }
    modifiedColumns.add("DATAFORMATSUPPORT");
    this.DATAFORMATSUPPORT = DATAFORMATSUPPORT;
  }
   public void setFollowjohn(final Integer FOLLOWJOHN) {
    if (validateData){
      DataValidator.validateData((Object)FOLLOWJOHN,"FOLLOWJOHN",4,10,0);
    }
    modifiedColumns.add("FOLLOWJOHN");
    this.FOLLOWJOHN = FOLLOWJOHN;
  }
   public void setOneminagg(final Integer ONEMINAGG) {
    if (validateData){
      DataValidator.validateData((Object)ONEMINAGG,"ONEMINAGG",4,10,0);
    }
    modifiedColumns.add("ONEMINAGG");
    this.ONEMINAGG = ONEMINAGG;
  }
   public void setFifteenminagg(final Integer FIFTEENMINAGG) {
    if (validateData){
      DataValidator.validateData((Object)FIFTEENMINAGG,"FIFTEENMINAGG",4,10,0);
    }
    modifiedColumns.add("FIFTEENMINAGG");
    this.FIFTEENMINAGG = FIFTEENMINAGG;
  }
   public void setEventscalctable(final Integer EVENTSCALCTABLE) {
    if (validateData){
      DataValidator.validateData((Object)EVENTSCALCTABLE,"EVENTSCALCTABLE",4,10,0);
    }
    modifiedColumns.add("EVENTSCALCTABLE");
    this.EVENTSCALCTABLE = EVENTSCALCTABLE;
  }
   public void setMixedpartitionstable(final Integer MIXEDPARTITIONSTABLE) {
    if (validateData){
      DataValidator.validateData((Object)MIXEDPARTITIONSTABLE,"MIXEDPARTITIONSTABLE",4,10,0);
    }
    modifiedColumns.add("MIXEDPARTITIONSTABLE");
    this.MIXEDPARTITIONSTABLE = MIXEDPARTITIONSTABLE;
  }
   public void setLoadfile_dup_check(final Integer LOADFILE_DUP_CHECK) {
    if (validateData){
      DataValidator.validateData((Object)LOADFILE_DUP_CHECK,"LOADFILE_DUP_CHECK",4,10,0);
    }
    modifiedColumns.add("LOADFILE_DUP_CHECK");
    this.LOADFILE_DUP_CHECK = LOADFILE_DUP_CHECK;
  }
   public void setSonagg(final Integer SONAGG) {
    if (validateData){
      DataValidator.validateData((Object)SONAGG,"SONAGG",4,10,0);
    }
    modifiedColumns.add("SONAGG");
    this.SONAGG = SONAGG;
  }
   public void setSonfifteenminagg(final Integer SONFIFTEENMINAGG) {
    if (validateData){
      DataValidator.validateData((Object)SONFIFTEENMINAGG,"SONFIFTEENMINAGG",4,10,0);
    }
    modifiedColumns.add("SONFIFTEENMINAGG");
    this.SONFIFTEENMINAGG = SONFIFTEENMINAGG;
  }
   public void setRopgrpcell(final String ROPGRPCELL) {
    if (validateData){
      DataValidator.validateData((Object)ROPGRPCELL,"ROPGRPCELL",12,255,0);
    }
    modifiedColumns.add("ROPGRPCELL");
    this.ROPGRPCELL = ROPGRPCELL;
  }
 

  

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * dbEquals method test wheather the objects primary key values are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanReturns", "PMD.SimplifyBooleanExpressions"})
  public boolean dbEquals(final Measurementtype o) {

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
  public boolean equals(final Measurementtype o) {

         if ((((this.TYPEID == null) || (o.TYPEID == null)) && (this.TYPEID != o.TYPEID))
            || (((this.TYPECLASSID == null) || (o.TYPECLASSID == null)) && (this.TYPECLASSID != o.TYPECLASSID))
            || (((this.TYPENAME == null) || (o.TYPENAME == null)) && (this.TYPENAME != o.TYPENAME))
            || (((this.VENDORID == null) || (o.VENDORID == null)) && (this.VENDORID != o.VENDORID))
            || (((this.FOLDERNAME == null) || (o.FOLDERNAME == null)) && (this.FOLDERNAME != o.FOLDERNAME))
            || (((this.DESCRIPTION == null) || (o.DESCRIPTION == null)) && (this.DESCRIPTION != o.DESCRIPTION))
            || (((this.STATUS == null) || (o.STATUS == null)) && (this.STATUS != o.STATUS))
            || (((this.VERSIONID == null) || (o.VERSIONID == null)) && (this.VERSIONID != o.VERSIONID))
            || (((this.OBJECTID == null) || (o.OBJECTID == null)) && (this.OBJECTID != o.OBJECTID))
            || (((this.OBJECTNAME == null) || (o.OBJECTNAME == null)) && (this.OBJECTNAME != o.OBJECTNAME))
            || (((this.OBJECTVERSION == null) || (o.OBJECTVERSION == null)) && (this.OBJECTVERSION != o.OBJECTVERSION))
            || (((this.OBJECTTYPE == null) || (o.OBJECTTYPE == null)) && (this.OBJECTTYPE != o.OBJECTTYPE))
            || (((this.JOINABLE == null) || (o.JOINABLE == null)) && (this.JOINABLE != o.JOINABLE))
            || (((this.SIZING == null) || (o.SIZING == null)) && (this.SIZING != o.SIZING))
            || (((this.TOTALAGG == null) || (o.TOTALAGG == null)) && (this.TOTALAGG != o.TOTALAGG))
            || (((this.ELEMENTBHSUPPORT == null) || (o.ELEMENTBHSUPPORT == null)) && (this.ELEMENTBHSUPPORT != o.ELEMENTBHSUPPORT))
            || (((this.RANKINGTABLE == null) || (o.RANKINGTABLE == null)) && (this.RANKINGTABLE != o.RANKINGTABLE))
            || (((this.DELTACALCSUPPORT == null) || (o.DELTACALCSUPPORT == null)) && (this.DELTACALCSUPPORT != o.DELTACALCSUPPORT))
            || (((this.PLAINTABLE == null) || (o.PLAINTABLE == null)) && (this.PLAINTABLE != o.PLAINTABLE))
            || (((this.UNIVERSEEXTENSION == null) || (o.UNIVERSEEXTENSION == null)) && (this.UNIVERSEEXTENSION != o.UNIVERSEEXTENSION))
            || (((this.VECTORSUPPORT == null) || (o.VECTORSUPPORT == null)) && (this.VECTORSUPPORT != o.VECTORSUPPORT))
            || (((this.DATAFORMATSUPPORT == null) || (o.DATAFORMATSUPPORT == null)) && (this.DATAFORMATSUPPORT != o.DATAFORMATSUPPORT))
            || (((this.FOLLOWJOHN == null) || (o.FOLLOWJOHN == null)) && (this.FOLLOWJOHN != o.FOLLOWJOHN))
            || (((this.ONEMINAGG == null) || (o.ONEMINAGG == null)) && (this.ONEMINAGG != o.ONEMINAGG))
            || (((this.FIFTEENMINAGG == null) || (o.FIFTEENMINAGG == null)) && (this.FIFTEENMINAGG != o.FIFTEENMINAGG))
            || (((this.EVENTSCALCTABLE == null) || (o.EVENTSCALCTABLE == null)) && (this.EVENTSCALCTABLE != o.EVENTSCALCTABLE))
            || (((this.MIXEDPARTITIONSTABLE == null) || (o.MIXEDPARTITIONSTABLE == null)) && (this.MIXEDPARTITIONSTABLE != o.MIXEDPARTITIONSTABLE))
            || (((this.LOADFILE_DUP_CHECK == null) || (o.LOADFILE_DUP_CHECK == null)) && (this.LOADFILE_DUP_CHECK != o.LOADFILE_DUP_CHECK))
            || (((this.SONAGG == null) || (o.SONAGG == null)) && (this.SONAGG != o.SONAGG))
            || (((this.SONFIFTEENMINAGG == null) || (o.SONFIFTEENMINAGG == null)) && (this.SONFIFTEENMINAGG != o.SONFIFTEENMINAGG))
            || (((this.ROPGRPCELL == null) || (o.ROPGRPCELL == null)) && (this.ROPGRPCELL != o.ROPGRPCELL))
          ){
    return false;
    } else
         if ((((this.TYPEID != null) && (o.TYPEID != null)) && (this.TYPEID.equals(o.TYPEID) == false))
            || (((this.TYPECLASSID != null) && (o.TYPECLASSID != null)) && (this.TYPECLASSID.equals(o.TYPECLASSID) == false))
            || (((this.TYPENAME != null) && (o.TYPENAME != null)) && (this.TYPENAME.equals(o.TYPENAME) == false))
            || (((this.VENDORID != null) && (o.VENDORID != null)) && (this.VENDORID.equals(o.VENDORID) == false))
            || (((this.FOLDERNAME != null) && (o.FOLDERNAME != null)) && (this.FOLDERNAME.equals(o.FOLDERNAME) == false))
            || (((this.DESCRIPTION != null) && (o.DESCRIPTION != null)) && (this.DESCRIPTION.equals(o.DESCRIPTION) == false))
            || (((this.STATUS != null) && (o.STATUS != null)) && (this.STATUS.equals(o.STATUS) == false))
            || (((this.VERSIONID != null) && (o.VERSIONID != null)) && (this.VERSIONID.equals(o.VERSIONID) == false))
            || (((this.OBJECTID != null) && (o.OBJECTID != null)) && (this.OBJECTID.equals(o.OBJECTID) == false))
            || (((this.OBJECTNAME != null) && (o.OBJECTNAME != null)) && (this.OBJECTNAME.equals(o.OBJECTNAME) == false))
            || (((this.OBJECTVERSION != null) && (o.OBJECTVERSION != null)) && (this.OBJECTVERSION.equals(o.OBJECTVERSION) == false))
            || (((this.OBJECTTYPE != null) && (o.OBJECTTYPE != null)) && (this.OBJECTTYPE.equals(o.OBJECTTYPE) == false))
            || (((this.JOINABLE != null) && (o.JOINABLE != null)) && (this.JOINABLE.equals(o.JOINABLE) == false))
            || (((this.SIZING != null) && (o.SIZING != null)) && (this.SIZING.equals(o.SIZING) == false))
            || (((this.TOTALAGG != null) && (o.TOTALAGG != null)) && (this.TOTALAGG.equals(o.TOTALAGG) == false))
            || (((this.ELEMENTBHSUPPORT != null) && (o.ELEMENTBHSUPPORT != null)) && (this.ELEMENTBHSUPPORT.equals(o.ELEMENTBHSUPPORT) == false))
            || (((this.RANKINGTABLE != null) && (o.RANKINGTABLE != null)) && (this.RANKINGTABLE.equals(o.RANKINGTABLE) == false))
            || (((this.DELTACALCSUPPORT != null) && (o.DELTACALCSUPPORT != null)) && (this.DELTACALCSUPPORT.equals(o.DELTACALCSUPPORT) == false))
            || (((this.PLAINTABLE != null) && (o.PLAINTABLE != null)) && (this.PLAINTABLE.equals(o.PLAINTABLE) == false))
            || (((this.UNIVERSEEXTENSION != null) && (o.UNIVERSEEXTENSION != null)) && (this.UNIVERSEEXTENSION.equals(o.UNIVERSEEXTENSION) == false))
            || (((this.VECTORSUPPORT != null) && (o.VECTORSUPPORT != null)) && (this.VECTORSUPPORT.equals(o.VECTORSUPPORT) == false))
            || (((this.DATAFORMATSUPPORT != null) && (o.DATAFORMATSUPPORT != null)) && (this.DATAFORMATSUPPORT.equals(o.DATAFORMATSUPPORT) == false))
            || (((this.FOLLOWJOHN != null) && (o.FOLLOWJOHN != null)) && (this.FOLLOWJOHN.equals(o.FOLLOWJOHN) == false))
            || (((this.ONEMINAGG != null) && (o.ONEMINAGG != null)) && (this.ONEMINAGG.equals(o.ONEMINAGG) == false))
            || (((this.FIFTEENMINAGG != null) && (o.FIFTEENMINAGG != null)) && (this.FIFTEENMINAGG.equals(o.FIFTEENMINAGG) == false))
            || (((this.EVENTSCALCTABLE != null) && (o.EVENTSCALCTABLE != null)) && (this.EVENTSCALCTABLE.equals(o.EVENTSCALCTABLE) == false))
            || (((this.MIXEDPARTITIONSTABLE != null) && (o.MIXEDPARTITIONSTABLE != null)) && (this.MIXEDPARTITIONSTABLE.equals(o.MIXEDPARTITIONSTABLE) == false))
            || (((this.LOADFILE_DUP_CHECK != null) && (o.LOADFILE_DUP_CHECK != null)) && (this.LOADFILE_DUP_CHECK.equals(o.LOADFILE_DUP_CHECK) == false))
            || (((this.SONAGG != null) && (o.SONAGG != null)) && (this.SONAGG.equals(o.SONAGG) == false))
            || (((this.SONFIFTEENMINAGG != null) && (o.SONFIFTEENMINAGG != null)) && (this.SONFIFTEENMINAGG.equals(o.SONFIFTEENMINAGG) == false))
            || (((this.ROPGRPCELL != null) && (o.ROPGRPCELL != null)) && (this.ROPGRPCELL.equals(o.ROPGRPCELL) == false))
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
  * return 255
  */
  public static int getTypeclassidColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTypeclassidDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getTypeclassidSQLType() {
    
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
  * return 128
  */
  public static int getVendoridColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVendoridDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getVendoridSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 128
  */
  public static int getFoldernameColumnSize() {
    
     return 128;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFoldernameDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getFoldernameSQLType() {
    
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
  * return 10
  */
  public static int getObjectversionColumnSize() {
    
     return 10;   
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
  * return 4
  */
  public static int getObjectversionSQLType() {
    
    return 4;   
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
  * return 255
  */
  public static int getJoinableColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getJoinableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getJoinableSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 16
  */
  public static int getSizingColumnSize() {
    
     return 16;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSizingDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getSizingSQLType() {
    
    return 12;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getTotalaggColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getTotalaggDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getTotalaggSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getElementbhsupportColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getElementbhsupportDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getElementbhsupportSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getRankingtableColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRankingtableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getRankingtableSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getDeltacalcsupportColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getDeltacalcsupportDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getDeltacalcsupportSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getPlaintableColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getPlaintableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getPlaintableSQLType() {
    
    return 4;   
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
  * return 10
  */
  public static int getVectorsupportColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getVectorsupportDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getVectorsupportSQLType() {
    
    return 4;   
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
  public static int getFollowjohnColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFollowjohnDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getFollowjohnSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getOneminaggColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getOneminaggDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getOneminaggSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getFifteenminaggColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getFifteenminaggDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getFifteenminaggSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getEventscalctableColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getEventscalctableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getEventscalctableSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getMixedpartitionstableColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getMixedpartitionstableDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getMixedpartitionstableSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getLoadfile_dup_checkColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getLoadfile_dup_checkDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getLoadfile_dup_checkSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getSonaggColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSonaggDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getSonaggSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 10
  */
  public static int getSonfifteenminaggColumnSize() {
    
     return 10;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getSonfifteenminaggDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 4
  */
  public static int getSonfifteenminaggSQLType() {
    
    return 4;   
  }
    
 
  /**
  * get columnSize
  * return 255
  */
  public static int getRopgrpcellColumnSize() {
    
     return 255;   
  }

 /**
  * get DecimalDigits
  * return 0
  */
  public static int getRopgrpcellDecimalDigits() {
    
     return 0;   
  }
  
 /**
  * get SQLType
  * return 12
  */
  public static int getRopgrpcellSQLType() {
    
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

  public Measurementtype getOriginal() {
    return original;
  }
   
  public void setOriginal(final Measurementtype original) {
    this.original = (Measurementtype) original.clone();
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
