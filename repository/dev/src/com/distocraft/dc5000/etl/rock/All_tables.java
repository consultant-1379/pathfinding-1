/**
 * ETL Repository access library.<br>
 * <br>
 * Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved.<br>
 * 
 * @author lemminkainen
 */
package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.util.Iterator;

import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;


@SuppressWarnings({"PMD.SuspiciousConstantFieldName"}) // eeipca : columns are in UPPERCASE
public class All_tables implements Cloneable {
  
  private String TABLE_NAME;

  private static final String timeStampName = "";

  private String[] columnsAndSequences = {};

  private final String[] primaryKeyNames = { "TABLE_NAME" };

  private RockFactory rockFact;

  private boolean newItem;

  private String private_table_namexyz = "ALL_TABLES";

  public All_tables(final RockFactory rockFact, final String dbLinkName) {
    this.rockFact = rockFact;
    this.setDbLinkName(dbLinkName);
    this.newItem = true;
    this.TABLE_NAME = null;
  }

  public All_tables(final RockFactory rockFact, final String TABLE_NAME, final String dbLinkName) throws SQLException, RockException {
    try {
      this.rockFact = rockFact;
      this.setDbLinkName(dbLinkName);
      this.TABLE_NAME = TABLE_NAME;
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final All_tables o = (All_tables) it.next();
        this.TABLE_NAME = o.getTable_name();
        results.close();
        this.newItem = false;
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "ALL_TABLES");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  public All_tables(final RockFactory rockFact, final All_tables whereObject, final String dbLinkName) throws SQLException, RockException {
    try {
      this.rockFact = rockFact;
      this.setDbLinkName(dbLinkName);
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final All_tables o = (All_tables) it.next();
        this.TABLE_NAME = o.getTable_name();
        results.close();
        this.newItem = false;
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "ALL_TABLES");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
    
  }

  /**
   * Method for getting the table name
   * 
   * @return String name of the corresponding table
   */
  public String getTableName() {
    return this.private_table_namexyz;
  }

  /**
   * Method for setting the database link name for the table name
   */
  public void setDbLinkName(final String dbLinkName) {

    this.private_table_namexyz = "ALL_TABLES@" + dbLinkName;
  }

  /**
   * The generated GET METHODS
   */
  public String getTable_name() {
    return this.TABLE_NAME;
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

  /**
   * The generated SET METHODS
   */
  public void setTable_name(final String TABLE_NAME) {
    this.TABLE_NAME = TABLE_NAME;
  }

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  /**
   * equals method test wheather the objects field values and and the parametrs objects field values
   * are equal.
   */
  @SuppressWarnings({"PMD.SimplifyBooleanExpressions", "PMD.SimplifyBooleanReturns"})
  public boolean equals(final All_tables o) {
    if ((((this.TABLE_NAME == null) || (o.TABLE_NAME == null)) && (this.TABLE_NAME != o.TABLE_NAME))) {
      return false;
    } else if ((((this.TABLE_NAME != null) && (o.TABLE_NAME != null)) && (this.TABLE_NAME.equals(o.TABLE_NAME) == false))) {
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
  
}
