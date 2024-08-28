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
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;


@SuppressWarnings({"PMD.SuspiciousConstantFieldName"}) // eeipca : columns are in UPPERCASE
public class All_tab_columns implements Cloneable {

  private String TABLE_NAME;

  private String COLUMN_NAME;

  private String DATA_TYPE;

  private Long DATA_LENGTH;

  private String NULLABLE;

  private static final String timeStampName = "";

  private String[] columnsAndSequences = {};

  private final String[] primaryKeyNames = { "TABLE_NAME", "COLUMN_NAME" };

  private RockFactory rockFact;

  private boolean newItem;

  private String private_table_namexyz = "ALL_TAB_COLUMNS";

  public All_tab_columns(final RockFactory rockFact, final String dbLinkName) {
    this.rockFact = rockFact;
    this.setDbLinkName(dbLinkName);
    this.newItem = true;
    this.TABLE_NAME = null;
    this.COLUMN_NAME = null;
    this.DATA_TYPE = null;
    this.DATA_LENGTH = null;
    this.NULLABLE = null;
  }

  public All_tab_columns(final RockFactory rockFact, final String TABLE_NAME, final String COLUMN_NAME, final String dbLinkName)
      throws SQLException, RockException {
    try {
      this.rockFact = rockFact;
      this.setDbLinkName(dbLinkName);
      this.TABLE_NAME = TABLE_NAME;
      this.COLUMN_NAME = COLUMN_NAME;
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final All_tab_columns o = (All_tab_columns) it.next();
        this.TABLE_NAME = o.getTable_name();
        this.COLUMN_NAME = o.getColumn_name();
        this.DATA_TYPE = o.getData_type();
        this.DATA_LENGTH = o.getData_length();
        this.NULLABLE = o.getNullable();

        results.close();
        this.newItem = false;
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "ALL_TAB_COLUMNS");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    }
  }

  public All_tab_columns(final RockFactory rockFact, final All_tab_columns whereObject, final String dbLinkName) throws SQLException,
      RockException {
    try {
      this.rockFact = rockFact;
      this.setDbLinkName(dbLinkName);
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final All_tab_columns o = (All_tab_columns) it.next();
        this.TABLE_NAME = o.getTable_name();
        this.COLUMN_NAME = o.getColumn_name();
        this.DATA_TYPE = o.getData_type();
        this.DATA_LENGTH = o.getData_length();
        this.NULLABLE = o.getNullable();
        results.close();
        this.newItem = false;
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "ALL_TAB_COLUMNS");
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
   * 
   * 
   */
  public void setDbLinkName(final String dbLinkName) {

    this.private_table_namexyz = "ALL_TAB_COLUMNS@" + dbLinkName;

  }

  /**
   * The generated GET METHODS
   */
  public String getTable_name() {
    return this.TABLE_NAME;
  }

  public String getColumn_name() {
    return this.COLUMN_NAME;
  }

  public String getData_type() {
    return this.DATA_TYPE;
  }

  public Long getData_length() {
    return this.DATA_LENGTH;
  }

  public String getNullable() {
    return this.NULLABLE;
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

  public void setTable_name(final String TABLE_NAME) {
    this.TABLE_NAME = TABLE_NAME;
  }

  public void setColumn_name(final String COLUMN_NAME) {
    this.COLUMN_NAME = COLUMN_NAME;
  }

  public void setData_type(final String DATA_TYPE) {
    this.DATA_TYPE = DATA_TYPE;
  }

  public void setData_length(final Long DATA_LENGTH) {
    this.DATA_LENGTH = DATA_LENGTH;
  }

  public void setNullable(final String NULLABLE) {
    this.NULLABLE = NULLABLE;
  }

  public void setcolumnsAndSequences(final String[] newColsAndSeqs) {
    this.columnsAndSequences = newColsAndSeqs;
  }

  @SuppressWarnings({"PMD.SimplifyBooleanExpressions", "PMD.SimplifyBooleanReturns"})
  public boolean equals(final All_tab_columns o) {
    if ((((this.TABLE_NAME == null) || (o.TABLE_NAME == null)) && (this.TABLE_NAME != o.TABLE_NAME))
        || (((this.COLUMN_NAME == null) || (o.COLUMN_NAME == null)) && (this.COLUMN_NAME != o.COLUMN_NAME))) {
      return false;
    } else if ((((this.TABLE_NAME != null) && (o.TABLE_NAME != null)) && (this.TABLE_NAME.equals(o.TABLE_NAME) == false))
        || (((this.COLUMN_NAME != null) && (o.COLUMN_NAME != null)) && (this.COLUMN_NAME.equals(o.COLUMN_NAME) == false))) {
      return false;
    } else {
      return true;
    }
  }

  public Object clone() {
    Object o = null;
    try {
      o = super.clone();
    } catch (CloneNotSupportedException e) {
      Logger.getLogger("etl.rock.All_tab_columns").log(Level.WARNING, "Cannot clone object", e);
    }
    return o;
  }

  public boolean isPrimaryDefined() {
    return this.primaryKeyNames.length > 0;
  }

  public void setDefaults() {

  }

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
