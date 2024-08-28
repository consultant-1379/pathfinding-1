/**
 * ETL Repository access library. <br>
 * <br>
 * Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved. <br>
 * 
 * @author melantie
 */
package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.util.Iterator;

import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;



public class User_constraints implements Cloneable {

  private String constraint_name;
  private String table_name;
  private String constraint_type;
  private static final String timeStampName = "";
  private final String[] columnsAndSequences = {};
  private final String[] primaryKeyNames = {};
  private RockFactory rockFact;
  private boolean newItem;

  /**
   * Constructor to initialize all objects to null
   */
  public User_constraints(final RockFactory rockFact) {
    this.rockFact = rockFact;
    this.newItem = true;
    this.constraint_name = null;
    this.constraint_type = null;
    this.table_name = null;
  }

  /**
   * Constructor for primary selection from database PRIMARY KEY MUST BE DEFINED
   * 
   * @params primarykeys
   * @exception SQLException
   */
  public User_constraints(final RockFactory rockFact, final String constraint_name) throws SQLException, RockException {
    try {
      this.rockFact = rockFact;
      this.constraint_name = constraint_name;
      final RockResultSet results = rockFact.setSelectSQL(true, this);
      final Iterator it = rockFact.getData(this, results);
      if (it.hasNext()) {
        final User_constraints o = (User_constraints) it.next();
        this.constraint_name = o.getConstraint_name();
        this.constraint_type = o.getConstraint_type();
        this.table_name = o.getTable_name();
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "USER_CONSTRAINTS");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    } catch (Exception e) {
    }
  }

  /**
   * Constructor to select the object according to whereObject from the db NO
   * PRIMARY KEY DEFINED
   * 
   * @param <this
   *          object type> whereObject the where part is constructed from this
   *          object
   * @exception SQLException
   */
  public User_constraints(final RockFactory rockFact, final User_constraints whereObject) throws SQLException, RockException {
    try {
      this.rockFact = rockFact;
      final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
      final Iterator it = rockFact.getData(whereObject, results);
      if (it.hasNext()) {
        final User_constraints o = (User_constraints) it.next();
        this.constraint_name = o.getConstraint_name();
        this.constraint_type = o.getConstraint_type();
        this.table_name = o.getTable_name();
        results.close();
        this.newItem = false;
      } else {
        results.close();
        throw new SQLException(FactoryRes.CANNOT_GET_TABLE_DATA + "USER_CONSTRAINTS");
      }
    } catch (SQLException sqlE) {
      throw sqlE;
    } catch (Exception e) {
    }
  }

  /**
   * The generated GET METHODS
   */
  public String getConstraint_name() {
    return this.constraint_name;
  }

  public String getConstraint_type() {
    return this.constraint_type;
  }

  public String getTable_name() {
    return this.table_name;
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
  public void setConstraint_name(final String constraint_name) {
    this.constraint_name = constraint_name;
  }

  public void setConstraint_type(final String constraint_type) {
    this.constraint_type = constraint_type;
  }

  public void setTable_name(final String table_name) {
    this.table_name = table_name;
  }

  /**
   * to enable a public clone method inherited from Object class (private
   * method)
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
   * 
   */
  public boolean isPrimaryDefined() {
    return this.primaryKeyNames.length > 0;
  }

  /**
   * Method for getting the table name
   * 
   * @return String name of the corresponding table
   */
  public String getTableName() {
    return "USER_CONSTRAINTS";
  }

}