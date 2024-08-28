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
import java.util.Vector;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;



public class User_constraintsFactory implements Cloneable {

  private final Vector vec;
  private RockFactory rockFact;

  /**
   * Constructor to initialize all objects to null
   */
  public User_constraintsFactory(final RockFactory rockFact, final User_constraints whereObject) throws SQLException, RockException {
    vec = new Vector();
    final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
    final Iterator it = rockFact.getData(whereObject, results);
    User_constraints o;
    while (it.hasNext()) {
      o = (User_constraints) it.next();
      this.vec.addElement(o);
    }
    results.close();
  }

  /**
   * Constructor to initialize all objects to null
   */
  public User_constraintsFactory(final RockFactory rockFact, final User_constraints whereObject, final String orderByClause)
      throws SQLException, RockException {
    vec = new Vector();
    final RockResultSet results = rockFact.setSelectSQL(false, whereObject, orderByClause);
    final Iterator it = rockFact.getData(whereObject, results);
    User_constraints o;
    while (it.hasNext()) {
      o = (User_constraints) it.next();
      this.vec.addElement(o);
    }
    results.close();
  }

  /**
   * Get an element from the vector
   * 
   * @param i
   *          the element indicator
   */
  public User_constraints getElementAt(final int i) {
    if (i < this.vec.size()) {
      return (User_constraints) this.vec.elementAt(i);
    }
    return null;
  }

  /**
   * The size of the RockFactory vector
   */
  public int size() {
    return this.vec.size();
  }

  /**
   * The generated GET METHODS
   */
  public Vector get() {
    return vec;
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
}