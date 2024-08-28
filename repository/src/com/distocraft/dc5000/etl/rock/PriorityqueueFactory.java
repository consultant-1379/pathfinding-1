
package com.distocraft.dc5000.etl.rock;

import java.sql.SQLException;
import java.sql.Timestamp; // NOPMD : eeipca : may not be used
import java.util.Date; // NOPMD : eeipca : may not be used
import java.util.Iterator;
import java.util.Vector;
import java.util.HashSet;
import ssc.rockfactory.FactoryRes; // NOPMD : eeipca : may not be used
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;



public class PriorityqueueFactory implements Cloneable {
  private final Vector<Priorityqueue> vec;

  private final RockFactory rockFact;

  private final Priorityqueue whereObject;

  public PriorityqueueFactory(final RockFactory rockFact, final Priorityqueue whereObject) throws SQLException,
      RockException {
    this.whereObject = whereObject;

    this.vec = new Vector<Priorityqueue>();
    this.rockFact = rockFact;
    final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
    @SuppressWarnings({"unchecked"}) final Iterator<Priorityqueue> it = rockFact.getData(whereObject, results);
    Priorityqueue o;

    while (it.hasNext()) {
      o = it.next();
      o.setModifiedColumns(new HashSet<String>());
      o.setNewItem(false);
      o.setOriginal(o);
      this.vec.addElement(o);
    }
    results.close();
  }

  public PriorityqueueFactory(final RockFactory rockFact, final Priorityqueue whereObject, final boolean validate) throws SQLException,
      RockException {
    this.whereObject = whereObject;

    this.vec = new Vector<Priorityqueue>();
    this.rockFact = rockFact;
    final RockResultSet results = rockFact.setSelectSQL(false, whereObject);
    @SuppressWarnings({"unchecked"}) final Iterator<Priorityqueue> it = rockFact.getData(whereObject, results);
    Priorityqueue o;

    while (it.hasNext()) {
      o = it.next();
      o.setModifiedColumns(new HashSet<String>());
      o.setNewItem(false);
      o.setValidateData(validate);
      o.setOriginal(o);  
      this.vec.addElement(o);
    }
    results.close();
  }

  public PriorityqueueFactory(final RockFactory rockFact, final Priorityqueue whereObject, final String orderByClause)
      throws SQLException, RockException {
    this.whereObject = whereObject;
    this.vec = new Vector<Priorityqueue>();
    this.rockFact = rockFact;
    final RockResultSet results = rockFact.setSelectSQL(false, whereObject, orderByClause);
    @SuppressWarnings({"unchecked"}) final Iterator<Priorityqueue> it = rockFact.getData(whereObject, results);
    Priorityqueue o;
    while (it.hasNext()) {
      o = it.next();
      o.setModifiedColumns(new HashSet<String>());
      o.setNewItem(false);
      o.setOriginal(o);
      this.vec.addElement(o);
    }
    results.close();
  }

  /**
   * Get an element from the vector
   * 
   * @param i
   *          the element indicator
   * @return The element at index i
   */
  public Priorityqueue getElementAt(final int i) {
    if (i < this.vec.size()) {
      return this.vec.elementAt(i);
    }
    return null;
  }

  /**
   * The size of the RockFactory vector
   * @return The size of the vector
   */
  public int size() {
    return this.vec.size();
  }

  /**
   * The generated GET METHODS
   * @return Vector<Priorityqueue>
   */
  public Vector<Priorityqueue> get() {
    return vec;
  }

  /**
   * equals method test wheather the objects field values and the parametrs objects field values
   * are equal.
   * @param otherVector The other list to check against
   * @return <code>TRUE</code> if they are equal, <code>FALSE</code> otherwise
   */
  public boolean equals(final Vector<Priorityqueue> otherVector) {
    if (this.vec == otherVector) {
      return true;
    }
    if ((this.vec == null) || (otherVector == null)) {
      return false;
    }
    if (this.vec.size() != otherVector.size()) {
      return false;
    }
    for (int i = 0; i < this.vec.size(); i++) {
      final Priorityqueue o = this.vec.elementAt(i);
      final Priorityqueue otherO = otherVector.elementAt(i);
      if (!o.equals(otherO)){
        return false;
      }
    }
    return true;
  }

  /**
   * Delete object contents from database
   * @return int Number of deleted rows.
   * @exception SQLException Errors
   * @exception RockException Errors
   */
  public int deleteDB() throws SQLException, RockException {
    return this.rockFact.deleteData(false, this.whereObject);
  }

  /**
   * to enable a public clone method inherited from Object class (private method)
   */
  @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException"})
  @Override
  public Object clone() {
    Object o = null;
    try {
      o = super.clone();
    } catch (CloneNotSupportedException e) {
      //
    }
    return o;
  }
  
}
