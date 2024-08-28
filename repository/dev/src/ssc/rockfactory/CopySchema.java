/**
 * ETL Repository access library.<br>
 * <br>
 * Copyright &copy; Distocraft Ltd. 2004-5. All rights reserved.<br>
 * 
 * @author lemminkainen
 */
package ssc.rockfactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class CopySchema {

  // The database connect
  private final RockFactory rockFact;

  // Catalog name (not used at the moment
  private final String catalog;

  // The database schema
  private final String schema;

  // A hash table of setmehods, oldvalues and new values
  private final Hashtable setValues;


  // The name for the project
  private final String packagePath;

  // A vector of already copied tables
  private final Vector copiedTables;

  /**
   * A class to copy a tables schema tree information.
   * 
   * @param rockFact
   *          The database connection
   * @param catalog db catalog
   * @param schema db schema
   * @param setValues values to copy
   * @param packagePath .
   * @throws RockException errors
   */
  public CopySchema(final RockFactory rockFact, final String catalog, final String schema, final Hashtable setValues, final String packagePath)
      throws RockException {

    if (rockFact == null) {
      throw new RockException(FactoryRes.VALUE_EQUALS_NULL + " (rockFact)");
    }

    if (schema == null) {
      throw new RockException(FactoryRes.VALUE_EQUALS_NULL + " (schema)");
    }

    this.rockFact = rockFact;
    this.catalog = catalog;
    this.schema = schema;
    this.setValues = setValues;

    this.packagePath = packagePath;

    this.copiedTables = new Vector();

  }

  /**
   * Finds all child tables for a table from database metadata. Makes a copy for each row containing
   * the old value and replaces the copies column value with the new value Calls itself to make a
   * copy of all elements in the schema tree.
   * 
   * @param parentTableName .
   * @exception SQLException .
   * @exception RockException .
   */
  public void copy(final String parentTableName) throws SQLException, RockException {

    if (parentTableName == null) {
      throw new RockException(FactoryRes.VALUE_EQUALS_NULL + " (parentTableName)");
    }
    ResultSet results = null;
    try {
      results = this.rockFact.getConnection().getMetaData().getExportedKeys(this.catalog, this.schema,
          parentTableName);

      while (results.next()) {

        final String tab = results.getString("FKTABLE_NAME").toLowerCase();

        if (results.getString("FKTABLE_SCHEM").toUpperCase().equals(this.schema.toUpperCase())) {

          if (!this.copiedTables.contains(tab)) {

            // rockFact.commit();

            // recursive call !!!
            copy(tab);
          }
        }
      }
    } finally {
      if(results != null){
        try{
          results.close();
        } catch (SQLException e){/**/}
      }
    }
  }

  /**
   * Copies one tables data .
   * 
   * @param tab .
   * @throws RockException errors
   */
  public void copyNonRecursive(final String tab) throws RockException {

    try {
      this.copiedTables.addElement(tab);

      String tableName = tab.substring(0, 1).toUpperCase();
      if (tab.length() > 1) {
        tableName = new StringBuffer(tableName).append(tab.substring(1, tab.length()).toLowerCase()).toString();
      }

      final String factoryElementStr = tableName + "Factory";
      final Class<?> factoryClass = Class.forName(this.packagePath + "." + factoryElementStr);
      final Method factorySizeMethod = factoryClass.getMethod("size", (Class[])null);
      final Method factoryGetElementMethod = factoryClass.getMethod("getElementAt", new Class[] { int.class });
      final Class<?> whereClass = Class.forName(this.packagePath + "." + tableName);

      final Constructor<?> whereConstr = whereClass.getConstructor(new Class[] { rockFact.getClass() });
      final Object whereObject = whereConstr.newInstance(new Object[] { rockFact });

      if (areMethodsIncluded(whereClass)) {

        invokeSetMethods(whereObject, false);

        final Constructor<?> factoryConstr = factoryClass.getConstructor(new Class[] { rockFact.getClass(),
            whereObject.getClass() });
        final Object factoryObject = factoryConstr.newInstance(new Object[] { rockFact, whereObject });

        for (int i = 0; i < ((Integer) (factorySizeMethod.invoke(factoryObject, (Object[])null))).intValue(); i++) {

          final Object itemObject = factoryGetElementMethod.invoke(factoryObject, new Object[] { new Integer(i) });

          final Method cloneMethod = itemObject.getClass().getMethod("clone", (Class[])null);

          final Object newItemObject = cloneMethod.invoke(itemObject, (Object[])null);

          invokeSetMethods(newItemObject, true);

          repairSeqValues(newItemObject);

          final Method insertMethod = newItemObject.getClass().getMethod("insertDB", (Class[])null);

          insertMethod.invoke(newItemObject, (Object[])null);

        }
      }
    } catch (Throwable e) {
      throw new RockException(e.getMessage());
    }

  }

  /**
   * Executes the objects setmethods with values.
   * 
   * @param obj
   *          The object to hold the methods.
   * @param newValues
   *          If true set method values are read from hashtable values[1] else from values[0]
   * @throws IllegalAccessException .
   * @throws NoSuchMethodException .
   * @throws java.lang.reflect.InvocationTargetException .
   */
  private void invokeSetMethods(final Object obj, final boolean newValues) throws InvocationTargetException, NoSuchMethodException,
      IllegalAccessException {
    final Enumeration keys = this.setValues.keys();

    while (keys.hasMoreElements()) {

      final String setMethodName = (String) keys.nextElement();
      int position = 0;
      if (newValues) {
        position = 1;
      }
      final Method setMethod = obj.getClass().getMethod(setMethodName,
          (new Class[] { (((Object[]) this.setValues.get(setMethodName))[position]).getClass() }));
      setMethod.invoke(obj, (new Object[] { (((Object[]) this.setValues.get(setMethodName))[position]) }));

    }
  }

  /**
   * Tests whether the class includes the setmethod names or not.
   * 
   * @param cl
   *          The class to test
   * @return .
   */
  private boolean areMethodsIncluded(final Class<?> cl) {

    final Method[] methods = cl.getMethods();

    final Enumeration keys = this.setValues.keys();

    while (keys.hasMoreElements()) {

      final String setMethodName = (String) keys.nextElement();
      boolean found = false;

      for (Method method : methods) {

        final String methodName = method.getName();

        if (methodName.equals(setMethodName)) {

          found = true;

        }
      }
      if (!found) {
        return false;
      }

    }
    return true;
  }

  /**
   * Those values that are set by the hashtable must not be included in the sequence list of the db
   * class.
   * 
   * @param newItemObject .
   * @exception InvocationTargetException .
   * @exception NoSuchMethodException .
   * @exception IllegalAccessException .
   */
  private void repairSeqValues(final Object newItemObject) throws InvocationTargetException, NoSuchMethodException,
      IllegalAccessException {

    boolean found = false;
    final Method getSeqsMethod = newItemObject.getClass().getMethod("getcolumnsAndSequences", (Class[])null);
    final String[] seqs = (String[]) getSeqsMethod.invoke(newItemObject, (Object[])null);

    for (int i = 0; i * 2 < seqs.length; i++) {
      if (seqs[i * 2].length() > 0) {
        String setMethod = "set" + seqs[i * 2].substring(0, 1).toUpperCase();
        if (seqs[i * 2].length() > 1) {
          setMethod = new StringBuffer(setMethod).append(seqs[i * 2].substring(1, seqs[i * 2].length()).toLowerCase())
              .toString();
        }

        if ((!this.setValues.containsKey(setMethod)) && (isSetMethodInPrimary(newItemObject))) {

          seqs[i * 2] = "";
          found = true;
        }
      }
    }

    if (found) {
      final Method setSeqsMethod = newItemObject.getClass().getMethod("setcolumnsAndSequences",
          new Class[] { String[].class });
      final Object[] values = { seqs };
      setSeqsMethod.invoke(newItemObject, values);
    }

  }

  /**
   * Test if the hashtable containing the setmethods has an element for at least primary key column.
   * 
   * @param newItemObject .
   * @return boolean
   * @exception InvocationTargetException .
   * @exception NoSuchMethodException .
   * @exception IllegalAccessException .
   */
  public boolean isSetMethodInPrimary(final Object newItemObject) throws InvocationTargetException, NoSuchMethodException,
      IllegalAccessException {

    final Method getPrimaryKeysMethod = newItemObject.getClass().getMethod("getprimaryKeyNames", (Class[])null);
    final String[] primaryKeys = (String[]) getPrimaryKeysMethod.invoke(newItemObject, (Object[])null);

    for (String primaryKey : primaryKeys) {
      if (primaryKey.length() > 0) {
        String setMethod = "set" + primaryKey.substring(0, 1).toUpperCase();
        if (primaryKey.length() > 1) {
          setMethod = new StringBuffer(setMethod).append(
            primaryKey.substring(1, primaryKey.length()).toLowerCase()).toString();
        }

        if (this.setValues.containsKey(setMethod)) {

          return true;
        }
      }
    }
    return false;

  }

  /**
   * Removes elements from the vector.
   * 
   */
  public void close() {
    this.copiedTables.removeAllElements();
  }

}