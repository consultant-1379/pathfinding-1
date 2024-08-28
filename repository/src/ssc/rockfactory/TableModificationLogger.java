package ssc.rockfactory;

import java.util.Vector;

/**
 * A class for logging modifications made to the database tables from the 
 * RockFactory class.
 * 
 * The TableModificationLogger class has a singleton instance, which can be 
 * accessed by calling the static method instance().
 * 
 * NOTE:
 * If the logger is wished to be used for logging more than one class, then its 
 * implementation must be modified for example by creating a map for instances,
 * from which they can be retrieved with an id number (i.e. with a method such
 * as TableModificationLogger.getInstance(int id)).
 * 
 * @author epiituo
 *
 */
public class TableModificationLogger {

  /**
   * Returns the singleton instance of the class.
   * 
   * @return singleton instance
   */
  public static TableModificationLogger instance() {
    return instance;
  }
  
  private static TableModificationLogger instance = new TableModificationLogger();
    
  private final Vector<String> tablesUsed;
  private boolean logging = false;
  
  private TableModificationLogger() {
    this.tablesUsed = new Vector<String>();
  }
  
  public void startLogging() {
    this.logging = true;
  }
  
  public void stopLogging() {
    this.logging = false;
  }
  
  /**
   * Adds a log entry. If the logging is not enabled, by calling startLogging(),
   * the method does nothing. 
   * 
   * @param tableName
   */
  public void add(final String tableName) {
    if (logging && !this.tablesUsed.contains(tableName)) {
      this.tablesUsed.add(tableName);
    }
  }
  
  /**
   * Returns the log entries.
   * 
   * @return log entries.
   */
  public String[] get() {
    final String[] result = new String[this.tablesUsed.size()];
    this.tablesUsed.toArray(result);
    return result;
  }
  
  /**
   * Resets the log entries.
   * 
   */
  public void reset() {
    this.tablesUsed.clear();
  }
  
}
