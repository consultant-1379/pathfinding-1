package com.distocraft.dc5000.etl.engine.sql;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class SQLActionExecute extends SQLOperation {

  // protected final String batchColumnName;
  protected final Meta_collections collection;

  private final Logger log;

  protected SQLActionExecute() {
    this.log = Logger.getLogger("etlengine.SQLActionExecute");
    this.collection = null;
  }

  /**
   * Constructor
   * 
   * @param versionNumber
   *          metadata version
   * @param collectionSetId
   *          primary key for collection set
   * @param collectionId
   *          primary key for collection
   * @param transferActionId
   *          primary key for transfer action
   * @param transferBatchId
   *          primary key for transfer batch
   * @param connectId
   *          primary key for database connections
   * @param rockFact
   *          metadata repository connection object
   * @param connectionPool
   *          a pool for database connections in this collection
   * @param trActions
   *          object that holds transfer action information (db contents)
   * 
   */
  public SQLActionExecute(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final ConnectionPool connectionPool, final Meta_transfer_actions trActions) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
        trActions);
    
    this.collection = collection;
    this.log = Logger.getLogger(getLoggerName(collectionSetId, rockFact, trActions.getTransfer_action_name()));
  }

  /**
   * Executes a SQL procedure
   */
  @Override
  public void execute() throws EngineException {
    try {
      log.fine("Beginning the execution");
      final String sqlClause = this.getTrActions().getAction_contents();
      log.finer("Unparsed sql:" + sqlClause);
      executeSQL(sqlClause);
    } catch (Exception e) {
      log.severe(e.getStackTrace() + "\r\n" + new String[] { this.getTrActions().getAction_contents() });
      throw new EngineException(EngineConstants.CANNOT_EXECUTE,
          new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
          EngineConstants.ERR_TYPE_EXECUTION);
    }
  }

  /**
   * Executes a SQL query
   */
  protected int executeSQLUpdate(final String sqlClause) throws SQLException {
    final RockFactory c = this.getConnection();
    Statement stmtc = null;

    int count = 0;

    try {
      // get max value from DB
      stmtc = c.getConnection().createStatement();
      stmtc.getConnection().commit();
      count = stmtc.executeUpdate(sqlClause);
      stmtc.getConnection().commit();

    } finally {
      if (stmtc != null) {
        try {
          stmtc.close();
        } catch (SQLException sql) {
          log.log(Level.FINE, "Statement close failed", sql);
        }
      }
    }

    return count;
  }

  /**
   * Executes a SQL procedure
   */
  protected void executeSQL(final String sqlClause) throws SQLException {
    final RockFactory c = this.getConnection();
    Statement stmtc = null;

    try {

      stmtc = c.getConnection().createStatement();
      stmtc.getConnection().commit();
      stmtc.execute(sqlClause);
      stmtc.getConnection().commit();

    } finally {
      if (stmtc != null) {
        try {
          stmtc.close();
        } catch (SQLException sql) {
          log.log(Level.FINE, "Statement close failed", sql);
        }
      }
    }

  }

  protected void executeSQL(final String sqlClause, final String tableName) throws Exception {

    final RockFactory c = this.getConnection();
    int error = 0;
    String transferName = "";

    Statement stmtc = null;
    try {
      stmtc = c.getConnection().createStatement();
      stmtc.getConnection().commit();
      stmtc.execute(sqlClause);
      stmtc.getConnection().commit();

    } catch (SQLException sybExc) {

      error = sybExc.getErrorCode();
      transferName = this.getTransferActionName();

      log.finest("sybase error code inside executeSQL:" + error);
      log.finest("this.getTransferActionName() inside executeSQL:" + this.getTransferActionName());

      // 8405 - Sybase Error Code for row locking issue

      if (error == 8405
          && (transferName.equalsIgnoreCase("UpdateMonitoringOnStartup") || transferName
              .equalsIgnoreCase("UpdateMonitoring"))) {

        log.warning("The set " + transferName + " is failed to execute"
            + " as another set aquires table level lock on the " + tableName + " table");
      }

      else {
        log.severe(sybExc.getMessage());
      }
    }

    catch (Exception e) {
      log.severe(e.getStackTrace() + "\r\n" + new String[] { sqlClause });
      throw new Exception(e);

    } finally {
      if (stmtc != null) {
        try {
          stmtc.close();
        } catch (SQLException sql) {
          log.log(Level.FINE, "Statement close failed", sql);
        }
      }
    }

  }

  protected int executeSQLUpdate(final String sqlClause, final String tableName) throws Exception {

    final RockFactory c = this.getConnection();

    int count = 0;
    int error = 0;
    String transferName = "";
    Statement stmtc = null;
    try {
      stmtc = c.getConnection().createStatement();
      stmtc.getConnection().commit();
      count = stmtc.executeUpdate(sqlClause);
      stmtc.getConnection().commit();

    } catch (SQLException sybExc) {

      error = sybExc.getErrorCode();
      transferName = this.getTransferActionName();

      if (error == 8405
          && (transferName.equalsIgnoreCase("UpdateMonitoringOnStartup") || transferName
              .equalsIgnoreCase("UpdateMonitoring"))) {

        log.warning("The set " + this.getTransferActionName() + " is failed to execute"
            + " as another set aquires table level lock on the " + tableName + " table");

      } else {
        log.fine("Inside else-->executeSQLUpdate");
        log.warning(sybExc.getMessage());
        // log.severe(sybExc.getMessage() + "\r\n" + new String[]{sqlClause});
      }

    } catch (Exception e) {
      log.severe(e.getStackTrace() + "\r\n" + new String[] { this.getTrActions().getAction_contents() });
      throw new Exception(e);

    } finally {
      if (stmtc != null) {
        try {
          stmtc.close();
        } catch (SQLException sql) {
          log.log(Level.FINE, "Statement close failed", sql);
        }
      }
    }

    return count;
  }

  private String getLoggerName(final Long collectionSetId, final RockFactory rockFact, final String actionName) {

    final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
    whereCollSet.setEnabled_flag("Y");
    whereCollSet.setCollection_set_id(collectionSetId);
    String techpack = "";
    try {
      final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);
      techpack = collSet.getCollection_set_name();
    } catch (Exception e) {

    }

    return "sql." + techpack + "." + collection.getSettype() + "." + actionName;

  }

}
