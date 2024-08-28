package com.distocraft.dc5000.etl.engine.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that holds common methods for all SQL actions
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLOperation extends TransferActionBase {

  private RockFactory connect;
  private ConnectionPool cpool;

  protected static final String DWHREP_CONNECTION_NAME = "dwhrep";
  protected static final String DWH_CONNECTION_NAME = "dwh";
  protected static final String USER = "USER";
  protected static final String DBA = "DBA";

  /**
   * Empty protected constructor
   * 
   */
  protected SQLOperation() {
    super();
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
   * @author Jukka Jaaheimo
   * @since JDK1.1
   */
  public SQLOperation(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectionId, final RockFactory rockFact,
      final ConnectionPool connectionPool, final Meta_transfer_actions trActions) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectionId, rockFact, trActions);

    this.cpool = connectionPool;
    this.connect = createRockFactoryFromConnectionId(version, connectionId);
  }

  /**
   * Extracted out for testing purposes
   * 
   * @param version
   * @param connectionId
   * @return
   * @throws EngineMetaDataException
   */
  protected RockFactory createRockFactoryFromConnectionId(final Meta_versions version, final Long connectionId)
      throws EngineMetaDataException {
    if (connectionId != null) {
      return cpool.getConnect(this, version.getVersion_number(), connectionId);
    }
    return null;
  }

  /**
   * Returns a connect object from the connectionPool
   */
  public RockFactory getConnection() {
    return this.connect;
  }

  /**
   * Mainly for subclasses to set their own query timeouts if required.
   *
   */
  public Statement getStatement() throws SQLException {
     return this.connect.getConnection().createStatement();
    }

  /**
   * Returns connectionpool from this class
   */
  public ConnectionPool getConnectionPool() {
    return this.cpool;
  }

  /**
   * Sets up a RockFactory (connection) for the data warehouse repository connection. The connection must be explicitly
   * closed after use.
   * 
   * @param etlRepRockFactory
   *          The etl metadata repository. The etl repository contains connection details for the dwh and dwhrep
   *          repository.
   * @param connectionName
   *          The name of the required connection (e.g. dwh or dwhrep).
   * @param autoCommit
   *          A flag indicating if transaction auto-commit is enabled.
   * @return A private RockFactory connection instance with auto-commit=True. This must be closed explicitly by the
   *         invoker after use.
   */
  protected RockFactory getPrivateRockFactory(final RockFactory etlRepRockFactory, final String connectionName,
      final boolean autoCommit, final String userTypeName) {
    RockFactory rockFactory = null;
    try {
      final Meta_databases metaDatabases = new Meta_databases(etlRepRockFactory);
      metaDatabases.setConnection_name(connectionName);
      metaDatabases.setType_name(userTypeName);
      final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(etlRepRockFactory, metaDatabases);
      final Vector<Meta_databases> metaDatabasesList = metaDatabasesFactory.get();

      if (metaDatabasesList == null || metaDatabasesList.size() != 1) {
        throw new RuntimeException(connectionName + " database is not defined in etlrep.Meta_databases.");
      }

      final Meta_databases metaDb = metaDatabasesList.get(0);

      rockFactory = getRockFactory(metaDb, autoCommit);
    } catch (final RockException e) {
      throw new RuntimeException(e);
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
    return rockFactory;
  }

  /**
   * Sets up a RockFactory (connection) for the data warehouse repository connection. The connection must be explictly
   * closed after use.
   * 
   * @param etlRepRockFactory
   *          The etl metadata repository. The etl repository contains connection details for the dwh and dwhrep
   *          repository.
   * @param connectionName
   *          The name of the required connection (e.g. dwh or dwhrep).
   * @return A private RockFactory connection instance with auto-commit=True. This must be closed explicitly by the
   *         invoker after use.
   */
  protected RockFactory getPrivateRockFactory(final RockFactory etlRepRockFactory, final String connectionName,
      final String userTypeName) {
    return getPrivateRockFactory(etlRepRockFactory, connectionName, true, userTypeName);
  }

  /**
   * Extracted out for testing purposes
   * 
   * @param db
   * @param autoCommit
   * @return
   * @throws SQLException
   * @throws RockException
   */
  protected RockFactory getRockFactory(final Meta_databases db, final boolean autoCommit) throws SQLException,
      RockException {
    return new RockFactory(db.getConnection_string(), db.getType_name(), db.getUsername(), db.getPassword(), db
        .getDriver_name(), db.getConnection_name(), autoCommit);
  }

  /**
   * Closes the ResultSet(s) associated with this statement and then closes the
   * statement
   * 
   * @param resultSet
   * @param statement
   * @param log
   */
  protected void close(ResultSet resultSet, Statement statement, final Logger log) {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (final SQLException e) {
      log.warning("ResultSet cleanup error - " + e.toString());
    }

    try {
      if (statement != null) {
        while (statement.getMoreResults()) {
          statement.getResultSet().close();
        }
        statement.close();
      }
    } catch (final SQLException e) {
      log.warning("Statement cleanup error - " + e.toString());
    }
    resultSet = null;
    statement = null;
  }
  
}
