/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.repository.cache;

import java.sql.SQLException;
import java.util.Vector;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;

/**
 * @author efinian
 * 
 */
public class DBUtils {

  protected static final String DWHREP_CONNECTION_NAME = "dwhrep";
  protected static final String DWH_CONNECTION_NAME = "dwh";

  private static final Logger LOG = Logger.getLogger(DBUtils.class.getSimpleName());

  private DBUtils() { /**/
  }

  /**
   * Sets up a RockFactory (connection) for the data warehouse repository connection. The connection must be explictly
   * closed after use.
   * 
   * @param etlRepRockFactory
   *          The etl metadata repository. The etl repository contains connection details for the dwh and dwhrep
   *          repository.
   * @return A private RockFactory connection instance with auto-commit=True. This must be closed explicitly by the
   *         invoker after use.
   */
  public static RockFactory createPrivateRockFactory(final RockFactory etlRepRockFactory, final String connectionName) {
    RockFactory rockFactory = null;
    try {
      final Meta_databases metaDatabases = new Meta_databases(etlRepRockFactory);
      metaDatabases.setConnection_name(connectionName);
      metaDatabases.setType_name("USER");
      final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(etlRepRockFactory, metaDatabases);
      final Vector<Meta_databases> metaDatabasesList = metaDatabasesFactory.get();

      if (metaDatabasesList == null || metaDatabasesList.size() != 1) {
        throw new IllegalStateException(connectionName + " database is not defined in etlrep.Meta_databases.");
      }

      final Meta_databases metaDb = metaDatabasesList.get(0);

      rockFactory = getRockFactory(metaDb);
    } catch (final RockException e) {
      throw new IllegalStateException(e);
    } catch (final SQLException e) {
      throw new IllegalStateException(e);
    }
    return rockFactory;
  }

  /**
   * Extracted out for testing purposes
   * 
   * @param db
   * @return
   * @throws SQLException
   * @throws RockException
   */
  protected static RockFactory getRockFactory(final Meta_databases dwhRepDb) throws SQLException, RockException {

    LOG
        .finest(String
            .format(
                "Creating RockFacttory: ConnectionString[%s], Type[%s], Username[%s], Password[%s], Driver[%s], ConnectionName[%s], autoCommit[%s]",
                dwhRepDb.getConnection_string(), dwhRepDb.getType_name(), dwhRepDb.getUsername(), dwhRepDb
                    .getPassword(), dwhRepDb.getDriver_name(), dwhRepDb.getConnection_name(), true));

    return new RockFactory(dwhRepDb.getConnection_string(), dwhRepDb.getType_name(), dwhRepDb.getUsername(), dwhRepDb
        .getPassword(), dwhRepDb.getDriver_name(), dwhRepDb.getConnection_name(), true);
  }

}
