package com.ericsson.eniq.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class DatabaseConnections {

  public static Logger log = Logger.getLogger("common.DatabaseConnections");

  public static String conf_dir = System.getProperty("CONF_DIR");

  private DatabaseConnections() {
    // hidden constructor
  }

  /**
   * Gets a rock factory for the ETL Rep Database
   */
  public static RockFactory getETLRepConnection() {
    RockFactory etlRep = null;
    String url = "";
    String userName = "";
    String password = "";
    String dbDriverName = "";
    String propertiesFile = "";
    try {

      if (conf_dir == null) {
        log.finest("System property CONF_DIR not defined. Using default");
        conf_dir = "/eniq/sw/conf";
      }

      if (!conf_dir.endsWith(File.separator)) {
        conf_dir += File.separator;
      }

      propertiesFile = conf_dir + "ETLCServer.properties";
      log.finest("Properties file: " + propertiesFile);

      final ETLCServerProperties etlcserverProperties = new ETLCServerProperties(propertiesFile);

      url = etlcserverProperties.getProperty(Constants.ENGINE_DB_URL);
      userName = etlcserverProperties.getProperty(Constants.ENGINE_DB_USERNAME);
      password = etlcserverProperties.getProperty(Constants.ENGINE_DB_PASSWORD);
      dbDriverName = etlcserverProperties.getProperty(Constants.ENGINE_DB_DRIVERNAME);
      log.fine("ETL Rep Connection using URL: " + url);
      log.finest("ETL Rep connection using the following details: \nURL:" + url + "\nUsername: " + userName
          + "\nDriverName: " + dbDriverName);
      etlRep = new RockFactory(url, userName, password, dbDriverName, "DatabaseConnections_ETLRep", true);

    } catch (FileNotFoundException e) {
      log.log(Level.WARNING, "File: " + propertiesFile + " does not exist" + e.getMessage(), e);
    } catch (IOException e) {
      log.log(Level.WARNING, "File: " + propertiesFile + "\n" + e.getMessage(), e);
    } catch (SQLException e) {
      log.log(Level.WARNING, "SQL Exception: " + e.getMessage(), e);
    } catch (RockException e) {
      log.log(Level.SEVERE, "Unable to initialize rockengine:" + e.getMessage(), e);
    }
    if (etlRep == null) {
      log.warning("Cannot connect to the ETL Repository Database.");
    }
    return etlRep;
  }

  /**
   * Get database rock factory for DWH database
   */
  public static RockFactory getDwhDBConnection() {
    log.finest("Retrieving Database connection for DWH DB.");
    final RockFactory etlRock = getETLRepConnection();

    if (etlRock == null) {
      log.warning("Cannot connect to the ETL Repository Database.");
      return null;
    }

    try {
      try {
        final Meta_databases dwhUser = new Meta_databases(etlRock);
        dwhUser.setConnection_name("dwh");
        dwhUser.setType_name("USER");

        final Meta_databasesFactory mdbf = new Meta_databasesFactory(etlRock, dwhUser);
        final Vector<Meta_databases> dbs = mdbf.get();

        if (dbs == null || dbs.size() != 1) {
          log.severe("dwh database not correctly defined in etlrep.Meta_databases.");
          return null;
        }

        final Meta_databases dwhdb = (Meta_databases) dbs.get(0);
        final String dbDriverNameDwh = dwhdb.getDriver_name();
        final String urlDwh = dwhdb.getConnection_string();
        final String userNameDwh = dwhdb.getUsername();
        final String passwordDwh = dwhdb.getPassword();

        log.fine("DWH DB Connection using URL: " + urlDwh);
        log.finest("DWH DB connection using the following details: \nURL:" + urlDwh + "\nUsername: " + userNameDwh
            + "\nPassword: " + passwordDwh + "\nDriverName: " + dbDriverNameDwh);
        final RockFactory dwhDB = new RockFactory(urlDwh, userNameDwh, passwordDwh, dbDriverNameDwh,
            "DatabaseConnections_DWHDB", true);
        return dwhDB;
      } finally {
        etlRock.getConnection().close();
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }
    return null;
  }
  
  /**
   * Get database rock factory for DWH database as DBA user
   */
  public static RockFactory getDwhDBConnectionWithDBA() {
    log.finest("Retrieving Database connection for DWH DB.");
    final RockFactory etlRock = getETLRepConnection();

    if (etlRock == null) {
      log.warning("Cannot connect to the ETL Repository Database.");
      return null;
    }

    try {
      try {
        final Meta_databases dwhUser = new Meta_databases(etlRock);
        dwhUser.setConnection_name("dwh");
        dwhUser.setType_name("DBA");

        final Meta_databasesFactory mdbf = new Meta_databasesFactory(etlRock, dwhUser);
        final Vector<Meta_databases> dbs = mdbf.get();

        if (dbs == null || dbs.size() != 1) {
          log.severe("dwh database not correctly defined in etlrep.Meta_databases.");
          return null;
        }

        final Meta_databases dwhdb = (Meta_databases) dbs.get(0);
        final String dbDriverNameDwh = dwhdb.getDriver_name();
        final String urlDwh = dwhdb.getConnection_string();
        final String userNameDwh = dwhdb.getUsername();
        final String passwordDwh = dwhdb.getPassword();

        log.fine("DWH DB Connection using URL: " + urlDwh);
        log.finest("DWH DB connection using the following details: \nURL:" + urlDwh + "\nUsername: " + userNameDwh
            + "\nPassword: " + passwordDwh + "\nDriverName: " + dbDriverNameDwh);
        final RockFactory dwhDB = new RockFactory(urlDwh, userNameDwh, passwordDwh, dbDriverNameDwh,
            "DatabaseConnections_DWHDB", true);
        return dwhDB;
      } finally {
        etlRock.getConnection().close();
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }
    return null;
  }


  /**
   * Get database rock factory for DWH Rep database
   */
  public static RockFactory getDwhRepConnection() {
    final RockFactory etlRock = getETLRepConnection();

    if (etlRock == null) {
      log.warning("Cannot connect to the ETL Repository Database.");
      return null;
    }

    try {
      try {
        final Meta_databases dwhRepUser = new Meta_databases(etlRock);
        dwhRepUser.setConnection_name("dwhrep");
        dwhRepUser.setType_name("USER");

        final Meta_databasesFactory mdbf1 = new Meta_databasesFactory(etlRock, dwhRepUser);

        final Vector<Meta_databases> dbs1 = mdbf1.get();

        if (dbs1 == null || dbs1.size() != 1) {
          log.severe("Dwh Rep database not correctly defined in etlrep.Meta_databases.");
          return null;
        }

        final Meta_databases repdb = (Meta_databases) dbs1.get(0);

        final String dbDriverNameDwhRep = repdb.getDriver_name();
        final String urlDwhRep = repdb.getConnection_string();
        final String userNameDwhRep = repdb.getUsername();
        final String passwordDwhRep = repdb.getPassword();

        log.fine("DWH DB Connection using URL: " + urlDwhRep);
        log.finest("DWH DB connection using the following details: \nURL:" + urlDwhRep + "\nUsername: "
            + userNameDwhRep + "\nPassword: " + passwordDwhRep + "\nDriverName: " + dbDriverNameDwhRep);

        final RockFactory dwhRep = new RockFactory(urlDwhRep, userNameDwhRep, passwordDwhRep, dbDriverNameDwhRep,
            "DatabaseConnections_DWHRep", true);
        return dwhRep;
      } finally {
        etlRock.getConnection().close();
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }
    return null;
  }
}

