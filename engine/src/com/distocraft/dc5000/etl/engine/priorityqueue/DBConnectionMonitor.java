package com.distocraft.dc5000.etl.engine.priorityqueue;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfile;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.TransferEngine;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;

/**
 * Monitors availability of DWH database connections. If DWH database goes down
 * PriorityQueue is suspended.
 * 
 * @author etuolem
 */
public class DBConnectionMonitor extends TimerTask {

  private final static Logger LOG = Logger.getLogger("etlengine.DBConnectionMonitor");

  private final PriorityQueue queue;

  private final ExecutionSlotProfileHandler profileHolder;

  private final TransferEngine engineInstance;

  private final String etlrepUrl;

  private final String etlrepUsr;

  private final String etlrepPwd;

  private final String etlrepDrv;

  /**
   * Used to stop monitoring when profile has been set through the engine CLI
   */
  private static String expectedProfile = "Normal";
  
  private static ReentrantLock mutex = new ReentrantLock();
  
  private static boolean isOnce = true;

  protected final Map<String, DBConnection> dbconnections = new HashMap<String, DBConnection>();

  public DBConnectionMonitor(final TransferEngine engineInstance, final PriorityQueue queue,
      final ExecutionSlotProfileHandler profileHolder, final String etlrepUrl, final String etlrepUsr,
      final String etlrepPwd, final String etlrepDrv) { // NOPMD
    this.engineInstance = engineInstance;
    this.queue = queue;
    this.profileHolder = profileHolder;
    this.etlrepUrl = etlrepUrl;
    this.etlrepUsr = etlrepUsr;
    this.etlrepPwd = etlrepPwd;
    this.etlrepDrv = etlrepDrv;

  }

  private static final Map<String, String> CONN_MONITOR_DETAILS = new TreeMap<String, String>();

  public static Map<String, String> getConnectionMonitorDetails() {
    return Collections.unmodifiableMap(CONN_MONITOR_DETAILS);
  }

  @Override
  public void run() {
	try {
	  mutex.lock();
		
	  if (expectedProfile.equalsIgnoreCase("Normal")) {
		isOnce = true;
	    try {
	      // Figure out database connections
	      if (!updateDatabases()) {
	        LOG.warning("Could not source database connection details");
	        disableEngine();
	        return;
	      }
	    } catch (Exception e) {
	      LOG.log(Level.WARNING, "Could not source database connection details", e);
	      disableEngine();
	      return;
	    }
	
	    boolean connectionsOK = true;
	
	    final Map<String, String> monitorDetails = new HashMap<String, String>(dbconnections.size());
	    for (DBConnection dbcon : dbconnections.values()) {
	      LOG.fine("Checking database connection " + dbcon.getName());
	      try {
	        checkDataBase(dbcon);
	        monitorDetails.put(dbcon.getName(), "Available");
	        LOG.fine("Database connection " + dbcon.getName() + " is fine.");
	      } catch (Exception e) {
	        monitorDetails.put(dbcon.getName(), "UnAvailable");
	        LOG.log(Level.WARNING, "Database connection to '" + dbcon.getName() + "' unavailable. ");
	        connectionsOK = false;
	        break;
	      }
	    }
	    synchronized (CONN_MONITOR_DETAILS) {
	      CONN_MONITOR_DETAILS.clear();
	      CONN_MONITOR_DETAILS.putAll(monitorDetails);
	    }
	
	    if (connectionsOK) {
	      if (!profileHolder.getActiveExecutionProfile().name().equalsIgnoreCase("Normal") || !queue.isActive()) {
	        LOG.warning("Database connections re-established, re-enabling Engine");
	        enableEngine();
	      }
	    } else if (!connectionsOK) {
	      if (!profileHolder.getActiveExecutionProfile().name().equalsIgnoreCase("NoLoads") || !queue.isActive()) {
	        LOG.warning("Database connections lost, disabling Engine");
	        disableEngine();
	      }
	    }
	  }
	  else {
	  	String currentProfile = profileHolder.getActiveExecutionProfile().name();
	   	if (!currentProfile.equalsIgnoreCase(expectedProfile) && expectedProfile.equalsIgnoreCase("NoLoads") && isOnce) {
	   	  LOG.info("Execution Profile mismatch. Requested Profile is " + expectedProfile + ". Disabling Engine.");
	   	  disableEngine();
	   	  isOnce = false;
	    }
	  }
	}
	finally {
	  if (mutex.isHeldByCurrentThread()) {
		mutex.unlock();
	  }
	}

  }

  private void disableEngine() {
    queue.setActive(true); // TODO (MultipleWriters: enable corresponding slots)

    // Also set profile to Normal
    boolean pro = false;
    try {
      pro = engineInstance.setActiveExecutionProfile("NoLoads", false);
    } catch (RemoteException e) {
      LOG.log(Level.SEVERE, "Could not put engine to NoLoads execution profile.", e);
    }
    if (pro) {
      LOG.warning("Engine profile set to NoLoads.");
    } else {
      LOG.severe("Could not put engine to NoLoads execution profile.");
    }
  }

  private void enableEngine() {
     // TODO (MultipleWriters: enable corresponding slots)
   // Also set profile to Normal
    boolean pro = false;
    try {
      engineInstance.reloadProperties();
      engineInstance.waitForCache();
      queue.setActive(true);
      pro = engineInstance.setActiveExecutionProfile("Normal", false);
    } catch (RemoteException e) {
      LOG.log(Level.SEVERE, "Could not put engine to Normal execution profile.", e);
    }
    if (pro) {
      LOG.info("Engine profile set to Normal.");
    } else {
      LOG.warning("Could not put engine to Normal execution profile.");
    }
  }

  public static void setExpectedProfile(final String profile) {
	try {
	  if (mutex.tryLock(2, TimeUnit.SECONDS)) {
		expectedProfile = profile;
	  }
	  else {
		LOG.info("[DBConnectionMonitor] Thread is currently running. "
				  + "Request for changing the Execution Profile via CLI will be handeled by next run.");
		expectedProfile = profile;
	  }
	} catch (InterruptedException e) {
	  LOG.warning("Exception occured while aquiring Lock as [DBConnectionMonitor] Thread is running. "
			  + "Request for changing the Execution Profile via CLI will be handeled by next run.");
	  expectedProfile = profile;
	}
	finally {
	  if (mutex.isHeldByCurrentThread()) {
		mutex.unlock();
	  }
	}
  }

  /**
   * Figure out database connection details for all DWH databases of current set
   * of execution slots.
   * 
   * @throws Exception
   *           if update fails.
   */
  boolean updateDatabases() throws Exception { // NOPMD

    boolean gotDatabaseConnections = false;

    final Set<String> newdbs = new HashSet<String>();

    final Map<String, ExecutionSlotProfile> allExecSlotProfiles = profileHolder.getAllExecutionProfiles();

    for (String profileName : allExecSlotProfiles.keySet()) {
      final Iterator<ExecutionSlot> slots = allExecSlotProfiles.get(profileName).getAllExecutionSlots();

      final List<String> ignored = new ArrayList<String>();
      while (slots.hasNext()) {
        final ExecutionSlot slot = slots.next();
        if (slot.getDBName() != null && !dbconnections.containsKey(slot.getDBName())) {
          final String slotService = slot.getDBName();
          if (!ignored.contains(slotService)) {
            if (isDatabase(slotService)) {
              LOG.fine("Monitoring database service called '" + slotService + "'");
              newdbs.add(slotService);
            } else {
              LOG.fine("Ignoring non database service called '" + slotService + "'");
              ignored.add(slotService);
            }
          }
        }
      }
    }

    // Check Coordinator also.
    if (!dbconnections.containsKey("dwh_coor")) {
      newdbs.add("dwh_coor");
    }

    RockFactory etlrep;
    try {
      etlrep = new RockFactory(etlrepUrl, etlrepUsr, etlrepPwd, etlrepDrv, "DBConMon", true);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Connection lost to repdb database", e);
      LOG.info("Connection lost to repdb database. Waiting for connection to come up.");
      engineInstance.slowGracefulPauseEngine();
      // Waiting for Repdb status
      while (true) {
        try {
          etlrep = new RockFactory(etlrepUrl, etlrepUsr, etlrepPwd, etlrepDrv, "DBConMon", true);
          if (etlrep != null) {
            //queue.setActive(true);
            LOG.info("Connection to repdb database established again. Activating Engine again.");
            break; // Connection established again
          }
        } catch (final Exception e1) {
          LOG.finest("Connection to repdb database still not Up. Waiting for connection to come up.");
          Thread.sleep(10000); // waiting for 10 seconds
        }
      }// while
    }// catch
    try {
      if (newdbs.size() > 0) {
        for (String dbName : newdbs) {
          final Meta_databases mdbCond = getNewMeta_databasesObject(etlrep);
          final String mappedDbName = mapServiceNameToDb(dbName);
          mdbCond.setConnection_name(mapServiceNameToDb(mappedDbName));
          mdbCond.setType_name("USER");
          final Meta_databasesFactory mdbFact = getNewMeta_databasesFactoryObject(etlrep, mdbCond);
          final Vector<Meta_databases> mdbs = mdbFact.get();
          if (mdbs != null && mdbs.size() >= 1) {
            final Meta_databases mdb = mdbs.firstElement();
            final DBConnection dbcon = getNewDBConnection(dbName, mdb.getConnection_string(), mdb.getUsername(),
                mdb.getPassword(), mdb.getDriver_name());
            dbconnections.put(dbName, dbcon);
          } else {
            LOG.severe("No connection details in meta_databases for type_name=USER, connection_name=" + mappedDbName
                + (!dbName.equals(mappedDbName) ? " (mapped from " + dbName + ")" : ""));
            return false;
          }
        }
        gotDatabaseConnections = true;
      } else {
        gotDatabaseConnections = true;
      }
    } finally {
      etlrep.getConnection().close();
    }

    return gotDatabaseConnections;
  }

  private String mapServiceNameToDb(final String dbName) {
    if (dbName.equals("dwhdb")) {
      return "dwh";
    }
    return dbName;
  }

  /**
   * @param dbName
   *          The database name
   * @param connection_string
   *          JDBC URL
   * @param username
   *          JDBC Username
   * @param password
   *          JDBC Password
   * @param driver_name
   *          JDBC Driver
   * 
   * 
   * 
   * 
   * 
   * @return DBConnection object
   */
  private DBConnection getNewDBConnection(final String dbName, final String connection_string, final String username,
      final String password, final String driver_name) {
    return new DBConnection(dbName, connection_string, username, password, driver_name);
  }

  /**
   * @param etlrep
   *          RockFactory connected to repdb:etlrep
   * @param mdbCond
   *          mdbCond
   * 
   * 
   * @return Meta_databasesFactory
   * @throws RockException
   *           errors
   * @throws SQLException
   *           errors
   */
  private Meta_databasesFactory getNewMeta_databasesFactoryObject(final RockFactory etlrep, final Meta_databases mdbCond)
      throws SQLException, RockException {

    return new Meta_databasesFactory(etlrep, mdbCond);
  }

  /**
   * @param etlrep
   *          .
   * 
   * @return .
   */
  private Meta_databases getNewMeta_databasesObject(final RockFactory etlrep) {
    return new Meta_databases(etlrep);
  }

  /**
   * Wrapper object for a database connection
   */
  public class DBConnection {

    protected final String name;

    protected final String dburl;

    protected final String dbusr;

    protected final String dbpwd;

    protected final String dbdrv;

    DBConnection(final String name, final String dburl, final String dbusr, final String dbpwd, final String dbdrv) {
      this.name = name;
      this.dburl = dburl;
      this.dbusr = dbusr;
      this.dbpwd = dbpwd;
      this.dbdrv = dbdrv;

      LOG.fine("New monitored database connection " + name);
    }

    /**
     * Returns RockFactory object of database connection.
     * 
     * @return RockFactory connected to database
     * @throws java.sql.SQLException
     *           errors
     * @throws ssc.rockfactory.RockException
     *           errors
     */
    protected RockFactory getRock() throws SQLException, RockException {
      return new RockFactory(dburl, dbusr, dbpwd, dbdrv, "DBConMon", true);
    }

    /**
     * Returns name of this database connection.
     * 
     * @return database connection name
     */
    String getName() {
      return name;
    }

  }

  // For Junit
  protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
    RockFactory rock = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      rock = dbcon.getRock();
      stmt = rock.getConnection().createStatement();
      rs = stmt.executeQuery("SELECT getdate()");
      rs.next();

    } finally {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (rock != null) {
        rock.getConnection().close();
      }
    }

  }

  boolean isDatabase(final String name) throws IOException {
    final List<String> iqNodes = ServicenamesHelper.getAllIQNodes();
    iqNodes.add("dwh");
    iqNodes.add("dwh_coor");
    if (iqNodes.contains(name)) {
      return true;
    }
    final Set<String> allNodes = ServicenamesHelper.getAllServiceNodes();
    if (!allNodes.contains(name)) {
      LOG.warning("Slot assigned to undefined service called '" + name + "'");
    }
    return false;
  }
}
