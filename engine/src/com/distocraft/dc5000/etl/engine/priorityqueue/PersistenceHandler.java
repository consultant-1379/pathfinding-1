/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.priorityqueue;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;
import com.distocraft.dc5000.etl.engine.sql.SQLOperation;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Priorityqueue;
import com.distocraft.dc5000.etl.rock.PriorityqueueFactory;

/**
 * PersistenceHandler is used to persist engine sets for case that engine gets
 * terminated before set is getting fully executed.
 *
 * @author etuolem
 */
public class PersistenceHandler {

	public static final String XP_TABLENAMES = "explicit_tablenames";
	public static final String SCHEDULING_INFO = "schedulingInfo";
	public static final String EARLIEST_EXEC = "earliestExecution";
	public static final String LATEST_EXEC = "latestExecution";
	public static final String PERSISTENT_ID = "persistentID";
	public static final String ACTIVE = "active";
	public static final String LOGGER_NAME = "loggerName";
	public static final String COL_NAME = "collectionName";
	public static final String COL_SET_NAME = "collectionSetName";
  public static final String SET_NAME = "setName";
	public static final String SETTYPE = "setType";
	public static final String PRIORITY = "priority";
	public static final String TIMELIMIT = "timelimit";

	private static final Logger LOG = Logger.getLogger("etlengine.PersistenceHandler");

	private final Map<Long, EngineThread> persistentSets = new Hashtable<Long, EngineThread>();

	private Long lastPersID = 0L;

	private String url = null;
	private String user = null;
	private String password = null;
	private String dbdrv = null;
  private transient RockFactory etlrep = null;

	/**
	 * Strictly for testing purposes only.
	 */
	public PersistenceHandler() {
		// for test init
	}

	/**
	 * Constructs PersistenceHandler. Connection details to etlrep database has to
	 * be provided as parameters.
	 */
	public PersistenceHandler(final String url, final String user, final String password, final String dbdrv)
			throws RockException {

		this.url = url;
		this.user = user;
		this.password = password;
		this.dbdrv = dbdrv;

		try {
		  connect();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Exception: ETLREP connection initially failed.", e);
      throw new RockException("ETLREP connection initially failed.");
    }
	}

	/**
	 * Persists a set. Does not throw exceptions.
	 */
  public void newSet(final EngineThread set) {
    if (set == null || !set.isPersistent()) {
      return;
    }

    LOG.finest(set + " new set.");

    try {

      if (set.getPersistentID() != null && persistentSets.containsKey(set.getPersistentID())) {
        LOG.finest(set + " already persisted.");
      } else {

        synchronized (lastPersID) {
          set.setPersistentID(++lastPersID);
        }

        final String obj = TransferActionBase.propertiesToString(set.getSerial());

        try {
          final Priorityqueue queue = new Priorityqueue(etlrep);
          queue.setQueueid(set.getPersistentID());
          queue.setObj(obj);
          queue.saveDB();
          LOG.finest(set + "Successfully added.");
        } catch (SQLException e) {
          int retriesForRepdb = 15;
          // Default retries for Repdb is 15. Sleep for a second
          // between successive retries. If repdb is not available for
          // more than 15 seconds, DBConnectionMonitor will anyway freeze the
          // priority queue.
          boolean successful = false;
          try {
            retriesForRepdb = findOutNumberOfRetries();
          } catch (Exception e1) {
            LOG.finest("Could not read numRetriesForRepDbConn from " + System.getProperty("dc5000.config.directory")
                + "/static.properties");
          }
          final int localcount = retriesForRepdb;
          LOG.finest("newSet: DB connection maybe closed. Will Retry " + retriesForRepdb + " times");

          while (retriesForRepdb > 0) {
            retriesForRepdb--;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // Do nothing.
            }
            try {
              LOG.finest("Retry : " + (localcount - retriesForRepdb));
              connect();
              final Priorityqueue queue = new Priorityqueue(etlrep);
              queue.setQueueid(set.getPersistentID());
              queue.setObj(obj);
              queue.saveDB();
              successful = true;
              LOG.finest(set + "Successfully added.");
              break;
            } catch (Exception exceptionObject) {
              if (retriesForRepdb <= 0) {
                LOG.log(Level.WARNING, "Could not add set with persistent id " + set.getPersistentID() + " to repdb.",
                    exceptionObject);
              } else {
                LOG.finest("Could not add set with persistent id " + set.getPersistentID() + " to repdb. Will try "
                    + retriesForRepdb + " more times");
              }
            }
          }
          if (!successful) {
            LOG.warning(set + ": Could not be added to repdb.");
            LOG.log(Level.WARNING,
                "Initial SQLException which triggered retry for set with persistent id :" + set.getPersistentID()
                    + " is : ", e);
          }
        }

        persistentSets.put(set.getPersistentID(), set);
        LOG.finest(set + " successfully persisted.");
      }
    } catch (Exception exp) {
      LOG.log(Level.WARNING, "Set persistence failed", exp);
    }
  }

	/**
	 * The set has been dropped from PriorityQueue.
	 */
	public void droppedSet(final EngineThread set) {
		executedSet(set);
	}

	/**
	 * The set has been successfully/unsucessfully executed.
	 */
  public void executedSet(final EngineThread set) {

    if (set.isPersistent() && set.getPersistentID() != null) {

      LOG.finest(set + " removing set.");

      try {
        try {
          final Priorityqueue queue = new Priorityqueue(etlrep);
          queue.setQueueid(set.getPersistentID());
          queue.deleteDB();
          LOG.finest(set + " successfully removed.");
        } catch (SQLException e) {

          int retriesForRepdb = 15;
          boolean successful = false;
          // Default numRetriesForRepDbConn is 15. Sleep for a second
          // between successive retries. If repdb is not available for
          // more than 15 seconds, DBConnectionMonitor will anyway make
          // priority queue inactive.
          try {
            retriesForRepdb = findOutNumberOfRetries();
          } catch (Exception e1) {
            LOG.finest("Could not read numRetriesForRepDbConn from " + System.getProperty("dc5000.config.directory")
                + "/static.properties");
          }

          LOG.finest("executedSet: DB connection maybe closed. Will Retry " + retriesForRepdb + " times");

          final int localCopyOfretriesForRepdb = retriesForRepdb;

          while (retriesForRepdb > 0) {
            retriesForRepdb--;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // Do nothing.
            }
            try {
              LOG.finest("Retry : " + (localCopyOfretriesForRepdb - retriesForRepdb));
              connect();
              final Priorityqueue queue = new Priorityqueue(etlrep);
              queue.setQueueid(set.getPersistentID());
              queue.deleteDB();
              successful = true;
              LOG.finest(set + ": Successfully removed.");
              break;
            } catch (Exception exceptionObject) {
              if (retriesForRepdb <= 0) {
                LOG.log(Level.WARNING, "Could not delete set with persistent id " + set.getPersistentID()
                    + " from repdb.", exceptionObject);
              } else {
                LOG.finest("Could not delete set with persistent id " + set.getPersistentID()
                    + " from repdb. Will try " + retriesForRepdb + " more times");
              }
            }
          }

          if (!successful) {
            LOG.warning(set + "Could not be deleted from repdb.");
            LOG.log(Level.WARNING,
                "Initial SQLException which triggered retry for set with persistent id :" + set.getPersistentID(), e);
          }
        }

        persistentSets.remove(set.getPersistentID());
        LOG.finest(set + " successfully persisted.");

      } catch (Exception exp) {
        LOG.log(Level.SEVERE, "Persistent remove failed exceptionally", exp);
      }
    }
  }


  /**
   * Finds out number of retries for repdb i.e numRetriesForRepDbConn from
   * /eniq/sw/conf/static.properties.
	 */
  private int findOutNumberOfRetries() {

    final String numRetriesForRepDbConn = StaticProperties.getProperty("numRetriesForRepDbConn", "15");
    final int retriesForRepdb = Integer.parseInt(numRetriesForRepDbConn);

    LOG.finest("retriesForRepdb is " + retriesForRepdb);

    return retriesForRepdb;

  }

  /**
	 * Load persisted sets from database. This method is executed on startup of
	 * engine.
	 */
	public List<EngineThread> getSets(final EngineCom eCom, final PluginLoader pLoader) throws RockException, SQLException {

		final List<EngineThread> sets = new ArrayList<EngineThread>();

		final Priorityqueue p_cond = new Priorityqueue(etlrep);
		final PriorityqueueFactory p_fact = new PriorityqueueFactory(etlrep, p_cond);

		for (Priorityqueue pers : p_fact.get()) {

			try {

				final Properties props = SQLOperation.stringToProperties(pers.getObj());

				final EngineThread set = createSet(eCom, pLoader, props);

				persistentSets.put(set.getPersistentID(), set);
				sets.add(set);

				if(set.getPersistentID() > lastPersID) {
					synchronized(lastPersID) {
						lastPersID = set.getPersistentID();
					}
				}

			} catch (Exception e) {
				LOG.log(Level.WARNING, "Failed to initialize persisted set", e);
				try {
					pers.deleteDB();
				} catch(Exception ex) {
					LOG.log(Level.WARNING, "Failed deleting troublemaking set", ex);
				}
			}

		} // foreach set in etlrep.Priorityqueue

		LOG.info("Found " + sets.size() + " persistent sets");

		return sets;

	}

	private EngineThread createSet(final EngineCom eCom, final PluginLoader pLoader, final Properties props)
			throws EngineMetaDataException {

		LOG.fine("Creating set " + props.getProperty(COL_NAME) + " techpack " + props.getProperty(COL_SET_NAME));

		String setName = props.getProperty(SET_NAME);
		final EngineThread set;
		if(setName == null || setName.isEmpty()){
		  set = new EngineThread(etlrep, props.getProperty(COL_SET_NAME), props.getProperty(COL_NAME),
	        pLoader, Logger.getLogger(props.getProperty(LOGGER_NAME)), eCom);
		} else{
      set = new EngineThread(etlrep, props.getProperty(COL_SET_NAME), props.getProperty(COL_NAME),
          setName, pLoader, Logger.getLogger(props.getProperty(LOGGER_NAME)), eCom);
    }
		
		set.setSetPriority(Long.valueOf(props.getProperty(PRIORITY)));
		set.setQueueTimeLimit(Long.valueOf(props.getProperty(TIMELIMIT)));
		set.setSetType(props.getProperty(SETTYPE));

		if (!Boolean.valueOf(props.getProperty(ACTIVE))) {
			set.setActive(false);
		}

		set.setPersistent(true);
		set.setPersistentID(Long.valueOf(props.getProperty(PERSISTENT_ID)));

		if (props.getProperty(EARLIEST_EXEC) != null) {
			set.setEarliestExection(getDate(props.getProperty(EARLIEST_EXEC)));
		}

		if (props.getProperty(LATEST_EXEC) != null) {
			set.setLatestExecution(getDate(props.getProperty(LATEST_EXEC)));
		}

		if (props.getProperty(SCHEDULING_INFO) != null) {
			set.setSchedulingInfo(props.getProperty(SCHEDULING_INFO));
		}

		if (props.getProperty(XP_TABLENAMES) != null) {
			final String[] tabs = props.getProperty(XP_TABLENAMES).split(",");
			for (String table : tabs) {
				if (table.length() > 0) {
					set.addSetTable(table);
				}
			}
		}

		return set;
	}


	/**
	 * Connecting to ELTREP database.
	 * @return
	 * @throws SQLException
	 * @throws RockException
	 */
  private void connect() throws SQLException, RockException {

    try {
      this.etlrep = new RockFactory(url, user, password, dbdrv, "PQPersistence", true);
    } catch (SQLException sql) {
      LOG.log(Level.FINEST, "SQLException: ETLREP connection failed");
      throw sql;
    } catch (RockException rock) {
      LOG.log(Level.WARNING, "RockException: ETLREP connection failed", rock);
      throw rock ;
    }
  }

	/**
	 * A method to fool PMD, because Date does not have proper
	 * Date.valueOf(long/string) function.
	 */
	private Date getDate(final String date) throws NumberFormatException {
		return new Date(Long.valueOf(date));
	}

}
