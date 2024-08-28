package com.distocraft.dc5000.etl.engine.structure;

import com.distocraft.dc5000.common.StaticProperties;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.main.TransferEngine;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_parameters;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_batches;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.DatabaseConnections;

/**
 * A class for transfer collection. A starting point for a transfer.
 * 
 * @author Jukka Jaaheimo
 */
public final class TrCollection {

  private static final String SCHEDULING_ALARM_INTERFACE_RD = "Scheduling_AlarmInterface_RD";

  public static final String SESSIONTYPE = "engine";

  private final Meta_versions version;

  private final Long collectionSetId;

  private final Meta_collections collection;

  private final RockFactory etlrep;

  private List<TransferAction> vecTrActions;

  private Meta_transfer_batches trBatch;

  private ConnectionPool connectionPool;

  // Unused parameters
  private int maxErrs;

  private int maxFkErrs;

  private int maxColConstErrs;

  private int fkErrors;

  private int colConstErrors;

  private String batchColumnName = null;

  private PluginLoader pLoader;

  private Meta_collection_sets collSet;

  private final SetContext sctx;

  private Logger log = Logger.getLogger("etlengine.TrCollection");
  
  private final EngineCom eCom;

  private TransferAction currentAction = null;

  private String setType;

  private final PriorityQueue priorityQueue;

  private int error = 0;

  private final String slotType;

  private static List<String> nonRemappableServices = null;//Arrays.asList("etlrep", "dwhrep", "dwh", "dwhdb");
  private static List<String> nonRemappableActions = null;//Arrays.asList("etlrep", "dwhrep");

  /**
   * Empty protected constructor. Testing only.
   */
  protected TrCollection() {
    this.sctx = new SetContext();
    this.eCom = null;
    this.version = null;
    this.collectionSetId = null;
    this.collection = null;
    this.etlrep = null;
    this.priorityQueue = null;
    this.slotType = null;
  }

  /**
   * Constructor for starting the transfer
   * 
   * @param rockFact
   *          the database connection for the metadata
   * @param versionNumber
   *          version number
   * @param collectionSetId
   *          the id of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   */
  public TrCollection(final RockFactory rockFact, final Meta_versions version, final Long collectionSetId,
      final Meta_collections collection, final PluginLoader pLoader, final EngineCom eCom,
      final PriorityQueue priorityQueue, final Integer slotId, final String slotType)
      throws EngineMetaDataException {

    this.eCom = eCom;
    this.etlrep = rockFact;
    this.version = version;
    this.collectionSetId = collectionSetId;
    this.collection = collection;
    this.pLoader = pLoader;
    this.priorityQueue = priorityQueue;
    this.slotType = slotType;

    this.maxErrs = this.collection.getMax_errors().intValue();
    this.maxFkErrs = this.collection.getMax_fk_errors().intValue();
    this.maxColConstErrs = this.collection.getMax_col_limit_errors().intValue();
    this.setType = collection.getSettype();

    setupNonMappableServiceTypes();

    this.sctx = new SetContext();
    boolean errorDetected = false;

    try {

      // Get collection set name
      final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
      whereCollSet.setEnabled_flag("Y");
      whereCollSet.setCollection_set_id(collectionSetId);
      collSet = new Meta_collection_sets(rockFact, whereCollSet);

      try {
        final Meta_parameters parameters = new Meta_parameters(this.etlrep, this.collection.getVersion_number());
        this.batchColumnName = parameters.getBatch_column_name();
      } catch (SQLException e) {
        log.log(Level.FINEST, "Get BatchColumnName failed", e);
      }

      this.connectionPool = new ConnectionPool(this.etlrep);

      this.trBatch = new Meta_transfer_batches(this.etlrep);
      this.trBatch.setVersion_number(this.collection.getVersion_number());
      this.trBatch.setCollection_set_id(this.collectionSetId);
      this.trBatch.setCollection_id(this.collection.getCollection_id());
      this.trBatch.setStart_date(new Timestamp(System.currentTimeMillis()));
      this.trBatch.setMeta_collection_set_name(this.collSet.getCollection_set_name());
      this.trBatch.setMeta_collection_name(this.collection.getCollection_name());
      this.trBatch.setSettype(this.collection.getSettype());
      this.trBatch.setStatus("STARTED");
      this.trBatch.setFail_flag("N");
      this.trBatch.setSlot_id(slotId);
      this.trBatch.setScheduling_info(TransferEngine.formatSchedulingInfo(this.collection.getScheduling_info()));
      this.trBatch.setId(Long.valueOf(SessionHandler.getSessionID(SESSIONTYPE)));
      if (this.slotType != null) {
        this.trBatch.setService_node(this.slotType.toLowerCase());
      } else {
        this.trBatch.setService_node(null);        
      }
      
      try {
        this.trBatch.insertDB();
      } catch (SQLException sybExc) {
        error = sybExc.getErrorCode();

        if (error == 2601) { // Primary key not Unique
          try {
            long maxId = maxIDCheck();
            SessionHandler.setDBMaxSessionID(maxId);
            maxId++;
            this.trBatch.setId(maxId);
            this.trBatch.insertDB();
            log.fine("ID has been changed to match the database value.");
          } catch (Exception ee) {
            log.log(Level.WARNING, "Exception occured while trying to recover from session ID mismatch", ee);
          }
        }
      }

      final Logger logPrev=log;
            
      log = Logger.getLogger("etl." + this.collSet.getCollection_set_name() + "." + this.collection.getSettype() + "."
          + collection.getCollection_name());
     
      if (log==null)
      {
    	  logPrev.warning("logger was null , using old logger");
    	  log=logPrev;
    	  
    	  }
      this.vecTrActions = getTransferActions();

      log.finest("Set initialized");

    } catch (EngineMetaDataException emd) {
    	errorDetected = true;
    	throw emd;
    } catch (Exception e) {
    	errorDetected = true;
    	log.log(Level.WARNING, "Set initialization failed", e);
    	throw new EngineMetaDataException("Set init failed exceptionally", e, "constructor");
    } finally {
    	if(errorDetected && this.connectionPool != null) {
    		log.log(Level.INFO,"Closing the initated connection Pool as an error detected with the set!");
    		int closedConnectionCount = this.connectionPool.cleanPool();
    		log.log(Level.FINER,closedConnectionCount+" connections have been closed from the connection pool");
    	}
    }

  }

  private void setupNonMappableServiceTypes() {
    if (nonRemappableServices == null) {
      final String slots = StaticProperties.getProperty("slots.nonmappable", "etlrep, dwhrep, dwh,dwhdb,engine");
      nonRemappableServices = new ArrayList<String>();
      splitAndAdd(slots, ",", nonRemappableServices);
    }
    if (nonRemappableActions == null) {
      final String actions = StaticProperties.getProperty("actions.nonmappable", "etlrep, dwhrep");
      nonRemappableActions = new ArrayList<String>();
      splitAndAdd(actions, ",", nonRemappableActions);
    }
  }
  
  private void splitAndAdd(final String string, final String delim, final List<String> list){
    final StringTokenizer st = new StringTokenizer(string, delim);
    while(st.hasMoreTokens()){
      list.add(st.nextToken().trim());
    }
  }

  private Long getRemappedConnectionId(Long currentConnectionId) {
    String currentTypeName = null;
    final Meta_databases currentWhere = new Meta_databases(this.etlrep);
    currentWhere.setConnection_id(currentConnectionId);
    try {
      Meta_databasesFactory fac = new Meta_databasesFactory(this.etlrep, currentWhere);
      final List<Meta_databases> meta_databases = fac.get();
      if (meta_databases.size() == 1) {
        currentTypeName = meta_databases.get(0).getType_name();
      } else {
        log.log(Level.WARNING, "Exact connection mapping for current connection " + currentConnectionId
            + " can not be found. (Found " + meta_databases.size() + " connections).");
      }
    } catch (SQLException e) {
      log.log(Level.WARNING, "Searching of the current connection " + currentConnectionId + " failed. SQL error: " + e.getMessage());
    } catch (RockException e) {
      log.log(Level.WARNING, "Searching of the current connection " + currentConnectionId + " failed. Rock error: " + e.getMessage());
    }
    
    if (currentTypeName != null) {
      final Meta_databases remappedWhere = new Meta_databases(this.etlrep);
      remappedWhere.setType_name(currentTypeName);
      remappedWhere.setConnection_name(this.slotType);
      try {
        Meta_databasesFactory fac = new Meta_databasesFactory(this.etlrep, remappedWhere);
        final List<Meta_databases> meta_databases = fac.get();
        if (meta_databases.size() == 1) {
          return meta_databases.get(0).getConnection_id();
        } else {
          log.log(Level.WARNING, "Exact connection mapping for SERVICE_NAME:" + slotType
              + " can not be found. (Found " + meta_databases.size() + " connections).");
        }
      } catch (SQLException e) {
        log.log(Level.WARNING, "Remapping of the connection " + currentTypeName + " / " + this.slotType + " failed. SQL error: " + e.getMessage());
      } catch (RockException e) {
        log.log(Level.WARNING, "Remapping of the connection " + currentTypeName + " / " + this.slotType + " failed. Rock error: " + e.getMessage());
      }
    }
    return null;
  }

  /**
   * Creates executed transfer action objects
   */
  private List<TransferAction> getTransferActions() throws EngineMetaDataException {
    log.finest("Starting to create actions...");

    final List<TransferAction> vec = new ArrayList<TransferAction>();
    final String collectionName = collection.getCollection_name();

    try {
      final Meta_transfer_actions whereActions = new Meta_transfer_actions(this.etlrep);
      whereActions.setVersion_number(this.collection.getVersion_number());
      whereActions.setCollection_set_id(this.collectionSetId);
      whereActions.setCollection_id(this.collection.getCollection_id());
      final Meta_transfer_actionsFactory dbTrActions = new Meta_transfer_actionsFactory(this.etlrep, whereActions,
          "ORDER BY ORDER_BY_NO");
      String baseTable = "";
      final String collecionSetName = this.collSet.getCollection_set_name();

      final List<Meta_transfer_actions> dbVec = dbTrActions.get();

      final String dcType = StaticProperties.getProperty("directory_checker_type", "new");
      final boolean userScriptedDirChecker = "new".equalsIgnoreCase(dcType);
           
      final String dirCheckerExclusion = StaticProperties.getProperty("directory_checker_exclusion", "Directory_Checker_DWH_MONITOR");
      final String[] dirCheckerExclusionArray = dirCheckerExclusion.split(",");
      final boolean dirCheckerExclusionCheck = Arrays.asList(dirCheckerExclusionArray).contains(collectionName);
           
      if (!dirCheckerExclusionCheck && userScriptedDirChecker && collectionName.startsWith("Directory_Checker_")) {
        log.fine("Using Scripted Directory Checker");
        final Logger alog = Logger.getLogger("etl." + collecionSetName + "." + setType + "." + collectionName + ".1");
        whereActions.setEnabled_flag("Y");
        
        whereActions.setAction_type("CreateDir");
        
        // Check if action should be remapped. Now number of IQ reader and writer nodes are
        // dynamic and can be extended or reduced at any time on a production system
        // so this checks if there is need to remap database connections.
        final Meta_transfer_actions dbact = new Meta_transfer_actions(this.etlrep, whereActions);
        if (this.slotType != null && !nonRemappableServices.contains(this.slotType)) {
          Long connectionId = getRemappedConnectionId(dbact.getConnection_id()); 
          if (connectionId!= null && !connectionId.equals(dbact.getConnection_id())) {
            alog.info("Remapped connection_id from " + dbact.getConnection_id() + " to " + connectionId);
            whereActions.setConnection_id(connectionId);
          }
        }
        
        whereActions.setAction_type("DirSetPermissions");

        final TransferAction trAction = new TransferAction(this.etlrep, this.version, this.collectionSetId,
            this.collSet, this.collection, (long) -1, whereActions, this.trBatch.getId(), this.connectionPool,
            this.batchColumnName, this.pLoader, this.sctx, alog, eCom, priorityQueue);
        vec.add(trAction);
      } else {
        for (Meta_transfer_actions dbTrAction : dbVec) {

          final String actionType = dbTrAction.getAction_type();
          final String actionName = dbTrAction.getTransfer_action_name();

          if (actionType.equals("Aggregation") && actionName.startsWith("Aggregator")) {
            baseTable = getAggregationBaseTable(actionName);
            log.finest("Base table for: " + actionType + " is " + baseTable);
          }

          if (actionType.equals("Loader")) {
            baseTable = actionName.substring("Loader_".length()) + "_RAW";
            log.finest("Base table for: " + actionType + " is " + baseTable);
          }

          if (actionType.equals("UnPartitioned Loader")) {
            baseTable = actionName.substring("UnPartitioned_Loader_".length()) + "_RAW";
            log.finest("Base table for: " + actionType + " is " + baseTable);
          }

          final Logger alog = Logger.getLogger("etl." + collecionSetName + "." + setType + "." + collectionName + "."
              + dbTrAction.getOrder_by_no().intValue());

          // Check if action should be remapped. Now number of IQ reader and writer nodes are
          // dynamic and can be extended or reduced at any time on a production system
          // so this checks if there is need to remap database connections.
          
      	// Does this connectionId need remapping - for e.g.
      	// the slotType could be dwh_reader_1 but the action could require dwhrep or etlrep
      	// In such case DO NOT remap.
          if (isConnectionIdRemappable(dbTrAction.getConnection_id()) && this.slotType != null && !nonRemappableServices.contains(this.slotType)) {        	  
            Long connectionId = getRemappedConnectionId(dbTrAction.getConnection_id());        
            if (connectionId != null && !connectionId.equals(dbTrAction.getConnection_id())) {
              alog.info("Remapped connection_id from " + dbTrAction.getConnection_id() + " to " + connectionId);
              dbTrAction.setConnection_id(connectionId);
            }
          }

          try {
            final TransferAction trAction = new TransferAction(this.etlrep, this.version, this.collectionSetId,
                this.collSet, this.collection, dbTrAction.getTransfer_action_id(), dbTrAction, this.trBatch.getId(),
                this.connectionPool, this.batchColumnName, this.pLoader, this.sctx, alog, eCom, priorityQueue);

            vec.add(trAction);
          } catch (EngineMetaDataException e) {
            log.warning("Could not create TransferAction \"" + actionName + "\"");

            throw e;
          }
        }
      }

      // this will add the TriggerAlarmAction to the Loader(Unpartitioned
      // Loaders also) and Aggregator Sets
      if ((setType.equals("Loader") || setType.equals("Aggregator")) && !baseTable.equals("")) {
        final RockFactory dwhrep = DatabaseConnections.getDwhRepConnection();
        try {
          if (hasSimultaneousReport(dwhrep, baseTable)) {
            log.info(collectionName + " has alarm reports defined for the " + baseTable + " table");
            final Meta_transfer_actions tempAction = (Meta_transfer_actions) dbVec.get(dbVec.size() - 1);
            final Meta_transfer_actions triggerAlarmAction = createTriggerAlarmAction(tempAction);

            final Logger actionLog = Logger.getLogger("etl." + collecionSetName + "." + setType + "." + collectionName
                + "." + tempAction.getOrder_by_no().intValue() + 1);

            try {
              log.info("Adding Dynamic Alarm Action " + triggerAlarmAction.getTransfer_action_name() + " to "
                  + collecionSetName + ".");
              final TransferAction trAction = new TransferAction(this.etlrep, this.version, this.collectionSetId,
                  this.collSet, this.collection, triggerAlarmAction.getTransfer_action_id(), triggerAlarmAction,
                  this.trBatch.getId(), this.connectionPool, this.batchColumnName, this.pLoader, this.sctx, actionLog,
                  eCom, priorityQueue);

              vec.add(trAction);
              log.info("Added Dynamic Alarm Action to " + collecionSetName + ".");
            } catch (EngineMetaDataException e) {
              log.warning("Could not create TransferAction \"" + triggerAlarmAction.getTransfer_action_name() + "\"");

              throw e;
            }
          } else {
            log.info(collecionSetName + " has no alarms defined for " + baseTable);
          }
        } finally {
          try {
            dwhrep.getConnection().close();
          } catch (SQLException e) {
            log.finest(this.collectionSetId + "/" + this.collection.getCollection_id()
                + " failed to close dwhrep connection.");
          }
        }

      }

      log.finer("Successfully created " + vec.size() + " actions");

    } catch (RockException re) {
      setBatchFailed();
      throw new EngineMetaDataException("Error querying META_TRANSFER_ACTIONS for " + this.collectionSetId + "/"
          + this.collection.getCollection_id(), re, "getTransferActions");
    } catch (SQLException se) {
      setBatchFailed();
      throw new EngineMetaDataException("Error querying META_TRANSFER_ACTIONS for " + this.collectionSetId + "/"
          + this.collection.getCollection_id(), se, "getTransferActions");
    } catch (EngineMetaDataException e) {
      setBatchFailed();
      throw e;
    }

    return vec;
  }

  /**
 * Checking whether to remap a connection id for an action which could be defined
 * for dwhrep or etlrep. The connection id is not remappable for this scenario.
 * Is should run on the co-ordinator.
 * @param connectionId
 * @return
 */
  private boolean isConnectionIdRemappable(final Long currentConnectionId){
	  boolean isRemappable = true;
	    final Meta_databases currentWhere = new Meta_databases(this.etlrep);
	    currentWhere.setConnection_id(currentConnectionId);
	    try {
	      Meta_databasesFactory fac = new Meta_databasesFactory(this.etlrep, currentWhere);
	      final List<Meta_databases> meta_databases = fac.get();
	      if (meta_databases.size() == 1) {
          final String connectionName = meta_databases.get(0).getConnection_name();
	    	  log.log(Level.FINEST,"Connection Name for Id:"+currentConnectionId+" is "+connectionName);
	    	  if(nonRemappableActions.contains(connectionName)){
	    		  isRemappable =  false;
	    	  }	    	  
	      } else {
	        log.log(Level.WARNING, "Exact connection mapping for current connection " + currentConnectionId
	            + " can not be found. (Found " + meta_databases.size() + " connections).");
	      }
	    } catch (SQLException e) {
	      log.log(Level.WARNING, "Searching of the current connection " + currentConnectionId + " failed. SQL error: " + e.getMessage());
	    } catch (RockException e) {
	      log.log(Level.WARNING, "Searching of the current connection " + currentConnectionId + " failed. Rock error: " + e.getMessage());
	    }
	    return isRemappable;
	  
  }
  /**
   * This will create the TriggerAlarmAction on the end of the loader Set.
   * 
   * @param action_contents
   * @param maxOrderNo
   * @param connectionID
   * @return
   */
  private Meta_transfer_actions createTriggerAlarmAction(final Meta_transfer_actions tempAction) {
    Long transferActionID = 0L;
    if (setType.equals("Loader")) {
      transferActionID = 1234567890L;
    } else if (setType.equals("Aggregator")) {
      transferActionID = 1234567899L;
    }

    final Meta_transfer_actions triggerAlarmAction = new Meta_transfer_actions(this.etlrep);

    triggerAlarmAction.setVersion_number(this.collection.getVersion_number());
    triggerAlarmAction.setTransfer_action_id(transferActionID);
    triggerAlarmAction.setCollection_id(this.collection.getCollection_id());
    triggerAlarmAction.setCollection_set_id(this.collectionSetId);
    triggerAlarmAction.setAction_type("TriggerScheduledSet");
    triggerAlarmAction.setTransfer_action_name("TriggerAlarmsAction_" + this.collection.getCollection_name());
    triggerAlarmAction.setOrder_by_no(tempAction.getOrder_by_no() + 1);
    triggerAlarmAction.setEnabled_flag("Y");
    triggerAlarmAction.setConnection_id(tempAction.getConnection_id());
    triggerAlarmAction.setAction_contents_01(SCHEDULING_ALARM_INTERFACE_RD);
    return triggerAlarmAction;
  }

  /**
   * Checks if the basetable has any reports
   * 
   * @return
   */
  private boolean hasSimultaneousReport(final RockFactory dwhrep, final String baseTable) {

    if (baseTable == null || baseTable.equals("")) {
      log.info(collection.getCollection_name() + " has no Alarm Report(s)");
      return false;
    }

    final Class<?> clas;
    try {
      clas = Class.forName("com.ericsson.eniq.etl.alarm.RockAlarmConfigCache");
    } catch (ClassNotFoundException e) {
      log.info("RockAlarmConfigCache not found. Alarm module might not be installed. " + e.getMessage());
      return false;
    }

    try {
      final Method metTest = clas.getMethod("hasSimultanousReport", String.class);
      final Boolean ret = (Boolean) metTest.invoke(null, baseTable);
      return ret.booleanValue();
    } catch (SecurityException e) {
      log.info("Security problem with Alarm module. " + e.getMessage());
      return false;
    } catch (NoSuchMethodException e) {
      log.info("Method hasSimultanousReport not found. Wrong Alarm module version might be installed. "
          + e.getMessage());
      return false;
    } catch (IllegalArgumentException e) {
      log.info("Method hasSimultanousReport signature changed. Wrong Alarm module version might be installed. "
          + e.getMessage());
      return false;
    } catch (InvocationTargetException e) {
      log.info("Calling method hasSimultanousReport failed. Wrong Alarm module version might be installed. "
          + e.getMessage());
      return false;
    } catch (IllegalAccessException e) {
      log.info("Running method hasSimultanousReport failed. Wrong Alarm module version might be installed. "
          + e.getMessage());
      return false;
    }

  }

  /**
   * Gets the name of the base table the alarm will be based on
   * 
   * @param aggregationName
   * @return baseTable name
   */
  private String getAggregationBaseTable(String aggregationName) {

    String baseTable = "";
    final StringBuilder sql = new StringBuilder();
    final RockFactory dwhdb = DatabaseConnections.getDwhDBConnection();
    try {
      try {
        aggregationName = aggregationName.substring("Aggregator_".length());
        log.finest("Aggregator: " + aggregationName + " will be checked for an existing Target Table.");

        sql.append(" Select Target_Table From Log_AggregationRules");
        sql.append(" Where Aggregation = '" + aggregationName + "'");
        final Statement stmt = dwhdb.getConnection().createStatement();
        try {
          final ResultSet rs = stmt.executeQuery(sql.toString());
          log.info("ResultSet is: " + rs);
          try {
            if (rs.next()) {
              baseTable = rs.getString("Target_Table");
              log.info("Base Table is: " + baseTable);
            } else {
              log.warning("No Aggregation exists in Log_AggregationRules table for: " + aggregationName);
            }
          } finally {
            rs.close();
          }
        } finally {
          stmt.close();
        }
      } finally {
        dwhdb.getConnection().close();
      }
    } catch (Exception e) {
      log.warning("Could not retrieve Aggregation Base Table: " + sql.toString());
    }
    return baseTable;
  }

  /**
   * Executes all transfer actions of this collection
   * 
   * @param SetListener
   */
  public void execute(final SetListener setListener) throws Exception {

    int lastExecutedIndex = 0;
    final long start = System.currentTimeMillis();

    try {
      if (this.vecTrActions == null) {
        throw new Exception("Trying to execute set without actions");
      }

      log.fine("Set execution started. " + this.vecTrActions.size() + " actions");

      for (int i = 0; i < vecTrActions.size(); i++) {
        final TransferAction trAction = vecTrActions.get(i);

        lastExecutedIndex = i;

        currentAction = trAction;

        trAction.execute(this.maxErrs, this.maxFkErrs, this.maxColConstErrs, this.fkErrors, this.colConstErrors,
            setListener);

        currentAction = null;

        this.fkErrors += trAction.getFkErrors();
        this.colConstErrors += trAction.getColConstErrors();

        if (trAction != null && trAction.isGateClosed()) {
          log.fine("Set execution interreupted by " + trAction.transferActionType);
          break;
        }
      }

      setBatchOk();

    } catch (EngineException ee) {
      log.log(Level.SEVERE, "Set execution failed to EngineException: " + ee.getMessage());
      if (ee.getCause() != null) {
        log.log(Level.SEVERE, "Original cause", ee.getCause());
      }
      if (ee.getNestedException() != null){
          removeDataFromTarget(lastExecutedIndex);
          setBatchFailed();
        if (ee.getNestedException().toString().contains("SQL Anywhere Error -1009134") && (collection.getCollection_name().toString().contains("Loader_DIM_")) ){
        	//Re-Triggering Topology loaders which failed due to insufficient buffers for 'sort'
        		 final EngineAdmin admin = new EngineAdmin();
        	        admin.startSet(collSet.getCollection_set_name(), collection.getCollection_name(), "");
        }
        else{
        	log.log(Level.SEVERE, "Nested exception", ee.getNestedException());
        }
      }
      throw ee;
    } catch (Exception e) {
      log.log(Level.SEVERE, "Set execution failed exceptionally", e);

      removeDataFromTarget(lastExecutedIndex);
      setBatchFailed();

      throw e;
    } finally {
      final long total = System.currentTimeMillis() - start;
      final String collectionName = collection.getCollection_name();
      log.fine("Executed " + vecTrActions.size() + " action(s) for Set " + collectionName + " in " + total + "msec");
    }
  }


private void setBatchFailed() {
    try {

      if (this.trBatch == null) {
        log.warning("trBatch object was null. No need to set it with FAILED because initialization failed");
      } else {
        this.trBatch.setEnd_date(new Timestamp(System.currentTimeMillis()));
        this.trBatch.setFail_flag("Y");
        this.trBatch.setStatus("FAILED");
        this.trBatch.updateDB();

        if (log != null) {
          log.info("Logged failed set execution");
        }
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "Update failed status to etlrep.Meta_transfer_batches failed", e);
    }
  }

  private void setBatchOk() {
    try {

      this.trBatch.setEnd_date(new Timestamp(System.currentTimeMillis()));
      this.trBatch.setStatus("FINISHED");
      this.trBatch.updateDB();

      log.info("Logged successful set execution");

    } catch (Exception e) {
      log.warning("Log successful set execution failed");
    }
  }

  private void removeDataFromTarget(final int lastElement) throws EngineMetaDataException, RemoveDataException {
    if (this.vecTrActions == null) {
      return;
    }

    for (int i = 0; i < lastElement + 1; i++) {
      final TransferAction trAction = (TransferAction) this.vecTrActions.get(i);
      trAction.removeDataFromTarget();
    }

  }

  /**
   * Checking the maximum ID from META_TRANSFER_BATCHES table.
   * 
   * @return long type maximum ID value.
   * @throws SQLException
   */
  private long maxIDCheck() throws SQLException {

    long maxID = 0L;

    Statement stmt = null;
    ResultSet resultSet = null;

    try {

      stmt = etlrep.getConnection().createStatement();

      // Hard coded query to database for efficiency reasons
      resultSet = stmt.executeQuery("SELECT max(ID) FROM META_TRANSFER_BATCHES");

      if (resultSet.next()) {
        maxID = resultSet.getLong(1);
      }

    } catch (Exception e) {
      throw new SQLException("Could not retrieve maximum ID from META_TRANSFER_BATCHES.", e);
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (Exception e) {
          log.log(Level.INFO, "Cleanup failed", e);
        }
      }

      if (stmt != null) {
        try {
          stmt.close();
        } catch (Exception e) {
          log.log(Level.INFO, "Cleanup failed", e);
        }
      }
    }

    return maxID;
  }

  public int cleanCollection() {
    return this.connectionPool.cleanPool();
  }

  public String getName() {
    return this.collection.getCollection_name();
  }

  public Long getPriority() {
    return this.collection.getPriority();
  }

  public Long getID() {
    return this.collection.getCollection_id();
  }

  public Long getQueuTimeLimit() {
    return this.collection.getQueue_time_limit();
  }

  public String getSettype() {
    return this.collection.getSettype();
  }

  public String getEnabledFlag() {
    return this.collection.getEnabled_flag();
  }

  public String getHoldFlag() {
    return this.collection.getHold_flag();
  }

  public TransferAction getCurrentAction() {
    return currentAction;
  }

  public PriorityQueue getPriorityQueue() {
    return priorityQueue;
  }

}
