package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.distocraft.dc5000.etl.scheduler.SchedulerConnect;
import com.ericsson.eniq.common.DatabaseConnections;

/**
 * triggers an set or list of sets in scheduler. sets are defined in action_contents column delimited by comma ','. sets
 * are retrieved as an Set from SetContext. SetContext key is "parsedMeastypes"
 * 
 * ex. set1,set2,set3 would trigger sets set1,set2 and set3. if triggered set is not in schedule or it is inactive (on
 * hold) set is not exeuted.
 * 
 */
public class TriggerSetListInSchedulerAction extends TransferActionBase {

  private final Logger log;

  private final Meta_transfer_actions actions;  
  private final SetContext sctx;
  private final String setType;

  private final String baseTable;

  /**
   * Empty protected constructor
   * 
   */
  protected TriggerSetListInSchedulerAction() {
  	this.actions = null;
  	this.log = Logger.getLogger("etlengine.TriggerSetList");
  	this.sctx = null;
  	this.setType = null;
  	this.baseTable = null;
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
  public TriggerSetListInSchedulerAction(final Meta_versions version, final Long collectionSetId,
      final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
      final RockFactory rockFact, final Meta_transfer_actions trActions, final SetContext sctx)
      throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

    this.actions = trActions;
    this.sctx = sctx;

    try {
      final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
      whereCollSet.setEnabled_flag("Y");
      whereCollSet.setCollection_set_id(collectionSetId);
      final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

      final String tech_pack = collSet.getCollection_set_name();
      final String set_type = collection.getSettype();
      final String set_name = collection.getCollection_name();
      this.log = Logger
          .getLogger("etl." + tech_pack + "." + set_type + "." + set_name + ".TriggerSetInSchedulerAction");

    } catch (Exception e) {
      throw new EngineMetaDataException("ExecuteSetAction unable to initialize loggers", e, "init");
    }

    this.setType = collection.getSettype();
    final String setName = collection.getCollection_name();
    if ("Aggregator".equalsIgnoreCase(setType)) {
      this.baseTable = getAggregationBaseTable(setName);
      log.finest("Base Table for " + setName + "is " + baseTable);
    } else if ("Loader".equalsIgnoreCase(setType)) {
      this.baseTable = setName.substring("Loader_".length()) + "_RAW";
      log.finest("Base Table for " + setName + "is " + baseTable);
    } else {
      this.baseTable = "";
    }
  }

  public void execute() throws EngineException {

    final List<String> list = new ArrayList<String>();

    final Map<String, String> map = new HashMap<String, String>();

    @SuppressWarnings("unchecked")
    Set<String> meastypes = (Set<String>) sctx.get("parsedMeastypes");

    if (meastypes == null) {
      meastypes = new HashSet<String>();
    }

    try {

      final List<String> tmp = new ArrayList<String>();
      tmp.addAll(meastypes);

      for (int i = 0; i < tmp.size(); i++) {
        list.add("Loader_" + tmp.get(i));
      }

      // read possible elements from action_content column
      final StringTokenizer token = new StringTokenizer(this.actions.getAction_contents(), ",");

      while (token.hasMoreElements()) {

        final String name = token.nextToken();

        if (!name.trim().equalsIgnoreCase("")) {
          list.add(name);
          log.fine("Reading sets from action_contents: " + name);
        }

      }

      if (list.size() > 0) {

        for (String listItem : list) {
          final String name = listItem;
          log.info("Triggering set " + name);
        }

        map.put("setType", setType);
        map.put("setBaseTable", baseTable);

        final ISchedulerRMI scheduler = connect();
        scheduler.trigger(list, map);

      }

    } catch (Exception e) {
      log.log(Level.INFO, "Failed to trigger following sets because scheduler is not running.");

      if (list.size() > 0) {
        final Iterator<String> iter = list.iterator();
        while (iter.hasNext()) {
          final String listItem = iter.next();
          final String name = (String) listItem;
          log.info(name);
          final String setType = (String) map.get("setType");
          final String baseTable = (String) map.get("setBaseTable");
          log.fine(" params " + setType + "_" + baseTable);
        }
      } else {
        log.info("No sets to trigger.");
      }
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
        Statement stmt = null;
        ResultSet rs = null;
        stmt = dwhdb.getConnection().createStatement();
        try {
          rs = stmt.executeQuery(sql.toString());
          try {
            if (rs.next()) {
              baseTable = rs.getString("Target_Table");
              log.finest("Base table for: " + aggregationName + " is " + baseTable);
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
	 * Return scheduler RMI object. This method is implemented for to be
	 * overwritten in unit tests.
	 */
	protected ISchedulerRMI connect() throws IOException, NotBoundException {
		return SchedulerConnect.connectScheduler();
	}
  
}
