package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.distocraft.dc5000.etl.scheduler.SchedulerConnect;

/**
 * triggers a set or list of sets in scheduler. sets are defined in
 * action_contents column delimited by comma ','.
 * 
 * ex. set1,set2,set3 would trigger sets set1,set2 and set3. if triggered set is
 * not in schedule or it is inactive (on hold) set is not exeuted.
 * 
 */
public class TriggerAction extends TransferActionBase {

	private Meta_transfer_actions actions;
	private final Logger log;

	/**
	 * Empty protected constructor
	 */
	protected TriggerAction() {
		log = Logger.getLogger("");
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
	public TriggerAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.actions = trActions;

		try {
			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

			final String tech_pack = collSet.getCollection_set_name();
			final String set_type = collection.getSettype();
			final String set_name = collection.getCollection_name();

			log = Logger.getLogger("etl." + tech_pack + "." + set_type + "." + set_name + ".action.TriggerSetInSchedulerAction");

		} catch (Exception e) {
			throw new EngineMetaDataException("ExecuteSetAction unable to initialize loggers", e, "init");
		}

	}

	public void execute() throws EngineException {

		final List<String> list = new ArrayList<String>();

		log.finest("Starting TriggerSetListInSchedulerAction");

		try {

			// read possible elements from action_content column
			final StringTokenizer token = new StringTokenizer(this.actions.getAction_contents(), ",");
			while (token.hasMoreElements()) {

				final String name = token.nextToken();

				if (!name.trim().equalsIgnoreCase("")) {
					list.add(name);
					log.fine("Reading sets from action_contents: " + name);
				}

			}

			if (list != null) {
				
				final ISchedulerRMI scheduler = connect();
				
				for (String name : list) {					
					log.fine("Triggering set " + name);
					scheduler.trigger(name);
				}

			}

		} catch (Exception e) {
			throw new EngineException("Exception in TriggerSetListInSchedulerAction", new String[] { "" }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);

		}

	}

	/**
	 * Return scheduler RMI object. This method is implemented for to be
	 * overwritten in unit tests.
	 */
	protected ISchedulerRMI connect() throws IOException, NotBoundException {
		return SchedulerConnect.connectScheduler();
	}
	
}
