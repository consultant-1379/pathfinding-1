package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_schedulings;
import com.distocraft.dc5000.etl.rock.Meta_schedulingsFactory;
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
public class SetTypeTriggerAction extends TransferActionBase {

	private Meta_transfer_actions actions;
	private RockFactory rockFact;

	private final Logger log;

	/**
	 * Empty protected constructor
	 * 
	 */
	protected SetTypeTriggerAction() {
		this.log = Logger.getLogger("");
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
	public SetTypeTriggerAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.log = Logger.getLogger(clog.getName() + ".SetTypeTrigger");
		this.rockFact = rockFact;
		this.actions = trActions;
	}

	public void execute() throws EngineException {

		try {

			final Properties properties = TransferActionBase.stringToProperties(this.actions.getAction_contents());

			int triggered = 0;

			final String setType = properties.getProperty("setType", "");

			log.info("Triggering " + setType + " sets.");

			final ISchedulerRMI scheduler = connect();
			
			final Meta_collection_sets sets = new Meta_collection_sets(rockFact);
			sets.setEnabled_flag("Y");
			final Meta_collection_setsFactory setsf = new Meta_collection_setsFactory(rockFact, sets);

			for (Meta_collection_sets tmp0 : setsf.get()) {

				final Meta_collections cols = new Meta_collections(rockFact);
				cols.setCollection_set_id(tmp0.getCollection_set_id());
				cols.setEnabled_flag("Y");
				cols.setSettype(setType);
				final Meta_collectionsFactory colsf = new Meta_collectionsFactory(rockFact, cols);

				for (Meta_collections tmp1 : colsf.get()) {

					final Meta_schedulings msche = new Meta_schedulings(rockFact);
					msche.setCollection_id(tmp1.getCollection_id());
					msche.setExecution_type("wait");
					final Meta_schedulingsFactory mschef = new Meta_schedulingsFactory(rockFact, msche);

					for (Meta_schedulings tmp2 : mschef.get()) {
						final String name = tmp2.getName();
						
						scheduler.trigger(name);
						triggered++;
					}
				}
			}

			log.info(triggered + " Sets Triggered.");

		} catch (Exception e) {

			log.severe(e.getStackTrace() + "\r\n" + new String[] { this.getTrActions().getAction_contents() });
			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	public void aexecute() throws EngineException {

		final List<String> list = new ArrayList<String>();

		log.finest("Starting SetTypeTriggerAction");

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
			throw new EngineException("Exception in SetTypeTriggerAction", new String[] { "" }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_SYSTEM);

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
