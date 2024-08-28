package com.distocraft.dc5000.etl.engine.system;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.Share;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * TODO intro TODO usage TODO used databases/tables TODO used properties
 * 
 * @author melantie Copyright Distocraft 2005 $id$
 */
public class TestAction extends TransferActionBase {

	private static final Logger log = Logger.getLogger("TestAction");

	/**
	 * Empty protected constructor
	 */
	protected TestAction() {
	}

	private Meta_collections collection;

	public TestAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);
		this.collection = collection;
	}

	/**
	 * Executes a SQL procedure
	 */

	public void execute() throws Exception {

		final String where = this.getTrActions().getWhere_clause();
		long executionTime = 60;

		String message = "Test: ";
		String errorMessage = "RANDOM ERROR";
		boolean createError = false;
		boolean showTime = true;
		int errorFrequency = 100;
		int errorWaitTime = 30;
		String sTables = null;

		try {
			if (where != null && where.length() > 0) {

				final Properties properties = TransferActionBase.stringToProperties(where);

				message = properties.getProperty("message", "");

				showTime = "TRUE".equalsIgnoreCase(properties.getProperty("showTime", "true"));
				errorMessage = properties.getProperty("errorMessage", "");

				createError = "TRUE".equalsIgnoreCase(properties.getProperty("createError", "false"));
				errorMessage = properties.getProperty("errorMessage", "");
				errorFrequency = Integer.parseInt(properties.getProperty("errorFrequency", "100"));
				errorWaitTime = Integer.parseInt(properties.getProperty("errorWaitTime", "0"));
				executionTime = Integer.parseInt(properties.getProperty("executionTime", "60"));
				sTables = properties.getProperty("addedtablename", "");
			}

		} catch (Exception e) {
			throw new Exception("Failed to read configuration from WHERE", e);
		}

		// Get all the basetablenames from tableName.
		final Share sh = Share.instance();
		log.fine("Trying to get first executionSlotProfile.");

		if (sh.get("executionSlotProfileObject") == null) {
			log.warning("share.get returned null.");
		} else {
			log.fine("share.get returned non null.");
		}

		final ExecutionSlotProfileHandler executionSlotProfile = (ExecutionSlotProfileHandler) sh
				.get("executionSlotProfileObject");

		final Iterator<ExecutionSlot> runningExecutionSlotsIterator = executionSlotProfile.getActiveExecutionProfile()
				.getAllRunningExecutionSlots();

		while (runningExecutionSlotsIterator.hasNext()) {
			final ExecutionSlot currentRunningExecutionSlot = runningExecutionSlotsIterator.next();
			log.info(" running set " + currentRunningExecutionSlot.getName());
		}

		final ExecutionSlotProfileHandler executionSlotHandler = (ExecutionSlotProfileHandler) sh
				.get("executionSlotProfileObject");
		final EngineThread set = executionSlotHandler.getActiveExecutionProfile().getRunningSet(
				this.collection.getCollection_name(), collection.getCollection_id().longValue());
		final List<String> settables = set.getSetTables();

		final StringTokenizer st = new StringTokenizer(sTables, ",");
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();
			settables.add(token);
		}

		log.info("Execution time: " + executionTime + " sec.");

		if (createError) {
			log.info("Error message: " + errorMessage);
			log.info("Error frequency: 1/" + errorFrequency);
			log.info("Error Wait time: " + errorWaitTime);

		} else {
			log.info("No errors created");
		}
			
		final Random rnd = new Random(System.currentTimeMillis());

		for (int i = 0; i < executionTime; i++) {
			Thread.sleep(1000);

			if (createError && i > errorWaitTime && rnd.nextInt(errorFrequency - 1) == 0) {
				throw new Exception(errorMessage);
			}
				
			if (showTime) {
				log.info(message + (executionTime - i));
			}
		}

	}
}