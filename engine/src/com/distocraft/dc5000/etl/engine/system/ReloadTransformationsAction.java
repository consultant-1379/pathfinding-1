package com.distocraft.dc5000.etl.engine.system;

import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class ReloadTransformationsAction extends TransferActionBase {

	private final Logger log;
	
	/**
	 * Empty protected constructor
	 */
	protected ReloadTransformationsAction() {
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
	public ReloadTransformationsAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		try {
			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

			final String tech_pack = collSet.getCollection_set_name();
			final String set_type = collection.getSettype();
			final String set_name = collection.getCollection_name();

			log = Logger.getLogger("etl." + tech_pack + "." + set_type + "." + set_name + ".action.reloadPropertiesAction");

		} catch (Exception e) {
			throw new EngineMetaDataException("ExecuteSetAction unable to initialize loggers", e, "init");
		}

	}

	public void execute() throws EngineException {

		try {

			final EngineAdmin admin = new EngineAdmin();
			log.info("Reloading Transformations.");
			admin.refreshTransformations();

		} catch (Exception e) {
			throw new EngineException("Exception in reloadTransformationsAction", new String[] { "" }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);

		}

	}

}
