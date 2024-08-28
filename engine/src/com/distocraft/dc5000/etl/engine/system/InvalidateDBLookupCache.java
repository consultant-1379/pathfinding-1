package com.distocraft.dc5000.etl.engine.system;

import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.DBLookupCache;

public class InvalidateDBLookupCache extends TransferActionBase {

	private final Logger log;

	public InvalidateDBLookupCache(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final Meta_transfer_actions trActions, final Logger logger)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.log = logger;

	}

	public void execute() throws EngineException {

		final DBLookupCache dblc = DBLookupCache.getCache();

		// String tables = this.getTrActions().getWhere_clause();
		// This version is kludge and doesn't care about tableNames
		// TODO implement tableNames when data interfaces supports naming them

		try {

			dblc.refresh();

		} catch (Exception e) {
			log.log(Level.FINE, "DBLookupCache refresh failed exceptionally", e);
		}

	}

}
