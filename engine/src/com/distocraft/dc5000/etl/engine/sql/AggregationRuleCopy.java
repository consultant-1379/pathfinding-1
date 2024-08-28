package com.distocraft.dc5000.etl.engine.sql;

import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;

/**
 * Handles the refreshing of aggregationRuleCache <br>
 * Rules have been copied to table log_aggregationrules and to
 * log_busyhourmapping<br>
 * with direct sql before this action and this action is responsible of
 * refreshing the cache<br>
 * 
 * @author etogust
 * 
 */
public class AggregationRuleCopy extends TransferActionBase {

	private final Logger log;

	public AggregationRuleCopy(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory etlreprock, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlreprock, trActions);

		log = Logger.getLogger(clog.getName() + ".AggregationRuleCopy");

	}

	public void execute() {

		try {

			AggregationRuleCache.getCache().revalidate();

		} catch (Exception e) {
			log.severe("Aggregation rule cache revalidation didn't succeed " + e.getStackTrace());
		}
	}
}
