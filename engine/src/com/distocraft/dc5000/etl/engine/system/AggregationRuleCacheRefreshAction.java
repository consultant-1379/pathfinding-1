package com.distocraft.dc5000.etl.engine.system;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import java.util.logging.Logger;
import ssc.rockfactory.RockFactory;

/**
 * User: eeipca
 * Date: 02/05/12
 * Time: 11:54
 */
public class AggregationRuleCacheRefreshAction extends TransferActionBase {
  private final Logger log;

  public AggregationRuleCacheRefreshAction(final Meta_versions version, final Long collectionSetId,
                                           final Meta_collections collection, final Long transferActionId,
                                           final Long transferBatchId, final Long connectId, final RockFactory rockFact,
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

      log = Logger.getLogger("etl." + tech_pack + "." + set_type + "." + set_name + ".action.reloadAggregationCache");

    } catch (Exception e) {
      throw new EngineMetaDataException("AggregationRuleCacheRefresh unable to initialize loggers", e, "init");
    }
  }

  @Override
  public void execute() throws Exception {
    try {
      final EngineAdmin admin = new EngineAdmin();
      log.info("Reloading Aggregation Cache.");
      admin.reloadAggregationCache();
      log.info("Aggregation Cache Reloaded.");
    } catch (Exception e) {
      throw new EngineException("Exception in reloadAggregationCache", new String[]{""}, e, this, this
        .getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);
    }
  }
}
