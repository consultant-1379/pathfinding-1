package com.distocraft.dc5000.etl.engine.system;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * Config action that acts only as placeholder for configuration.
 */
public class Config extends TransferActionBase {

  /**
   * Empty protected constructor
   * 
   */
  protected Config() {
  }

  public Config(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
  		final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
  		final Meta_transfer_actions trActions) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

  }

  /**
   * Executes a SQL procedure
   * 
   */

  public void execute() throws EngineException {

  }

}
