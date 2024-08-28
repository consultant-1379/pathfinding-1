package com.distocraft.dc5000.etl.engine.system;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class ReloadDBLookupsAction extends TransferActionBase {

  private String logString;

  private String tableName;

  /**
   * Empty protected constructor
   * 
   */
  protected ReloadDBLookupsAction() {
  }

  private SetContext sctx = null;
  private Logger log;
  
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
   */
  public ReloadDBLookupsAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final Meta_transfer_actions trActions, final SetContext sctx, final Logger clog) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);
    
    this.log = Logger.getLogger(clog.getName() + ".ReloadDBLookupsAction");
    this.sctx = sctx;
    
  }

  public void execute() throws EngineException {

    try {

      final Integer rowsAffected = (Integer) sctx.get("RowsAffected");
      if (rowsAffected != null && rowsAffected.intValue() < 1){
        log.finer("No rows affected -> nothing reloaded");
        return;
      }
      
      final Properties properties = TransferActionBase.stringToProperties(this.getTrActions().getAction_contents());
      
      tableName = properties.getProperty("tableName", null);
      if (tableName != null && tableName.equalsIgnoreCase("")) {
          tableName = null;
      }

    } catch (Exception e) {
      throw new EngineException("Failed to read configuration from WHERE", new String[] { "" }, e, this, this
          .getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);

    }

    try {

      final EngineAdmin admin = new EngineAdmin();
      Logger.getLogger(this.logString + ".execute").log(Level.INFO, "Reloading Database lookups.");
      admin.refreshDBLookups(tableName);

    } catch (Exception e) {
      throw new EngineException("Exception in reloadDBLookupsAction", new String[] { "" }, e, this, this.getClass()
          .getName(), EngineConstants.ERR_TYPE_SYSTEM);

    }

  }

}
