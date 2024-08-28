package com.distocraft.dc5000.etl.engine.system;

import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpOutput;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.main.EngineAdminFactory;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements a system call execution
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SystemCall extends TransferActionBase {

	private static final Logger log = Logger.getLogger("etlengine.CreateDirAction");
	
	protected SystemCall() {	

	}

	/**
	 * Constructor
	 * 
	 * @param version
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collection
	 *          collection object
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SystemCall(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		
	}

	/**
	 * Executes a SQL procedure
	 * 
	 */
	public void execute() throws EngineException {
		final String systemClause = this.getTrActions().getAction_contents();
		try {

			log.info("Executing systemCall \"" + systemClause + "\"");

			if (systemClause != null) {
				
				String delimitor = " ";
				String[] systemClauseSplit  = systemClause.split(delimitor);
				
				//Start Code Changes for TR HQ29011				
				
				boolean check=false;
				
				for(int i=0; i< systemClauseSplit.length ; i++){
					if(systemClauseSplit[i].equals("reloadAggregationCache")){
					final EngineAdmin engineAdmin = EngineAdminFactory.getInstance();
					try {
						engineAdmin.reloadAggregationCache();
						log.info("ReloadAggregationCache reload succeeded.");
					} catch (Exception e) {
						log.severe("ReloadAggregationCache reload failed. " + e.getMessage());
					}
					} else if (check==false) {
					log.info("Going to else");
          final LwpOutput result = LwProcess.execute(systemClause, true, log);
          log.info("STDOUT: " +  result.getStdout());
					if (result.getExitCode() != 0) {
						log.warning("Returned with abnormal exit code " + result.getExitCode());
					}
				}
			}
		} }
			catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { systemClause }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_SYSTEM);
		}

	}

}
