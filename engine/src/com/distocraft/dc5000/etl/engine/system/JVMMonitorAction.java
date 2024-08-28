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

/**
 * Created on Mar 9, 2005
 * 
 * @author lemminkainen
 */
public class JVMMonitorAction extends TransferActionBase {

	private static final Logger log = Logger.getLogger("etlengine.JVMMonitor");

	/**
	 * Empty protected constructor
	 * 
	 */
	protected JVMMonitorAction() {
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
	public JVMMonitorAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

	}

	/**
	 * Queries required information and prints to Logger
	 * 
	 */
	public void execute() throws EngineException {

		try {

			final Runtime rt = Runtime.getRuntime();

			String max = String.valueOf(rt.maxMemory() / 1048576L);
			while (max.length() < 5) {
				max = " " + max;
			}
			
			String allocated = String.valueOf(rt.totalMemory() / 1048576L);
			while (allocated.length() < 5) {
				allocated = " " + max;
			}
			
			String free = String.valueOf(rt.freeMemory() / 1048576L);
			while (free.length() < 5) {
				free = " " + max;
			}

			log.fine(rt.availableProcessors() + " processors " + max + " MB memory. " + allocated + " MB allocated " + free
					+ " MB free.");

		} catch (Exception e) {
			log.log(Level.INFO, "Monitor failed", e);
		}

	}

}
