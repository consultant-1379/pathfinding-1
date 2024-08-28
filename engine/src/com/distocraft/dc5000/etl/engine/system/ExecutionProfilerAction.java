/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * ExecutionProfiler is for setting execution profiles and slots based on <br>
 * Configuration table's parameters and CPU physical core amount. CPU core
 * amount is queried from DefaultLicensingCache.<br>
 * <br>
 * Profiles are inserted into META_EXECUTION_SLOT_PROFILE table and slots are
 * inserted -- based on the formula -- into META_EXECUTION_SLOT table. <br>
 * <br>
 * Example of profiles' data in Configuration table:<br>
 * <br>
 * <table>
 * <td><i>Name</i></td>
 * <td>&nbsp;</td>
 * <td><i>Value</i></td>
 * <tr>
 * <td>executionProfile.0.Normal</td>
 * <td>&nbsp;</td>
 * <td>Y</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.NoLoads</td>
 * <td>&nbsp;</td>
 * <td>N</td>
 * </tr>
 * </table>
 * <br>
 * Example of slots' data in Configuration table:<br>
 * <br>
 * <table>
 * <td><i>Name</i></td>
 * <td>&nbsp;</td>
 * <td><i>Value</i></td>
 * <tr>
 * <td>executionProfile.0.slot1.0.execute</td>
 * <td>&nbsp;</td>
 * <td>adapter,Adapter,Alarm,Install,Mediation,Topology</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot1.0.formula</td>
 * <td>&nbsp;</td>
 * <td>0.9n</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot2.1.execute</td>
 * <td>&nbsp;</td>
 * <td>Loader</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot2.1.formula</td>
 * <td>&nbsp;</td>
 * <td>0.1n</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot3.2.execute</td>
 * <td>&nbsp;</td>
 * <td>Loader,Aggregator</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot3.2.formula</td>
 * <td>&nbsp;</td>
 * <td>0.1n</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot4.3.execute</td>
 * <td>&nbsp;</td>
 * <td>Aggregator</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot4.3.formula</td>
 * <td>&nbsp;</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot5.4.execute</td>
 * <td>&nbsp;</td>
 * <td>Partition,Service,Support</td>
 * </tr>
 * <tr>
 * <td>executionProfile.0.slot5.4.formula</td>
 * <td>&nbsp;</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot1.5.execute</td>
 * <td>&nbsp;</td>
 * <td>adapter,Adapter,Support</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot1.5.formula</td>
 * <td>&nbsp;</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot2.6.execute</td>
 * <td>&nbsp;</td>
 * <td>adapter,Adapter,Support</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot2.6.formula</td>
 * <td>&nbsp;</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot3.7.execute</td>
 * <td>&nbsp;</td>
 * <td>adapter,Adapter,Support</td>
 * </tr>
 * <tr>
 * <td>executionProfile.1.slot3.7.formula</td>
 * <td>&nbsp;</td>
 * <td>1</td>
 * </tr>
 * </table>
 * 
 * @author eharrka
 * @author etuolem
 */
public class ExecutionProfilerAction extends TransferActionBase {

	private final RockFactory etlrep;
	private final Logger log;
	
	private static SlotRebuilder slotRebuilder;

	public ExecutionProfilerAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory etlRepRock, final Meta_transfer_actions trActions, final Logger parentLog)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlRepRock, trActions);

		this.log = Logger.getLogger(parentLog.getName() + ".ExecutionProfiler");
		this.etlrep = etlRepRock;
		
		slotRebuilder = new SlotRebuilder(etlrep, log);

	}

	/**
	 * Execute this action
	 * @throws SQLException 
	 * @throws RockException 
	 * @throws IOException 
	 */
	@Override
	public void execute() throws SQLException, RockException, IOException {
		slotRebuilder.rebuildSlots();
	}
	
	public static void setSlotRebuilder(SlotRebuilder rebuilder) {
		slotRebuilder = rebuilder;
	}
}