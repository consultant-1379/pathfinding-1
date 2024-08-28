package com.distocraft.dc5000.etl.engine.system;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;

public class TestTriggerSetListInSchedulerAction extends TriggerSetListInSchedulerAction {

	private final TestScheduler scheduler = new TestScheduler();

	public TestTriggerSetListInSchedulerAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final Meta_transfer_actions trActions, final SetContext sctx)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions, sctx);
	}

	protected ISchedulerRMI connect() {
		return scheduler;
	}

}
