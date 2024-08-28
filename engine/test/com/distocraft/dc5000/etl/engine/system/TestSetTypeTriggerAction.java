package com.distocraft.dc5000.etl.engine.system;

import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;

public class TestSetTypeTriggerAction extends SetTypeTriggerAction {
		
		private final TestScheduler scheduler = new TestScheduler();
		
		public TestSetTypeTriggerAction(final Meta_versions version, final Long collectionSetId,
				final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
				final RockFactory rockFact, final Meta_transfer_actions trActions, final Logger clog) throws EngineMetaDataException {

			super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
					rockFact, trActions, clog);
		}
			
		protected ISchedulerRMI connect() {
			return scheduler;
		}
				
}
