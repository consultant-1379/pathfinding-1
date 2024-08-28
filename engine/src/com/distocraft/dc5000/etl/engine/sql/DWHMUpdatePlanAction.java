package com.distocraft.dc5000.etl.engine.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class DWHMUpdatePlanAction extends SQLOperation {

	private final Logger log;
	private final RockFactory etlrepRockFact;

	public DWHMUpdatePlanAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
			final Logger log) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.etlrepRockFact = getConnection();
		this.log = log;
	}

	public void execute() throws EngineException {
		try {

			final Class<?> c = Class.forName("com.distocraft.dc5000.dwhm.UpdatePlanAction");
			final Class<?>[] parameterTypes = { RockFactory.class, Logger.class };

			final Object args[] = { this.etlrepRockFact, this.log };

			final Constructor<?> cont = c.getConstructor(parameterTypes);

			final Object action = cont.newInstance(args);

			final Method execute = c.getMethod("execute");

			execute.invoke(action);
			
		} catch (Exception e) {
			this.log.log(Level.SEVERE, "DWHMUpdatePlanAction.execute failed.", e);
		}
	}

}
