package com.distocraft.dc5000.etl.engine.system;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLOperation;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class AlarmMarkupActionWrapper extends SQLOperation {

	private final Logger log;

	private final Long collectionSetId;

	private final Meta_collections collection;

	private final Meta_transfer_actions trActions;

	private final SetContext setcontext;

	public AlarmMarkupActionWrapper(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
			final SetContext setcontext, final Logger log) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.collectionSetId = collectionSetId;
		this.collection = collection;
		this.trActions = trActions;
		this.setcontext = setcontext;
		this.log = log;
	}

	public void execute() throws EngineException {
		try {

			final Class<?> raccClass = Class.forName("com.ericsson.eniq.etl.alarm.RockAlarmConfigCache");
			final Method method = raccClass.getMethod("getInstance");

			final Object racc = method.invoke(null);

			this.setcontext.put("alarmConfig", racc);

			final Class<?> c = Class.forName("com.distocraft.dc5000.etl.alarm.AlarmMarkupAction");
			final Class<?>[] parameterTypes = { Long.class, Meta_collections.class, RockFactory.class,
					Meta_transfer_actions.class, SetContext.class, Logger.class };

			final Object args[] = { this.collectionSetId, this.collection, getConnection(), this.trActions, this.setcontext,
					this.log };

			final Constructor<?> cont = c.getConstructor(parameterTypes);

			final Object action = cont.newInstance(args);

			final Method execute = c.getMethod("execute");

			execute.invoke(action);

		} catch (Exception e) {
			throw new EngineException(e.getMessage(), new String[] { "" }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_SYSTEM);
		}
	}

}
