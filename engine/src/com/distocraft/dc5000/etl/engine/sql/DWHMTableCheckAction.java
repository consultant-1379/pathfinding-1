package com.distocraft.dc5000.etl.engine.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class DWHMTableCheckAction extends SQLOperation {

	private final Logger log;

	private RockFactory dwhreprock;
	private RockFactory dbadwhrock;
	private final String mode;
	private final Properties conf;

	public DWHMTableCheckAction(final Meta_versions version, final Long techPackId, final Meta_collections set,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, techPackId, set, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.log = clog;

		try {

			final Meta_databases md_cond = new Meta_databases(rockFact);
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(rockFact, md_cond);




			for (Meta_databases db : md_fact.get()) {

				if (db.getConnection_name().equalsIgnoreCase("dwhrep") && db.getType_name().equals("USER")) {
					dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				} else if (db.getConnection_name().equalsIgnoreCase("dwh") && db.getType_name().equals("DBA")) {
					dbadwhrock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				}

			} // for each Meta_databases

			if (dbadwhrock == null || dwhreprock == null) {
				throw new Exception("Database (dwh dba or dwhrep) is not defined in Meta_databases");



			}

			if (trActions.getWhere_clause() == null || trActions.getWhere_clause().length() <= 0) {
				mode = "READONLY";
			} else {
				mode = trActions.getWhere_clause();
			}

		} catch (Exception e) {
			// Failure -> cleanup connections

			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dbadwhrock != null) {
					dbadwhrock.getConnection().close();
				}
			} catch (Exception ne) {
			}

			throw new EngineMetaDataException("Init error: " + e.getMessage(), e, "constructor");

		}

		this.conf = TransferActionBase.stringToProperties(trActions.getAction_contents());

	}

	public void execute() throws Exception {

		log.fine("Executing TableCheck in mode " + mode);

		try {

			final Class<?> c = Class.forName("com.distocraft.dc5000.dwhm.DWHTableCheckAction");
			final Class<?>[] parameterTypes = { RockFactory.class, RockFactory.class, Logger.class, Properties.class,
					String.class };

			final Object args[] = { dwhreprock, dbadwhrock, log, conf, mode };

			final Constructor<?> cont = c.getConstructor(parameterTypes);

			final Object action = cont.newInstance(args);

			final Method execute = c.getMethod("execute");

			execute.invoke(action);

		} finally {
			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dbadwhrock != null) {
					dbadwhrock.getConnection().close();
				}
			} catch (Exception e) {
			}
		}

	}

}
