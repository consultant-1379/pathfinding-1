package com.distocraft.dc5000.etl.engine.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.dwhrep.Dwhtechpacks;
import com.distocraft.dc5000.repository.dwhrep.DwhtechpacksFactory;

public class DWHSanityCheckerAction extends SQLOperation {

	private final Logger log;

	private RockFactory etlreprock = null;
	private RockFactory dwhreprock = null;
	private RockFactory dwhrock = null;

	private final Dwhtechpacks dtp;

	private final boolean all;

	public DWHSanityCheckerAction(final Meta_versions version, final Long techPackId, final Meta_collections set,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, techPackId, set, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		etlreprock = rockFact;
		this.log = clog;

		try {

			final Meta_databases md_cond = new Meta_databases(etlreprock);
			md_cond.setType_name("USER");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);




			for (Meta_databases db : md_fact.get()) {

				if (db.getConnection_name().equalsIgnoreCase("dwhrep")) {
					dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				} else if (db.getConnection_name().equalsIgnoreCase("dwh")) {
					dwhrock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				}

			} // for each Meta_databases

			if (dwhrock == null || dwhreprock == null) {
				throw new Exception("Database (dwh or dwhrep) is not defined in Meta_databases");



			}

			final Meta_collection_sets mcs_cond = new Meta_collection_sets(etlreprock);
			mcs_cond.setCollection_set_id(techPackId);
			final Meta_collection_setsFactory mcs_fact = new Meta_collection_setsFactory(etlreprock, mcs_cond);

			final Meta_collection_sets tp = mcs_fact.get().get(0);

			final Properties conf = TransferActionBase.stringToProperties(trActions.getAction_contents());

			final String mode = conf.getProperty("mode", null);

			if (mode != null && mode.equalsIgnoreCase("ALL")) {
				all = true;
				dtp = null;
			} else {
				all = false;

				final String tpName = tp.getCollection_set_name();

				if (tpName == null) {
					throw new Exception("Unable to resolve TP name");
				}

				final Dwhtechpacks dtp_cond = new Dwhtechpacks(dwhreprock);
				dtp_cond.setTechpack_name(tpName);
				final DwhtechpacksFactory dtp_fact = new DwhtechpacksFactory(dwhreprock, dtp_cond);

				final List<Dwhtechpacks> dtps = dtp_fact.get();

				if (dtps == null || dtps.size() != 1) {
					throw new Exception("Unable to resolve DWHTechPacks for " + tpName);
				}

				dtp = (Dwhtechpacks) dtps.get(0);

			}

		} catch (Exception e) {
			// Failure cleanup connections
			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dwhrock != null) {
					dwhrock.getConnection().close();
				}

			} catch (Exception ze) {
			}

			throw new EngineMetaDataException("Init error: " + e.getMessage(), e, "constructor");

		}

	}

	public void execute() throws Exception {

		try {

			final Class<?> c = Class.forName("com.distocraft.dc5000.dwhm.SanityChecker");
			final Class<?>[] parameterTypes = { RockFactory.class, RockFactory.class, Logger.class };

			final Object args[] = { dwhreprock, dwhrock, log };

			final Constructor<?> cont = c.getConstructor(parameterTypes);

			final Object action = cont.newInstance(args);

			if (all) {
				log.fine("SanityCheck executing for all techpacks");
				final Method execute = c.getMethod("sanityCheck");
				execute.invoke(action);
			} else {
				log.fine("SanityCheck executing for techpack " + dtp.getTechpack_name());
				final Method execute = c.getMethod("sanityCheck", Dwhtechpacks.class);
				execute.invoke(action, dtp);
			}

			log.fine("SanityCheck successfully finished");

		} finally {
			try {
				dwhreprock.getConnection().close();
			} catch (Exception e) {
			}

			try {
				dwhrock.getConnection().close();
			} catch (Exception e) {
			}
		}

	}

}
