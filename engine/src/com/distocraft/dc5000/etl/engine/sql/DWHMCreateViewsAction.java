package com.distocraft.dc5000.etl.engine.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.ericsson.eniq.common.TechPackType;

public class DWHMCreateViewsAction extends SQLOperation {

	private final Logger log;

	private RockFactory etlreprock = null;
	private RockFactory dwhreprock = null;
	private RockFactory dwhrock = null;
	private RockFactory dbadwhrock = null;

	private final String tpName;

	private final List<Dwhtype> types;


	public DWHMCreateViewsAction(final Meta_versions version, final Long techPackId, final Meta_collections set,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, techPackId, set, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.log = clog;
		etlreprock = rockFact;

		try {





			final Meta_databases md_cond = new Meta_databases(etlreprock);
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);

			for (Meta_databases db : md_fact.get()) {

				if (db.getConnection_name().equalsIgnoreCase("dwhrep") && db.getType_name().equals("USER")) {
					dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				} else if (db.getConnection_name().equalsIgnoreCase("dwh") && db.getType_name().equals("USER")) {
					dwhrock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				} else if (db.getConnection_name().equalsIgnoreCase("dwh") && db.getType_name().equals("DBA")) {
					dbadwhrock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				}

			} // for each Meta_databases

			if (dwhrock == null && dwhreprock == null && dbadwhrock == null) {
				throw new Exception("Database connection (dwh, dwhrep or dwh dba) is not defined in Meta_databases");




			}

			final Meta_collection_sets mcs_cond = new Meta_collection_sets(etlreprock);
			mcs_cond.setCollection_set_id(techPackId);
			final Meta_collection_setsFactory mcs_fact = new Meta_collection_setsFactory(etlreprock, mcs_cond);

			final Meta_collection_sets tp = mcs_fact.get().get(0);

			tpName = tp.getCollection_set_name();

			if (tpName == null) {
				throw new Exception("Unable to resolve TP name");
			}

			final Dwhtype typeCond = new Dwhtype(this.dwhreprock);
			typeCond.setTechpack_name(tpName);
			final DwhtypeFactory typeFact = new DwhtypeFactory(dwhreprock, typeCond,
					" ORDER BY typename, tablelevel DESC ");

			types = typeFact.get();

			if (types == null || types.size() <= 0) {
				throw new Exception("Unable to resolve Types");
			}

		} catch (Exception e) {
			// Failure -> cleanup connections
			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dwhrock != null) {
					dwhrock.getConnection().close();
				}
				if (dbadwhrock != null) {
					dbadwhrock.getConnection().close();
				}
			} catch (Exception se) {
			}











			throw new EngineMetaDataException("Init error: " + e.getMessage(), e, "constructor");

		}

	}

	public void execute() throws Exception {

		log.fine("CreateViews executing for techpack " + tpName);

		try {

			final List<String> viewNames = new ArrayList<String>();

			TechPackType techPackType = TechPackType.valueOf(tpName); // JVesey
																		// 10/08/2011
																		// Add
																		// missing
																		// TechPackType
																		// parameter

			for (Dwhtype type : types) {

				try {

					final Class<?> c = Class.forName("com.distocraft.dc5000.dwhm.CreateViewsAction");
					final Class<?>[] parameterTypes = { RockFactory.class, RockFactory.class, RockFactory.class,
							Dwhtype.class, Logger.class, TechPackType.class }; // JVesey
																				// 10/08/2011
																				// Add
																				// missing
																				// TechPackType
																				// parameter

					final Object args[] = { dbadwhrock, dwhrock, dwhreprock, type, log, techPackType }; // JVesey
																										// 10/08/2011
																										// Add
																										// missing
																										// TechPackType
																										// parameter

					final Constructor<?> cont = c.getConstructor(parameterTypes);

					final Object action = cont.newInstance(args);

					final Method getViewName = c.getMethod("getViewName");

					final String viewName = (String) getViewName.invoke(action);

					if (!viewNames.contains(viewName)) {
						viewNames.add(viewName);
					}

				} catch (Exception e) {
					log.log(Level.WARNING,
							"Unable to create view for " + type.getTypename() + " (" + type.getTablelevel() + ")", e);
				}

			} // foreach type

			if (!viewNames.isEmpty()) {

				try {
					final Class<?> c = Class.forName("com.distocraft.dc5000.dwhm.CreateOverallViewsFactory");
					final Class<?>[] parameterTypes = {}; // JVesey 10/08/2011
															// Remove parameters
															// to suit
															// dwhm.CreateOverallViewsFactory
															// class...not clear
															// what was
															// intention here.

					final Object args[] = {};

					final Constructor<?> cont = c.getConstructor(parameterTypes);

					cont.newInstance(args);

				} catch (Exception e) {
					log.log(Level.WARNING, "Unable to create view for " + tpName + ")", e);
				}


			}

		} finally {
			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dwhrock != null) {
					dwhrock.getConnection().close();
				}
				if (dwhrock != null) {
					dbadwhrock.getConnection().close();
				}


			} catch (Exception e) {
			}
		}

	}

}
