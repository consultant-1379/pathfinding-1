package com.distocraft.dc5000.etl.engine.sql;

import java.util.List;
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
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;

/**
 * This action checks that techpack executing this action has correct version in
 * DWH
 * 
 * @author lemminkainen
 * 
 */
public class DWHMVersionCheckAction extends SQLOperation {

	private final Logger log;

	private RockFactory etlreprock;
	private RockFactory dwhreprock;

	private final Long techPackId;
	private final String etlversion;

	public DWHMVersionCheckAction(final Meta_versions mversion, final Long techPackId, final Meta_collections set,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(mversion, techPackId, set, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.log = Logger.getLogger(clog.getName() + ".VersionCheck");
		etlreprock = rockFact;
		this.techPackId = techPackId;

		try {

			final Meta_databases md_cond = new Meta_databases(etlreprock);
			md_cond.setType_name("USER");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);



			for (Meta_databases db : md_fact.get()) {

				if (db.getConnection_name().equalsIgnoreCase("dwhrep")) {
					dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "DWHMgr", true);
				}

			} // for each Meta_databases

			if (dwhreprock == null) {
				throw new Exception("Database dwhrep is not defined in Meta_databases");


			}

		} catch (Exception e) {
			// Failure -> cleanup connections

			try {
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
			} catch (Exception se) {
			}

			throw new EngineMetaDataException("Init error: " + e.getMessage(), e, "constructor");
		}

		etlversion = mversion.getVersion_number();

	}

	public void execute() throws Exception {

		log.fine("Checking TechPack version ETL <-> DWH");

		try {

			final Meta_collection_sets mcs_cond = new Meta_collection_sets(etlreprock);
			mcs_cond.setCollection_set_id(techPackId);
			final Meta_collection_setsFactory mcs_fact = new Meta_collection_setsFactory(etlreprock, mcs_cond);

			final Meta_collection_sets tp = mcs_fact.get().get(0);

			final String tpName = tp.getCollection_set_name();

			if (tpName == null) {
				throw new Exception("Unable to resolve TP name");
			}

			final Tpactivation tpa_cond = new Tpactivation(dwhreprock);
			tpa_cond.setTechpack_name(tpName);
			final TpactivationFactory tpa_fact = new TpactivationFactory(dwhreprock, tpa_cond);

			final List<Tpactivation> tpas = tpa_fact.get();

			if (tpas == null || tpas.size() != 1) {
				throw new Exception("Techpack " + tpName + " is not active");
			}

			final Tpactivation tpa = (Tpactivation) tpas.get(0);

			final String dwhVersionID = tpa.getVersionid();
			final String etlVersionID = tpName + ":" + etlversion;

			log.fine("Comparing \"" + etlVersionID + "\" <-> \"" + dwhVersionID + "\"");

			if (etlVersionID.equals(dwhVersionID)) {
				throw new Exception("TechPack version in etl (" + etlVersionID + ") does not match to dwh version ("
						+ dwhVersionID + ")");
			}

		} finally {
			try {
				dwhreprock.getConnection().close();
			} catch (Exception e) {
			}
		}

	}

}
