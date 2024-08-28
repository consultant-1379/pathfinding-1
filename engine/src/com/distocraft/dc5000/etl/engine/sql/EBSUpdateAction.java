package com.distocraft.dc5000.etl.engine.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * This class encapsulates a series of actions that perform an update on a
 * techpack's sets.
 * 
 * @author epiituo
 */
public class EBSUpdateAction extends TransferActionBase {

	protected final Logger log;
	protected Meta_collections collection;

	protected final RockFactory etlRepRockFactory;
	protected final RockFactory dwhRockFactory;
	protected final RockFactory dwhRepRockFactory;
	
	private final Class<?> ebsupdaterClass;
	private final Object ebsUpdater;

	/**
	 * Empty protected constructor
	 */
	protected EBSUpdateAction() {
		this.etlRepRockFactory = null;
		this.dwhRockFactory = null;
		this.dwhRepRockFactory = null;
		this.log = Logger.getLogger("EBSUpdateAction");
		this.ebsupdaterClass = null;
		this.ebsUpdater = null;
	}

	public EBSUpdateAction(final Meta_versions version, final Long techPackId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions, final Logger clog) throws Exception {

		super(version, techPackId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.log = Logger.getLogger(clog.getName() + ".EBSUpdateAction");
		this.etlRepRockFactory = rockFact;

		this.dwhRockFactory = getDwhRockFactory(this.etlRepRockFactory);
		if (this.dwhRockFactory == null) {
			throw new Exception("Database dwh is not defined in Meta_databases");
		}

		this.dwhRepRockFactory = getDwhRockFactory(this.etlRepRockFactory);
		if (this.dwhRockFactory == null) {
			throw new Exception("Database dwhrep is not defined in Meta_databases");
		}

		final Properties properties = TransferActionBase.stringToProperties(trActions.getWhere_clause());

		final Meta_collection_sets mcs = new Meta_collection_sets(etlRepRockFactory);
		mcs.setCollection_set_id(techPackId);
		final Meta_collection_setsFactory mcsF = new Meta_collection_setsFactory(etlRepRockFactory, mcs);

		properties.put("tpName", mcsF.getElementAt(0).getCollection_set_name());

  	ebsupdaterClass = Class.forName("com.ericsson.eniq.etl.ebsHandler.action.EBSUpdater");
		final Class<?>[] parameterTypes = { Properties.class, RockFactory.class, Logger.class };

		final Object args[] = { properties, rockFact, log };

		final Constructor<?> cont = ebsupdaterClass.getConstructor(parameterTypes);

		ebsUpdater = cont.newInstance(args);

	}

	/**
	 * Executes an SQL procedure.
	 * 
	 * @throws Exception
	 * 
	 *           (non-Javadoc)
	 * @see com.distocraft.dc5000.etl.engine.structure.TransferActionBase#execute()
	 */
	public void execute() throws Exception {

		final Method execute = ebsupdaterClass.getMethod("execute", SetListener.class);
		execute.invoke(ebsUpdater, this.setListener);

	}

	protected final RockFactory getDwhRockFactory(final RockFactory etlRepRock) {
		return getRockFactory(etlRepRock, "dwhrep");
	}

	protected final RockFactory getDwhRepRockFactory(final RockFactory etlRepRock) {
		return getRockFactory(etlRepRock, "dwh");
	}

	protected final RockFactory getRockFactory(final RockFactory etlRepRock, final String name) {

		RockFactory result = null;

		try {
			final Meta_databases md_cond = new Meta_databases(etlRepRock);
			md_cond.setType_name("USER");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlRepRock, md_cond);

			for (Meta_databases db : md_fact.get()) {
			
				if (db.getConnection_name().equalsIgnoreCase(name)) {
					result = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db.getDriver_name(),
							"EBSUpdate", true);
				}
			}
		} catch (Exception e) {
			result = null;
		}

		return result;
	}

}