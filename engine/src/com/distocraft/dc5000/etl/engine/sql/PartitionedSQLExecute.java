package com.distocraft.dc5000.etl.engine.sql;

import java.io.StringWriter;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.ericsson.eniq.common.VelocityPool;

/**
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>Basetablename</td>
 * <td>typename</td>
 * <td>Defines the basetablename for the partitioned table where the sql clause
 * is executed.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>SQL Template</td>
 * <td>Action_contents</td>
 * <td>Defines the SQL clause (velocity template) that is executed in every
 * active partition in basetablename pointed partitioned table. The actual
 * tablename is given to the SQL template in $tableName container.</td>
 * <td>&nbsp;</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * 
 * @author lemminkainen
 * @author savinen
 * 
 */
public class PartitionedSQLExecute extends SQLOperation {

	private final Logger log;

	private final String storageid;

	private final boolean aggStatusCacheUpdate;

	private final String clause;

	private final SetContext setContext;

	private final boolean useOnlyLoadedPartitions;

	protected PartitionedSQLExecute() {
		this.log = Logger.getLogger("etlengine.PartitionedSQLExecute");
		this.setContext = null;
		this.storageid = null;
		this.aggStatusCacheUpdate = false;
		this.clause = null;
		this.useOnlyLoadedPartitions = false;
	}

	/**
	 * Constructor
	 * 
	 * @param versionNumber
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collectionId
	 *          primary key for collection
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectId
	 *          primary key for database connections
	 * @param etlrep
	 *          metadata repository connection object
	 * @param connectionPool
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 */
	public PartitionedSQLExecute(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory etlrep, final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
			final SetContext sctx, final Logger clog) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlrep, connectionPool,
				trActions);

		this.log = Logger.getLogger(clog.getName() + ".PartitionedSQLExecute");
		this.setContext = sctx;

		final Properties prop = TransferActionBase.stringToProperties(trActions.getWhere_clause());

		RockFactory dwhreprock = null;

		try {

			final Meta_databases md_cond = new Meta_databases(etlrep);
			md_cond.setType_name("USER");
			md_cond.setConnection_name("dwhrep");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlrep, md_cond);

			final List<Meta_databases> dbs = md_fact.get();

			if (dbs.size() <= 0) {
				throw new Exception("Database dwhrep is not defined in Meta_databases");
			}

			final Meta_databases db = dbs.get(0);

			dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db.getDriver_name(),
					"PartitionedSQLExecute", true);

			final String basetablename = prop.getProperty("typeName", prop.getProperty("tablename"));

			aggStatusCacheUpdate = "true".equalsIgnoreCase(prop.getProperty("aggStatusCacheUpdate", "false"));
			if (aggStatusCacheUpdate) {
				log.fine("Updating log_aggregationstatus partitions via cache");
			}

			useOnlyLoadedPartitions = "1".equalsIgnoreCase(prop.getProperty("useOnlyLoadedPartitions", "0"));

			log.finer("basetablename: " + basetablename);

			if (basetablename == null) {
				throw new Exception("Parameter basetablename must be defined");
			}

			if (!useOnlyLoadedPartitions) {
				final Dwhtype dt = new Dwhtype(dwhreprock);
				dt.setBasetablename(basetablename);
				final DwhtypeFactory dtf = new DwhtypeFactory(dwhreprock, dt);
				final Dwhtype dtr = dtf.getElementAt(0);

				if (dtr == null) {
					throw new Exception("Basetablename " + basetablename + " Not found from DWHType");
				}

				storageid = dtr.getStorageid();

				log.finer("storageid: " + storageid);
			} else {
				log.fine("Using only loaded partitions.");
				storageid = null;
			}

			clause = trActions.getAction_contents();

			log.finer("SQLclause: " + clause);

			if (clause == null) {
				throw new Exception("Parameter clause must be defined");
			}

		} catch (Exception e) {

			throw new EngineMetaDataException("Initialization failed", e, "constructor");

		} finally {

			try {
				dwhreprock.getConnection().close();
			} catch (Exception se) {
			}

		}
	}

	public void execute() throws Exception {

		VelocityEngine ve = null;

		try {

			final VelocityContext ctx = new VelocityContext();

			ve = VelocityPool.reserveEngine();

			List<String> tables = null;

			if (useOnlyLoadedPartitions) {

				tables = (List<String>) this.setContext.get("tableList");

			} else {

				final PhysicalTableCache ptc = PhysicalTableCache.getCache();
				tables = ptc.getActiveTables(storageid);

			}

			Exception exp = null;

			for (String table : tables) {

				try {

					ctx.put("tableName", table);

					final StringWriter writer = new StringWriter();

					ve.evaluate(ctx, writer, "", clause);

					final String sqlClause = writer.toString();

					log.finer("Trying to execute: " + sqlClause);
					if (aggStatusCacheUpdate) {
						AggregationStatusCache.update(sqlClause);
					} else {
						this.getConnection().executeSql(sqlClause);
					}
				} catch (Exception e) {
					log.log(Level.WARNING, "SQL execution failed to exception", e);
					exp = e;
				}

			} // foreach table

			if (exp != null) {
				log.severe("Exception occured during execution. Failing set.");
				throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.getTrActions()
						.getAction_contents() }, exp, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);

			}

		} finally {

			VelocityPool.releaseEngine(ve);

		}

	}

}
