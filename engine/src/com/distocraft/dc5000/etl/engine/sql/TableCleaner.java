package com.distocraft.dc5000.etl.engine.sql;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class TableCleaner extends SQLActionExecute {

	private final Logger log;
	private final Logger sqlLog;

	private final Meta_transfer_actions actions;

	protected TableCleaner() {
		this.log = Logger.getLogger("etlengine.TableCleaner");
		this.sqlLog = Logger.getLogger("etlegnine.TableCleaner");
		this.actions = null;
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
	 * @param rockFact
	 *          metadata repository connection object
	 * @param connectionPool
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 */
	public TableCleaner(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory etlreprock,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException, RockException, SQLException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlreprock,
				connectionPool, trActions);

		this.log = Logger.getLogger(clog.getName() + ".TableCleaner");
		this.sqlLog = Logger.getLogger(clog.getName() + ".TableCleaner");

		// Get collection set name
		final Meta_collection_sets whereCollSet = new Meta_collection_sets(etlreprock);
		whereCollSet.setEnabled_flag("Y");
		whereCollSet.setCollection_set_id(collectionSetId);

		this.actions = trActions;

	}

	/**
	 * Executes a SQL procedure
	 */
	public void execute() throws EngineException {

		try {

			final Properties properties = TransferActionBase.stringToProperties(this.actions.getAction_contents());

			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			final String tablename = properties.getProperty("tablename");
			final String datecolumn = properties.getProperty("datecolumn");
			final int threshold = Integer.parseInt(properties.getProperty("threshold"));

			final GregorianCalendar cal = new GregorianCalendar();
			cal.setTime(new Date(System.currentTimeMillis()));
			cal.add(GregorianCalendar.DATE, -threshold);

			final String sql = "delete from " + tablename + " where " + datecolumn + " <= '" + sdf.format(cal.getTime())
					+ "'";
			sqlLog.finest("SQL: " + sql);
			final int count = executeSQLUpdate(sql);

			log.info(count + " Rows removed from " + tablename + " with " + datecolumn + " <= " + sdf.format(cal.getTime()));

		} catch (Exception e) {
			log.severe(e.getStackTrace() + "\r\n" + new String[] { this.getTrActions().getAction_contents() });

			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		}
	}
}
