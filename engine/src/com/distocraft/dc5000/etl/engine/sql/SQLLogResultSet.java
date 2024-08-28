package com.distocraft.dc5000.etl.engine.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * SQLLogResultSet action executes SQL Select statement (action_content) into
 * database and logs result into log with Logging level defined in where_clause.
 * 
 * @author lemminkainen
 * 
 */
public class SQLLogResultSet extends SQLOperation {

	private Logger log;

	private Level lvl = Level.INFO;

	protected SQLLogResultSet() {
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
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SQLLogResultSet(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.log = Logger.getLogger(clog.getName() + ".SQLLogResultSet");

	}

	public void execute() throws SQLException {

		final String sqlClause = this.getTrActions().getAction_contents();

		log.finer("Trying to execute: " + sqlClause);

		if (sqlClause == null || sqlClause.length() <= 0) {
			log.warning("Select clause must be defined");
			return;
		}

		final String sLevel = this.getTrActions().getWhere_clause();
		if (sLevel != null && sLevel.length() > 0) {
			try {
				lvl = Level.parse(sLevel);
			} catch (Exception e) {
				log.warning("Illegal level " + sLevel + " using INFO");
			}
		}

		final RockFactory r = this.getConnection();

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = r.getConnection().createStatement();
			rs = stmt.executeQuery(sqlClause);
			boolean first = true;

			int colcount = 0;
			int rowcount = 0;

			while (rs.next()) {
				if (first) {
					final ResultSetMetaData md = rs.getMetaData();
					colcount = md.getColumnCount();
					final StringBuffer res = new StringBuffer();

					for (int i = 1; i <= colcount; i++) {
						res.append(md.getColumnName(i)).append("\t");
					}

					log.log(lvl, res.toString());
					first = false;
				}

				final StringBuffer res = new StringBuffer();
				for (int i = 1; i <= colcount; i++) {
					res.append(rs.getString(i)).append("\t");
				}

				log.log(lvl, res.toString());

				rowcount++;

			} // while (rs.next())

			log.log(lvl, "Query resulted " + rowcount + " rows");

		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			
		}

	}

}
