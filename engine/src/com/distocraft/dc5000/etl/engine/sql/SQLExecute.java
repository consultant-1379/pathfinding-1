package com.distocraft.dc5000.etl.engine.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.DatabaseConnections;

public class SQLExecute extends SQLOperation {

	private final Logger log;

	private final SetContext sctx;

	protected SQLExecute() {
		this.log = Logger.getLogger("SQLExecute");
		this.sctx = null;
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
	public SQLExecute(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final SetContext sctx,
			final Logger clog) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.log = Logger.getLogger(clog.getName() + ".SQLExecute");
		this.sctx = sctx;

	}

	public void execute() throws EngineException {
		RockFactory dwhrep = null;
		RockResultSet rockResultSetRBS = null;
		RockResultSet rockResultSetRBSG2 = null;
		RockFactory connectionFactory = null;
		RockResultSet statementResultsRBS = null;
		RockResultSet statementResultsRBSG2 = null;
		String sqlClause = null;
		
//		Connection conn = null; //NOPMD Do not close ConPool connection
		Statement s = null;
		try {
			final Properties properties = TransferActionBase.stringToProperties(this.getTrActions().getWhere_clause());

			final boolean useSystemProperties = "TRUE".equalsIgnoreCase(properties
					.getProperty("useSystemProperties", "false"));

			sqlClause = this.getTrActions().getAction_contents();
			
			connectionFactory = this.getConnection();
			dwhrep = DatabaseConnections.getDwhRepConnection();
			String setName = "FFAXW_MEAN_SD_LOADER";
			//New code
			String transferActionName = this.getTransferActionName();
			if (transferActionName.equalsIgnoreCase(setName)) {
				String sqlQueryRBS = "SELECT Max(rbs_aggregated.date_id) FROM ( SELECT rbs_day.date_id AS date_id, Sum(rbs_day.agg_count) AS sum_agg_count FROM DC_E_RBS_RADIOLINKS_V_day rbs_day WHERE Now(*) <= Dateadd(dd, 90, rbs_day.date_id) AND  rbs_day.date_id >= ( SELECT Min(ci_table.created) FROM dim_e_ffaxw_criteria ci_table) AND ( SELECT log_table.status FROM log_aggregationstatus log_table WHERE  log_table.datadate = rbs_day.date_id AND log_table.typename = 'DC_E_RBS_RADIOLINKS_V' AND log_table.timelevel = 'DAY' AND log_table.aggregationscope = 'DAY' ) IN ('AGGREGATED' ) GROUP  BY rbs_day.date_id ) AS rbs_aggregated  LEFT JOIN ( SELECT ffaxw_day.date_id AS date_id, Sum(ffaxw_day.agg_count) AS sum_agg_count FROM dc_e_ffaxw_kpi ffaxw_day GROUP BY ffaxw_day.date_id) AS ffaxw_aggregated ON rbs_aggregated.date_id = ffaxw_aggregated.date_id  WHERE  rbs_aggregated.sum_agg_count != Ifnull(ffaxw_aggregated.sum_agg_count, 0, ffaxw_aggregated.sum_agg_count)";
				String sqlQueryRBSG2 = "SELECT Max(rbsg2_aggregated.date_id) FROM ( SELECT rbsg2_day.date_id AS date_id, Sum(rbsg2_day.agg_count) AS sum_agg_count FROM dc_e_rbsg2_carrier_v_day rbsg2_day WHERE Now(*) <= Dateadd(dd, 90, rbsg2_day.date_id) AND rbsg2_day.date_id >= ( SELECT Min(ci_table.created) FROM dim_e_ffaxw_criteria ci_table) AND ( SELECT log_table.status FROM log_aggregationstatus log_table WHERE  log_table.datadate = rbsg2_day.date_id AND log_table.typename = 'DC_E_RBSG2_CARRIER_V' AND log_table.timelevel = 'DAY' AND log_table.aggregationscope = 'DAY' ) IN ('AGGREGATED' ) GROUP  BY rbsg2_day.date_id ) AS rbsg2_aggregated LEFT JOIN ( SELECT ffaxw_day.date_id AS date_id, Sum(ffaxw_day.agg_count) AS sum_agg_count FROM dc_e_ffaxw_kpi ffaxw_day GROUP BY ffaxw_day.date_id) AS ffaxw_aggregated ON rbsg2_aggregated.date_id = ffaxw_aggregated.date_id WHERE  rbsg2_aggregated.sum_agg_count != Ifnull(ffaxw_aggregated.sum_agg_count, 0, ffaxw_aggregated.sum_agg_count)";

				// for G1 calculation
				statementResultsRBS = connectionFactory.setSelectSQL(sqlQueryRBS);
				String dateIdRBS = getDateId(statementResultsRBS);
				log.finest("Fetched DateId for RBS is : " + dateIdRBS);

				String selectStringRBS = "select TABLENAME from dwhrep.DWHPartition where STORAGEID='dc_e_rbs_radiolinks_v:DAY' and " + "'" + dateIdRBS + "'" + " between STARTTIME  and ENDTIME and ENDTIME != " + "'" + dateIdRBS + "'";
				rockResultSetRBS = dwhrep.setSelectSQL(selectStringRBS);
				sqlClause = getUpdatedSql(dateIdRBS, rockResultSetRBS, "DC_E_RBS_RADIOLINKS_V_DAY", sqlClause);

				// for G2 calculation
				statementResultsRBSG2 = connectionFactory.setSelectSQL(sqlQueryRBSG2);
				String dateIdRBSG2 = getDateId(statementResultsRBSG2);
				log.finest("Fetched DateId for RBSG2 is : " + dateIdRBSG2);
				
				String selectStringRBSG2 = "select TABLENAME from dwhrep.DWHPartition where STORAGEID='DC_E_RBSG2_CARRIER_V:DAY' and " + "'" + dateIdRBSG2 + "'" + " between STARTTIME  and ENDTIME and ENDTIME != " + "'" + dateIdRBSG2 + "'";
				rockResultSetRBSG2 = dwhrep.setSelectSQL(selectStringRBSG2);
				sqlClause = getUpdatedSql(dateIdRBSG2, rockResultSetRBSG2, "DC_E_RBSG2_CARRIER_V_DAY", sqlClause);

			}
			
			if (useSystemProperties) {
				sqlClause = parseSystemProperties(sqlClause);
			}

			log.finer("Trying to execute: " + sqlClause);

			int newRowsAffected = 0;
			Integer rowsAffected = null;

				s = connectionFactory.getConnection().createStatement();

				rowsAffected = (Integer) sctx.get("RowsAffected");
				if (rowsAffected == null) {
					rowsAffected = new Integer(0);
				}

				log.finer("Executing statement");

				s.execute(sqlClause);

				log.finer("Counting affected rows for each statement");
				int rows = s.getUpdateCount();
				while (s.getMoreResults() || rows != -1) {
					if (rows != -1) {
						newRowsAffected += rows;
						log.finer("New rows affected:" + rows);
						log.finer("All together now:" + newRowsAffected);
					}

					rows = s.getUpdateCount();
				}


			if (newRowsAffected > 0) {
				rowsAffected = new Integer(rowsAffected.intValue() + newRowsAffected);
				log.finer("Executed: \n" + sqlClause + "\nRows Affected " + newRowsAffected + "\nRows Affected for the set:"
						+ rowsAffected);
				if (rowsAffected != null) {
					sctx.put("RowsAffected", rowsAffected);
				}
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "SQL execution failed to exception", e);
			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			closeConnections(dwhrep, rockResultSetRBS, rockResultSetRBSG2, statementResultsRBS, statementResultsRBSG2,
					s);
		}
		
	}

	private void closeConnections(RockFactory dwhrep, RockResultSet rockResultSetRBS, RockResultSet rockResultSetRBSG2,
			RockResultSet statementResultsRBS, RockResultSet statementResultsRBSG2, Statement s)
			throws EngineException {
		try {
			if (rockResultSetRBS != null) {
				rockResultSetRBS.close();
			}
			if (rockResultSetRBSG2 != null) {
				rockResultSetRBSG2.close();
			}
			if (dwhrep != null) {
				dwhrep.getConnection().close();
			}
			if (statementResultsRBS != null) {
				statementResultsRBS.close();
			}
			if (statementResultsRBSG2 != null) {
				statementResultsRBSG2.close();
			}
			if (s != null ) {
				s.close();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception caught while closing the connection - ", e);
			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	private String getUpdatedSql(String dateId, RockResultSet rockResultSet, String viewName,
			String sqlClause) throws SQLException, RockException {
		String newSql = sqlClause;
		if (dateId != null) {
			newSql = getAndReplacePartition(viewName, rockResultSet, newSql);
		}
		return newSql;
		
	}

	private String getAndReplacePartition(String viewName, RockResultSet rockResultSets, String newSql) throws SQLException, RockException {

		ResultSet resultSets;
		String partitionTableName = null;
		resultSets = rockResultSets.getResultSet();
		if (resultSets.isBeforeFirst()) {
			while (resultSets.next()) {
				partitionTableName = resultSets.getString(1);
			}
		}
		log.finest("Fetched Partition name for view "+ viewName +" is : " + partitionTableName);

		log.info("Replacing "+viewName+" view with exact partition name "+ partitionTableName);
		
		return newSql.replace(viewName, partitionTableName);
		
		
	}

	private String getDateId(RockResultSet statementResults) throws SQLException {
		ResultSet resultSets = statementResults.getResultSet();
		String dateId = null;
		if (resultSets.isBeforeFirst()) {
			while (resultSets.next()) {
				dateId = resultSets.getString(1);
			}
		}
		return dateId;
	}

	/**
	 * Replaces Systemproperties with corresponding values
	 * 
	 * @param sqlClause
	 * @return
	 */
	private String parseSystemProperties(final String sqlClause) {
		if (!sqlClause.contains("${")) {
			return sqlClause;
		}

		final HashMap<String, String> propMap = new HashMap<String, String>();
		final String[] clauseParts = sqlClause.split("\\$\\{");

		for (String part : clauseParts) {
			if (!part.contains("}")) {
				continue;
			}
			final String key = part.substring(0, part.indexOf('}'));
			final String value = System.getProperty(key, null);
			if (value != null) {
				propMap.put(key, value);
			}
		}

		String clause = sqlClause;

		for (String key : propMap.keySet()) {
			final String value = propMap.get(key);
			clause = clause.replaceAll("\\$\\{" + key + "\\}", value);
		}

		return clause;
	}
}
