package com.ericsson.eniq.backuprestore.backup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.cache.*;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpOutput;
import com.jcraft.jsch.JSchException;
import com.distocraft.dc5000.repository.cache.BackupConfigurationCache;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

/**
 * This class takes a backup of previous day aggregated data and create a zipped
 * file.
 * 
 * @author xsarave
 * 
 */
public class BackupAggregatedData extends TransferActionBase implements Runnable {

	

	private RockFactory dwhdb;
	private RockFactory repdb;
	private RockFactory etlrep;
	private Logger log;
	
	
	private static final String dbisqlPath = "bash /eniq/sw/bin/backupaggregation.bsh";
	
	
	
	private StringBuilder insertQuery = new StringBuilder(
			"insert into LOG_BackupAggregation(TARGET_TABLE,DATE_ID,TIMELEVEL,TYPENAME) select distinct target_table,date_id,timelevel,typename from dc.log_aggregationstatus as a join dc.log_aggregationrules as b on a.AGGREGATION = b.AGGREGATION and a.aggregationscope not in ('month','week')  where (convert(date,INITIAL_AGGREGATION)=(CURRENT DATE-1) or convert(date,LAST_AGGREGATION)=(CURRENT DATE-1)) and b.aggregationscope not in ('month','week') and a.status = 'aggregated' and target_table in(");
	private DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
	private Date datecheck;

	/**
	 * This function is the constructor of this BackupAggregatedData class.
	 * 
	 * @param parentlog
	 * @param etlrep
	 * 
	 * 
	 */
	public BackupAggregatedData(Logger parentlog, RockFactory etlrep) {
		this.etlrep = etlrep;
		this.log = Logger.getLogger(parentlog.getName() + ".BackupAggregatedData");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (BackupConfigurationCache.getCache().isBackupStatus()
				&& (BackupConfigurationCache.getCache().getBackupLevel().equals("AGGREGATED")
						|| BackupConfigurationCache.getCache().getBackupLevel().equals("BOTH"))) {
			log.info("Backup is enabled trying to take backup for aggregation");
			backupAggregateTable();
			logaggregationBackup();
		} else{
			log.info("Backup is not enabled for aggregation.Hence Exiting!");
		}

	}

	public String checkDate() {
		String checkState = null;
		try {

			StaticProperties.reload();
			String date = StaticProperties.getProperty("BackupAggregationCheck");

			datecheck = new Date();
			String check = dateFormat.format(datecheck);
			if (date.equals("NIL") || !date.equals(check)) {
				checkState = "checked";
				log.finest("Backup for aggregation is running for first time for the date:" + check);
			} else {
				checkState = "AlreadyRan";
				log.finest("Backup for aggregation is already ran for the date:" + check);
			}
			// System.out.println("out:"+s);

		} catch (IOException e) {
			checkState = "Exception";
			log.info("Exception while checking  date in static properties file.." + e);
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			checkState = "Exception";
			log.info("Exception while checking  date in static properties file.." + e);
			e.printStackTrace();
		}
		return checkState;
	}

	void backupAggregateTable() {

		try {

			log.finest("trying to execute the aggregate method");

			dwhdb = DatabaseConnections.getDwhDBConnection();
			repdb = DatabaseConnections.getDwhRepConnection();

			String preCheck = checkDate();
			if (preCheck.equals("checked")) {

				if (insertTable()) {

					try {
						StaticProperties.reload();
						datecheck = new Date();
						String dateEntry = dateFormat.format(datecheck);
						boolean entryCheck = StaticProperties.setProperty("BackupAggregationCheck", dateEntry);
						if (entryCheck) {
							log.info("staticproperties file is updated with todays date:" + dateEntry);
						} else {
							log.info("Error in updating staticproperties file with todays date:" + dateEntry
									+ "...trying for one more time");
							boolean entryCheck1 = StaticProperties.setProperty("BackupAggregationCheck", dateEntry);
							if (entryCheck1) {
								log.info("staticproperties file is updated with todays date:" + dateEntry);
							} else {

								log.info("Error in updating staticproperties file for second time with todays date:"
										+ dateEntry);
							}

						}

					} catch (IOException e) {
						log.info("Exception while updating todays date into static properties file.." + e);
						e.printStackTrace();
					}

				}

			} else if (preCheck.equals("AlreadyRan")) {
				log.info("Backup for Aggregation already ran for today's date:" + dateFormat.format(datecheck));
			}

			// remaining code

			unloadQueryCreation();

		} catch (Exception e) {
			log.info("exception occured in backup of aggregation in aggregate method::" + e);
		} finally {

			try {

				dwhdb.getConnection().close();
				repdb.getConnection().close();
			} catch (SQLException e) {
				log.info("Exception occured while closing the connection:" + e);
				e.printStackTrace();
			}

		}
	}

	boolean insertTable() {
		String query = getInsertQuery();
		log.finest("Insert query to insert into LOG_BackupAggregation Table:" + query);
		boolean check = false;
		Statement stmt = null;
		try {
			stmt = dwhdb.getConnection().createStatement();
			int action = stmt.executeUpdate(query);

			if (action > 0) {
				check = true;
				log.finest("Inserting data into LOG_BackupAggregation table is successful");
			} else {
				check = false;
				log.finest(
						"No aggregated data in the Log_aggregationstatus table so Inserting data into LOG_BackupAggregation table is not happening...");
			}

		} catch (Exception e) {

			log.info("Exception occured while inserting data into LOG_BackupAggregation table.." + e);
		} finally {

			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				log.warning("Could not close Statement" + e.getMessage());
			}
		}

		return check;

	}

	String getInsertQuery() {
		String getEnabledTypenameQuery = "select distinct d.basetablename from versioning as v,dwhtype as d, BackupConfiguration as b WHERE v.VERSIONID in (select distinct VERSIONID from dwhrep.tpActivation where STATUS='ACTIVE') and v.TECHPACK_NAME = d.TECHPACK_NAME and b.ENABLED_FLAG = 'Y' group by v.LICENSENAME,d.basetablename,b.LICENSEID having v.LICENSENAME like '%'+b.LICENSEID+'%'";
		ResultSet result = null;
		String insertQuerywithTablename = null;

		try {
			result = repdb.getConnection()
					.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
					.executeQuery(getEnabledTypenameQuery);
			while (result.next()) {
				insertQuery.append("'");
				insertQuery.append(result.getString("basetablename"));
				insertQuery.append("',");
			}
			insertQuerywithTablename = insertQuery.substring(0, insertQuery.length() - 1) + ")";
		} catch (Exception e) {
			log.info("Exception occured during getting the tablename for which the backup is enabled.." + e);
		} finally {
			try {

				if (result != null) {
					result.close();
				}
			} catch (SQLException e) {
				log.info("Exception while closing the resultset of getInsertQuery:" + e);
				e.printStackTrace();
			}
		}

		return insertQuerywithTablename;
	}

	void unloadQueryCreation() {
		BackupAggregationThreadHandling backupAggregation = BackupAggregationThreadHandling.getinstance();

		String query = "select TARGET_TABLE,DATE_ID,TIMELEVEL,TYPENAME from LOG_BackupAggregation";

		ResultSet result = null;

		try {

			result = dwhdb.getConnection().createStatement().executeQuery(query);

			while (result.next()) {
				log.finest("Data from the LOG_BackupAggregation table::");
				log.finest(
						"Tablename:" + result.getString("target_table") + ":::Date_id:" + result.getString("date_id"));
				backupAggregation.processMessage(result.getString("target_table"), result.getString("date_id"),
						result.getString("timelevel"), result.getString("typename"), log);
			}
		} catch (SQLException e) {
			log.info("Exception occured while selecting data from LOG_BackupAggregation table.." + e);
			e.printStackTrace();
		} finally {
			try {

				if (result != null) {
					result.close();
				}
			} catch (SQLException e) {
				log.info("Exception while closing the resultset of unloadQueryCreation:" + e);
				e.printStackTrace();
			}
		}

	}

	void logaggregationBackup() {
		try {
			final LwpOutput dbisqlresult = LwProcess.execute(dbisqlPath, true, log);
			log.log(Level.FINEST,
					"Executing the script " + dbisqlPath + " to perform backup for log_aggregationstatus table");
			if (dbisqlresult.getExitCode() != 0) {
				log.log(Level.WARNING,
						"Error executing the backupaggregation script while taking backup for log_aggregationstatus table:: "
								+ dbisqlresult);
			}
		} catch (Exception e) {
			log.severe(
					"Exception while executing the backupaggregation script while taking backup for log_aggregationstatus table "
							+ e.getMessage());
		}
	}

}
