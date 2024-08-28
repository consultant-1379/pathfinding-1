package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.backuprestore.backup.Utils;

public class Restore_LOG_AggregationStatus {

	private final Utils utils;
	private final Connection dwhdb_conn;
	public Logger log;
	private final String tableName;
	private final File bkupFile;
	private final String storageID;
	private final String temp = "set temporary option CONVERSION_ERROR = OFF;\nset temporary option escape_character = ON;\ncommit;\n";
	private Statement stmt = null;

	public Restore_LOG_AggregationStatus(final Connection conn, final Logger log) {
		

		this.dwhdb_conn = conn;
		this.log = log;
		log.info("inside Restore_LOG_AggregationStatus cons..");
		this.tableName = "LOG_AggregationStatus";

		final String backupDir = Utils.backupDir;
		final File folder = new File(backupDir, "Log_AggregationStatus");
		final File[] BackupFiles = folder.listFiles();
		this.bkupFile = BackupFiles[0];

		this.storageID = "LOG_AggregationStatus:PLAIN";

		this.utils = new Utils(this.log, dwhdb_conn);

	}

	public void createTempTable(final String tableName) throws SQLException {
		
		//log.info("inside createTempTable");
		// SELECT * into temp_table FROM LOG_AggregationStatus where 1=2
		final String sql = "SELECT * into temp_" + tableName + " FROM " + tableName + " where 1=2";
		try {
			stmt = dwhdb_conn.createStatement();
			stmt.execute(sql);
		} catch (final SQLException e) {
			log.severe("could not create temp table");
			e.printStackTrace();
		} finally {
			stmt.close();
		}

	}

	public void dropTempTable(final String tableName) throws SQLException {
		
		//log.info("inside dropTempTable");
		final String sql = "DROP TABLE IF EXISTS TEMP_" + tableName;
		try {
			stmt = dwhdb_conn.createStatement();
			stmt.execute(sql);
		} catch (final SQLException e) {
			log.severe("could not execute drop table statement");
			e.printStackTrace();
		} finally {
			stmt.close();
		}
	}

	public void setTempOptions() throws SQLException {
		
		//log.info("inside setTempOptions");
		try {
			stmt = dwhdb_conn.createStatement();
			stmt.execute(temp);
		} catch (final SQLException e) {
			log.severe("could not set temporary options");
			e.printStackTrace();
		} finally {
			stmt.close();
		}
	}

	public void execute() throws SQLException {

		//log.info("drop if already exists");
		dropTempTable(tableName);

		//log.info(" create temp table");
		createTempTable(tableName);

		//log.info("set temporary options");
		setTempOptions();

		//log.info(" load into temp table from backup file ");
		loadInTempTable(bkupFile.getAbsolutePath(), tableName);

		//log.info(" get distinct datadates from temp table ");
		final List<String> datadates = getDatadates(tableName);

		//log.info(" insert into partitions");
		insert(datadates, storageID, tableName);

		//log.info(" drop temp table");
		dropTempTable(tableName);

		log.info(" delete backup file");
		final boolean del = bkupFile.delete();
		if (del) {
			log.info("backup file deleted successfully");
		} else {
			log.warning("Error deleting backup file");
		}

	}

	public List<String> getDatadates(final String tableName) throws SQLException {
		
		//log.info("inside getDatadates");
		// select distinct datadate from temp_table
		final String sql = "SELECT DISTINCT DATADATE FROM TEMP_" + tableName;
		final List<String> datadates = new ArrayList<>();
		ResultSet rs = null;
		try {
			stmt = dwhdb_conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				log.finest("rs.getString(1) " + rs.getString(1));
				datadates.add(rs.getString(1));
			}
		} catch (final SQLException e) {
			log.severe("could not execute select statement to fetch datadate from temp table");
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
		}
		return datadates;
	}

	public String getPartition(final String storageID, final long datadate) {
		final PhysicalTableCache cache = PhysicalTableCache.getCache();
		if (cache == null) {
			log.warning("PhysicalTableCache is null");
			return null;
		}
		log.finest("cache is :" + cache);
		final String partition = cache.getTableName(storageID, datadate);
		log.finest("partition : " + partition);
		return partition;
	}

	public void insert(final List<String> datadates, final String storageID, final String tableName)
			throws SQLException {
		
	//	log.info("inside insert");

		final ArrayList<String> tableList = new ArrayList<>();
		for (final String datadate : datadates) {
			long dateLong = 0;
			try {
				dateLong = new SimpleDateFormat("yyyy-MM-dd").parse(datadate).getTime();
			} catch (final ParseException e1) {
				log.severe("could not parse datadate");
				e1.printStackTrace();
			}
			log.finest("datelong " + dateLong);
			final String partition = getPartition(storageID, dateLong);

			// delete everything from the partition before inserting first time
			if (!tableList.contains(partition)) {
				truncateTable(partition);
				tableList.add(partition);
			}
			// insert into partition (select * from temptable where datadate =
			// 'datadate')
			final String sql = "insert into " + partition + " (select * from temp_" + tableName + " where datadate='"
					+ datadate + "')";
			log.finest("running sql :" + sql);
			try {
				stmt = dwhdb_conn.createStatement();
				stmt.executeUpdate(sql);
				log.finest("inserted successfully");
			} catch (final SQLException e) {
				log.severe("could not insert into partitions from temp table");
				e.printStackTrace();
			} finally {
				stmt.close();
			}
		}
	}

	public void truncateTable(final String partition) throws SQLException {
		final String sql = "TRUNCATE TABLE " + partition;
		try {
			stmt = dwhdb_conn.createStatement();
			stmt.execute(sql);
		} catch (final SQLException e) {
			log.severe("could not set temporary options");
			e.printStackTrace();
		} finally {
			stmt.close();
		}
	}

	public void loadInTempTable(final String bkupFile, final String MoName) {
		
		//log.info("inside loadInTempTable");
		try {
			final String header = utils.extractHeader(bkupFile);
			final int row = utils.LoadTable("temp_" + MoName, header, bkupFile, dwhdb_conn);
			log.info(row + " rows inserted succesfully");
		} catch (final IOException e) {
			log.severe("could not fetch column header from the file");
			e.printStackTrace();
		}
	}

	public static void main(final String[] args) {

		// final Connection conn =
		// DatabaseConnections.getDwhDBConnection().getConnection();
		//
		// final Restore_LOG_AggregationStatus obj = new
		// Restore_LOG_AggregationStatus(conn, log);
		// obj.execute();
		// try {
		// conn.close();
		// } catch (final SQLException e) {
		// log.severe("could not close connection");
		// e.printStackTrace();
		// }
	}
}
