package com.ericsson.eniq.backuprestore.backup;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.DatabaseConnections;



public class DataUnloadThread implements Runnable {

	private String tableName;
	private String dateId;
	private String typeName;
	private Logger log;

	private Connection dwhdbConn;
	private Connection dwhrepConn;
	private boolean unloadStatus;
	private boolean directoryCheck;
	
	private String timeLevel;
	private String unloadappend = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private MonitorBackupAggregation deleteEntry;
	private ColumnHeadingsAggregation column;

	DataUnloadThread(String tableName, String dateId, String timeLevel, String typeName, Logger log) {
		this.tableName = tableName;
		this.dateId = dateId;
		this.timeLevel = timeLevel;
		this.typeName = typeName;
		this.log = log;

		this.unloadStatus = false;
		this.directoryCheck = false;
		
	}

	@Override
	public void run() {

		try {

			// TODO Auto-generated method stub
			log.finest("Started the execution of thread for table::" + tableName + " and date:" + dateId);

			dwhdbConn = DatabaseConnections.getDwhDBConnection().getConnection();
			dwhrepConn = DatabaseConnections.getDwhRepConnection().getConnection();
			String filename = "/eniq/flex_data_bkup/" + tableName + "/" + tableName + "_" + dateId + ".txt";

			directoryCheck = directoryCreation(tableName);
			if (directoryCheck) {
				log.finest("Directory for table " + tableName + " is present so continue to unload..");
				String table_partition = tablePartition(tableName, dateId);
				log.finest("Partition name for tablename:" + tableName + " and dateid:" + dateId + " is:"
						+ table_partition);
				if (table_partition != null) {
					column = ColumnHeadingsAggregation.getConnect();

					unloadappend = column.columnHeadersCreation(dwhrepConn, filename, typeName, timeLevel, log);
					if (checkUnload(table_partition, dateId, tableName)) {
						log.finest("Starting to unload for table:" + tableName + " with date_id:" + dateId);
						unloadStatus = unloadQueryExecute(unloadappend, table_partition, dateId, tableName);
					} else {
						log.finest("No entry in the table " + table_partition + " for the date " + dateId
								+ " to unload the data...So unload operation will not proceed.");
					}
					if (unloadStatus) {
						log.finest("Successfully unloaded the data from the table:" + tableName + " with date_id:"
								+ dateId);
						File file = new File(filename);
						if (file.exists()) {

							if (duplicateFileCheck(filename)) {

								createZippedFile(filename);
								deleteEntry = MonitorBackupAggregation.getConnect();

								deleteEntry.deleteTableEntry(dwhdbConn, tableName, dateId, log);

							} else {
								log.info("Duplicate check for filename " + filename + ".gz failed..");
							}
						}

					}

					else {
						log.info("Unload operation failed for the table with tablename:" + tableName + " and dateid:"
								+ dateId);
					}

				} else {
					log.info("Partition for the table with tablename:" + tableName + " and dateid:" + dateId
							+ " is not found");
				}
			}

		}

		catch (Exception e) {
			log.info("Exception while executing thread with tablename::" + tableName + " and date_id::" + dateId + "::"
					+ e);

			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			log.info("stack trace for exception while executing thread with tablename::" + tableName + " and date_id::"
					+ dateId + "::" + exceptionAsString);

		} finally {

			try {

				dwhdbConn.close();
				dwhrepConn.close();
			} catch (SQLException e) {
				log.info("Exception occured while closing the connection:" + e);
				e.printStackTrace();
			}

		}

	}

	boolean checkUnload(String table_partition, String dateId, String tableName) {

		String query = "Select * from " + table_partition + " WHERE DATE_ID='" + dateId + "'";

		boolean check = false;
		ResultSet rs = null;
		try {
			rs = dwhdbConn.createStatement().executeQuery(query);

			while (rs.next()) {
				check = true;
			}

		} catch (SQLException e) {
			log.info("Exception while quering to check the data is available to unload for tablename:" + tableName
					+ " and dateid:" + dateId + "::" + e);
			e.printStackTrace();
		} finally {

			try {
				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
				log.info("Exception while closing the connection of RockFactory object in checkUnload :" + e);
				e.printStackTrace();
			}

		}
		return check;

	}

	boolean unloadQueryExecute(String unloadappend, String table_partition, String dateId, String tableName) {

		String unloadQuery = "UNLOAD Select " + unloadappend + " from " + table_partition + " WHERE DATE_ID='" + dateId
				+ "' INTO FILE '/eniq/flex_data_bkup/" + tableName + "/" + tableName + "_" + dateId
				+ ".txt' APPEND ON;";
		log.finest("unload query for tablename:" + tableName + " ::" + unloadQuery);
		
		boolean queryCheck = false;
		try {
			dwhdbConn.createStatement().execute(unloadQuery);
			queryCheck = true;

		} catch (SQLException e) {
			log.info("Exception occured while doing unload the table for tablename:" + tableName
					+ " and dateid:" + dateId + "::" + e);
			log.finest("unload query for which exception occured::"+unloadQuery);
			e.printStackTrace();
		}

		return queryCheck;
	}

	String tablePartition(String tableName, String dateId) {
		Date partitionDate = null;
		String storageId = tableName.replaceFirst("(?s)_(?!.*?_)", ":");
		try {
			partitionDate = sdf.parse(dateId);

			

		} catch (ParseException e) {
			log.info("Exception occured while parsing the Date element::" + e);
			e.printStackTrace();
		}

		return PhysicalTableCache.getCache().getTableName(storageId, partitionDate.getTime());

		}

	boolean directoryCreation(String tableName) {

		boolean result = false;
		String filename = "/eniq/flex_data_bkup/" + tableName + "/";
		try {

			log.fine("File creation path:" + filename);
			File theDir = new File(filename);

			// if the directory does not exist, create it
			if (!theDir.exists()) {
				log.info("Creating directory with the name: " + filename);

				try {
					theDir.mkdir();
					result = true;
					log.finest("Directory " + filename + " created successfully");
				} catch (Exception se) {
					log.log(Level.SEVERE, "Error in creating Directory: " + filename + "::" + se);
				}

			} else {
				result = true;
				log.finest("Directory already present with the name: " + filename);
			}
		} catch (Exception e) {
			log.info("Exception occured during directory creation.." + e);
		}
		return result;

	}

	boolean duplicateFileCheck(String filename) {
		boolean duplicate_check = false;

		try {

			File file = new File(filename + ".gz");
			if (file.exists()) {
				file.delete();
				log.finest("Duplicate is found for the:" + filename + ".gz and it is deleted successfully..");
			}
			duplicate_check = true;
		} catch (Exception e) {
			log.info("Exception occured during checking duplicate check for file:" + filename + ".gz" + "::" + e);
		}
		return duplicate_check;

	}

	void createZippedFile(String filename) {

		String OUTPUT_GZIP_FILE = filename + ".gz";
		log.finest("destination zip folder:" + OUTPUT_GZIP_FILE);

		byte[] buffer = new byte[1024];

		try {

			GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(OUTPUT_GZIP_FILE));

			FileInputStream in = new FileInputStream(filename);

			int len;
			while ((len = in.read(buffer)) > 0) {
				gzos.write(buffer, 0, len);
			}

			in.close();

			gzos.finish();
			gzos.close();
			File file = new File(filename);
			file.delete();
			log.finest("Zipping the file with filename:" + OUTPUT_GZIP_FILE + " is successfully created");
		} catch (IOException ex) {
			log.info("Exception while zipping the files:" + ex);
		}

	}

	

}
