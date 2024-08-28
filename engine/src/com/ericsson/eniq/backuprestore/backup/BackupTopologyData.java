package com.ericsson.eniq.backuprestore.backup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.eniq.backuprestore.backup.Utils;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.repository.cache.BackupConfigurationCache;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpOutput;

import ssc.rockfactory.RockFactory;

/**
 * Implementation for backing up topology data.
 * 
 * @author xhussho
 *
 */

public class BackupTopologyData extends TransferActionBase implements Runnable {

	private Utils utils;

	Logger log = null;

	Connection dwh_conn = null;

	private List<String> dimTableList = new ArrayList<String>();

	File interFile = null;
	
	Statement stmt;
	
	ResultSet rs;

	public BackupTopologyData(final Logger parentlog) throws SQLException, ParseException, IOException {

		log = Logger.getLogger(parentlog.getName() + ".BackupTopologyData");
	}

	private void cleanup() {

		new File(Utils.savePath).delete();

		new File(Utils.FilePath).delete();

	}

	/**
	 * Method to extract data from DIM tables and storing it in a backup
	 * directory.
	 * 
	 * @author xhussho
	 * @param dimTableList
	 * @param dwh_conn
	 * @throws IOException
	 */

	public void extractData(List<String> dimTableList, Connection dwh_conn){
		List<String> dimFile = new ArrayList<String>();
		
		String timeStamp = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		
		String command = Utils.Script + " " + Utils.savePath + " " + Utils.backupDir;
		
		BufferedWriter out = null;
		
		FileWriter fstream;
		
		// write column headers into the file
		for (String tableName : dimTableList) {
			try {
				StringBuilder columnString=new StringBuilder();
				String columnHeader = null;
				final String sql = "select cname from sys.SYSCOLUMNS where tname ='"+tableName+"' and creator = 'dc'";
				 stmt = dwh_conn.createStatement();
				 rs = stmt.executeQuery(sql);
				fstream = new FileWriter(Utils.backupDir + File.separator + tableName + File.separator + tableName + "_" + timeStamp + ".txt");
				out = new BufferedWriter(fstream);
				while (rs.next()) {
					columnString.append(rs.getString(1).trim());
					columnString.append(",");
					}
				columnHeader=columnString.substring(0,columnString.length()-1);
				out.write(columnHeader);
				out.write("\n");
			} catch (SQLException e) {
				log.warning("SQL Exception caught while fetching column names from dwhcolumn " + e.getMessage());
			} catch (Exception e) {
				log.warning("Exception caught while writing headers into text files " + e.getMessage());
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						log.warning("Could not close BufferdWriter" + e.getMessage());
					}
				}
					try{
						if(rs != null)
						rs.close();
					}
					catch(Exception e){
						log.warning("Could not close ResultSet" + e.getMessage());
					}
					try{
						if(stmt != null)
						stmt.close();
					}
					catch(Exception e){
						log.warning("Could not close Statement" + e.getMessage());
					}
			}
		}
		log.finest("Completed writing column headers into text file");
		//perform bcp operations
		try {
			final LwpOutput bcpresult = LwProcess.execute(command, true, log);
			log.log(Level.FINEST, "Executing the script " + Utils.Script + " to perform unload operation.");
			if (bcpresult.getExitCode() != 0) {
				log.log(Level.WARNING, "Error executing the unload command " + bcpresult);
			}
		} catch (Exception e) {
			log.severe("Exception while running unload command " + e.getMessage());
		}

		try {
			dimFile = getMonames();
		} catch (IOException e) {
			log.log(Level.WARNING, "Error while fetching MO names "+ e.getMessage());
		}
		utils.gzipIt(dimFile);

	}

	/**
	 * Method for fetching the list of Dim Tables which have been backed.
	 * 
	 * @author xhussho
	 * @throws IOException
	 */

	public List<String> getMonames() throws IOException {
		Scanner s = new Scanner(new File(Utils.FilePath));
		ArrayList<String> files = new ArrayList<String>();
		while (s.hasNext()) {
			files.add(s.next());
		}
		s.close();
		return files;
	}

	/**
	 * Method for deleting the previous day files.
	 * 
	 * @author xhussho
	 * @throws IOException
	 */
	public void deletePrevious(){

		List<String> filedelete = new ArrayList<String>();
		try {
			filedelete = getMonames();
		} catch (IOException e) {
			log.log(Level.WARNING, "Error while fetching MO names "+ e.getMessage());
		}
		for (String mo : filedelete) {
			File file = new File(Utils.backupDir + File.separator + mo);
			File[] fname = file.listFiles();

			for (File path : fname) {
				String str = path.toString();
				String filedate = str.substring(str.lastIndexOf("_") + 1, str.indexOf("."));
				DateFormat format = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
				Date yesterDate;
				try {
					yesterDate = format.parse(filedate);
					Date latestDate = format
							.parse(new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
					if (yesterDate.compareTo(latestDate) < 0) {
						path.delete();
						log.log(Level.FINE, file.getName() + " is deleted!");

					} else {
						log.log(Level.FINE, "No old Backup file found.s");
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	@Override
	public void run() {
		String timeStamp = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());

		log.log(Level.INFO, "Topology Backup started at " + timeStamp);

		try {

			dwh_conn = DatabaseConnections.getDwhDBConnection().getConnection();
			

			utils = new Utils(log, dwh_conn);

			dimTableList = utils.preCheck(); // calling the function to get
												// the
												// list of enabled MO's
			utils.createDir(dimTableList);
			// extract the data from dim tables using bcp
			extractData(dimTableList, dwh_conn);
			// delete the previous day data if exists
			deletePrevious();

			log.log(Level.INFO, dimTableList.size() + " number of files backed up.");

			log.log(Level.INFO, "Topology Backup ended at " + timeStamp);

		} catch (Exception e) {
			log.log(Level.INFO, "Exception while backing up topology data " + e.getMessage());
		} finally {
			
			cleanup();
			
			if (dwh_conn != null) {
				try {
					dwh_conn.close();
				} catch (SQLException e) {
					/**/}
			}
		}
	}

}
