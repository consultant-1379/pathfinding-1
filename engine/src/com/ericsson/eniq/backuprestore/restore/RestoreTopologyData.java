package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.ericsson.eniq.backuprestore.backup.Utils;
import com.ericsson.eniq.common.DatabaseConnections;

import ssc.rockfactory.RockFactory;

/**
 * The main class to restore the topology data.
 *
 * @author xdivykn
 * 
 */

public class RestoreTopologyData {

	private RockFactory rockfact;

	private Connection dwhrep_conn;

	private Connection dwhdb_conn;

	private String collectionSetName = null;

	private Logger log;

	private ArrayList<String> dimTables = new ArrayList<String>();

	private boolean topologyRestoreFlag = false;

	private String columnHeader = null;

	private String MoName = null;

	private int row = 0;

	private List<Map<String, String>> runningSets;

	private List<Map<String, String>> queuedSets;
	

//	private final String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());

	private final String temp = "set temporary option CONVERSION_ERROR = OFF;\nset temporary option escape_character = ON;\ncommit;\n";

	public RestoreTopologyData() {

		log = Logger.getLogger("etlengine.RestoreTopologyData");

		rockfact = DatabaseConnections.getETLRepConnection();

		dwhrep_conn = DatabaseConnections.getDwhRepConnection().getConnection();

		dwhdb_conn = DatabaseConnections.getDwhDBConnection().getConnection();
	}

	/**
	 * Invokes the method to restore topology data
	 * 
	 * 
	 */
	public void restoreTopo() {

		try {
			restore();
			
			log.info("Restore of topology data ended at " + new Date().getTime());
		} catch (Exception e) {
			log.warning("Exception in restoring topology data" + e);
		} finally {
			try {
				if (dwhrep_conn != null) {
					dwhrep_conn.close();
				}
				if (dwhdb_conn != null) {
					dwhdb_conn.close();
				}
			} catch (Exception e) {
				log.warning("Exception while closing connection" + e.getMessage());
			}
		}
	}
	/**
	 * Method to load into DIM tables and trigger appropriate Topology updaters
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws MalformedURLException 
	 * 
	 * 
	 */
	public void restore() throws MalformedURLException, RemoteException, NotBoundException {

		log.info("Restore of topology data started at " + new Date().getTime());
		
		dimTables = getAllDimTables();
		log.info("The number of tables to be restored are " + dimTables.size());
		
		final Iterator<String> iterator = dimTables.iterator();
		Utils utilObject = new Utils(log);
		File directory = null;
		String nextMO = null;
		String UpdaterMO = null;
		Statement statmnt = null;
		File flagDir = new File("/var/tmp");
		File flagList[] = flagDir.listFiles();
		File flagFile = null;
		
		for(File f : flagList)
		{
			if(f.getName().startsWith("flag_topologyrestore_"))
			{
				flagFile = new File(String.valueOf(File.separator) + "var" + File.separator + "tmp"
						+ File.separator + f.getName());
			}
		}
		log.fine("flag file : " +flagFile.getName());
		final ITransferEngineRMI termi = (ITransferEngineRMI) Naming
				.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());

		// get currently executing/queued sets
		runningSets = termi.getRunningSets();
		log.finest("running sets retrieved....");
		queuedSets = termi.getQueuedSets();
		log.finest("queued sets retrieved....");
		
		try {
			statmnt = dwhdb_conn.createStatement();
			statmnt.execute(temp);
		} catch (SQLException e) {
			log.warning("Error occured while running " + temp + ". " + e.getMessage());
		} finally {
			if (statmnt != null) {
				try {
					statmnt.close();
				} catch (SQLException e) {
					log.warning("Failed to close statment. " + e.getMessage());
				}
			}
		}
		//start the actual restore process
		while (iterator.hasNext()) {

			nextMO = iterator.next();
			UpdaterMO = "TopologyUpdater_" + nextMO;
			directory = new File(String.valueOf(Utils.backupDir) + File.separator + nextMO);

			final File[] listOfFiles = directory.listFiles();

			for (int i = 0; i < listOfFiles.length; i++) {

				if (listOfFiles[i].isFile()) {

					final String fileName = String.valueOf(Utils.backupDir) + File.separator + nextMO + File.separator
							+ listOfFiles[i].getName();
					// obtain the columns from the back up files

					try {
						columnHeader = utilObject.extractHeader(fileName);
					} catch (IOException e) {
						e.printStackTrace();
					}

					MoName = nextMO + "_CURRENT_DC";
					// check if current loader or updater is running in the
					// execution slot

					if (!(utilObject.listContainsIgnoreCase(runningSets, "TopologyLoader_" + MoName)
							|| utilObject.listContainsIgnoreCase(queuedSets, "TopologyLoader_" + MoName))) {
						try {
							row = utilObject.LoadTable(MoName, columnHeader, fileName, dwhdb_conn);


							collectionSetName = utilObject.getTechpackName(UpdaterMO, rockfact);

							if (row > 0 && collectionSetName != null) {
								log.info("Restored " + MoName + ". Triggering updater for " + nextMO);
								triggerUpdater(collectionSetName, UpdaterMO, "");
								log.info("Restored " +nextMO);
							}
							deleteTopology(nextMO);
						} catch (Exception e) {
							
							log.warning("Error in executing the load statement for " + MoName + ". " + e.getMessage());
						}
					}else{
						log.info("Topology Loader for the "+ MoName + "is running. Hence restore cannot be done.");
					}
				}

			}
		}
		log.info("Topology restore completed!!");
		// check if any MO failed to restore
		if (getAllDimTables().size() == 0) {
			topologyRestoreFlag = true;
		}
		// Delete the flagfile if restore is successful
		if (topologyRestoreFlag) {

			flagFile.delete();
			log.info("Deleted restore flag as topology restored successfully");
	}else{
		log.info("Could not delete topology flag file " + flagFile);
	}
	}

	/**
	 * Once the topology data is restored delete the file available in the
	 * backup directory
	 * 
	 * 
	 */
	private boolean deleteTopology(String MO) {
		boolean deletesuccesful = false;
		File file = new File(Utils.backupDir + File.separator + MO);
		if (file.isDirectory()) {

			for (File fileList : file.listFiles()) {
				fileList.delete();
				log.fine("Deleted file " + fileList + "sucessfully.");
			}
			deletesuccesful = file.delete();
			log.fine("Deleted directory " + file + "sucessfully.");
		}
		
		return deletesuccesful;
	}

	/**
	 * Triggers Topology Updater for the collectionSetName.
	 *
	 * 
	 * 
	 */
	private void triggerUpdater(final String collectionSetName, final String collectionName,
			final String scheduleInfo) {
		try {
			final EngineAdmin admin = new EngineAdmin();
			admin.startSet(collectionSetName, collectionName, "");
		} catch (Exception ex) {
			log.warning("Triggering updater " + collectionName + " failed");
		}
	}

	/**
	 * Returns a list of Files that are to be restored available in the backup
	 * directory.
	 *
	 * 
	 * 
	 */
	private ArrayList<String> getAllDimTables() {
		final File file = new File(Utils.backupDir);
		File[] listFiles;
		ArrayList<String> dimTables = new ArrayList<>();
		for (int length = (listFiles = file.listFiles()).length, i = 0; i < length; ++i) {
			final File f = listFiles[i];
			if(f.getName().startsWith("DIM_") || f.getName().startsWith("DC_E_FFAX_"))
			{
//				log.info(f.getAbsolutePath());
				dimTables.add(f.getName());

			}
		}
		return dimTables;
	}
}
