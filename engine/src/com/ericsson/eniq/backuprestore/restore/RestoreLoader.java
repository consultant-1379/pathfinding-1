package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.distocraft.dc5000.common.ConsoleLogFormatter;
import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.ericsson.eniq.backuprestore.backup.Utils;
import com.ericsson.eniq.common.DatabaseConnections;
import ssc.rockfactory.RockFactory;

public class RestoreLoader {

	static Logger log;
	static RockFactory rockFact;
	// local restore flag to check if restore is completed for all valid files
	static boolean restoreCompleteFlag = true;

	public RestoreLoader() {
		// TODO Auto-generated constructor stub
	}

	public static void main(final String[] args) {

		try {

			// flag file
			final File flag_file = new File(args[1]);
			// check the date to which we have to restore
			final Date restoreDate = new Date(new Long(args[2]));

			// create Logger
			log = Logger.getLogger("com.ericsson.eniq.backuprestore.restore");
			final FileHandler fh = new FileHandler(args[0], true);
//			fh.setLevel(Level.FINEST);
//			final SimpleFormatter formatter = new ConsoleLogFormatter();
			fh.setFormatter(new ConsoleLogFormatter());
			log.addHandler(fh);
//			uncomment below line to see the finest level logs
//			log.setLevel(Level.FINEST);
			
			// create rockfactory connection
			rockFact = DatabaseConnections.getETLRepConnection();
			log.info("rock factory connection created successfully");
//			System.out.println("this is printed on console");
			// get all the directories in bkup directory
			final String backupDir = Utils.backupDir;
			final File folder = new File(backupDir);
			final File[] listOfTables = folder.listFiles();
			final ArrayList<String> tableList = new ArrayList<>();
			
			//create Utils object
			Utils utils = new Utils();

			File filePath = null;

			// get all the DC_E tables
			for (File table : listOfTables) {
				if (table.isDirectory()) {
					String tableName = table.getName();
					// Full gzip File path
					filePath = new File(backupDir + File.separator + tableName + File.separator + "raw");
					if (filePath.exists()) {
						tableList.add(tableName);
						log.finest("tableName " + tableName);
					}					

				}
			}

			List<Map<String, String>> runningSets;
			List<Map<String, String>> queuedSets;
			String loader_set, techpack;

			// transferEngine = new TransferEngine(log);
			final ITransferEngineRMI termi = (ITransferEngineRMI) Naming
					.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());

			// get currently executing/queued loader sets
			runningSets = termi.getRunningSets();
			log.finest("running sets retrieved....");
			queuedSets = termi.getQueuedSets();
			log.finest("queued sets retrieved....");

			// log.finest("tableList.size() : " + tableList.size());
			// for each table in bkup dir
			for (String typeName : tableList) {
				filePath = new File(backupDir + File.separator + typeName + File.separator + "raw");
					for (final File gzip : filePath.listFiles()) {
						final String filename = gzip.getName();
						log.finest("file name : " + gzip.getName());
						final String str = filename.substring(0, filename.lastIndexOf("_"));
						String str1 = str.substring(str.lastIndexOf("_") + 1);
						if(str1.startsWith("b")){//For handling filenames with/without buildNo
							str1 = str.substring(0, str.lastIndexOf("_"));
							str1 = str1.substring(str1.lastIndexOf("_") + 1);
						}
						str1 = removeIndex(str1);
						final Long lastModified = new Long(str1);
						Date lastMod = new Date(lastModified);
						if (lastMod.compareTo(restoreDate) < 0) {
							// valid file found
							restoreCompleteFlag = false;

							// log.finest("i = " + i);			
							if(typeName.endsWith("_PREV"))
							{
								typeName = typeName.substring(0, typeName.indexOf("_PREV"));
							}
							// get loader set name
							loader_set = "Loader_" + typeName;
							log.finest("Loader set name : " + loader_set);

							log.finest(filename + " is a valid file to be restored");

							// if already running/queued
							if (utils.listContainsIgnoreCase(runningSets, loader_set)
									|| utils.listContainsIgnoreCase(queuedSets, loader_set)) {
								log.info(loader_set + " already running or in queue...");
								// do nothing
							} else {
								// get techpack name
								techpack = utils.getTechpackName(loader_set, rockFact);
								log.finest("techpack name : " + techpack);

								log.info("starting Loader Set " + loader_set);
								termi.execute(techpack, loader_set, "");
								log.info("Loader set triggered successfully");
							}
							// if any one file to be restored found trigger the
							// loader and check for another table
							break;
						} else {
							log.finest("file backed up after restore started. No need to restore");
						}
					}
			}
			// rockFact.getConnection().close();
			// to test
			// restoreCompleteFlag = true;
			if (restoreCompleteFlag) {
				log.info("restore completed. deleting flag");
				// delete the restore flag
				flag_file.delete();
			}

		} catch (final RemoteException e1) {
			log.severe("Could not get running sets... " + e1.getMessage());
			e1.printStackTrace();
		} catch (final Exception e) {
			log.severe("Exception occurred " +e);
			e.printStackTrace();
		}

	}
    /**
     * @param epochString
     * @return
     */
    private static String removeIndex(String epochString) {
		final String currentEpoch = Long.toString(System.currentTimeMillis());
		if(epochString.length() > currentEpoch.length()){
			log.finest("Timestamp in filename has index - " + epochString);
			epochString = epochString.substring(0,currentEpoch.length());
		}
		return epochString;
	}

}
