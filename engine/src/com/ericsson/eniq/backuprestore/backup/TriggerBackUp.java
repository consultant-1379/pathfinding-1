package com.ericsson.eniq.backuprestore.backup;

import java.io.File;
import java.io.FileFilter;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.repository.cache.BackupConfigurationCache;

import ssc.rockfactory.RockFactory;

public class TriggerBackUp extends TransferActionBase {

	final static String varDir = "/var/tmp";

	public TriggerBackUp(Logger log, RockFactory rockFact, String transferActionType) {

		boolean status = false;

		Thread t = null;

		try {
			status = BackupConfigurationCache.getCache().isBackupStatus();

			if (status == true) {

				if (transferActionType.contains("Topology")) {
					if (!checkForFlags(log)) {
						t = new Thread(new BackupTopologyData(log));
						log.log(Level.INFO, "Triggering " + transferActionType + " action.");
						t.start();
					} else {
						log.log(Level.INFO, "OMBS Restore ongoing. Backup for Topology cannot be triggered");
					}
				} else if (transferActionType.contains("Aggregation")) {
					t = new Thread(new BackupAggregatedData(log, rockFact));
					log.log(Level.INFO, "Triggering " + transferActionType + " action.");
					t.start();
				}
			} else {
				log.log(Level.INFO, " 2 weeks backup is not enabled. ");
			}

		} catch (Exception e) {
			log.log(Level.INFO, "Exception in triggering the action - " + transferActionType + e.getMessage());
		}

	}

	private boolean checkForFlags(Logger log) {
		boolean flag = false;
		File file = new File(varDir + File.separator + ".flex_restore_inprogress");
		File dir = new File(varDir);
		File[] matchingFiles = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().startsWith("flag_");
			}
		});

		if ((matchingFiles.length == 0) && (file.exists())) {
			return true;
		} else {
			for (int i = 0; i < matchingFiles.length; i++) {
				if (file.exists() || matchingFiles[i].getName().contains("flag_topologyrestore")) {
					flag = true;
					break;
				} else {
					flag = false;
				}
			}
		}
		log.log(Level.FINEST, " Returning flag  " + flag);
		return flag;
	}

}
