package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.io.FileFilter;
import java.util.logging.Logger;

public class TriggerDataRestoreProcess {
	private Logger log;
	private boolean triggerTopology = false;
	private boolean triggerAggregation = false;

	public TriggerDataRestoreProcess() {
		log = Logger.getLogger("etlengine.TriggerDataRestoreProcess");

		// Check if the flag files are present for topology and aggregation
		File dir = new File("/var/tmp");
		File[] matchingFiles = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				return pathname.getName().startsWith("flag_");
			}
		});

		for (int i = 0; i < matchingFiles.length; i++) {
			if (matchingFiles[i].getName().contains("flag_topologyrestore")) {
				triggerTopology = true;
			} else if (matchingFiles[i].getName().contains("flag_aggregationrestore_")) {
				triggerAggregation = true;
			}
		}

		triggerProcess();
	}

	private void triggerProcess() {
		// Trigger the topology restore process
		if (triggerTopology) {
			log.info("Triggering the Topology Restore process!");
			final RestoreTopologyData restoreTopology = new RestoreTopologyData();
			restoreTopology.restoreTopo();
		} else {
			log.warning("Topology Restore process not triggered!");
		}

		if (triggerAggregation) {
			log.info("Triggering the Aggregation Restore process!");
			TraverseFlexDataBackupFS triggerAggregation = new TraverseFlexDataBackupFS();
			Thread triggerAggRestore = new Thread(triggerAggregation);
			triggerAggRestore.start();
		} else {
			log.warning("Aggregation Restore process not triggered!");
		}

	}
}
