package com.distocraft.dc5000.etl.engine.system;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpOutput;
import com.jcraft.jsch.JSchException;

import ssc.rockfactory.RockFactory;

public class TriggerDeltaViewCreation extends TransferActionBase {

	private static String scriptPath = "bash /eniq/sw/installer/deltaviewcreation.bsh";
	private static String logPath = "/eniq/log/sw_log/engine";
	private static String flagFilePath = "/eniq/sw/installer";
	private ITransferEngineRMI transferEngine;
	Logger log = null;
	Boolean toRunDeltaView = true;
	RockFactory dwhrep_rf = null;
	RockFactory dwhdb_rf = null;
	RockFactory dwhdb_rf1 = null;
	Connection conn_repdb = null;
	Connection conn_dwhdb = null;
	Connection conn_dwhdb1 = null;
	Statement stmt = null;
	ResultSet rs = null;
	boolean flag_dwhm;
	boolean check_flag = false;

	// For TR HV42766
	public boolean checkforrestore(String techpackname)
	{   
		dwhdb_rf1 = DatabaseConnections.getDwhDBConnection();
		conn_dwhdb1 = dwhdb_rf1.getConnection();
		int counttab =0;
		try {
			stmt = conn_dwhdb1.createStatement();
			String tab_name = techpackname + "%";
					
			rs = stmt.executeQuery(
					"Select count(*) as tabcount from systable where table_name like '"+tab_name+"'") ;
			while (rs.next()) {
				counttab = rs.getInt("tabcount");
				//log.log(Level.INFO, "countview :" + countview);
			}
			if (counttab== 0)
			{
				check_flag =true;
				
				
			}
			
			else
			{
				check_flag= false;
			}
			
			
			
		}
		catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn_dwhdb1.close();
				
			} catch (SQLException e) {
			 log.log(Level.SEVERE, "exception while closing connections");
			}
			
			dwhdb_rf1 = null;
			
		}
		return check_flag;
	}
		
		
	

	public boolean checkdwhmset(List<String> techpacklist) {
		flag_dwhm = true;

		List<Map<String, String>> runningsets = new ArrayList<Map<String, String>>();
		List<Map<String, String>> queuedsets = new ArrayList<Map<String, String>>();

		try {
			if (techpacklist != null && (!techpacklist.isEmpty())) {

				runningsets = transferEngine.getRunningSets(techpacklist);
				queuedsets = transferEngine.getQueuedSets();

				log.log(Level.FINEST, " Running sets info " + runningsets);
				log.log(Level.FINEST, " Queued sets info " + queuedsets);

				runningsets.addAll(queuedsets);
				log.log(Level.FINEST, " Running/Queued sets info " + runningsets);

				for (Map<String, String> sets : runningsets) {
					if (sets.get("setName").startsWith("DWHM_Install")) {
						log.log(Level.INFO, " Setting dwhm flag to false. DeltaViewCreation will not be executed");
						flag_dwhm = false;
						break;
					}
				}

			}
		} catch (RemoteException e) {
			log.log(Level.INFO, "Exception while getting the sets in execution slots/queue");
		}
		return flag_dwhm;

	}

	public void runDWHM() {
		dwhrep_rf = DatabaseConnections.getDwhRepConnection();
		dwhdb_rf = DatabaseConnections.getDwhDBConnection();
		conn_repdb = dwhrep_rf.getConnection();
		conn_dwhdb = dwhdb_rf.getConnection();
		boolean check_restore ;
		boolean setcheck;
		String view;
		// Get all the techpacks that has DELTA support
		final List<String> deltaTechpacks = new ArrayList<String>();

		// Get all the techpacks for which the DWHM Install set has to be run
		final List<String> dwhminstalllist = new ArrayList<String>();

		try {
			stmt = conn_repdb.createStatement();
			rs = stmt.executeQuery(
					"select techpack_name from versioning where versionid in (select DISTINCT versionid from MeasurementDeltaCalcSupport where versionid in (select VERSIONID from TPACTIVATION where status ='ACTIVE' ))");

			while (rs.next()) {
				deltaTechpacks.add(rs.getString("techpack_name"));
			}

			for (String tname : deltaTechpacks) {
				int countview = 0;
				
				stmt = conn_dwhdb.createStatement();
				if (tname.contains("DC_E_IMSGW_SBG")) {
					view = "DC_E_IMSSBG_%_DELTA";
				} else {
					view = tname + "_%_DELTA";
				}
				rs = stmt.executeQuery(
						"Select count(distinct viewname) as syscount from SYSVIEWS WHERE viewname like '" + view + "'");
				while (rs.next()) {
					countview = rs.getInt("syscount");
					//log.log(Level.INFO, "countview :" + countview);
				}
				check_restore=checkforrestore(tname);
				if ((countview == 0) && !(check_restore)) {
					if (!(dwhminstalllist.contains(tname))) {
						dwhminstalllist.add(tname);
					}
				}
			}

			log.log(Level.INFO, "Delta Techpacks in Server are " + deltaTechpacks);
			log.log(Level.INFO, "DWHM INSTALL sets to be executed for " + dwhminstalllist);

			// Check if the DWHM_INSTALL Set is already in the Execution slot or
			// it is Queued.
			setcheck = checkdwhmset(deltaTechpacks);

			if (dwhminstalllist != null && (!dwhminstalllist.isEmpty())) {
				log.log(Level.FINEST, "There are few Techpacks for which DWHM_INSTALL set has to be run");

				if (setcheck) {
					//log.log(Level.FINEST, "coming inside 2nd loop");
					toRunDeltaView = false;

					for (String tpname : dwhminstalllist) {
						try {
							String command = "engine -e startSet " + tpname + " DWHM_Install_" + tpname;
							final LwpOutput dwhm_installresult = LwProcess.execute(command, true, log);

							if (dwhm_installresult.getExitCode() != 0) {
								log.log(Level.WARNING, "Error executing the dwhm install  " + dwhm_installresult);
							}

						} catch (Exception e) {
							log.severe("Exception while running dwhm install " + e.getMessage());
						}
					}
				} else {
					toRunDeltaView = true;
				}
			}
			if (toRunDeltaView) {
				if (setcheck) {
					log.log(Level.FINEST, "Entries in Sysviews are correct.");
					runScript();
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn_dwhdb.close();
				conn_repdb.close();
			} catch (SQLException e) {
			 log.log(Level.SEVERE, "exception while closing connections");
			}
			
			dwhdb_rf = null;
			dwhrep_rf = null;
		}
	}

	public TriggerDeltaViewCreation(final Logger parentlog)
			throws RemoteException, MalformedURLException, NotBoundException, SQLException {

		log = Logger.getLogger(parentlog.getName() + ".DeltaViewCreation");
		String timeStamp = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		String flagFileName = flagFilePath + "/flag_delta_" + timeStamp;
		File flagFile = new File(flagFileName);
		log.log(Level.INFO, " Flag file name " + flagFileName);
		if (flagFile.exists()) {
			log.log(Level.FINEST, " Flag file is present");
			log.log(Level.INFO, "Script was already executed. Please check the logs at " + logPath);
		} else {
			log.log(Level.FINEST, " Flag file is not present");
			transferEngine = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());
			runDWHM();
		}

	}

	public void runScript() {
		String timeStamp = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		log.log(Level.INFO, "Script is triggered to recreate the Delta views");
			try {
				RemoteExecutor.executeComandSshKey("dcuser", "scheduler", scriptPath);
				// Check if any view recreation is failed
				File logDir = new File(logPath);
				for (File file : logDir.listFiles()) {
					if (file.getName().startsWith("failed_delta_views_" + timeStamp)) {
						log.log(Level.SEVERE, "Check for the failed views in the log: " + file.getAbsolutePath());
					}
				}

			} catch (JSchException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
}