/**
 * 
 */
package com.ericsson.eniq.enminterworking.rat;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;

import ssc.rockfactory.RockFactory;

/**
 * @author xarjsin
 *
 */
public class RoleAssignment {
	
	static Logger log;

	public static String makeMasterSlave(String slaveIP) throws MalformedURLException, RemoteException, NotBoundException {
		log = Logger.getLogger("symboliclinkcreator.RoleAssignmentTool");
		String result = null;
		IEnmInterworkingRMI multiEs = null;
		IEnmInterworkingRMI multiEsMaster = null;
		try {
			String masterIP = EnmInterCommonUtils.getEngineIP();
			multiEsMaster = (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(masterIP));
			multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(slaveIP));
			String otherRole = multiEs.getCurrentRole();
			String ownRole = EnmInterCommonUtils.getSelfRole();
			log.info("logger name :"+log.getName());
			log.info("Remote server role - " + otherRole);
			log.info("Current server role - " + ownRole);

			if((ownRole.equals("MASTER") || ownRole.equals("UNASSIGNED")) && otherRole.equals("UNASSIGNED")){
				log.info("Valid condition - will update remote server as slave");
				
				String slaveHost = multiEs.updateSlave(EnmInterCommonUtils.getEngineHostname(), masterIP);
				if(!slaveHost.equals("FAIL")){
					multiEs.refreshNodeAssignmentCache();
					if (ownRole.equals("UNASSIGNED")){
						result = insertMaster(slaveHost,slaveIP);
						multiEsMaster.refreshNodeAssignmentCache();
						log.info("Initializing health monitor from makeMaster , own role = "+ownRole);
						multiEsMaster.startHealthMonitor();
					}
					else{
						result = updateMaster(slaveHost, slaveIP);
						multiEsMaster.refreshNodeAssignmentCache();
						log.info("Initializing health monitor from makeMaster , own role = "+ownRole);
						multiEsMaster.startHealthMonitor();
					}
					if (("FAIL").equals((multiEs.updateSlaveNAT(masterIP)))) {
						result = "FAIL";
					}
				}
				else{
					log.warning("Slave update through RMI method call failed");
					result = "FAIL";
				}
			}
			else {
				log.info("The server specified by the IP " + slaveIP + " is already assigned a role");
				result = "ASSIGNED";
			}
		}
		catch (Exception e){
			log.log(Level.WARNING,"Slave update failed",e);
			result = "FAIL";
		}
		log.finest("Result of makeMasterSlave - " + result);
		return result;
	}

	private static String updateMaster(String slaveHost, String slaveIP) {
		log = Logger.getLogger("symboliclinkcreator.RoleAssignmentTool");
		String result = null;
		RockFactory rockCon = DatabaseConnections.getDwhRepConnection();
		String slaveInsert = "Insert into RoleTable (ENIQ_ID, IP_ADDRESS, ROLE) Values ('" 
				+ slaveHost + "', '" 
				+ slaveIP + "', 'SLAVE')";
		log.finest("Slave insert SQL - " + slaveInsert);
		try {
			rockCon.getConnection().createStatement().executeUpdate(slaveInsert);
			result = "UPDATED";
			log.info("Slave details updated successful");
		}
		catch (Exception e) {
			log.warning("Failed to update Slave details in RoleTable");
			return "FAIL";
		}
		finally {
			try {
				if (rockCon.getConnection() != null) {
		        	rockCon.getConnection().close();
			    }
		    } 
			catch (SQLException e) {
		    	  log.log(Level.SEVERE, "Exception: " + e);
			}
		}
		log.finest("Result of updateMaster - " + result);
		return result;
	}

	private static String insertMaster(String slaveHost, String slaveIP) {
		log = Logger.getLogger("symboliclinkcreator.RoleAssignmentTool");
		String result = null;
		RockFactory rockCon = DatabaseConnections.getDwhRepConnection();
		String masterInsert = "Insert into RoleTable (ENIQ_ID, IP_ADDRESS, ROLE) Values ('" 
				+ EnmInterCommonUtils.getEngineHostname() + "', '" 
				+ EnmInterCommonUtils.getEngineIP() + "', 'MASTER')";
		String slaveInsert = "Insert into RoleTable (ENIQ_ID, IP_ADDRESS, ROLE) Values ('" 
				+ slaveHost + "', '" 
				+ slaveIP + "', 'SLAVE')";
		log.finest("Slave insert SQL - " + slaveInsert);
		log.finest("Master insert SQL - " + masterInsert);
		try {
			rockCon.getConnection().createStatement().executeUpdate(slaveInsert);
		}
		catch (Exception e) {
			log.warning("Failed to update Slave details in RoleTable");
			return "FAIL";
		}
		try{
			rockCon.getConnection().createStatement().executeUpdate(masterInsert);
			result = "UPDATED";
		}
		catch (Exception e) {
			log.warning("Failed to update Master details in RoleTable");
			return "FAIL";
		}
		finally {
			try {
				if (rockCon.getConnection() != null) {
		        	rockCon.getConnection().close();
			    }
		    } 
			catch (SQLException e) {
		    	  log.log(Level.SEVERE, "Exception: " + e);
			}
		}
		log.finest("Result of insertMaster - " + result);
		return result;
	}
}
