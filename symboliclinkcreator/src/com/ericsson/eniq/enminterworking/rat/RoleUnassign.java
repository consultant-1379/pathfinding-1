package com.ericsson.eniq.enminterworking.rat;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.EnmInterworking;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.flssymlink.fls.Main;

import ssc.rockfactory.RockFactory;

/**
 * Processes that perform unassign roles for Multi ES
 * @author xarjsin
 *
 */
public class RoleUnassign {
	
	static Logger log ;
	static IEnmInterworkingRMI multiEs;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			log = Logger.getLogger("symboliclinkcreator.nat");
//			multiEs = (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterCommonUtils.getEngineIP()));
//			log = (Logger) multiEs.getFLSMainInstance();
			
			System.setSecurityManager(new com.distocraft.dc5000.etl.engine.ETLCSecurityManager());
			String ownRole = EnmInterCommonUtils.getSelfRole();
			log.info("ownRole : " + ownRole);
			if(ownRole.equalsIgnoreCase("MASTER")){
				unAssignSlaves();
				unAssignSelf();
			}
			else if(ownRole.equalsIgnoreCase("SLAVE")){
				unAssignMaster();
				unAssignSelf();
			}
			else{
				log.info("The server is not assigned any role");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void unAssignSlaves() {
		log = Logger.getLogger("symboliclinkcreator.nat");
		List<String> slaveIPs = EnmInterCommonUtils.getSlaveIPList();
		for(String slaveIP : slaveIPs){
			try {
				multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(slaveIP));
				String unAssignMsg = multiEs.unAssignSelfSlave();
				log.info(unAssignMsg + " for server - " + slaveIP);
			} catch (Exception e) {
				log.warning("Slave unassign failed for " + slaveIP + " with error " + e);
				log.info("Slave unassign failed for " + slaveIP);
			}
		}
	}

	private static void unAssignMaster() {
		log = Logger.getLogger("symboliclinkcreator.nat");
		String masterIP = EnmInterCommonUtils.getMasterIP();
		try {
			multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(masterIP));
			String unAssignMsg = multiEs.unAssignSpecSlave(EnmInterCommonUtils.getEngineHostname(),EnmInterCommonUtils.getEngineIP());
			log.info(unAssignMsg + " in master server - " + masterIP);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			log.warning("Master unassign failed in " + masterIP + " with error " + e);
			log.info("Master unassign failed in " + masterIP);
		}
	}

	private static void unAssignSelf() {
		log = Logger.getLogger("symboliclinkcreator.nat");
		String deleteRole = "DELETE FROM ROLETABLE";
		RockFactory rockCon = DatabaseConnections.getDwhRepConnection();
		log.finest("Delete from roletable SQL - " + deleteRole);
		try {
			rockCon.getConnection().createStatement().executeUpdate(deleteRole);
			log.info("No role assigned to server now");
			log.info("No role assigned to server now");
		} catch (SQLException e) {
			log.warning("Failed to unassign server" + e);
			log.info("Failed to unassign server");
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
	}
}
