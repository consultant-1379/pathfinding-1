/**
 * 
 */
package com.ericsson.eniq.enminterworking;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.utilities.DbUtilities;

import ssc.rockfactory.RockFactory;

/**
 * @author xsukany
 *
 */
public class IRMIManualNodeAssignment {

	/**
	 * @param args
	 */
	
	private String node_fdn;
	private String eniq_identifier;
	private RockFactory rf_etlrep;
	private RockFactory rf_dwhrep;
	private Connection con;
	Statement stmt;
	private Logger log;
	final String roleSql = "Select IP_ADDRESS from RoleTable where ROLE='SLAVE'";
	
	public static void main(String[] args) {
		//System.out.println("calling IRMIManualNodeAssignment. Arguments: " + args[0] + "\t" + args[1] + "\t" + args[2] );
		IRMIManualNodeAssignment irmiManualNodeAssignment = new IRMIManualNodeAssignment();
		irmiManualNodeAssignment.ManualNodeAssignment(args[0], args[1] , args[2]);
	}
	
	public void ManualNodeAssignment ( String eniq_id, String nodeFDN , String eniq_alias ) {
		log = Logger.getLogger("symboliclinkcreator.nat");
		
		boolean isRmiUpdated = false;
		ResultSet rs = null;
		eniq_identifier = eniq_id;
		node_fdn = nodeFDN;
//		log.info("Re-Assigned the node FDN " + node_fdn + " to " + eniq_identifier + " ENIQ-S server");
		try{
			DbUtilities utility = new DbUtilities();
			rf_etlrep = utility.connectToEtlrep();
			rf_dwhrep = utility.connectTodb(rf_etlrep, "dwhrep");
			con = rf_dwhrep.getConnection();
			stmt = con.createStatement();
			try {
				rs = stmt.executeQuery(roleSql);
			} catch (Exception e) {
				log.finest("Exception occured while fetching all the SLAVE servers IP from ROLE Table" + e.getMessage());
				try{
					closeConnections();							
					rf_etlrep = utility.connectToEtlrep();
					rf_dwhrep = utility.connectTodb(rf_etlrep, "dwhrep");
			    	con = rf_dwhrep.getConnection();
			    	
			        stmt = con.createStatement();
			        rs = stmt.executeQuery(roleSql);
//			        log.finest("Successfully fetched all the SLAVE servers IP from ROLE Table");
			          
				} catch ( Exception e1 ){
					log.warning("Exception while re-trying. Failed to fetch all the SLAVE servers IP from ROLE Table " + e1.getMessage());
					closeConnections();
					return;
				}
			}
			try{
				while (rs.next()) {
					final IEnmInterworkingRMI RMI = (IEnmInterworkingRMI) Naming
							.lookup((RmiUrlFactory.getInstance()).getMultiESRmiUrl(rs.getString("IP_ADDRESS")));
					
					isRmiUpdated = RMI.natUpdate(eniq_identifier, node_fdn , eniq_alias);
					
					if (isRmiUpdated) {
//						log.info("Successfully Re-Assigned the node FDN " + node_fdn + " to " + eniq_identifier + " ENIQ-S server in "+ rs.getString("IP_ADDRESS"));
					}
				}
			} catch (SQLException e) {
				log.warning("SQLException occured while Manually updating NAT table through RMI.   " + e.getMessage());
			} catch (MalformedURLException e) {
				log.warning("MalformedURLException occured while Manually updating NAT table through RMI.   " + e.getMessage());
			} catch (RemoteException e) {
				log.warning("RemoteException occured while Manually updating NAT table through RMI.   " + e.getMessage());
			} catch (NotBoundException e) {
				log.warning("NotBoundException occured while Manually updating NAT table through RMI.   " + e.getMessage());
			}
		}
		catch (Exception e){
			log.warning("Exception in ManualNodeAssignment" + e.getMessage());
		}
		finally{
			closeConnections();
		}
	}
	
	private void closeConnections() {
		try {
			if(con != null){
				con.close();
			}
			if(rf_dwhrep.getConnection() != null){
				rf_dwhrep.getConnection().close();
			}
			if(rf_etlrep.getConnection() != null){
				rf_etlrep.getConnection().close();
			}
		} catch (SQLException e) {
			log.warning("Error while closing the connections " + e.getMessage());
		}
	}
}
