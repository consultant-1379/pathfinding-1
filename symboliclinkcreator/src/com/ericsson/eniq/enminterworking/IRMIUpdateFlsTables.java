/**
 * @author xnagdas
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
import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.utilities.DbUtilities;
import ssc.rockfactory.RockFactory;


public class IRMIUpdateFlsTables {

	private RockFactory rf_etlrep;
	private RockFactory rf_dwhrep;
	private Connection con;
	Statement stmt;
	int exitCode = 0;
	
	/**
	 * @param args
	 * args[0]- old host name
	 * args[1]- old IP ADDRESS
	 * args[2]- new host name
	 * args[3]- new IP ADDRESS
	 */
	
	public static void main(String[] args) {
		IRMIUpdateFlsTables irmiUpdateFlsTables = new IRMIUpdateFlsTables();
		irmiUpdateFlsTables.updateFlsTables(args[0], args[1], args[2], args[3]);
	}
	
	public void updateFlsTables ( String oldHostName, String oldIpAddress , String newHostName, String newIpAddress ) {
		System.setSecurityManager(new com.ericsson.eniq.enminterworking.ETLCSecurityManager());
		
		boolean isRmiUpdated = false;
		ResultSet rs = null;
		
		try{
			DbUtilities utility = new DbUtilities();
			rf_etlrep = utility.connectToEtlrep();
			rf_dwhrep = utility.connectTodb(rf_etlrep, "dwhrep");
			con = rf_dwhrep.getConnection();
			stmt = con.createStatement();
			final String roleSql = "SELECT IP_ADDRESS FROM RoleTable WHERE IP_ADDRESS != '" + newIpAddress + "'";

			try {
				rs = stmt.executeQuery(roleSql);
			} catch (Exception e) {
				System.out.println("Exception occured while fetching all the ENIQ-S servers IP from RoleTable: " + e.getMessage());
				try{
					closeConnections();							
					rf_etlrep = utility.connectToEtlrep();
					rf_dwhrep = utility.connectTodb(rf_etlrep, "dwhrep");
			    	con = rf_dwhrep.getConnection();
			    	
			        stmt = con.createStatement();
			        rs = stmt.executeQuery(roleSql);
			          
				} catch ( Exception e1 ){
					System.out.println("Exception while re-trying. Failed to fetch all the ENIQ-S servers IP from RoleTable: " + e1.getMessage());
					closeConnections();
					exitCode = 6;
				}
			}
			try{
				while (rs.next()) {
					final IEnmInterworkingRMI RMI = (IEnmInterworkingRMI) Naming
							.lookup((RmiUrlFactory.getInstance()).getMultiESRmiUrl(rs.getString("IP_ADDRESS")));
					isRmiUpdated = RMI.updateFlsTables(oldHostName, oldIpAddress , newHostName, newIpAddress);;
					
					if (isRmiUpdated) {
						System.out.println("updated Slave FLS tables successfully");
						exitCode = 0;
					}
				}
			} catch (SQLException e) {
				System.out.println("SQLException occured while updating the FLS tables through RMI:   " + e.getMessage());
				exitCode = 7;
			} catch (MalformedURLException e) {
				System.out.println("MalformedURLException occured while updating the FLS tables through RMI:   " + e.getMessage());
				exitCode = 8;
			} catch (RemoteException e) {
				System.out.println("RemoteException occured while updating the FLS tables through RMI:   " + e.getMessage());
				exitCode = 9;
			} catch (NotBoundException e) {
				System.out.println("NotBoundException occured while updating the FLS tables through RMI:   " + e.getMessage());
				exitCode = 10;
			} catch (Exception e){
				System.out.println("Exception while updating the FLS tables through RMI: " + e.getMessage());
				exitCode = 11;
			}
		} catch (Exception e){
			System.out.println("Exception while updating the FLS tables: " + e.getMessage());
			exitCode = 12;
		}
		finally{
			closeConnections();
			System.exit(exitCode);
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
			System.out.println("Error while closing the connections:  " + e.getMessage());
			exitCode = 13;
		}
	}
}
