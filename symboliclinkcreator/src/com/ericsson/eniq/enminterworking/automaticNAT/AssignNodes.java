package com.ericsson.eniq.enminterworking.automaticNAT;

import java.rmi.Naming;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.enminterworking.utilities.DbUtilities;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import static com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache.*;

public class AssignNodes {

	private String node_type;
	private String node_fdn;
	private String node_fdn_copy;
	private String enmHostName;
	private String masterIP;
	private String eniqId;
	private boolean isRetention = false;
	private boolean isSingleConnectedEnmForSlave = false;
	private String mixedNodeTechnologies;
	private boolean deletePolicy = false;
	private RockFactory rf_etlrep;
	private RockFactory rf_dwhrep;
	private Connection con;
	private Logger log;
	private List<Eniq_Role> roleTableList = null;
	private String existingEniqAssignment = null;
	final String roleSql = "Select IP_ADDRESS, ENIQ_ID from RoleTable where ROLE='SLAVE'";
	DbUtilities utility;
	private static final String MANAGED_ELEMENT_REGEX = "(SubNetwork=.+,MeContext=.+)(,ManagedElement=1)";
	private static final String MANAGED_ELEMENT = ",ManagedElement=1";
	private static final String UNASSIGNED = "";
	private static final String SLAVE = "SLAVE";
	private static final String MASTER = "MASTER";
	private static final String MIXED_TECHNOLOGY_DELIMITER ="_";
	
	public AssignNodes (String node_type, String node_fdn, String enmHostName, String eniqId, boolean isSingleConnectedEnmForSlave, boolean isRetention) {
		this.node_type = node_type;
		this.enmHostName = enmHostName;
		node_fdn_copy = node_fdn;
		this.eniqId = eniqId;
		this.isRetention = isRetention;
		this.isSingleConnectedEnmForSlave = isSingleConnectedEnmForSlave;
		log = Logger.getLogger("symboliclinkcreator.nat");

		if( Pattern.compile(MANAGED_ELEMENT_REGEX).matcher(node_fdn).matches() ){
			this.node_fdn = node_fdn.substring(0, node_fdn.indexOf(MANAGED_ELEMENT));
		}
		else {
			this.node_fdn = node_fdn;
		}
	}
	
	public AssignNodes (String node_type, String node_fdn, String enmHostName, String eniqId, boolean isSingleConnectedEnmForSlave, boolean isRetention, String mixedNodeTechnologies) {
		this(node_type, node_fdn,enmHostName,eniqId,isSingleConnectedEnmForSlave,isRetention);
		this.mixedNodeTechnologies = mixedNodeTechnologies;
	}
	
	public AssignNodes (String node_type, String node_fdn, String enmHostName, String eniqId, boolean isSingleConnectedEnmForSlave, boolean isRetention, String mixedNodeTechnologies, boolean deletePolicy) {
		this(node_type, node_fdn,enmHostName,eniqId,isSingleConnectedEnmForSlave,isRetention);
		this.mixedNodeTechnologies = mixedNodeTechnologies;
		this.deletePolicy = deletePolicy;
	}
	
	public void init()
	{
		try{
			initializeRoleTableList();
			if(EnmInterCommonUtils.getSelfRole_existing(log).equalsIgnoreCase("SLAVE")){
				IEnmInterworkingRMI natMaster = (IEnmInterworkingRMI) Naming
						.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(masterIP));
				if (isRetention) {
					log.info("Retention : Adding "+node_fdn_copy+" to Master's Blocking Queue");
					natMaster.addingToBlockingQueue(node_type, node_fdn_copy, enmHostName, eniqId, false, isRetention);
				} else if (ConnectionStatus.SINGLE == getConnectionStatus(enmHostName)) {
					String eniq = EnmInterCommonUtils.getEngineHostname();
					log.finest("Triggering the Master's Blocking Queue for Node Assignment " + node_fdn +" with eniqId :"+eniq);
					natMaster.addingToBlockingQueue(node_type, node_fdn_copy, enmHostName, eniq, true, false);
				}  
				else {
					log.finest("Triggering the Master's Blocking Queue for Node Assignment " + node_fdn);
					natMaster.addingToBlockingQueue(node_type, node_fdn_copy, enmHostName);
				}
			} else {
				con = getConnection();
				boolean isAssigned = checkNAT();
				if (!isAssigned ) {
					if(isSingleConnectedEnmForSlave || isRetention){
						assignNodeForSingleES();
					} else if (ConnectionStatus.SINGLE == getConnectionStatus(enmHostName)) {
						assignNodeForSingleES();
					}
					else {
						assignNodeForMultiES();
					}
				} else if (isAssigned && (ConnectionStatus.MULTI == getConnectionStatus(enmHostName)) && deletePolicy){
					log.fine("FDN " + node_fdn + " is already assigned and needs to be reassigned");
					assignNodeForMultiES();
				} else if (isAssigned && (mixedNodeTechnologies != null) && (ConnectionStatus.MULTI == getConnectionStatus(enmHostName))){
					log.fine("FDN " + node_fdn + " is already assigned or unassigned but need to asses again for Mixed Node ");
					assignNodeForMultiES();
				} else {
					log.fine("FDN " + node_fdn + " is already assigned or unassigned. ");
				}
			}
		} catch (Exception e){
			log.log(Level.WARNING, "Exception in AssignNodes", e);
		}
		finally{
			closeConnections();
		}
	}
	
	private void initializeRoleTableList() {
		roleTableList = NodeAssignmentCache.getRoleTableContents();
		for (Eniq_Role es : roleTableList) {
			if (es.getRole().equals(MASTER)) {
				masterIP = es.getIp_address();
				break;
			}
		}
	}

	private void assignNodeForSingleES() {
		String whichEniq = null;
		if (isSingleConnectedEnmForSlave || isRetention) {
			if (eniqId != null) {
				whichEniq = eniqId;
			} else {
				whichEniq = UNASSIGNED;
			}
		} else {
			whichEniq = EnmInterCommonUtils.getEngineHostname();
		}
		log.finest("Single ENIQ-S configuration. Assigning the node FDN "+ node_fdn +" to ENIQ-S server : "
				+ whichEniq);
		insertIntoNAT(whichEniq);
		insertIntoSlave(whichEniq);
	}
	
		
	private String getEniqToAssign(List<String> technologyList, Map<String, ArrayList<Map<String, String>>> enmPolicyMap) {
		List<ArrayList<Map<String, String>>> namingConventionsLists = new ArrayList<>();
		Set<String> eniqIds = new HashSet<String>();
		boolean isEniqIdNull = true ;
		StringBuilder result = new StringBuilder();
		if (technologyList != null && enmPolicyMap != null) {
			for (String tech : technologyList) {
				namingConventionsLists.add(enmPolicyMap.get(tech));
			}
			String eniqId = null;
			for (ArrayList<Map<String, String>> namingConventionsList : namingConventionsLists) {
				eniqId = checkNodeAgainstPolicies(namingConventionsList, node_fdn);
				if (eniqId != null) {
					if (eniqIds.add(eniqId)) {
						result.append(eniqId);
						result.append(",");
						isEniqIdNull = false;
					}
				}
			}
		} else if (technologyList == null){
			log.warning("Not able to Map Node_Type : "+node_type+" to any Technology. Concerned FDN : "+node_fdn);
		}
		String result2 = "";
		if(result.length()>1)
			result2 = isEniqIdNull ? UNASSIGNED : result.substring(0, result.length() - 1);
		return result2;
	}
	
	private boolean isNcMatch(String nodeName, String namingConvention) {
		if (namingConvention.equals("*")) {
			log.finest("Node " + nodeName + " matches with policy '*'");
			return true;
		}
		else {
			Pattern pattern = Pattern.compile(namingConvention);
			Matcher matcher = pattern.matcher(nodeName);
			return matcher.matches();
		}
	}
		
	/**
	 * Method that checks the policy and criteria and returns the ENIQ-S to which the node should be assigned.
	 * 
	 * @return ENIQ-S to which the node should be assigned. NULL if no matching policy is found.
	 */
	public String matchPolicyAndGetAssignment() {
		List<String> technologylist = null;
		if (mixedNodeTechnologies != null) {
			technologylist =  Arrays.asList(mixedNodeTechnologies.split(MIXED_TECHNOLOGY_DELIMITER));
			log.log(Level.FINEST,"matchPolicyAndGetAssignment technologylist from mixedNodeTechnologies :"+technologylist +" fdn : "+node_fdn);
		} else {
			Map<String, ArrayList<String>> TechnoMap = NodeAssignmentCache.getTechnologyNodeMap();
			technologylist = TechnoMap.get(node_type);
		}
		Map<String,Map<String, ArrayList<Map<String, String>>>> policyCriteria = NodeAssignmentCache.getPolicyCriteria();
		Map<String, ArrayList<Map<String, String>>> enmPolicyMap = policyCriteria.get(enmHostName);
		return getEniqToAssign(technologylist, enmPolicyMap);
	}
	
	private void assignNodeForMultiES() {
		log.fine("Multi ENIQ-S configuration");
		String eniqToAssign = matchPolicyAndGetAssignment();
		if (isReAssign(eniqToAssign)) {
			log.info("Reassigning Node FDN "+node_fdn+" to : "+eniqToAssign);
			String updateSql = "update ENIQS_Node_Assignment set ENIQ_IDENTIFIER='"+eniqToAssign+"' "
					+ "where FDN='"+node_fdn+"' ";
			updateNAT(updateSql);
			updateSlave(updateSql);
		}else if (existingEniqAssignment == null) {
			log.info(" Node FDN "+node_fdn+" Assignment :" + 
					(eniqToAssign.equals(UNASSIGNED) ? "UNASSIGNED" : eniqToAssign));
			insertIntoNAT(eniqToAssign);
			insertIntoSlave(eniqToAssign);
		}
	}
	
	private boolean isReAssign(String eniqToAssign) {
		if (existingEniqAssignment != null && eniqToAssign != null) {
			log.log(Level.FINEST,"Multi ENIQ-S configuration Existing Assignment: "
					+ "existingEniqAssignment :"+existingEniqAssignment +" eniqToAssign : "+eniqToAssign);
			if(!existingEniqAssignment.equals(eniqToAssign)) {
				return true;
			}
		}
		return false;
	}
		
	private void insertIntoNAT(String eniqId) {
		String sql = "insert into ENIQS_Node_Assignment (ENIQ_IDENTIFIER , FDN , NeType,ENM_HOSTNAME ) values ( '"
				+ eniqId + "' , '" + node_fdn + "' , '" + node_type + "','"+enmHostName+"' )";
		try (Statement stmt = con.createStatement()){
			stmt.execute(sql);
		} catch ( SQLException e) {
			if (e.getMessage().matches(".*would not be unique.*")){
				log.log(Level.INFO, " The Entry for Node FDN "+node_fdn+" is already present in NAT");
				return;
			} else {
				log.log(Level.WARNING,"SQLException for : "+sql+" MESSAGE : "+e.getMessage());
			}
			try (Statement stmt = handleSQLException(e)) {
				if (stmt != null) {
					stmt.execute(sql);
				}
			} catch (SQLException ie) {
				log.warning("Exception while re-trying. FDN " + node_fdn + " is not added into ENIQS_Node_Assignment table. " + ie.getMessage());
			}
		} catch ( Exception e1 ) {
			log.log(Level.WARNING,"Exception while re-trying. FDN " + node_fdn + " is not added into ENIQS_Node_Assignment table. ",e1);
		}
	}
	
	private void updateNAT(String sql) {
		try (Statement stmt = con.createStatement()){
			stmt.execute(sql);
		} catch ( SQLException e) {
			log.log(Level.WARNING,"SQLException for : "+sql+" MESSAGE : "+e.getMessage());
			try (Statement stmt = handleSQLException(e)) {
				if (stmt != null) {
					stmt.execute(sql);
				}
			} catch (SQLException ie) {
				log.warning("Exception while re-trying. FDN " + node_fdn + " is not added into ENIQS_Node_Assignment table. " + ie.getMessage());
			}
		} catch ( Exception e1 ) {
			log.log(Level.WARNING,"Exception while re-trying. FDN " + node_fdn + " is not added into ENIQS_Node_Assignment table. ",e1);
		}
	}
	
	private void insertIntoSlave(String eniq) {
		String serverIP = null;
		String eniqId = null;
		try{
			for (Eniq_Role eniqStat : roleTableList) {
				if (eniqStat.getRole().equals(SLAVE)) {
					serverIP = eniqStat.getIp_address();
					eniqId = eniqStat.getEniq_identifier();
					final IEnmInterworkingRMI RMI = (IEnmInterworkingRMI) Naming
							.lookup((RmiUrlFactory.getInstance()).getMultiESRmiUrl(serverIP));
					log.fine("RMI : ENIQ-S Server : "+eniqId+" with IP : " + serverIP 
							+ "\t Role : " + RMI.getCurrentRole() + " \t"+ RMI.toString());
					eniq = (eniq == null) ? UNASSIGNED : eniq;
					try {
						if (RMI.natInsert(eniq, node_fdn, node_type,enmHostName)) {
							log.fine("Inserted the FDN " + node_fdn + 
									" into ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId);
						}
					} catch (Exception e) {
						log.log(Level.WARNING,"RMI: Error while writing FDN " + node_fdn 
								+ " to the  ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId ,e.getMessage());
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING,"RMI: Error while writing FDN " + node_fdn 
					+ " to the  ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId ,e.getMessage());
		} 
	}
	
	private void updateSlave(String sql) {
		String serverIP = null;
		String eniqId = null;
		try{
			for (Eniq_Role eniqStat : roleTableList) {
				if (eniqStat.getRole().equals(SLAVE)) {
					serverIP = eniqStat.getIp_address();
					eniqId = eniqStat.getEniq_identifier();
					final IEnmInterworkingRMI RMI = (IEnmInterworkingRMI) Naming
							.lookup((RmiUrlFactory.getInstance()).getMultiESRmiUrl(serverIP));
					log.fine("RMI : ENIQ-S Server : "+eniqId+" with IP : " + serverIP 
							+ "\t Role : " + RMI.getCurrentRole() + " \t"+ RMI.toString());
					try{
						if (RMI.executeSql(sql)) {
							log.fine("Updated the FDN " + node_fdn + 
									" in ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId);
						}
					} catch ( Exception e ){
						log.log(Level.WARNING,"RMI: Error while writing FDN " + node_fdn 
								+ " to the  ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId ,e);
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING,"RMI: Error while writing FDN " + node_fdn 
					+ " to the  ENIQS_Node_Assignment table of Slave ENIQ-S :"+eniqId ,e);
		} 
	}
		
	private Connection getConnection() {
		try {
			if (con != null && !con.isClosed()) {
				return con;
			} else {
				utility = new DbUtilities();
				rf_etlrep = utility.connectToEtlrep();
				rf_dwhrep = utility.connectTodb(rf_etlrep, "dwhrep");
				con = rf_dwhrep.getConnection();
				return con;
			}
		} catch (SQLException e) {
			log.log(Level.WARNING, "Exception while getting connection", e);
		}
		return con;
	}


	private Statement handleSQLException(SQLException e) {
		Statement stmt = null;
		final String msg = e.getMessage();
		if ((msg.contains(RockException.CONN_CLOSED)) || (msg.contains(RockException.CONN_TERMINATED))){
			try{				
				return getConnection().createStatement();
			} 
			catch ( SQLException e1 ){
				log.warning("Exception while re-trying. " + e1.getMessage());
			}
		}
		return stmt;
	}

	private String checkNodeAgainstPolicies(ArrayList<Map<String, String>> arrayList, String nodeFdn) {
		boolean matchFlag = false;
		String eniqId = null;
		if (arrayList != null) {
			String nodeName = nodeFdn.substring(nodeFdn.lastIndexOf("=") + 1, nodeFdn.length());
			for (Map<String,String> s: arrayList) {
				Set<String> namingConventions = s.keySet();
				for(String namingConvention : namingConventions) {
					try {
						if (isNcMatch(nodeName, namingConvention)) {
							log.finest("Pattern " + namingConvention + " matches with Node : " + nodeName);
							eniqId = s.get(namingConvention);
							matchFlag = true;
							break;
						}
					} catch (PatternSyntaxException pe){
						log.warning("Exception occured. Please check the Naming Convention Policy: "+ s +" is invalid. " + pe.getMessage());
					}
					catch (Exception e){
						log.warning("Exception occured while matching node FDN "+ nodeFdn +" against the Naming Convention Policy!!" + e.getMessage());
					}
				}
				if (matchFlag){
					break;
				}
			}
		}
		log.finest("Returning eniqId : "+eniqId);
		return eniqId;
	}

	private boolean checkNAT() {
		// Check if this fdn is already assigned/present in the NAT table
		boolean flag = false;
		String natSql = "select NETYPE,FDN,ENIQ_IDENTIFIER from ENIQS_Node_Assignment where FDN='" + node_fdn +"' and ENM_HOSTNAME='" + enmHostName +"'";
		try (Statement st1 = con.createStatement();
				ResultSet rs1 = st1.executeQuery(natSql);) {
			if(rs1.next()){
				flag = true;
				String oldNeType = rs1.getString(1);
				existingEniqAssignment = rs1.getString(3);
				if (node_type != null && !node_type.equals(oldNeType)){
					log.info("NE Type for FDN : "+node_fdn+" is changed from : "+oldNeType+" to "+node_type);
					log.info("NAT table will be updated for FDN : "+node_fdn);
					String updateSql = "update ENIQS_Node_Assignment set NETYPE='"+node_type+"' where FDN='"+node_fdn+"' ";
					updateNAT(updateSql);
					updateSlave(updateSql);
				}
			}
		} 
		catch ( SQLException e ){
			try (Statement st1 = handleSQLException(e);
					ResultSet rs1 = st1.executeQuery(natSql);) {
				if(rs1.next()){
					flag = true;
				}
			} catch (Exception ie) {
				log.log(Level.WARNING,"Exception while re-trying. Failed to check the FDN " + node_fdn + " in the ENIQS_Node_Assignment table!" , ie);
			}
		}
		return flag;
	}

	private void closeConnections() {
		try {
			if(con != null){
				con.close();
			}
		}
		catch(Exception e){
			log.warning("Exception while closing dwh_rep connection=" +e.getMessage() );
		}
	}

}
