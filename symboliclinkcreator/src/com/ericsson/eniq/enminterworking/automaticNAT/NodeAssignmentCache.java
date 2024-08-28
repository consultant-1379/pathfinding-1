package com.ericsson.eniq.enminterworking.automaticNAT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.enminterworking.utilities.DbUtilities;
import com.ericsson.eniq.flssymlink.fls.Main;

import ssc.rockfactory.RockFactory;


public class NodeAssignmentCache {

	private static Logger log = Logger.getLogger("symboliclinkcreator.nat");
	private static int noOfServers = 0;
	private static List<Eniq_Role> roleTableContents = null;
	private static Map<String, ArrayList<String>> technologyNodeMap = null;
	private static Map<String,Map<String, ArrayList<Map<String, String>>>> policyCriteriaMap = null;
	private static HashMap<String,ConnectionStatus> enmConnectionCache = new HashMap<>();
	private static Queue<Eniq_Role> retryQueue;
	private static IEnmInterworkingRMI multiEs;
	private static Set<String> connectedEnms = new HashSet<>();
	private static final Long RETRY_DELAY = 10000L;
	private static final Long RETRY_INTERVAL = 10000L;
	private static final Object lock = new Object();
	private static final String NODE_TECHNOLOGY_MAPPING_FILE = "/eniq/sw/conf/NodeTechnologyMapping.properties";
	
	public enum ConnectionStatus {
		SINGLE,
		MULTI
	}

	private NodeAssignmentCache() {
	}
	
	public static void init() throws IOException {
		populateTechnologyNodeMap();
	}
	
	public static Map<String, ConnectionStatus> getEnmConnectionCache() {
		@SuppressWarnings("unchecked")
		Map<String, ConnectionStatus> result =((Map<String, ConnectionStatus>)enmConnectionCache.clone());
		result.putAll(enmConnectionCache);
		return result;
	}
	
	public static NodeAssignmentCache.ConnectionStatus getConnectionStatus(String enm){
		return enmConnectionCache.get(enm);
	}
		
	private static void refreshEnmRoleCache() {
		connectedEnms = Main.getHostNameSet();
		//Initialize - default connectionStatus of all connected ENMs as SINGLE
		for (String enm : connectedEnms) {
			enmConnectionCache.put(enm, ConnectionStatus.SINGLE);
		}
		log.info("enmConnectionCache before calling peer :"+ enmConnectionCache.toString());
		if (noOfServers > 1) {
			//Part of a Master Slave group
			for (Eniq_Role es: roleTableContents) {
				if (!es.getEniq_identifier().equals(Main.getEniqName())) {
					log.info("Checking with peer : "+es.getEniq_identifier());
					checkWithPeer(es);
				}
			}
		}
	}
		
	private static void checkWithPeer(Eniq_Role es) {
		synchronized(lock) {
			try {
				multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(es.getIp_address()));
				List<String> peerEnms = multiEs.getFlsEnabledEnms();
				log.info("Peer : "+es.getEniq_identifier()+" is connected to ENMs :"+peerEnms.toString());
				for (String enm : peerEnms) {
					if (connectedEnms.contains(enm)) {
						//Somebody from my group have connected to this ENM
						enmConnectionCache.put(enm, ConnectionStatus.MULTI);
					}
				}
				log.info("enmConnectionCache after calling peer :"+ enmConnectionCache.toString());
			} catch (MalformedURLException me ) {
				log.log(Level.WARNING, "Exception while trying to connect to Peer ENIQ-S : "+es.getEniq_identifier()+", Exception:"+me.getMessage());
			} catch (RemoteException | NotBoundException  e) {
				//log.log(Level.WARNING, "Exception while trying to connect to Peer ENIQ-S : "+es.getEniq_identifier()+", Exception:"+e.getMessage());
				//adding to retry list
				if (retryQueue != null) {
					retryQueue.add(es);
				} else {
					retryQueue = new ArrayBlockingQueue<Eniq_Role>(20);
					retryQueue.add(es);
					Timer timer = new Timer();
					timer.scheduleAtFixedRate(new NodeAssignmentCache.RetryThread(), RETRY_DELAY, RETRY_INTERVAL );
				}
			}
		}
	}
	
	private static class RetryThread extends TimerTask {
		@Override
		public void run() {
			if (retryQueue != null && !retryQueue.isEmpty()) {
				int size = retryQueue.size();
				while (size > 0) {
					checkWithPeer(retryQueue.poll());
					size--;
				}
			}
		}
	}

	public static int checkNoOfServers() throws SQLException {
		Eniq_Role roleTable = null;
		roleTableContents = new ArrayList<>();
		DbUtilities utility = new DbUtilities();
		RockFactory etlrep = utility.connectToEtlrep();
		RockFactory repdb = utility.connectTodb(etlrep, "dwhrep");
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			con = repdb.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery("select * from  RoleTable");
			policyCriteriaMap = readPolicyCriteriaData(con, repdb);
			while (rs.next()) {
				roleTable = new Eniq_Role(rs.getString(1), rs.getString(2), rs.getString(3));
				roleTableContents.add(roleTable);
			}
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.log(Level.FINE, "Exception while closing the result set",e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
					log.log(Level.FINE, "Exception while closing the statement",e);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					log.log(Level.FINE, "Exception while closing the connection",e);
				}
			}
			etlrep = null;

		}
		noOfServers = roleTableContents.size();
		log.info("noOfServers = " +noOfServers);
		refreshEnmRoleCache();
		return noOfServers;
	}

	
	
	public static Map<String, Map<String, ArrayList<Map<String, String>>>> readPolicyCriteriaData(Connection con, RockFactory repdb) {
		String sql = "select ENM_HOSTNAME, TECHNOLOGY,NAMINGCONVENTION, ENIQ_IDENTIFIER from ENIQS_Policy_Criteria";
		Logger log = Logger.getLogger("symboliclinkcreator.nat");
		try (Statement stmt = con.createStatement();
				ResultSet rs1 = stmt.executeQuery(sql);) {
			policyCriteriaMap = new HashMap<>();
			String enmHostname = null;
			String technologyName = null;
			String namingConvention = null;
			String eniqId = null;
			ArrayList<Map<String, String>> namingConventionsList = null;
			ArrayList<Map<String, String>> namingConventionsListTemp = null;
			Map<String,ArrayList<Map<String, String>>> technologyNamingconventionsMap = null;
			while (rs1.next()) {
				enmHostname = rs1.getString(1);
				technologyName = rs1.getString(2);
				namingConvention = rs1.getString(3);
				eniqId = rs1.getString(4);
				if (policyCriteriaMap.containsKey(enmHostname)) {
					technologyNamingconventionsMap = policyCriteriaMap.get(enmHostname);
					if (technologyNamingconventionsMap.containsKey(technologyName)) {
						namingConventionsList = technologyNamingconventionsMap.get(technologyName);
						namingConventionsListTemp = new ArrayList<>();
						HashMap<String, String> map = new HashMap<>();
						for (Map<String, String> namingConventionMap : namingConventionsList) {
							if(namingConventionMap.get(namingConvention) != null) {
								namingConventionMap.put(namingConvention, eniqId);
							} else {
								map.put(namingConvention, eniqId);
								namingConventionsListTemp.add(map);
							}
						}
						namingConventionsList.add(map);
					} else {
						namingConventionsList = new ArrayList<>();
						HashMap<String, String> map = new HashMap<>();
						map.put(namingConvention, eniqId);
						namingConventionsList.add(map);
					}
				} else {
					technologyNamingconventionsMap = new HashMap<>();
					namingConventionsList = new ArrayList<>();
					HashMap<String, String> map = new HashMap<>();
					map.put(namingConvention, eniqId);
					namingConventionsList.add(map);
				}
				technologyNamingconventionsMap.put(technologyName, namingConventionsList);
				policyCriteriaMap.put(enmHostname, technologyNamingconventionsMap);
			}
			log.info("Populated PolicyCriteria Map = "+ policyCriteriaMap.toString());
		} catch (Exception e) {
			log.log(Level.WARNING,"Exception while populating  Policy Criteria Map",e);
		} 
		return policyCriteriaMap;
	}

	private static void populateTechnologyNodeMap() throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(new File(NODE_TECHNOLOGY_MAPPING_FILE)))) {
			String line;
			technologyNodeMap = new HashMap<>();
			while ((line = br.readLine()) != null) {
				if (line.contains("-")) {
					String technology = line.substring(0, line.indexOf('-'));
					String values = line.substring(line.indexOf('-') + 1, line.length());
					String[] valuesArray = values.split(",");
					for (String nodeType : valuesArray) {
						ArrayList<String> technologyList = technologyNodeMap.get(nodeType);
						if (technologyList != null) {
							technologyList.add(technology);
						} else {
							technologyList = new ArrayList<>();
							technologyList.add(technology);
						}
						technologyNodeMap.put(nodeType, technologyList);
					}
				}
				}
			log.info("Populated technology Node Map = " + technologyNodeMap.toString());
		}
	}

	public static List<Eniq_Role> getRoleTableContents() {
		return roleTableContents;
	}
	
	public static int getNoOfServers() {
		return noOfServers;
	}

	public static Map<String, ArrayList<String>> getTechnologyNodeMap() {
		return technologyNodeMap;
	}
	
	public static Map<String,Map<String, ArrayList<Map<String, String>>>> getPolicyCriteria() {
		return policyCriteriaMap;
	}
	
	
}
