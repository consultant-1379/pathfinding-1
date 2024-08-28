package com.ericsson.eniq.enminterworking;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.ENIQRMIRegistryManager;
import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.enminterworking.automaticNAT.AssignNodesQueueHandler;
import com.ericsson.eniq.enminterworking.automaticNAT.Eniq_Role;
import com.ericsson.eniq.enminterworking.automaticNAT.HealthMonitor;
import com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache;
import com.ericsson.eniq.flssymlink.fls.Main;
import com.ericsson.eniq.flssymlink.fls.PmQueueHandler;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

/**
 * @author xarjsin
 *
 */

public class EnmInterworking extends UnicastRemoteObject implements IEnmInterworkingRMI {
	
	private static final long serialVersionUID = -2564842512593322084L;
	private static final Object lock = new Object();
	public static final String ALL = "ALL";
	public static final String ENM_ALIAS = "ENM ALIAS";
	public static final String ENM_HOST_NAME = "ENM HOSTNAME";
	public static final String ACTIVE_PROFILE = "ACTIVE PROFILE";
	public static final String RESET = "\u001B[0m";
	public static final String BLACK = "\u001B[30m";
	public static final String RED = "\u001B[31m";
	public static final String GREEN = "\u001B[32m";
	public static final String YELLOW = "\u001B[33m";
	public static final String BLUE = "\u001B[34m";
	public static final String PURPLE = "\u001B[35m";
	public static final String CYAN = "\u001B[36m";
	
	private Logger log;
	
	private String serverIP = EnmInterCommonUtils.getEngineIP();
	private int serverPort = 1200;
	private String serverRefName = "MultiESController";
	private long startedAt = 0L;
	private String activeProfile;
	
	public EnmInterworking() throws RemoteException {
		super();
		this.startedAt = System.currentTimeMillis();
		
	}
	
	public EnmInterworking(int flsUserprocessPort) throws RemoteException {
		super(flsUserprocessPort);
		this.startedAt = System.currentTimeMillis();
		
	}

	@Override
	public String getCurrentRole() throws RemoteException {
		return EnmInterCommonUtils.getSelfRole();
	}

	@Override
	public String updateSlave(String masterHost, String masterIP) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		String slaveInsert = "Insert into RoleTable (ENIQ_ID, IP_ADDRESS, ROLE) Values ('" 
				+ EnmInterCommonUtils.getEngineHostname() + "', '" 
				+ EnmInterCommonUtils.getEngineIP() + "', 'SLAVE')";
		String masterInsert = "Insert into RoleTable (ENIQ_ID, IP_ADDRESS, ROLE) Values ('" 
				+ masterHost + "', '" 
				+ masterIP + "', 'MASTER')";
		log.info("slave insert : " + slaveInsert);
		log.info("master insert : " + masterInsert);
		if (EnmInterCommonUtils.getSelfRole().equals("UNASSIGNED")) {
			try
			{
				rockCon = DatabaseConnections.getDwhRepConnection();
				rockCon.getConnection().createStatement().executeUpdate(masterInsert);
				rockCon.getConnection().createStatement().executeUpdate(slaveInsert);
				return EnmInterCommonUtils.getEngineHostname();
			}
			catch (Exception e)
			{
				log.log(Level.WARNING, "Exception occured while updating slave ENIQS_Node_Assignment table : ", e);
				return "FAIL";
			}
			finally
			{		
				try{
					if (rockCon != null && rockCon.getConnection() != null) {
						rockCon.getConnection().close();
					}
				} catch (SQLException e) {
					log.log(Level.WARNING, "Exception while closing the connection: ",e);
				}			
			}
		}		
		return "FAIL";
	}
	
	@Override
	public String updateSlaveNAT(String masterIP) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		log.info("Updating slave NAT table");
		RockFactory rockCon = null;
		RockResultSet rockResultSet = null;
		RockResultSet slaveNatResult = null;
		ArrayList<String> masterNatList=null;
		ArrayList<String> slaveNatList = new ArrayList<>();
		PreparedStatement ps = null;
		try { 
			IEnmInterworkingRMI multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(masterIP));
			masterNatList = multiEs.MasterNATDetail();
		}catch(Exception e){
			log.log(Level.WARNING, "Exception occured while receiveing Master NAT Details :", e);
		}

		try {
			rockCon = DatabaseConnections.getDwhRepConnection();
			String slaveNatCount="select count(*) from ENIQS_Node_Assignment";
			String slaveNatInfo = "select * from ENIQS_Node_Assignment";
			rockResultSet = rockCon.setSelectSQL(slaveNatCount);
			ResultSet rs= rockResultSet.getResultSet();
			ResultSet rs2 = null;
			if(rs.next()){
				int eniq_nat_count=rs.getInt(1);
				if(eniq_nat_count>0){
					slaveNatResult = rockCon.setSelectSQL(slaveNatInfo);
					rs2 = slaveNatResult.getResultSet();
					while (rs2.next()) {
						String s = rs2.getNString("ENIQ_IDENTIFIER")+"::"+rs2.getNString("FDN")+"::"
								+rs2.getNString("NETYPE")+"::"+rs2.getNString("ENM_HOSTNAME");
						slaveNatList.add(s);
					}
					log.info("Slave ENIQS_Node_Assignment is having entries.so deleting all the entries before copying Master NAT details");
					String slaveDeleteSql="DELETE FROM ENIQS_Node_Assignment";
					rockCon.executeSql(slaveDeleteSql);
				}
			}
			String preparedSqlStr = "Insert into ENIQS_Node_Assignment(ENIQ_IDENTIFIER,FDN,NETYPE,ENM_HOSTNAME) Values(?,?,?,?)";
			ps = rockCon.createPreparedSqlQuery(preparedSqlStr);
			for(String s : masterNatList){
				String[] arr=s.split("::");
				try{
					log.finest("id, fdn, type, enmHostName " + arr[0] + "," + arr[1] + ","+arr[2] + "," + arr[3]);
					ps.setString(1, arr[0]);
					ps.setString(2, arr[1]);
					ps.setString(3, arr[2]);
					ps.setString(4, arr[3]);
					ps.execute();
				} catch (Exception e) {
					log.log(Level.WARNING,"Exception while copying master NAT to slave. Data encountered : "+s, e);
				}
			}
			for (String s : slaveNatList){
				String[] arr = s.split("::");
				try{
					addingToBlockingQueue(arr[2],arr[1], arr[3], arr[0], false, true);
				} catch (Exception e) {
					log.log(Level.WARNING,
							"Exception while adding slave NAT list to blocking Queue. Data encountered : "+s, e);
				}
			}
			return EnmInterCommonUtils.getEngineHostname();
		}catch(Exception e){
			log.log(Level.WARNING, "Exception occured while adding  Master NAT Details to slave NAT:", e);
			
			return "FAIL";
		}
		finally
		{		
			if (slaveNatResult != null) {
				try {
					slaveNatResult.close();
				} catch (SQLException e) {
				}
			}
			closeCon(rockResultSet, ps, rockCon);				
		}
	}

	@Override
	public ResultSet getMaster() throws RemoteException  {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		ResultSet masterDetails = null;
		String masterSql = "Select ENIQ_ID, IP_ADDRESS from RoleTable where Role = 'MASTER'";
		if(getCurrentRole().equals("SLAVE"))
		{
			try
			{
				rockCon = DatabaseConnections.getDwhRepConnection();
				masterDetails = rockCon.getConnection().createStatement().executeQuery(masterSql);
			}
			catch (Exception e)
			{
				log.warning( "Exception occured while getting the MASTER server data from RoleTable: " + e.getMessage());
			}
			finally
			{
				try {
					if (masterDetails != null) {
						masterDetails.close();
			        }
				}
				catch(Exception e){
					log.warning("Exception while closing result set: " +e.getMessage());
				}
				try{
			        if (rockCon.getConnection() != null) {
			        	rockCon.getConnection().close();
				        }
			      } catch (Exception e) {
			    	  log.warning( "Exception while closing the connection: " + e.getMessage());
			      }
			}
		}
		return masterDetails;
	}
	
	public static void main(final String args[]){
		System.setSecurityManager(new com.distocraft.dc5000.etl.engine.ETLCSecurityManager());
		
		try {
			final EnmInterworking enmInter = new EnmInterworking();
			if(!enmInter.initRMI()){
				//System.out.println("Initialisation failed... exiting");
			    System.exit(0);
			}
			else{
				//System.out.println("RMI initialisation done");
			}
		}
		catch(Exception e) {
//			System.out.println( "Initialization failed exceptionally", e);
			//e.printStackTrace();
		}
	}

	/**
	 * Method to bind MultiES RMI
	 * @return
	 */
	public boolean initRMI() {
		log = Logger.getLogger("symboliclinkcreator.nat");
		log.info("Initializing.....MultiES RMI");
	    final String rmiRef = "//" + serverIP + ":" + serverPort + "/" + serverRefName;
	    log.info("RMI url is - " + rmiRef);
	    ENIQRMIRegistryManager rmiRgty = new ENIQRMIRegistryManager(serverIP, serverPort);
		try {
			Registry rmi = rmiRgty.getRegistry();
			log.info("MultiES server is rebinding to rmiregistry on host: " + serverIP + " on port: " + serverPort + 
	    			" with name: " + serverRefName);
			rmi.rebind(serverRefName, this);
			log.info("MultiES Server binds successfully to already running rmiregistry");
		} catch (final Exception e) {
			log.warning("Unable to bind to the rmiregistry using refrence: " + rmiRef + "\n" + e.getMessage());
			return false;
		}
		log.info("MultiES has been initialized.");
	    return true;
	}
	
	/**
	   * Method to shutdown MultiES RMI.
	   */
	  /**
	 * @
	 */
	public void shutdownRMI()  {
		  try{
			  	log = Logger.getLogger("symboliclinkcreator.nat");
			  	log.info("Shuting Down Multi ES RMI...");
			    ENIQRMIRegistryManager rmi = new ENIQRMIRegistryManager(serverIP, serverPort);
			    try {
			     	Registry registry = rmi.getRegistry();
			        registry.unbind(serverRefName);
			        UnicastRemoteObject.unexportObject(this, false);
			    } catch (final Exception e) {
			    	log.warning("Could not unregister Multi ES RMI service, quiting anyway."+e.getMessage());
			    }
			    new Thread() {
			    	@Override
			    	public void run() {
			    		try {
			    			sleep(2000);
			    		} catch (InterruptedException e) {
			    			// No action to take
			    		}
			    		System.exit(0);
			    	}//run
			    }.start();
	    } catch (Exception e) {
	    		log.warning( "Shutdown failed exceptionally: "+ e.getMessage());
	     
	    }
	}
	
	@Override
	public boolean executeSql(String sql) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		try
		{
			rockCon = DatabaseConnections.getDwhRepConnection();
			rockCon.getConnection().createStatement().executeUpdate(sql);
			return true;
		}
		catch (Exception e)
		{
			log.warning( "Exception occured while updating the slave ENIQS_Node_Assignment table: " + e.getMessage());
			return false;
		}
		finally
		{
			try {
		        if (rockCon.getConnection() != null) {
		        	rockCon.getConnection().close();
			        }
		      } catch (SQLException e) {
		    	  log.warning( "Exception occured while closing the connection: " + e.getMessage());
		      }
		}
	}
	
	@Override
	public boolean natInsert(String eniqIdentifier,String fdn ,String neType,String enmHostName) throws RemoteException{
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		String slaveInsert = "Insert into ENIQS_Node_Assignment (ENIQ_IDENTIFIER, FDN, NETYPE, ENM_HOSTNAME) Values ('" 
		+ eniqIdentifier + "','" 
		+ fdn + "','"+neType+"','"+enmHostName+"')";		
		
			try
			{
				rockCon = DatabaseConnections.getDwhRepConnection();
				rockCon.getConnection().createStatement().executeUpdate(slaveInsert);
				return true;
			}
			catch (Exception e)
			{
				if (e.getMessage().matches(".*would not be unique.*")){
					log.log(Level.INFO, " The Entry for Node FDN "+fdn+" is already present in Slave NAT");
				} else {
					log.warning( "Exception occured while inserting into the slave ENIQS_Node_Assignment table: " + e.getMessage());
				}
				return false;
			}
			finally
			{
				try {
			        if (rockCon.getConnection() != null) {
			        	rockCon.getConnection().close();
				        }
			      } catch (SQLException e) {
			    	  log.warning( "Exception occured while closing the connection: " + e.getMessage());
			      }
			}
		}
	
		
	
	@Override
	public boolean natUpdate(String eniq_identifier, String nodeFDN , String enmHostName) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		String sqlUpdate = " update ENIQS_Node_Assignment set ENIQ_IDENTIFIER='" + eniq_identifier + "' where FDN = '" + nodeFDN + "' and ENM_HOSTNAME='"+enmHostName+"'";
		try
			{
				rockCon = DatabaseConnections.getDwhRepConnection();
				rockCon.getConnection().createStatement().executeUpdate(sqlUpdate);
				log.info("Successfully Re-Assigned the node FDN "+ nodeFDN +" to " + eniq_identifier + " ENIQ-S server ");
				return true;
			}
			catch (Exception e)
			{
				log.warning( "Exception while updating the ENIQS_Node_Assignment table: " + e.getMessage());
				return false;
			}
			finally
			{
				try {
			        if (rockCon.getConnection() != null) {
			        	rockCon.getConnection().close();
				        }
			      } catch (SQLException e) {
			    	  log.warning( "Exception at natUpdate Method finally clause: " + e.getMessage());
			      }
			}
	}

	@Override
	@Deprecated
	public boolean flsConfExists() throws RemoteException {
		  try { 
		    	final File flsConf = new File("/eniq/installation/config/fls_conf");
				if(!flsConf.exists()){
					return false;
				}else{
					return true;
				}
			}
		  catch(Exception e){
			  return false;
		  }
	  }
	
	@Override
	public boolean IsflsServiceEnabled() throws RemoteException {
		return Main.isFlsEnabled();
	}
	
	@Override
	public List<String> getFlsEnabledEnms() throws RemoteException{
		return new ArrayList<>(Main.getHostNameSet());
	}
	
	@Override
	public Map<String, String> getOssIdToHostNameMap() throws RemoteException{
		return Main.getOssIdToHostNameMap();
	}

	@Override
	public void addingToBlockingQueue(String node_type, String node_fdn, String enmHostName) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
		log.finest("Calling addingToBlockingQueue.....");
		String enmAlias = Main.getOssAliasWithShostName(enmHostName);
		String nodeName = PmQueueHandler.getNodeNameKey(node_fdn);
		String mixedNodeTechnologies = Main.getFlsTask(enmAlias).getmCache().getMixedNodeTechnologyType(nodeName, log);
		if (mixedNodeTechnologies != null && !mixedNodeTechnologies.equals("NO_KEY") && !mixedNodeTechnologies.equals("EMPTY_CACHE")) {
			Main.assignNodesQueue.add(new AssignNodesQueueHandler( node_type, node_fdn, enmHostName, mixedNodeTechnologies) );
		} else {
			Main.assignNodesQueue.add(new AssignNodesQueueHandler( node_type, node_fdn, enmHostName ) );
		}
		log.finest("Added the FDN: " + node_fdn + " to the BlockingQueue");
		}
		catch(Exception e){
			log.warning("Exception at addingToBlockingQueue method: "+e.getMessage());
		  }
	}
	
	@Override
	public void addingToBlockingQueue(String node_type, 
			String node_fdn, 
			String ENM_hostname, 
			String eniqId,
			boolean isSingleConnectedEnmForSlave,
			boolean isRetention) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
		log.finest("Calling addingToBlockingQueue.....");
		Main.assignNodesQueue.add(new AssignNodesQueueHandler( node_type, node_fdn, ENM_hostname,eniqId, isSingleConnectedEnmForSlave, isRetention) );
		log.finest("Added the FDN: " + node_fdn + " to the BlockingQueue");
		}
		catch(Exception e){
			log.warning("Exception at addingToBlockingQueue method: "+e.getMessage());
		  }
	}
			
	@Override
	public boolean isFlsMonitorOn(String ossId) throws RemoteException{
		log = Main.getFlsTask(ossId).getLog();
		try{
			boolean adminFlag = Main.getFlsTask(ossId).isFlsAdminFlag();
			log.finest("fls_admin_flag is "+adminFlag);
			return adminFlag;
		}
		catch(Exception e){
			log.warning("Exception at isFlsMonitorOn method: "+e.getMessage());
			return false;
		}
		
	}
	
	@Override
	public List<String> getFlsToMonitorList() throws RemoteException {
		List<String> resultList = new ArrayList<>();
		for (String oss : Main.getOssIdSet()) {
			if(Main.getFlsTask(oss).isFlsAdminFlag()) {
				resultList.add(oss);
			}
		}
		return resultList;
	}
		
	@Override
	public void adminuiFlsQuery(String ossId, String flsUserStartDateTime) throws RemoteException{
		log = Main.getFlsTask(ossId).getLog();
		try{
			Main main=Main.getInstance();
			main.pmCallbyAdminUi(ossId, flsUserStartDateTime);
			log.info("User provided date and time set for FLS querying");
		}
		catch(Exception e){
		log.warning("Exception at adminuiFlsQuery method"+e.getMessage());	
		}
		
	}

	@Override
	public void refreshNodeAssignmentCache() throws RemoteException{
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
			NodeAssignmentCache.checkNoOfServers();
			log.info("Policy and Criteria Cache Refreshed!");
			handleUnassignedNodes();
		}
		catch(Exception e){
			log.warning("Exception at refreshNodeAssignmentCache method: "+e.getMessage());
		}
	}
	
	@Override
	public void shutDownMain() throws RemoteException{
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
		log.info("calling shutDown FLS ");
		Main main=Main.getInstance();
	
		log.info("Main object"+main.toString()+"is shutDown done");
		shutdownRMI();
		//RestClientInstance restClientInstance=new RestClientInstance();
		///if( restClientInstance.getSessionCheck()){
	        //restClientInstance.closeSession();
	       // }
		main.endProcess();
		}
		catch(Exception e){
		//	log.warning("Exception at EnmInterworking shutDownMain method: "+e);
		}	
	}
	
	@Override
	public String unAssignSelfSlave() throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		
		String deleteRole = "DELETE FROM ROLETABLE";
		RockFactory rockCon = DatabaseConnections.getDwhRepConnection();
		log.finest("Delete from roletable SQL - " + deleteRole);
		try {
			rockCon.getConnection().createStatement().executeUpdate(deleteRole);
			log.info("No role assigned to server now");
		} catch (SQLException e) {
			log.warning("Failed to unassign server" + e);
			return "Failed to unassign server";
		}
		finally {
			try {
				if (rockCon.getConnection() != null) {
		        	rockCon.getConnection().close();
			    }
		    } 
			catch (SQLException e) {
		    	  log.warning("Exception occured while closing the connection: " + e);
			}
		}
		return "No role assigned to server now";
	}

	@Override
	public String unAssignSpecSlave(String engineHostname, String engineIP) throws RemoteException{
		log = Logger.getLogger("symboliclinkcreator.nat");
		
		String deleteSpecRole = "DELETE FROM RoleTable WHERE ROLE='SLAVE' AND "
				+ "ENIQ_ID='" + engineHostname + "' AND"
				+ "IP_ADDRESS='" + engineIP + "'";
		RockFactory rockCon = DatabaseConnections.getDwhRepConnection();
		log.finest("Delete from roletable SQL - " + deleteSpecRole);
		try {
			rockCon.getConnection().createStatement().executeUpdate(deleteSpecRole);
			log.info("Slave server with Hostname - " + engineHostname + " and IP - " + engineIP 
					+ "has been unassigned from master server");
		} catch (SQLException e) {
			log.warning("Failed to unassign server with Hostname - " 
					+ engineHostname + " and IP - " + engineIP + e);
			return "Failed to unassign server with Hostname - " 
					+ engineHostname + " and IP - " + engineIP;
		}
		finally {
			try {
				if (rockCon.getConnection() != null) {
		        	rockCon.getConnection().close();
			    }
		    } 
			catch (SQLException e) {
		    	  log.warning("Exception occured while closing the connection: " + e);
			}
		}
		return "Slave server with Hostname - " + engineHostname + " and IP - " + engineIP 
				+ "has been unassigned from master server";
	}

	@Override
	public ArrayList<String> MasterNATDetail()throws RemoteException{
		ArrayList<String> masterNatList = new ArrayList<String>();
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory dwhrep = null;
		ResultSet masterNat = null;
		RockResultSet rockResultSet = null;
		try{
			dwhrep = DatabaseConnections.getDwhRepConnection();
			String natSql="select * from ENIQS_Node_Assignment";
			rockResultSet = dwhrep.setSelectSQL(natSql);
			masterNat = rockResultSet.getResultSet();
			while(masterNat.next()) {
				String s = masterNat.getNString("ENIQ_IDENTIFIER")+"::"+masterNat.getNString("FDN")+"::"
						+masterNat.getNString("NETYPE")+"::"+masterNat.getNString("ENM_HOSTNAME");
				masterNatList.add(s);
			}
		}
		catch(Exception e){
			log.log(Level.WARNING, "Exception while querying the master nat table: ", e);
		}
		finally{
			closeCon(rockResultSet, null, dwhrep);
		}
		return masterNatList;		
	}
		
	@Override
	public List<Boolean> changeProfile(String ossId, String profileName) throws RemoteException {
		List<Boolean> resultList = new ArrayList<Boolean>();
		boolean exceptionallyFailed = false;
		boolean noValidInstance = false;
		boolean noMount=false;
		log = Logger.getLogger("symboliclinkcreator.fls");
		if (Main.getFlsTask(ossId) != null || ALL.equalsIgnoreCase(ossId)) {
			
			try{
				if (ALL.equalsIgnoreCase(ossId)) {
					for (String oss : Main.getOssIdSet()) {
						log = Main.getFlsTask(oss).getLog();
						if(profileName.equals("Normal"))
						{
							File flagFile=new File("/eniq/sw/conf/.Mount_"+oss+"_flagfile");
							if(flagFile.exists())
							{
								noMount=true;
								log.info("FLS Mount points are not up.FLS Profile for "+oss+" will not be changed to "+profileName);
							}
							else
							{
								Main.getEnmInterWorking(oss).setActiveProfile(profileName);
								log.info("FLS Profile for "+oss+" successfully changed to " + profileName);
							}
						}
						else
						{
							Main.getEnmInterWorking(oss).setActiveProfile(profileName);
							log.info("FLS Profile for "+oss+" successfully changed to " + profileName);
						}
						
					}
				} else {
					log = Main.getFlsTask(ossId).getLog();
					if (Main.getEnmInterWorking(ossId) != null) {
						if(profileName.equals("Normal"))
						{
							File flagFile=new File("/eniq/sw/conf/.Mount_"+ossId+"_flagfile");
							if(flagFile.exists())
							{
								noMount=true;
								log.info("FLS Mount points are not up.FLS Profile for "+ossId+" will not be changed to "+profileName);
							}
							else
							{
								Main.getEnmInterWorking(ossId).setActiveProfile(profileName);
								log.info("FLS Profile for "+ossId+" successfully changed to " + profileName);
							}
						}
						else
						{
							Main.getEnmInterWorking(ossId).setActiveProfile(profileName);
							log.info("FLS Profile for "+ossId+" successfully changed to " + profileName);
						}
						
					} else {
						log.warning(ossId+ " does not have a valid instance");
						noValidInstance = true;
					}
				}
			}
			catch(Exception e){
				if (!ALL.equalsIgnoreCase(ossId)) {
					log.log(Level.WARNING,"Failed to change FLS Profile for "+ossId+ " to " + profileName + " .  Exception :", e);
				} else {
					log.log(Level.WARNING,"Failed to globally set the profile "+profileName+ ".  Exception :", e );
				}
				exceptionallyFailed = true;
			}
		} else {
			log.warning(ossId+ " does not have a valid instance");
			noValidInstance = true;
		}
		resultList.add(exceptionallyFailed);
		resultList.add(noValidInstance);
		resultList.add(noMount);
		return resultList;
	}

	/**
	 * Method to display the detailed status of FLS process
	 * 
	 * @return status of FLS process
	 * @throws RemoteException
	 */
	@Override
	public List<List<String>> status() throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.fls");
		final List<List<String>>  status = new ArrayList<>();
		final List<String> statusForCli = new ArrayList<String>();
		final List<String> statusForLog = new ArrayList<String>();
		List<String> headers = new ArrayList<String>();
		List<List<String>> rows = new ArrayList<>();
		try {
			//status.add("FLS Profile");
			headers.add(ENM_ALIAS);
			headers.add(ENM_HOST_NAME);
			headers.add(ACTIVE_PROFILE);
			for (String ossId: Main.getEnmInterWorkingMap().keySet()) {
				List<String> row = new ArrayList<String>();
				row.add(ossId);
				row.add(Main.getHost(ossId));
				row.add(Main.getEnmInterWorking(ossId).getActiveProfile());
				rows.add(row);
			}
			EnmInterworking.Table table = (new EnmInterworking()).new Table(headers,rows);
			statusForCli.add(getUpTime(true));
			statusForCli.add(table.createTable(true));
			statusForCli.add(getJvmStatus(true));
			statusForLog.add(getUpTime(false));
			statusForLog.add(table.createTable(false));
			statusForLog.add(getJvmStatus(false));
			status.add(statusForCli);
			status.add(statusForLog);
		} catch (final Exception e) {
			log.warning("Exception occured while getting status.  " + e);
		}
		
		return status;

	}
	
	private String getUpTime(boolean isForCli) {
		StringBuffer result = new StringBuffer();
		final long currentTime = System.currentTimeMillis();
		long upTime = currentTime - startedAt;
		final long days = upTime / (1000 * 60 * 60 * 24);
		upTime -= (days * (1000 * 60 * 60 * 24));
		final long hours = upTime / (1000 * 60 * 60);
		if(isForCli) {
			result.append(PURPLE+"Uptime"+RESET+": " + days + " days " + hours + " hours");
		} else {
			result.append("Uptime: " + days + " days " + hours + " hours");
		}
		result.append("\n");
		result.append("");
		return result.toString();
	}
	
	private String getJvmStatus(boolean isForCli) {
		StringBuffer result = new StringBuffer();
		result.append("\n");
		if(isForCli) {
			result.append(PURPLE);
			result.append("Java VM");
			result.append(RESET);
		} else {
			result.append("Java VM");
		}
		final Runtime rt = Runtime.getRuntime();
		result.append("\n");
		result.append("  Available processors: " + rt.availableProcessors());
		result.append("\n");
		result.append("  Free Memory: " + rt.freeMemory());
		result.append("\n");
		result.append("  Total Memory: " + rt.totalMemory());
		result.append("\n");
		result.append("  Max Memory: " + rt.maxMemory());
		result.append("\n");
		result.append("");
		return result.toString();
	}
	
	public List<String> status(String ossId) throws RemoteException {
		log = Main.getFlsTask(ossId).getLog();
		final List<String> status = new ArrayList<String>();
	    final long currentTime = System.currentTimeMillis();
	    long upTime = currentTime - startedAt;
	    final long days = upTime / (1000 * 60 * 60 * 24);
	    upTime -= (days * (1000 * 60 * 60 * 24));
	    final long hours = upTime / (1000 * 60 * 60);
	    status.add("Uptime: " + days + " days " + hours + " hours");
	    status.add("");
	    try {
	      final String flsStatusProfile = Main.getEnmInterWorking(ossId).getActiveProfile();
	      status.add("FLS Profile");
	      status.add("  Current FLS Query Status: " + flsStatusProfile);
	    } catch (final Exception e) {
	    	log.warning("Exception occured while getting status.  " + e);
	    }
	    status.add("Java VM");
	    final Runtime rt = Runtime.getRuntime();
	    status.add("  Available processors: " + rt.availableProcessors());
	    status.add("  Free Memory: " + rt.freeMemory());
	    status.add("  Total Memory: " + rt.totalMemory());
	    status.add("  Max Memory: " + rt.maxMemory());
	    status.add("");
	    return status;
	}

	/**
	 * @return the startedAt
	 */
	public long getStartedAt() {
		return startedAt;
	}

	/**
	 * @param startedAt the startedAt to set
	 */
	public void setStartedAt(long startedAt) {
		this.startedAt = startedAt;
	}

	/**
	 * @return the activeProfile
	 */
	public String getActiveProfile() {
		return activeProfile;
	}

	/**
	 * @param activeProfile the activeProfile to set
	 */
	public void setActiveProfile(String activeProfile) {
		startedAt = System.currentTimeMillis();
		this.activeProfile = activeProfile;
	}

	@Override
	public boolean policyCriteriaInsert(String technology, String namingConvention, String eniqIdentifier,
			String enmHostName) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		String slaveInsert = "Insert into ENIQS_Policy_Criteria (TECHNOLOGY, NAMINGCONVENTION, ENIQ_IDENTIFIER, ENM_HOSTNAME) Values ('" 
				+ technology + "', '" 
				+ namingConvention + "', '"+eniqIdentifier+"', '"+enmHostName+"')";


		try
		{
			rockCon = DatabaseConnections.getDwhRepConnection();
			rockCon.getConnection().createStatement().executeUpdate(slaveInsert);
			refreshNodeAssignmentCache();
			return true;
		}
		catch (Exception e)
		{
			log.warning("Exception occured while updating the slave ENIQS_Policy_Criteria table: " + e.getMessage());
			return false;
		}
		finally
		{
			try {
				if (rockCon.getConnection() != null) {
					rockCon.getConnection().close();
				}
			} catch (SQLException e) {
				log.warning( "Exception occured while closing the connection: " + e.getMessage());
			}
			
		}
	}

	@Override
	public boolean policyCriteriaUpdate(String... values) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		String sqlUpdate = " update ENIQS_Policy_criteria set " + " TECHNOLOGY=\'" + values[0] + "' , "
				+ " NAMINGCONVENTION=\'" + values[1] + "' , " + " ENIQ_IDENTIFIER=\'" + values[2] + "' , " 
				+ " ENM_HOSTNAME=\'" + values[3] + "' where "
				+ " TECHNOLOGY=\'" + values[4] + "' and " + " NAMINGCONVENTION=\'" + values[5] + "' and "
				+ " ENIQ_IDENTIFIER=\'" + values[6] + "' and" + " ENM_HOSTNAME=\'" + values[7] +"'";

		try
		{
			rockCon = DatabaseConnections.getDwhRepConnection();
			rockCon.getConnection().createStatement().executeUpdate(sqlUpdate);
			refreshNodeAssignmentCache();
			return true;
		}
		catch (Exception e)
		{
			log.warning("Exception occured while updating the slave ENIQS_Policy_Criteria table: " + e.getMessage());
			return false;
		}
		finally
		{
			try {
				if (rockCon.getConnection() != null) {
					rockCon.getConnection().close();
				}
			} catch (SQLException e) {
				log.warning( "Exception occured while closing the connection: " + e.getMessage());
			}
			
		}
	}
	
	@Override
	public boolean deletePolicyCriteria(String[] data) {
		boolean result = true;
		log = Logger.getLogger("symboliclinkcreator.nat");
		RockFactory rockCon = null;
		Statement stmt = null;
		String sqlDwhrep = "delete from ENIQS_POLICY_CRITERIA where Technology = '"+data[0]+"' AND NAMINGCONVENTION = "
				+ "'"+data[1]+"' AND ENIQ_IDENTIFIER = '"+data[2]+"' AND ENM_HOSTNAME = '"+data[3]+"'";
		try
		{
			rockCon = DatabaseConnections.getDwhRepConnection();
			stmt = rockCon.getConnection().createStatement();
			stmt.executeUpdate(sqlDwhrep);
			refreshNodeAssignmentCache();
		}
		catch (Exception e)
		{
			log.warning("Exception occured while deleting a row in the slave ENIQS_Policy_Criteria table: " + e.getMessage());
			result = false;
		}
		finally
		{
			try {
				if (rockCon.getConnection() != null) {
					rockCon.getConnection().close();
				}
				if(stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				log.warning( "Exception occured while closing the connection: " + e.getMessage());
			}
			
		}
		return result;
	}
	
	private void closeCon(RockResultSet rockResultSet, PreparedStatement ps, RockFactory dwhrep ) {
		try{
			if(rockResultSet != null) {
				rockResultSet.close();
			}
		} catch (SQLException e) {
			log.log(Level.WARNING, "Exception while closing the ResultSet: ",e);
		}
		try {
			if(ps != null && dwhrep != null) {
				dwhrep.closePreparedSqlQuery(ps);
			}
		} catch (SQLException | RockException e) {
			log.log(Level.WARNING, "Exception while closing the connection: ",e);
		}
		try{
			if (dwhrep.getConnection() != null) {
				dwhrep.getConnection().close();
			}
		} catch (SQLException e) {
			log.log(Level.WARNING, "Exception while closing the connection: ",e);
		}
	}
	
	public class Table {
		private List<String> headers;
		private List<List<String>> rows;
		private StringBuffer buffer;
		public static final String SPACER = " ";
		public static final String DASH = "-";
		public static final String NEWLINE = "\n";
		public static final String COLUMN_SEPERATOR = "|";
		public static final String JOINT = "+";
		HashMap<Integer, Integer> columnWidths = new HashMap<Integer, Integer>();

		public Table(List<String> headers, List<List<String>> rows) {
			this.headers = headers;
			this.rows = rows;
		}

		public String createTable(boolean isForCli) {
			buffer = new StringBuffer();
			getColumnWidth();
			createHDecorator();
			buffer.append(NEWLINE);
			for (int i=0;i<headers.size();i++) {
				buffer.append(COLUMN_SEPERATOR);
				buffer.append(SPACER);
				if (isForCli) {
					buffer.append(PURPLE);
					buffer.append(headers.get(i));
					buffer.append(RESET);
				} else {
					buffer.append(headers.get(i));
				}
				for (int j= (headers.get(i).length()+1); j<(columnWidths.get(i)+4) ; j++) {
					buffer.append(SPACER);
				}
			}
			buffer.append(COLUMN_SEPERATOR);
			buffer.append(NEWLINE);
			createHDecorator();

			for (List<String> row : rows) {
				buffer.append(NEWLINE);
				for (int i=0;i<row.size();i++) {
					buffer.append(COLUMN_SEPERATOR);
					buffer.append(SPACER);
					buffer.append(row.get(i));
					for (int j= (row.get(i).length()+1); j<(columnWidths.get(i)+4) ; j++) {
						buffer.append(SPACER);
					}
				}
				buffer.append(COLUMN_SEPERATOR);
			}
			buffer.append(NEWLINE);
			createHDecorator();
			return buffer.toString();
		}

		private void createHDecorator() {
			for (int i=0; i<columnWidths.size() ; i++) {
				buffer.append(JOINT);
				for (int j=0; j<(columnWidths.get(i)+4);j++) {
					buffer.append(DASH);
				}
			}
			buffer.append(JOINT);
		}

		private HashMap<Integer, Integer> getColumnWidth() {
			for (int i=0; i<headers.size() ; i++) {
				columnWidths.put(i,headers.get(i).length());
			}
			for (List<String> row : rows) {
				for (int i=0 ; i<row.size() ; i++) {
					String data = row.get(i); 
					if (data.length() > (columnWidths.get(i))) {
						columnWidths.put(i,data.length());
					}
				}
			}
			return columnWidths;
		}
	}
	
	@Override
	public boolean updateFlsTables(String oldHostName, String oldIpAddress , String newHostName, String newIpAddress) throws RemoteException {
		
		log = Logger.getLogger("symboliclinkcreator.fls");
		RockFactory rockCon = null;
		String rolaTableUpdate = "UPDATE RoleTable SET ENIQ_ID = '" + newHostName + "' , IP_ADDRESS = '" + newIpAddress + "'"
								+ " WHERE ENIQ_ID = '" + oldHostName + "' AND IP_ADDRESS = '" + oldIpAddress + "'";
		String pncTableUpdate = "UPDATE ENIQS_Policy_Criteria SET ENIQ_IDENTIFIER = '" + newHostName + "' WHERE ENIQ_IDENTIFIER = '" + oldHostName + "'";
		String natUpdate = "UPDATE ENIQS_Node_Assignment SET ENIQ_IDENTIFIER = '" + newHostName + "' WHERE ENIQ_IDENTIFIER = '" + oldHostName + "'";
		try
			{
				rockCon = DatabaseConnections.getDwhRepConnection();
				rockCon.getConnection().createStatement().executeUpdate(rolaTableUpdate);
				rockCon.getConnection().createStatement().executeUpdate(pncTableUpdate);
				rockCon.getConnection().createStatement().executeUpdate(natUpdate);
				
				log.info("Successfully updated ENIQ-S server ");
				return true;
			}
			catch (Exception e)
			{
				log.warning( "Exception while updating the FLS tables: " + e.getMessage());
				return false;
			}
			finally
			{
				try {
			        if (rockCon.getConnection() != null) {
			        	rockCon.getConnection().close();
				        }
			      } catch (SQLException e) {
			    	  log.warning( "Error while closing the connections: " + e.getMessage());
			      }
			}
		
	}
	private void handleUnassignedNodes(){
		if(EnmInterCommonUtils.getSelfRole().equals("MASTER")) {
			log = Logger.getLogger("symboliclinkcreator.nat");
			log.info("Change in policy and criteria , Node Assignment will be revisited for unassigned nodes.");
			synchronized(lock) {
				Map<String, ArrayList<String>> unassigned = new HashMap<>();
				RockFactory rockCon = null;
				String fetchUnassigned = "Select FDN,NETYPE,ENM_HOSTNAME from ENIQS_Node_Assignment where ENIQ_IDENTIFIER =''" ;
				String deleteUnassigned = "Delete from ENIQS_Node_Assignment where ENIQ_IDENTIFIER =''";
				try{
					rockCon = DatabaseConnections.getDwhRepConnection();
					ResultSet rs = rockCon.getConnection().createStatement().executeQuery(fetchUnassigned);
					ArrayList<String> details;
					while(rs.next()){
						String fdn = rs.getString("FDN");
						String ne_type = rs.getString("NETYPE");
						String enmhostName = rs.getString("ENM_HOSTNAME");
						log.finest("Node FDN = " + fdn + " , Node type = " + ne_type);
						details = new ArrayList<>();
						details.add(ne_type);
						details.add(enmhostName);
						unassigned.put(fdn, details);
					}
					rockCon.getConnection().createStatement().executeUpdate(deleteUnassigned);
					for (Eniq_Role es : NodeAssignmentCache.getRoleTableContents()) {
						if (("SLAVE").equals(es.getRole())) {
							try {
								log.info("Triggering deletion of unassigned nodes in : "+es.getEniq_identifier());
								IEnmInterworkingRMI multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(es.getIp_address()));
								multiEs.executeSql(deleteUnassigned);
							} catch ( Exception ermi){
								log.warning("Exception occured while deleting the un-Assigned nodes from ENIQS_Node_Assignment in " + es.getEniq_identifier() +
										"  SLAVE ENIQ Statistics server: " + ermi);
							}							
						}
					}
					log.info("Triggering Node Assignment Process for " + unassigned.size() + " nodes.");
					for (Map.Entry<String, ArrayList<String>> entry : unassigned.entrySet()){
						log.fine("Triggering Node Assignment Process for Node FDN = " + entry.getKey() + " and having details = " + entry.getValue() );
						addingToBlockingQueue(entry.getValue().get(0), entry.getKey(),entry.getValue().get(1));
					}
				}
				catch (Exception e){
					log.warning("Exception occurred while fetching unassigned nodes from ENIQS_Node_Assignment : " + e.getMessage());
				}
				finally{
					try {
						if (rockCon.getConnection() != null) {
							rockCon.getConnection().close();
						}
					}
					catch (SQLException e) {
						log.warning( "Exception occurred while closing the connection: " + e.getMessage());
					}
				}
			}
		}
	}
	
	private  Map<String,String> getPopulatedMap(){
		log = Logger.getLogger("symboliclinkcreator.retention");
		Map<String, String> ossAliasToHostNameMap = new HashMap<>();
		List<String> ip = new ArrayList<String>();
		List<String> oss_id = new ArrayList<String>();
		String ref = "/eniq/sw/conf/.oss_ref_name_file";
		String enm = "/eniq/sw/conf/enmserverdetail";
		try (
				BufferedReader inputOSS = new BufferedReader(new FileReader(new File(ref)));
				BufferedReader inputENM = new BufferedReader(new FileReader(new File(enm)));
				){
			String line;
			while ((line = inputOSS.readLine()) != null) {
				String[] odd = line.split("\\s+");
				oss_id.add(odd[0]);
				ip.add(odd[1]);
			}
			String line1;
			while ((line1 = inputENM.readLine()) != null) {
				String[] enmDetails = line1.split("\\s+");
				for (int j = 0; j < ip.size(); j++) {
					if (enmDetails[0].equals(ip.get(j))) {
						ossAliasToHostNameMap.put(oss_id.get(j), enmDetails[1].split("\\.")[0]);
					}
				}
			}
		} catch (FileNotFoundException e) {
			log.warning("Files .oss_ref_name_file or enmserverdetail not found Exception and exception is:"+e.getMessage());
		} catch (Exception e) {
			log.warning("Exception while retaining existing nodes"+e.getMessage());
		}
		System.out.println("populated Map : "+ossAliasToHostNameMap.toString());
		return ossAliasToHostNameMap;
	}

	@Override
	public void retainNodes(String inputFile, String enmHostName) throws RemoteException{
		log = Logger.getLogger("symboliclinkcreator.retention");
		BufferedReader br = null;
		PreparedStatement ps = null;
		RockFactory rockCon = null;
		Map<String,String> ossAliasToHostNameMap = getPopulatedMap();
		try {
			br = new BufferedReader(new FileReader(new File(inputFile)));
			String line = null;
			rockCon = DatabaseConnections.getDwhRepConnection();
			String preparedSqlStr = "Insert into ENIQS_Node_Assignment(ENIQ_IDENTIFIER,FDN,NETYPE,ENM_HOSTNAME) Values(?,?,?,?)";
			ps = rockCon.createPreparedSqlQuery(preparedSqlStr);
			while ((line = br.readLine()) != null) {
				try{
					String[] input = line.split("\\|");
					ps.setString(1, input[0]);
					ps.setString(2, input[1]);
					ps.setString(3, input[2]);
					if (enmHostName != null) {
						ps.setString(4, enmHostName);
					} else {
						ps.setString(4, ossAliasToHostNameMap.get(input[3]));
					}
					ps.execute();
				} catch (Exception e) {
					log.log(Level.WARNING,"Exception while updating NAT table for Data: "+line,e);
				}
			}
		} catch (Exception e) {
			log.warning("Exception while updating NAT table "+e.getMessage());
		} finally {
			try {
				if(br != null) {
					br.close();
				}
			} catch (Exception e) {
				
			}
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (Exception e) {
				
			}
			try {
				if (rockCon != null && rockCon.getConnection() != null) {
					rockCon.getConnection().close();
				}
			} catch (Exception e) {
				
			}
		}
	}
	
	@Override
	public boolean checkHealth() throws RemoteException {
		// returning true because the peer ENIQ-S has accessed this method via RMI without any problem
		return true;
	}
	
	@Override
	public void startHealthMonitor() throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		log.info("Starting Health monitor");
		HealthMonitor.init();
	}

	@Override
	public String getMixedNodeTechnologyType(String nodeFdn, String ossId) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		String nodeName = PmQueueHandler.getNodeNameKey(nodeFdn);
		return Main.getFlsTask(ossId).getmCache().getMixedNodeTechnologyType(nodeName, log);
	}

	@Override
	public boolean IsflsServiceEnabled(String ossId) throws RemoteException {
		return Main.isFlsEnabled(ossId);
	}

	@Override
	public void addingToBlockingQueue(String node_type, String node_fdn, String ENM_hostname, String mixedNodeTechnologies) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
			log.finest("Calling addingToBlockingQueue.....");
			Main.assignNodesQueue.add(new AssignNodesQueueHandler( node_type, node_fdn, ENM_hostname,mixedNodeTechnologies) );
			log.finest("Added the FDN: " + node_fdn + " to the BlockingQueue");
		}
		catch(Exception e){
			log.warning("Exception at addingToBlockingQueue method: "+e.getMessage());
		}
	}
	
	@Override
	public void addingToBlockingQueue(String node_type, String node_fdn, String ENM_hostname, boolean deletePolicy) throws RemoteException {
		log = Logger.getLogger("symboliclinkcreator.nat");
		try{
			log.finest("Calling addingToBlockingQueue.....");
			String enmAlias = Main.getOssAliasWithShostName(ENM_hostname);
			String nodeName = PmQueueHandler.getNodeNameKey(node_fdn);
			String mixedNodeTechnologies = Main.getFlsTask(enmAlias).getmCache().getMixedNodeTechnologyType(nodeName, log);
			Main.assignNodesQueue.add(new AssignNodesQueueHandler( node_type, node_fdn, ENM_hostname, mixedNodeTechnologies, deletePolicy) );
			log.finest("Added the FDN: " + node_fdn + " to the BlockingQueue");
		}
		catch(Exception e){
			log.warning("Exception at addingToBlockingQueue method: "+e.getMessage());
		}
	}
}