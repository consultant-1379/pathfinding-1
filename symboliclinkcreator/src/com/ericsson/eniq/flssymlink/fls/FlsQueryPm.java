package com.ericsson.eniq.flssymlink.fls;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.ws.rs.client.Client;

import javax.ws.rs.core.Response;

import org.glassfish.jersey.message.internal.MessageBodyProviderNotFoundException;

import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.automaticNAT.AssignNodes;
import com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache.ConnectionStatus;
import com.ericsson.eniq.flssymlink.StaticProperties;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import static com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache.*;

public class FlsQueryPm {
						
	String HOST;
	ENMServerDetails cache;
	//Client client;
	//RestClientInstance restClientInstance;
	Logger log;
	private String enmHostName;
	final String PATH = "file/v1/files";
	
	private static final String MANAGED_ELEMENT_REGEX = "(SubNetwork=.+,MeContext=.+)(,ManagedElement=1)";
	private static final String MANAGED_ELEMENT = ",ManagedElement=1";
	
	//queue for storing JSON Objects
	public LinkedBlockingQueue<Runnable> pmQueue;
	
	public enum QueryType {
		WITH_ID,
		WITH_TIME,
		TYPE_3,
		TYPE_4
	}
	
	public FlsQueryPm() {
		
	}
	
	public FlsQueryPm(ENMServerDetails cache, Logger log, LinkedBlockingQueue<Runnable> pmQueue, String enmHostName) {
		this.cache=cache;
		this.log=log;
		this.pmQueue=pmQueue;
		this.enmHostName = enmHostName;
	}
	

	 /** Send FLS Query REQUEST to ENM in REST GET request and get the response in the form of JSON
	 * object.
	 * @param nodeFdnList 
	 * @param nodeName,nodeList, isNewNode,id
	 */
	
	//To collect Nodes assigned to particular Eniq-S
	public ArrayList<String> getNodeFdnList(String neType,String eniqName) throws SQLException
	{
		RockFactory dwhrep=null;
		Connection con = null;
		String selectString=null;
		final RockResultSet rockResultSet;
		try{
			dwhrep=DatabaseConnections.getDwhRepConnection();
			con = dwhrep.getConnection();
			selectString="select FDN from ENIQS_Node_Assignment where  NETYPE='"+neType+"' and ( ENIQ_IDENTIFIER='" + Main.getEniqName()
                    + "' or ENIQ_IDENTIFIER like '%" + Main.getEniqName() + "%' ) ";
			rockResultSet = dwhrep.setSelectSQL(selectString);
			ResultSet rs= rockResultSet.getResultSet();
			ArrayList<String> returnNodeFdnList=new ArrayList<String>();
			
			while(rs.next()){
				returnNodeFdnList.add(rs.getString(1));
				log.finest("FDN value "+rs.getString(1));
			}
			return returnNodeFdnList;
		}catch(Exception e){
			log.warning("Exception in getNodeList method "+e.getMessage());
			return null;
		}		
		finally{
			try{
				if(con != null){
					con.close();
				}
			}
			catch(Exception e){
				log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
			}
		}
		
	}
	
	//To collect Nodes assigned to any Eniq-S
	public ArrayList<String> getAssignedNodeFdnList(String neType) throws SQLException
	{
		RockFactory dwhrep=null;
		Connection con = null;
		String selectString=null;
		final RockResultSet rockResultSet;
		try{
			dwhrep=DatabaseConnections.getDwhRepConnection();
			con = dwhrep.getConnection();
			selectString="select FDN from ENIQS_Node_Assignment where  NETYPE='"+neType+"' and ENIQ_IDENTIFIER != ''";
			rockResultSet = dwhrep.setSelectSQL(selectString);
			ResultSet rs= rockResultSet.getResultSet();
			ArrayList<String> returnNodeFdnList=new ArrayList<String>();
			
			while(rs.next()){
				returnNodeFdnList.add(rs.getString(1));
				log.finest("FDN value "+rs.getString(1));
			}
			
			
			return returnNodeFdnList;
			
		}catch(Exception e){
			log.warning("Exception in getAssignedNodeFdnList method"+e.getMessage());
			return null;
		}		
		finally{
			try{
				if(con != null){
					con.close();
				}
			}
			catch(Exception e){
				log.warning("Exception while closing dwh-rep connection: " +e.getMessage());
			}
		}
		
	}
//To collect Nodes in NAT table

		public ArrayList<String> getNodeAllFdnList(String neType) throws SQLException
		{
			RockFactory dwhrep=null;
			Connection con = null;
			String selectString=null;
			final RockResultSet rockResultSet;
			try{
				dwhrep=DatabaseConnections.getDwhRepConnection();
				con = dwhrep.getConnection();
				selectString="select FDN from ENIQS_Node_Assignment where  NETYPE='"+neType+"'";
				rockResultSet = dwhrep.setSelectSQL(selectString);
				ResultSet rs= rockResultSet.getResultSet();
				ArrayList<String> returnNodeFdnList=new ArrayList<String>();
				
				while(rs.next()){
					returnNodeFdnList.add(rs.getString(1));
					log.finest("FDN value "+rs.getString(1));
				}
				
				
				return returnNodeFdnList;
				
			}catch(Exception e){
				log.warning("exception in getNodeList method"+e);
				return null;
			}		
			finally{
				try{
					if(con != null){
						con.close();
					}
				}
				catch(Exception e){
					log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
				}
			}
			
		}
		
	private String getFormattedDataType(String dataType) {
		StringBuilder builder = new StringBuilder("");
		builder.append("dataType==");
		builder.append(dataType);
		return builder.toString();
	}
		
	public Map<String,Object> queryPM(String nodeType,long id,String dateTime,QueryType queryType,RestClientInstance restClientInstance,Client client,String dataType) {
		
		String ossId = Main.getOssIdForHost(cache.getHost());
		FlsTask flsTask = Main.getFlsTask(ossId);
		long resultId=id;
		String fileCreationTimeInOss = dateTime;
		Map<String,Object> intokenMap = new HashMap<>();
		intokenMap.put(FlsTask.ID, resultId);
		intokenMap.put(FlsTask.TIME, fileCreationTimeInOss);

		if (dataType == null || dataType.isEmpty()) {
			log.info("PM Query will not be sent for the nodeType " + nodeType
					+ ".Either NodeType not present in the NodeTypeDataTypeMapping.properties file or it is mapped against wrong technology");
			return intokenMap;
		} 
		String pmDataType = getFormattedDataType(dataType);
		log.finest("Node Type: " + nodeType + " PM DataType : " + pmDataType);
		

		Date startTime = new Date(System.currentTimeMillis());

		int eniq_Stats_count = 0;
		PmJsonArray pmJsonArray = null;
		Response flsResponse = null;
		try{
			
			HOST = "https://" + cache.getHost();
			if(restClientInstance.getSessionCheck()){
				
				PmJson pmJson;
				RockFactory dwhrep=null;
			    Connection con = null;
				String selectString=null;
				final RockResultSet rockResultSet;
				try{
					dwhrep=DatabaseConnections.getDwhRepConnection();
					con = dwhrep.getConnection();
					selectString="select count(*) from RoleTable";
					rockResultSet = dwhrep.setSelectSQL(selectString);
					ResultSet rs= rockResultSet.getResultSet();
					if(rs.next()){
						eniq_Stats_count=rs.getInt(1);
					}
				}catch(Exception e){
					log.warning("Exception occured while checking the Roletable "+e.getMessage());
				}
				finally{
					try{
						if(con != null){
							con.close();
						}
					}
					catch(Exception e){
						log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
					}
				}
				
				ArrayList<String> nodeFdnList=null;
				ArrayList<String> fullNodeFdnList=null;
				ArrayList<String> assignedNodeFdnList=null;
				if(eniq_Stats_count>0){
					try {
						fullNodeFdnList=getNodeAllFdnList(nodeType);
						assignedNodeFdnList=getAssignedNodeFdnList(nodeType);
						nodeFdnList=getNodeFdnList(nodeType,Main.getEniqName());
					} catch (SQLException e) {
						log.warning("Exception occured while getting the Nodelist "+e.getMessage());
					}
				}
				try{
					flsTask.setBefore_time(System.currentTimeMillis());
		    		switch(queryType){
		    		case WITH_ID:
		    			flsResponse =client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";id=gt="+id)
		    			.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType")
		    			.request("application/hal+json").get();
		    			log.fine("PM query with token id URL : \n  " + 
		    					client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";id=gt="+id)
		    					.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType"));
		
		    			break;
		    		case WITH_TIME:
		    			flsResponse =client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";fileCreationTimeInOss=ge="+dateTime)
		    			.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType")
		    			.request("application/hal+json").get();
		    			log.fine("PM query with time stamp URL :  \n  " + 
		    					client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";fileCreationTimeInOss=ge="+dateTime)
		    					.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType"));
		    			break;
		    		case TYPE_3:
		    			flsResponse =client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";fileCreationTimeInOss=ge="+dateTime+";fileCreationTimeInOss=lt="+flsTask.getFlsStartDateTimeAdminUi())
		    			.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType")
		    			.request("application/hal+json").get();
		    			log.fine("PM query with time stamp URL :  \n  " + 
		    					client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";fileCreationTimeInOss=ge="+dateTime+";fileCreationTimeInOss=lt="+flsTask.getFlsStartDateTimeAdminUi())
		    					.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType"));
		    			break;
		    		case TYPE_4:
		    			flsResponse =client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";id=gt="+id+";fileCreationTimeInOss=lt="+flsTask.getFlsStartDateTimeAdminUi())
		    			.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType")
		    			.request("application/hal+json").get();
		    			log.fine("PM query with time stamp URL :  \n  " + 
		    					client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType+";id=gt="+id+";fileCreationTimeInOss=lt="+flsTask.getFlsStartDateTimeAdminUi())
		    					.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType"));
		    			break;
		    		default:
		    			flsResponse =client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType)
		    			.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType")
		    			.request("application/hal+json").get();
		    			log.fine("PM query with time stamp URL :  \n  " + 
		    					client.target(HOST).path(PATH).queryParam("filter","("+pmDataType+");nodeType=="+nodeType)
		    					.queryParam("select", "fileLocation,id,nodeName,nodeType,fileCreationTimeInOss,dataType"));
		    			break;
		    		}
				}catch(Exception e){
					log.warning("Exception at PM Querying for NodeType: "+nodeType+"Exception is :"+e);
				}
				flsTask.setLastDate(new Date());
				String nodeTypeDataType = NodeTypeDataTypeCache.getNodeTypeDataTypeKey(nodeType, dataType);
				if(flsResponse !=null){
					flsTask.setAftertime(System.currentTimeMillis());
					pmJsonArray =flsResponse.readEntity(PmJsonArray.class);
					log.info("PM response: "+flsResponse + "\nresponse status: "+ flsResponse.getStatus() );
					ArrayList<PmJson> pmJsonArrayList=pmJsonArray.getFiles();
					if(pmJsonArrayList!=null){
						for(ListIterator<PmJson> iterator=pmJsonArrayList.listIterator();iterator.hasNext();){
							pmJson=iterator.next();
							if(eniq_Stats_count ==0){
								log.fine("Adding to the PM Queue of node type: "+nodeType+" file name: "+pmJson.getFileLocation());
								pmQueue.put(new PmQueueHandler(pmJson,log, flsTask.getmCache(), ossId));
							}
							else{
								boolean nodeFlag=Boolean.parseBoolean(StaticProperties.getProperty("NODE_FLAG","false"));
								if(!nodeFlag){
									String nodeName = null;
									String pmResponseNodeName = pmJson.getNodeName();
									if( Pattern.compile(MANAGED_ELEMENT_REGEX).matcher(pmResponseNodeName).matches() ){
										nodeName = pmResponseNodeName.substring(0, pmResponseNodeName.indexOf(MANAGED_ELEMENT));
									}
									else {
										nodeName = pmResponseNodeName;
									}
									if(checkFdn(nodeFdnList, nodeName)){
										log.fine("Adding to the pmqueue of node type: "+nodeType+" file name: "+pmJson.getFileLocation());
										pmQueue.put(new PmQueueHandler(pmJson,log, flsTask.getmCache(), ossId));
									}
									else if(checkFdn(assignedNodeFdnList, nodeName)){
										log.fine("NodeFdn: "+pmJson.getNodeName()+" is not belongs to this Eniq-S server");
									}
									else if(checkFdn(fullNodeFdnList, nodeName)){
										log.warning("NodeFdn: "+nodeName+" is not assigned to any server ");
									}else{
										if (isNodeOwner(nodeType, nodeName, enmHostName, flsTask.getmCache())) {
											log.info(" processing new FDN file and creating symlink");
											pmQueue.put(new PmQueueHandler(pmJson,log, flsTask.getmCache(), ossId));
										}else {
											log.warning("NodeFdn: "+nodeName+" "
													+ "is new FDN and not processing the data inline with the defined Policy & Criteria");
										}
									}
								}
								else{
									String nodeName=pmJson.getNodeName();
									for(ListIterator<String> i=nodeFdnList.listIterator();i.hasNext();){
										String nodeFdn=(String)i.next();
										if(Pattern.compile(".*="+nodeName+".*").matcher(nodeFdn).matches()){
											log.fine("Adding to the PM Queue of node type: "+nodeType+" file name: "+pmJson.getFileLocation());
											pmQueue.put(new PmQueueHandler(pmJson,log, flsTask.getmCache(), ossId));
										}
															  
									}
								}
							}
						}
						if(!pmJsonArrayList.isEmpty()){
							
							resultId = pmJsonArray.files.get(pmJsonArrayList.size()-1).getId();
							fileCreationTimeInOss = pmJsonArray.files.get(pmJsonArrayList.size()-1).getFileCreationTimeInOss();
							int timeZoneIndex = fileCreationTimeInOss.indexOf('+');
							if (timeZoneIndex != -1) {
								fileCreationTimeInOss = fileCreationTimeInOss.substring(0, timeZoneIndex);
							}
							
							intokenMap.put(FlsTask.ID, resultId);
							intokenMap.put(FlsTask.TIME, fileCreationTimeInOss);
							
						}
						Date endTime = new Date(System.currentTimeMillis());
						Long diff=endTime.getTime()-startTime.getTime();
						log.info("Time taken(in ms) to query fls for NodeType: "+nodeTypeDataType+" is: "+ diff + 
								" . Total number of files received in the response is:" + pmJsonArray.files.size());

					}
					//storing last id value
					}else{
						log.warning("PM Response is null for : "+nodeTypeDataType);
						
						return intokenMap;
					}
				}	
			}
			catch(MessageBodyProviderNotFoundException e){
				log.warning("MessageBodyProviderNotFoundException in PM Subscription  "+e.getMessage());
			}
			catch(Exception e){
				log.warning("Exception in PM Subscription  "+e.getMessage());
			}
			finally{
				if(flsResponse != null)
					flsResponse.close();
			}
		return intokenMap;
	}
	
	private boolean isNodeOwner(String nodeType, String nodefdn, String enmHostName, MixedNodeCache mcache) {
		try {
			if(ConnectionStatus.SINGLE == getConnectionStatus(enmHostName)) {
				log.info("Checking missing NAT table entry for single connected ENM "+enmHostName);
				Main.getEnmInter().addingToBlockingQueue(nodeType, nodefdn, enmHostName);
				return true;
			} else {
				log.info("Checking missing NAT table entry for multi connected ENM "+enmHostName);
				String mixedNodeTechnologies = mcache.getMixedNodeTechnologyType(PmQueueHandler.getNodeNameKey(nodefdn), log);
				AssignNodes assignNodes = null;
				if (mixedNodeTechnologies != null && !mixedNodeTechnologies.equals("NO_KEY") && !mixedNodeTechnologies.equals("EMPTY_CACHE")) {
					assignNodes = new AssignNodes(nodeType, nodefdn,enmHostName, null, false, false,mixedNodeTechnologies);
				} else {
					assignNodes = new AssignNodes(nodeType, nodefdn,enmHostName, null, false, false);
				}
				
				String eniq = assignNodes.matchPolicyAndGetAssignment();
				if (EnmInterCommonUtils.getEngineHostname().equals(eniq)) {
					//Should handle this node according to the policy & criteria defined.
					//Passing the retention flag as true to avoid checking against policy and criteria one more time
					Main.getEnmInter().addingToBlockingQueue(nodeType, nodefdn, enmHostName, eniq, false, true);
					return true;
				} else {
					return false;
				}
			}
		} catch (Exception e) {
			return false;
		}
	}
	
	public boolean checkFdn ( ArrayList<String> nodeFdnList, String nodeName ){
		boolean isFdnPresent = false;
		
		for(ListIterator<String> nodeFdnsIterator = nodeFdnList.listIterator(); nodeFdnsIterator.hasNext(); ){
			String nodeFdn=(String)nodeFdnsIterator.next();
			if( nodeFdn.contains(nodeName) ){
				isFdnPresent = true;
				break;
			}
		}
		return isFdnPresent;
	}
	
	
	
}