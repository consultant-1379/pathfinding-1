package com.ericsson.eniq.flssymlink.fls;

import java.util.ArrayList;
import java.util.Date;
import java.util.ListIterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

public class FlsQueryTopology {
	
	String HOST;
	ENMServerDetails cache;
	Logger log;
	LinkedBlockingQueue<Runnable> topologyQueue;
	final String PATH = "file/v1/files";
		
	public FlsQueryTopology( ENMServerDetails cache,Logger log,LinkedBlockingQueue<Runnable> topologyQueue) {
		this.log=log;
		this.cache=cache;
		this.topologyQueue=topologyQueue;
	}
		
	public Topology queryTopology(String dataType,String time,ArrayList<Long> idList,String dateTime,int choice,RestClientInstance restClientInstance,Client client) {
		Topology topology = new Topology(time,idList);
		Date startTime = new Date(System.currentTimeMillis());
		String ossId = Main.getOssIdForHost(cache.getHost());
		FlsTask flsTask = Main.getFlsTask(ossId);
		Response flsResponse=null ;
		String creationTime = null;
		if(time != null){
			int index=time.indexOf('+');
			if (index != -1) {
				creationTime = time.substring(0,index);
			} else {
				creationTime = time;
			}
			
		}
		TopologyJsonArray topologyJsonArray=null;
		try{
			HOST = "https://" + cache.getHost();
			if(restClientInstance.getSessionCheck()){
				
				try{
					switch(choice){
						case 1:
						
						log.info("Topology query with file creation time in OSS URL : \n  " + 
							 client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*;fileCreationTimeInOss=ge="+creationTime)
								.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc"));
						
						flsResponse =client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*;fileCreationTimeInOss=ge="+creationTime)
						.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc")
						.request("application/hal+json").get();
						
						break;
						
						case 2:
						
						log.info("Topology query for the first time URL :  \n  " + 
							 client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*")
								.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc"));
			    	 
						flsResponse =client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*")
						.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc")
						.request("application/hal+json").get();
				 
			    		break;
			    	
						default:
						log.fine("Topology query for the first time URL :  \n  " + 
						client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*")
						.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc"));
		    	
						flsResponse =client.target(HOST).path(PATH).queryParam("filter","dataType==TOPOLOGY*")
						.queryParam("select", "fileLocation,id,fileCreationTimeInOss,dataType,nodeType,nodeName").queryParam("orderBy", "fileCreationTimeInOss%20asc")
						.request("application/hal+json").get();
				
						break;
					}
			    
				}
				catch(Exception e){
					log.info("Exception while querying for topology = " +e);
				
				}
				flsTask.setLastDate(new Date());
				if(flsResponse !=null){
					topologyJsonArray =flsResponse.readEntity(TopologyJsonArray.class);
					log.info("TOPOLOGY response "+flsResponse + " response status: "  + flsResponse.getStatus() );
					//get request to enm server for topology files			
					ArrayList<TopologyJson> topologyArrayList=topologyJsonArray.getFiles();
					TopologyJson topologyJson=null;
					//adding topology json objects to queue
					if(topologyArrayList != null){
						for(ListIterator<TopologyJson> iterator=topologyArrayList.listIterator();iterator.hasNext();){
							topologyJson=iterator.next();
							log.finest("Adding to the Topology Queue of nodeType: " + topologyJson.getNodeType() + " file name: " + topologyJson.getFileLocation() );
							if(choice == 1 && topologyJson.getFileCreationTimeInOss().equals(time)){//filter out id's only when it queries again using file creation time
								if(!idList.contains(topologyJson.getId())){ 
									topologyQueue.put(new TopologyQueueHandler(topologyJson, log, ossId));
									populateMixedNodeCache(topologyJson, flsTask);
								}
							}
							else{
								topologyQueue.put(new TopologyQueueHandler(topologyJson, log, ossId));
								populateMixedNodeCache(topologyJson, flsTask);
							}
						}
					flsTask.getmCache().persistMixedNodeCache(log);
					
					if(topologyArrayList.size() > 0 ){
						time = topologyArrayList.get(topologyArrayList.size()-1).getFileCreationTimeInOss();
						topology.setTime(time);
					
					}
					idList = new ArrayList<Long>();
					for (ListIterator<TopologyJson> it = topologyArrayList.listIterator(topologyArrayList.size()); it.hasPrevious();) {
						topologyJson = (TopologyJson)it.previous();
						if(topologyJson.getFileCreationTimeInOss().equals(time)){
							idList.add(topologyJson.getId());
						}
						else{
							break;
						}
					} 
					topology.setIds(idList);
					} else {
						if (flsResponse.getStatus() == 503) {
							log.warning("FLS service unavailable in ENM server");
						}
						log.finest("Topology Response has no files");
						return topology;
					}
				}
			}						
		}
		catch(Exception e){
			log.warning("Exception at Fls TOPOLOGY Query : "+e.getMessage());
		}
		finally{
			if(flsResponse != null)
				flsResponse.close();

		}
		Date endTime = new Date(System.currentTimeMillis());
		Long diff=endTime.getTime()-startTime.getTime();
		log.fine("Time taken(in ms) to query fls for Topology is: "+diff);
		return topology;
	}

	/**
	 * @param topologyJson
	 */
	private void populateMixedNodeCache(TopologyJson topologyJson, FlsTask flsTask) {
		try{
			log.finest("FlsQueryTopology calling cache build. nodeName: " + topologyJson.getNodeName() +
					"   dataType: " + topologyJson.getDataType() + "   nodeType: " + topologyJson.getNodeType());
			flsTask.getmCache().putIntoRadioNodeToTechnologyTypeMap(topologyJson, log);
		} catch (Exception e){
			log.warning("FlsQueryTopology Exception while calling cache build  " + e.getMessage());
		}
	}

}
