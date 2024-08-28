package com.ericsson.eniq.flssymlink.fls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.ericsson.eniq.flssymlink.symlink.SymbolicLinkCreationHelper;

public class TopologyQueueHandler implements Runnable {
		
	private TopologyJson topologyJson;
	Logger log;
	private String ossId;
		
	public TopologyQueueHandler(TopologyJson topologyJson, Logger log, String ossId) {
		super();
		this.topologyJson= topologyJson;
		this.log = log;
		this.ossId = ossId;
	}
	
	@Override
	public void run() {
		try {
			Date startTime_Topology = new Date(System.currentTimeMillis());
			String nodeType = topologyJson.getNodeType();
			String dataType = topologyJson.getDataType();
			String fileLocation = topologyJson.getFileLocation();
			
			//These checks need to be re-factored.
			if(dataType.matches("TOPOLOGY_.+_.+")){
				if( nodeType.equalsIgnoreCase("RadioNode") ){
					List<String> extractedNodeType = new ArrayList<String>(Arrays.asList(dataType.split("\\_")));
					extractedNodeType.remove(0);
					for(String eachNodeType : extractedNodeType){
						SymbolicLinkCreationHelper.createSymbolicLink("TOPOLOGY_"+eachNodeType+"_" + nodeType, fileLocation, log, startTime_Topology, nodeType+"_Topology", ossId);
					}
				} else if( nodeType.equalsIgnoreCase("BULK_CM")){
					if(Main.isBulkcmInstalled()) {
						SymbolicLinkCreationHelper.createSymbolicLink(dataType, fileLocation, log, startTime_Topology, nodeType+"_Topology", ossId);
					}
					else {
						log.info("BULK_CM TP is either not installed or ACTIVE in the Server. Hence symboliclink will not be created for BULK_CM");
					}
				} else {
					SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType, fileLocation, log, startTime_Topology, nodeType+"_Topology", ossId);
				}
			}
			else{
				SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType, fileLocation, log, startTime_Topology, nodeType+"_Topology", ossId);
			}
		} catch ( Exception e ) {
				log.warning( "Exception occurred while creating Topology symbolic link for  : " + topologyJson.getFileLocation() );
		}
	}
}
