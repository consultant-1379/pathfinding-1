package com.ericsson.eniq.enminterworking.automaticNAT;

public class AssignNodesQueueHandler implements Runnable{

	private String node_type = null;
	private String node_fdn = null;
	private String enmHostName = null;
	private String eniqId = null;
	private boolean isSingleConnectedEnmForSlave = false;
	private boolean isRetention =  false;
	private String mixedNodeTechnologies = null;
	private boolean deletePolicy = false;
	
			
	public  AssignNodesQueueHandler(String node_type ,String node_fdn,String enmHostName) {
		super();
		this.node_type = node_type;
		this.node_fdn = node_fdn;
		this.enmHostName = enmHostName;
	}
	
	public  AssignNodesQueueHandler(String node_type ,String node_fdn,String enmHostName, String mixedNodeTechnologies ) {
		super();
		this.node_type = node_type;
		this.node_fdn = node_fdn;
		this.enmHostName = enmHostName;
		this.mixedNodeTechnologies = mixedNodeTechnologies;
	}
	
	public  AssignNodesQueueHandler(String node_type ,String node_fdn,String enmHostName, String mixedNodeTechnologies, boolean deletePolicy ) {
		super();
		this.node_type = node_type;
		this.node_fdn = node_fdn;
		this.enmHostName = enmHostName;
		this.mixedNodeTechnologies = mixedNodeTechnologies;
		this.deletePolicy = deletePolicy;
	}
	
	public  AssignNodesQueueHandler(String node_type ,
			String node_fdn,
			String enmHostName, 
			String eniqId, 
			boolean isSingleConnectedEnmForSlave, 
			boolean isRetention) {
		super();
		this.node_type = node_type;
		this.node_fdn = node_fdn;
		this.enmHostName = enmHostName;
		this.eniqId = eniqId;
		this.isSingleConnectedEnmForSlave = isSingleConnectedEnmForSlave;
		this.isRetention = isRetention;
	}
	
	@Override
	public void run() {
		
		//Call AssignNodes constructor
		AssignNodes assign = null;
		if (deletePolicy) {
			assign = new AssignNodes(node_type, node_fdn,enmHostName, eniqId, isSingleConnectedEnmForSlave, isRetention, mixedNodeTechnologies, deletePolicy);
			assign.init();
		} else if(mixedNodeTechnologies != null) {
			assign = new AssignNodes(node_type, node_fdn,enmHostName, eniqId, isSingleConnectedEnmForSlave, isRetention, mixedNodeTechnologies);
			assign.init();
		} else {
			assign = new AssignNodes(node_type, node_fdn,enmHostName, eniqId, isSingleConnectedEnmForSlave, isRetention);
			assign.init();
		}
	}

}
