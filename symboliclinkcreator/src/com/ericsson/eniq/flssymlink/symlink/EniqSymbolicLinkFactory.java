package com.ericsson.eniq.flssymlink.symlink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.eniq.flssymlink.StaticProperties;

public class EniqSymbolicLinkFactory extends AbstractSymbolicLinkFactory {

    /**
     * Map which stores
     * @SymbolicLinkSubDirConfiguration data for all NeTypes parsed from eniq.xml
     * using
     * @SymLinkDataSAXParser.
     */
    protected ConcurrentHashMap<String,List<Map<String,SymbolicLinkSubDirConfiguration>>> statsSymbolicLinkSubDirConfigurations = new ConcurrentHashMap<>();
	/**
	* All supported NeTypes by this factory Mapping between the nodeType
	* representation by
	* 
	* @MONodeType and String representation in eniq.xml
	*/
	//protected final Map<String, Short> configurationNameToNodeType = new HashMap<String, Short>();

	/**
	* max number of links per directory rule If this rule is set, the factory
	* will go to next sub-directory only after the current directory reached
	* its maximum capacity.
	*/
	public static final int MAX_PER_DIRECTORY = 0;

	/**
	* max number of links per iteration rule, which is default
	*/
	public static final int MAX_PER_ITERATION = 1;

	/**
	* Holder for rule selected by the user
	*/
	protected int symLinkDirectoryRule = MAX_PER_ITERATION;
		
	/**
	* Map<NeType, SubDirStatus> Which stores
	* 
	* @SubDirStatus for each NeType.
	* @SubDirStatus represents the number of links created and currently using
	*               sub directory for each NeType
	*/
	private final Map<String,Map<String,SubDirStatus>> subDirStatusPerNodeType = new ConcurrentHashMap<>();


	public EniqSymbolicLinkFactory(String ossId) {
		super(ossId);
	}
   
    @Override
    void finalizeConfiguration() {

    	statsSymbolicLinkSubDirConfigurations = symbolicLinkSubDirConfigurations;
    	log.info("statsSymbolicLinkSubDirConfigurations: "+statsSymbolicLinkSubDirConfigurations.toString());
    	loadExclusionList();
        symLinkDirectoryRule = Integer.parseInt("1");
    }
    
    private void loadExclusionList() {
    	subNetworkExclusionList = new ArrayList<String>();
    	try {
    		String exclusionList = StaticProperties.getProperty("SUB_NETWORK_EXCLUSION_LIST", "TOPOLOGY_BULK_CM" );
        	String[] tokens = exclusionList.split(",");
        	for (String nodeType : tokens) {
        		subNetworkExclusionList.add(nodeType);
        	}
    	} catch (Exception e) {
    		log.log(Level.WARNING,"Exception while loading Exclusion list",e);
    	}
    }

    /**
     * Called only once during
     *  initialization, so no performance issues
     */

    @Override
    void createRequiredSubDirectories() {
        for (final List<Map<String,SymbolicLinkSubDirConfiguration>> mapList : statsSymbolicLinkSubDirConfigurations
                .values()) {
        	for( Map<String,SymbolicLinkSubDirConfiguration> map : mapList ) {
        		for (SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration : map.values()) {
        			createDirectories(symbolicLinkSubDirConfiguration.getNodeTypeDir(),
                            symbolicLinkSubDirConfiguration.getSubDirs());
        		}
             }
        }
    }

	@Override
	List<Map<String, SymbolicLinkSubDirConfiguration>> getSymbolicLinkSubDirConfiguration(final String nodeType, Logger log) {
		this.log = log;
		return statsSymbolicLinkSubDirConfigurations.get(nodeType);
	}

	@Override
	String getLinkSubDir(
		final SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration,
		final String nodeName, String nodeTypeDir){
		try{
			final SubDirStatus subDirStatus = getSubDirStatus(symbolicLinkSubDirConfiguration
					.getName(), nodeTypeDir);
			final String result;

			/*
			 * Lock the status until we have counted and moved folder if required.
			 * Other Threads simply have to wait for this but it should be quick
			 * Many threads can be creating symbolic links hence the need for
			 * Synchronization
			 */
			synchronized (subDirStatus) {
				subDirStatus.numberOfLinks++; // incrementing for this symbolic
				// link creation iteration (in
				// advance)
				if (subDirStatus.numberOfLinks > symbolicLinkSubDirConfiguration
						.getMaxNumLinks()) {
					log.info(Thread.currentThread().getName() + "Reached max no links for directory number "+ subDirStatus.subDirNumber
							+ " NodeTypeDir : "+nodeTypeDir + " nodeName : "+nodeName);
					subDirStatus.findNextAvailableSubDirectory(symbolicLinkSubDirConfiguration);
				}
				// subDirStatus is now updated with the sub dir number to be used
				// for this iteration

				result = symbolicLinkSubDirConfiguration.getSubDirs().get(subDirStatus.subDirNumber);
				log.fine(Thread.currentThread().getName() + " nodeName : "+nodeName + " Sub dir to be used for NodeTypeDir : "+nodeTypeDir + " is "+result
						+ " Total links : "+subDirStatus.numberOfLinks);
			}
			return result;
		}
		catch(Exception e){
			log.warning(Thread.currentThread().getName() + " Exception at getLinkSubDir method "+e.getMessage());
		}
		return null;
	}

	/**
	* @param name
	* @return current
	* @SubDirStatus for a given NeType
	*/
	private SubDirStatus getSubDirStatus(final String name, String nodeTypeDir) {
		SubDirStatus currentSubDirStatus = getSDStatus(name, nodeTypeDir);
		if (currentSubDirStatus == null) {
			synchronized (subDirStatusPerNodeType) {
				currentSubDirStatus = getSDStatus(name, nodeTypeDir);
				if (currentSubDirStatus == null) {
					currentSubDirStatus = getSubDirStatus(log);
					Map<String, SubDirStatus> innerMap = subDirStatusPerNodeType.get(name);
					if (innerMap != null) {
						log.log(Level.FINE,"Already some contents are present in status Map :"+subDirStatusPerNodeType);
						innerMap.put(nodeTypeDir, currentSubDirStatus);
						log.log(Level.FINE,"New contents in status Map :"+subDirStatusPerNodeType);
					} else {
						innerMap = new HashMap<>();
						innerMap.put(nodeTypeDir, currentSubDirStatus);
						subDirStatusPerNodeType.put(name, innerMap);
					}
					log.log(Level.INFO, "This Object :" + this + " " + Thread.currentThread().getName()
							+ " Created Subdir status for : " + name + " dir : " + nodeTypeDir);
				}
			}
		}
		return currentSubDirStatus;
	}
	
	private SubDirStatus getSDStatus(final String name, String nodeTypeDir) {
		SubDirStatus currentSubDirStatus = null;
		if (subDirStatusPerNodeType.get(name) != null) {
			currentSubDirStatus = subDirStatusPerNodeType.get(name).get(nodeTypeDir);
		}
		return currentSubDirStatus;
	}

	/**
	* @return a new
	* @SubDirStatus object
	*/
	protected SubDirStatus getSubDirStatus(Logger log) {
	return new SubDirStatus(this, 0, 0, log);
	}

}
