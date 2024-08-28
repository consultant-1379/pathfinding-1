/**
 * 
 */
package com.ericsson.eniq.flssymlink.fls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache;

/**
 * @author xnagdas
 *
 */
/**
 * @author xnagdas
 *
 */
public class MixedNodeCache {
	
	public static ConcurrentHashMap<String, String> mixedNodeToTechnologyTypeMap = new ConcurrentHashMap<String, String>();
	
	public static final String EMPTY_CACHE = "EMPTY_CACHE";
	
	public static final String NO_FDN = "NO_KEY";
	
	public static final String DUPLICATE_KEY = "KEY_EXISTS";
	
	public static final String RADIO_NODE = "RadioNode";
	
	public static final String LRAT = "LTE";
	
	public static final String WRAT = "WCDMA";
	
	public static final String MIXED = "LTE_WCDMA";
	
	public static final String MIXED_NODE_PATTERN = "([a-zA-Z]+_[^_][a-zA-Z_]+)";
	
//	public static final String LRAT = "1";
	
//	public static final String WRAT = "2";
	
//	public static final String MIXED = "3";
	
	Logger log;
	
	private String ossId;
	
	private static Set<String> mixedNodeTypeSet;
	
	public MixedNodeCache (String ossId, Logger log) {
		this.ossId = ossId;
		this.log = log;
		initMixedNodeSet();
	}
	
	private void initMixedNodeSet() {
		mixedNodeTypeSet = new HashSet<>();
		Map<String, ArrayList<String>> technologyNodeMap = NodeAssignmentCache.getTechnologyNodeMap();
		for (String key : technologyNodeMap.keySet()) {
			if (technologyNodeMap.get(key).size()>1) {
				//mixed node type encountered, store it in the mixedNodeType set.
				mixedNodeTypeSet.add(key);
			}
		}
		log.log(Level.INFO, "Mixed Node Type Set Initialized :"+mixedNodeTypeSet);
	}
	
	
	/**
	 * @param radioNodeToTechnologyTypeMap the radioNodeToTechnologyTypeMap to set
	 */
	public void setRadioNodeToTechnologyTypeMap(ConcurrentHashMap<String, String> radioNodeToTechnologyTypeMap) {
		MixedNodeCache.mixedNodeToTechnologyTypeMap = radioNodeToTechnologyTypeMap;
	}
	
	/**
	 * @return the radioNodeTechnologyTypeMapping
	 * @return radioNodeToTechnologyTypeMap
	 */
	public Map<String, String> getRadioNodeTechnologyTypeMapping() {
		return mixedNodeToTechnologyTypeMap;
	}

	/**
	 * @param radioNodeTechnologyTypeMapping
	 */
	public void putIntoRadioNodeToTechnologyTypeMap(TopologyJson topologyJson, Logger log) {
		String dataType = topologyJson.getDataType();
		if (!(topologyJson.getDataType().equalsIgnoreCase("TOPOLOGY_TWAMP"))) {
			String nodeType = topologyJson.getNodeType();
			if (mixedNodeTypeSet.contains(nodeType)) {
				String nodeName = topologyJson.getNodeName();
				String technologies = dataType.substring(dataType.indexOf("_")+1);
				mixedNodeToTechnologyTypeMap.put(nodeName, technologies);
				log.finest("MixedNodeCache: updated : NodeName: "+  nodeName+ 
						"   technologies :" + technologies + "   nodeType: " + nodeType);
			}
		}
	}
	
	/**
	 * @param mixedNodeFDN
	 * @return mixedNodeType
	 */
	public String getMixedNodeTechnologyType( String nodeName, Logger log ) {
		String nodeType = null;
		if( !mixedNodeToTechnologyTypeMap.isEmpty() ){
			if ( mixedNodeToTechnologyTypeMap.containsKey(nodeName) ){
				nodeType = mixedNodeToTechnologyTypeMap.get(nodeName);
			}
			else {
				nodeType = NO_FDN;
			}
		}
		else {
			nodeType = EMPTY_CACHE;
		}
		log.finest("MixedNodeCache: getMixedNodeTechnologyType. nodeName: " + nodeName + " nodeType: "+ nodeType );
		return nodeType;
	}
	
	/**	
	 * Persisted MixedNodeCache restore
	 * @param log
	 */	
	public void restorePersistedMixedNodeCache( Logger log ) {
		try {
			File mixedNodeCachePersistedFile = new File(System.getProperty("CONF_DIR") + File.separator + "MixedNodeCachePersisted_"+ossId+".ser");
			PersistedMixedNodeCache persistedMixedNodeCache = new PersistedMixedNodeCache();
			if ( mixedNodeCachePersistedFile.exists() ) {
				ObjectInputStream in = null;
				try {
					FileInputStream mixedNodeCachePersistedFileInputStream = new FileInputStream(
							System.getProperty("CONF_DIR") + File.separator + "MixedNodeCachePersisted_"+ossId+".ser");
					if ( mixedNodeCachePersistedFileInputStream.getChannel().size() > 0 ) {
						in = new ObjectInputStream(mixedNodeCachePersistedFileInputStream);
						persistedMixedNodeCache = (PersistedMixedNodeCache) in.readObject();
						setRadioNodeToTechnologyTypeMap( persistedMixedNodeCache.radioNodeToTechnologyTypeMap );
						if( !mixedNodeToTechnologyTypeMap.isEmpty() ){
							log.finest("Persisted MixedNodeCache map size:  "  + mixedNodeToTechnologyTypeMap.size() + 
									" and values after FLS restart is:   "  + persistedMixedNodeCache.radioNodeToTechnologyTypeMap.toString());
						}
						else {
							log.info("Persisted MixedNodeCache is empty!!");
						}	
					}
				} catch (Exception e) {
					log.warning("Exception during retrieving the Persisted MixedNodeCache value from the MixedNodeCachePersisted_"+ossId+".ser file:   " + e.getMessage());										
				} finally {
					if ( in != null) {
						in.close();
					}
				}
			}else{
				log.info("Persisted MixedNodeCachePersisted_"+ossId+".ser file does not exists!!");
			 }
		} catch (Exception e) {
			log.warning("Exception during restoring the Persisted MixedNodeCache from the MixedNodeCachePersisted_"+ossId+".ser file:   " + e.getMessage());	
		}
	}
	
	/**
	 * Persisting MixedNodeCache
	 * @param log
	 */
	public void persistMixedNodeCache( Logger log ) {
		ObjectOutputStream out = null;
		try {
			PersistedMixedNodeCache persistedMixedNodeCache = new PersistedMixedNodeCache();
			FileOutputStream fout = new FileOutputStream(
					System.getProperty("CONF_DIR") + File.separator + "MixedNodeCachePersisted_"+ossId+".ser");
			out = new ObjectOutputStream(fout);
			persistedMixedNodeCache.setRadioNodeToTechnologyTypeMap(mixedNodeToTechnologyTypeMap);
			log.finest("Persisting MixedNodeCache map size:  "  + mixedNodeToTechnologyTypeMap.size() +
					 "Persisting MixedNodeCache map value " + persistedMixedNodeCache.getRadioNodeToTechnologyTypeMap());
			out.writeObject(persistedMixedNodeCache);
			out.flush();
		} catch (Exception e) {
			log.warning("Exception occurred while persisting MixedNodeCache! " + e.getMessage());
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					
				}
			}
		}
		
	}
	
	/**
	 * Method that returns the mixedNode types when the mixed node cache is not empty.
	 * @return
	 */
	public Set<String> getMixedNodesToReassign() {
		if (!mixedNodeToTechnologyTypeMap.isEmpty()) {
			return mixedNodeTypeSet;
			
		}
		return null;
	}

}
