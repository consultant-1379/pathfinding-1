package com.ericsson.eniq.flssymlink.fls;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.eniq.flssymlink.StaticProperties;
import com.ericsson.eniq.flssymlink.fls.NodeType.NodeTypeBuilder;

/**
 * @author xramear	
 *
 */
/**
 * @author xramear
 *
 */
class NodeTypeDataTypeCache {
	private static Map<String, NodeType> cache = new ConcurrentHashMap<>();

	private static final String CONFIG_DIR = "/eniq/sw/installer/";
	private static final String NODETYPEDATATYPEMAPPINGFILE = "NodeTypeDataTypeMapping.properties";
	
	public static final String EVENTS = "Events";
	private static final String COLON = ":";
	public static final String UNDERSCORE = "_";
	public static final String DEFAULT_DATA_TYPE = "PM_STATISTICAL";
	private static final int DEFAULT_REPEAT_COUNT = 1;
	public static final int DEFAULT_REPEAT_COUNT_ML = 3;
	
	public static final String ONE_MIN = "1MIN";
	public static final String FIVE_MIN = "5MIN";
	public static final String FIFTEEN_MIN = "15MIN";
	public static final String THIRTY_MIN = "30MIN";
	public static final String ONE_HOUR = "1HOUR";
	public static final String TWENTY_FOUR_HOUR = "24HOUR";
	public static final String MINI_LINK = "MINI-LINK";

	private NodeTypeDataTypeCache() {

	}
	
	public static void init(Logger log) throws IOException {
		Map<String, NodeTypeBuilder> builderMap = null;
		try {
			builderMap = readFile();
			builderMap.forEach((name, builder) -> cache.put(name, builder.build()));
			log.log(Level.INFO, "Built NodeTypeDataTypeCache : " + cache);
		} finally {
			if (builderMap != null) {
				builderMap.clear();
			}
		}
	}

	public static void refresh(Logger log) throws IOException {
		init(log);
	}

	public static Map<String, NodeType> getCache() {
		return Collections.unmodifiableMap(cache);
	}
	
	private static Map<String, NodeTypeBuilder> readFile() throws IOException {
		File nodeDataTypeMapping = new File(CONFIG_DIR + NODETYPEDATATYPEMAPPINGFILE);
		String line;
		Map<String, NodeTypeBuilder> builderMap = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(nodeDataTypeMapping))) {
			while ((line = br.readLine()) != null) {
				if (line.contains(COLON)) {
					String[] tokens = line.split(COLON, 3);
					String name = tokens[0];
					String dataType = tokens[1];
					String technology = tokens[2];
					NodeTypeBuilder builder = builderMap.get(name);
					if (builder == null) {
						builder = new NodeTypeBuilder(name);
						builderMap.put(name, builder);
					}
					builder.put(technology, dataType);
				}
			}
		}
		return builderMap;
	}
	
	public static Set<String> getDataTypes(String nodeTypeName, Set<String> granularities) {
		Set<String> dataTypes = new HashSet<>();
		NodeType nodeType = cache.get(nodeTypeName);
		nodeType.getTechnologyDataTypeMap().forEach( (technology, dataTypeSet) -> 
			dataTypeSet.forEach(dataType -> getDataType(dataType, technology, granularities, dataTypes))
		);
		return dataTypes;
	}

	private static void getDataType(String dataType, String technology, Set<String> granularities,
			Set<String> dataTypes) {
		if (EVENTS.equals(technology)) {
			dataTypes.add(dataType);
			return;
		}
		for (String granularity : granularities) {
			if (!FIFTEEN_MIN.equals(granularity)) {
				String datatypeWithGran = dataType.concat(UNDERSCORE).concat(granularity);
				dataTypes.add(datatypeWithGran);
			} else {
				dataTypes.add(dataType);
			}
		}
	}

	/**
	 * Provides the count that specifies the number of PM queries 
	 * that needs to be sent for a node type based on the nodeType and dataType combination
	 * 
	 * @param nodeTypeName
	 * @param dataType
	 * @param log
	 * @return
	 */
	public static int getRepeatCount(String nodeTypeName, String dataType, Logger log) {
		String key = getNodeTypeDataTypeKey(nodeTypeName,dataType);
		int repeatCount = DEFAULT_REPEAT_COUNT;
		if (nodeTypeName.startsWith(MINI_LINK)) {
			return getMiniLinkrepeatCount(key, log);
		}
		try {
			repeatCount = Integer.parseInt(StaticProperties.getProperty(key,StaticProperties.DEFAULT_REPEAT_COUNT_STRING));
		} catch (Exception e) {
			log.log(Level.WARNING, "Invalid value present for : " +key + " , "
					+ "default value : "+repeatCount + " will be used");
		}
		return repeatCount;
	}
	
	private static int getMiniLinkrepeatCount(String key, Logger log) {
		int repeatCount = DEFAULT_REPEAT_COUNT_ML;
		try {
			repeatCount = Integer.parseInt(StaticProperties.getProperty(key,StaticProperties.DEFAULT_REPEAT_COUNT_ML_STRING));
		} catch (Exception e) {
			log.log(Level.WARNING, "Invalid value present for : " +key + " , "
					+ "default value : "+repeatCount + " will be used");
		}
		return repeatCount;
		
	}
	
	public static String getNodeTypeDataTypeKey(String nodeType, String dataType) {
		return nodeType.concat(NodeTypeDataTypeCache.UNDERSCORE).concat(dataType);
	}
	
	public static String getDataTypeForSymLink(String nodeType, String dataType, Logger log) {
		log.log(Level.FINEST,"getDataTypeForSymLink : parameters received nodeType : "+nodeType
				+" dataType : "+dataType);
		Map<String, Set<String>> technologyDataTypeMap = NodeTypeDataTypeCache.getCache()
				.get(nodeType).getTechnologyDataTypeMap();
		String result = dataType;
		if(technologyDataTypeMap.isEmpty()) {
			log.log(Level.FINEST,"No data type present for node type : "+nodeType);
			return "";
		}
		for (Map.Entry<String, Set<String>> entry : technologyDataTypeMap.entrySet()) {
			if (NodeTypeDataTypeCache.EVENTS.equalsIgnoreCase(entry.getKey())) {
				continue;
			}
			for(String refDataType : entry.getValue()) {
				if(dataType.contains(refDataType) && !dataType.equals(refDataType)) {
					result = refDataType;
					log.log(Level.FINEST,"removed granularity suffix and returning datatype as: "+result);
				}
			}
		}
		return result;
	}
	

	public static class NodeTypeDataTypeCacheException extends Exception {

		private static final long serialVersionUID = 5638032724451133532L;

		public NodeTypeDataTypeCacheException(String message) {
			super(message);
		}

		public NodeTypeDataTypeCacheException(Exception e) {
			super(e);
		}

	}
}
