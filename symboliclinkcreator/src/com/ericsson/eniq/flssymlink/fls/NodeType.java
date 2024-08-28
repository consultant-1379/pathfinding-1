package com.ericsson.eniq.flssymlink.fls;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NodeType {
	private String name;
	private Map<String, Set<String>> technologyDataTypeMap;
	
	private NodeType() {

	}

	public String getName() {
		return name;
	}

	public Map<String, Set<String>> getTechnologyDataTypeMap() {
		return Collections.unmodifiableMap(technologyDataTypeMap);
	}


	public static class NodeTypeBuilder {

		private String name;
		private Map<String, Set<String>> technologyDataTypeMap;

		public NodeTypeBuilder(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
		
		public NodeTypeBuilder put(String technology, String dataType) {
			if (technology == null || dataType == null || technology.isEmpty() || dataType.isEmpty()) {
				return this;
			}
			if (technologyDataTypeMap == null) {
				technologyDataTypeMap = new HashMap<>();
			}
			Set<String> dataTypes = technologyDataTypeMap.get(technology);
			if (dataTypes == null) {
				dataTypes = new HashSet<>();
				technologyDataTypeMap.put(technology, dataTypes);
			}
			dataTypes.add(dataType);
			return this;
		}
		

		public NodeType build() {
			NodeType nodeType = new NodeType();
			nodeType.name = this.name;
			nodeType.technologyDataTypeMap = new HashMap<>(technologyDataTypeMap);
			return nodeType;
		}

	}
}
