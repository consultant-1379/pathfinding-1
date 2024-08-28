package com.ericsson.eniq.teckpack.tpm;

import java.util.HashMap;
import java.util.Map;

public class Transformation {
	String rowID = null;
	String transformerID = null;
	Map<String, Object> properties = null;
	String _hash = null;

	Transformation(String rowID, String transformerID) {
		this.rowID = rowID;
		this.transformerID = transformerID;
		this.properties = new HashMap<String, Object>();
		this._hash = null;
	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		if (xlsDict != null) {
			for (Map.Entry<String, Object> val : xlsDict.entrySet()) {
				String Name = val.getKey();
				String value = (String) val.getValue();
				this.properties.put(Name, value);
			}
		}
	}

}
