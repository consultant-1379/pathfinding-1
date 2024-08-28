package com.ericsson.eniq.teckpack.tpm;

import java.util.HashMap;
import java.util.Map;

import com.ericsson.eniq.teckpack.util.Utils;

public class VectorImpl {

	String typeid = null;
	String parentAttName = null;
	public String VendorRelease = null;
	String Index = null;
	String Qty = null;
	Map<String, Object> properties = null;

	VectorImpl(String typeid, String parentAttName, String Index, String VendorRelease, String Quantity) {
		this.typeid = typeid;
		this.parentAttName = parentAttName;
		this.VendorRelease = VendorRelease;
		this.Index = Index;
		this.Qty = Quantity;
		this.properties = new HashMap<String, Object>();

	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		for (Map.Entry<String, Object> entry : xlsDict.entrySet()) {
			String Name = entry.getKey();
			Map<String, Object> Value = (Map<String, Object>) entry.getValue();
			this.properties.put(Name, Value);
		}
	}

	public String toJSON(VectorImpl vectorImpl) {
		String offset = "\t";
		String os = "\n" + offset;
		String os2 = os + offset;
		String dq = "\"";

		String outputJSON = "{"+ os + dq + "VendorRelease" + dq + ":" + dq
				+ vectorImpl.VendorRelease + dq + "," 
				+ os + dq + "index" + dq + ":" + dq + vectorImpl.Index + dq + ",";
		outputJSON += os + dq + "properties" + dq + ":{";
		for (Map.Entry<String, Object> attr : vectorImpl.properties.entrySet()) {
			outputJSON += os2 + dq + attr.getKey() + dq + ":" + dq + Utils.escape("" + attr.getValue()) + dq + ",";
		}
		if(vectorImpl.properties.size()>0)
		{
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		}
		outputJSON += os + "}},";
		return outputJSON;
	}

	public Map<String, Object> populateRepDbDicts(VectorImpl vector) {
		Map<String, Object> repDbDict = vector.properties;
		repDbDict.put("VINDEX", Utils.strFloatToInt(vector.Index));
		repDbDict.put("TYPEID", vector.typeid);
		repDbDict.put("DATANAME", vector.parentAttName);
		repDbDict.put("QUANTITY", Utils.strFloatToInt(vector.Qty));

		return repDbDict;

	}

}
