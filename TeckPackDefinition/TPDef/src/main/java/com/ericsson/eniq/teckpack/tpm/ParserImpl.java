package com.ericsson.eniq.teckpack.tpm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParserImpl {
	String versionID = null;
	String parserType = null;
	String parentTableName = null;
	List<Object> transformations = null;
	Map<String, Object> attributeTags = null;
	List<Object> dataTags = null;
	String dataFormatID = null;
	String transformerID = null;
	List<Object> parentAttributeNames = null;

	public ParserImpl(String versionID, String parentTableName, String parserType) {

		this.versionID = versionID;
		this.parserType = parserType;
		this.parentTableName = parentTableName;
		this.transformations = new ArrayList<Object>();
		this.attributeTags = new HashMap<String, Object>();
		this.dataTags = new ArrayList<Object>();// # stores the mappings between table names and their data tags
		this.dataFormatID = this.versionID + ":" + this.parentTableName + ":" + this.parserType;
		this.transformerID = this.versionID + ":" + this.parentTableName + ":" + this.parserType;
		this.parentAttributeNames = new ArrayList<Object>();// This is used to only get TP specific information. Should
															// not load data from base TP
	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		if (xlsDict != null) {
			Set<String> set = xlsDict.keySet();
			List<String> rowIDs = new ArrayList<String>(set);
			for (int i = 0; i < rowIDs.size(); i++) {
				String rowid = String.valueOf(rowIDs.get(i));
				if (!rowid.equals("DATATAGS") && !rowid.equals("ATTRTAGS")) {
					Transformation transformation = new Transformation(rowid, this.transformerID);
					transformation.getPropertiesFromXls((Map<String, Object>) xlsDict.get(rowid));
					this.transformations.add(transformation);
				}
			}
			if (xlsDict.containsKey("DATATAGS")) {
				String DATATAGS = (String) xlsDict.get("DATATAGS");
				if (DATATAGS.contains(";")) {

					String tableTags[] = DATATAGS.split("\\;");
					for (String tag : tableTags) {
						if (tag != "") {
							this.dataTags.add(tag.strip());
						}
					}
				}
			} else {
				this.dataTags.add(((String) xlsDict.get("DATATAGS")));
			}

			if (set.contains("ATTRTAGS")) {

				Map<String, Object> ATTRTAGSTemp = (Map<String, Object>) xlsDict.get("ATTRTAGS");
				for (Map.Entry<String, Object> val : ATTRTAGSTemp.entrySet()) {
					String key = val.getKey();
					String value = (String) val.getValue();
					this.attributeTags.put(key, value);
				}
			}
		}
	}

	public String toJSON(ParserImpl parserImpl) {
		String offset = "\t";
		String os = "\n" + offset;
		String os2 = os + offset;
		String os3 = os2 + offset;
		String os4 = os3 + offset;
		String dq = "\"";

		String outputJSON = os + "{" + dq + "type" + dq + ":" + dq + parserImpl.parserType + dq + ",";

		if (parserImpl.transformations.size() > 0) {
			outputJSON += os2 + dq + "Transformations" + dq + ":[]";
		}

		outputJSON += os + "},";
		return outputJSON;
	}
}
