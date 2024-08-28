package com.ericsson.eniq.teckpack.tpm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ericsson.eniq.teckpack.util.Utils;

public class TableImpl {
	public String versionID = null;
	public String tableType = null;
	public String name = null;
	public Map<String, Object> properties = null;
	public Map<String, Object> attributes = null;
	public Map<String, Object> parsers = null;
	public String typeid = null;
	public String universeClass = null;
	public String typeClassID = null;
	public List<Object> mtableIDs = null;

	public TableImpl(String versionID, String tablename, String tableType) {
		this.versionID = versionID;
		this.tableType = tableType;
		this.name = tablename;
		this.properties = new HashMap<String, Object>();
		this.attributes = new HashMap<String, Object>();
		this.parsers = new HashMap<String, Object>();
		this.universeClass = "";
		this.typeid = versionID + ":" + tablename;
		this.typeClassID = "";
		this.mtableIDs = new ArrayList<Object>();
	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		Map<String, Object> tables = (Map<String, Object>) xlsDict.get("Tables");
		Map<String, Object> topNameTemp = (Map<String, Object>) tables.get(this.name);
		for (Map.Entry<String, Object> topEntry : topNameTemp.entrySet()) {
			String Name = (String) topEntry.getKey();
			if (Name.equals("measurementKey") || Name.equals("measurementCounter") || Name.equals("referenceKey")) {
				Map<String, Object> values = (Map<String, Object>) topEntry.getValue();
				for (Map.Entry<String, Object> val : values.entrySet()) {
					String entry = val.getKey();
					Map<String, Object> properties = (Map<String, Object>) val.getValue();
					AttributeImpl attributeImpl = new AttributeImpl(entry, this.typeid, Name);
					attributeImpl.getPropertiesFromXls(properties);
					this.attributes.put(entry, attributeImpl);
				}
			} else if (Name.equals("Parser")) {
				Map<String, Object> values = (Map<String, Object>) topEntry.getValue();
				for (Map.Entry<String, Object> val : values.entrySet()) {
					String entry = val.getKey();
					Map<String, Object> properties = (Map<String, Object>) val.getValue();
					if (this.tableType.equals("Measurement")) {
						Map<String, Object> nameTemp = (Map<String, Object>) tables.get(this.name);
						if (nameTemp.containsKey("DATATAGS")) {
							Map<String, Object> parserTemp = (Map<String, Object>) nameTemp.get("Parser");
							Map<String, Object> entryTemp = (Map<String, Object>) parserTemp.get(entry);
							String DATATAGSTemp = (String) entryTemp.get("DATATAGS");
							properties.put("DATATAGS", DATATAGSTemp);
						}
						
					} else if (this.tableType.equals("Reference")) {
						Map<String, Object> nameTemp = (Map<String, Object>) tables.get(this.name);
						Map<String, Object> parserTemp = (Map<String, Object>) nameTemp.get("Parser");
						Map<String, Object> entryTemp = (Map<String, Object>) parserTemp.get(entry);

						if (entryTemp.containsKey("DATATAGS")) {
							String DATATAGSTemp = (String) entryTemp.get("DATATAGS");
							properties.put("DATATAGS", DATATAGSTemp);
						}
					}
					ParserImpl parserImpl = new ParserImpl(this.versionID, this.name, entry);
					parserImpl.getPropertiesFromXls(properties);
					this.parsers.put(entry, parserImpl);
				}
			} else if (Name.equals("CLASSIFICATION")) {
				this.universeClass = (String) topEntry.getValue();
			} else if (Name.equals("OBJECTBH")) {
				//
			} else {
				if (!Name.equals("TABLETYPE")) {
					this.properties.put(Name, topEntry.getValue());
				}
			}
		}

		completeModel();

	}

	private void completeModel() {
		if (this.tableType != null && this.tableType.equals("Measurement")) {
			if (!this.properties.containsKey("ELEMENTBHSUPPORT") || this.properties.get("ELEMENTBHSUPPORT") == "") {
				this.properties.put("ELEMENTBHSUPPORT", "0");
			}
			if (!this.properties.containsKey("PLAINTABLE") || this.properties.get("PLAINTABLE") == "") {
				this.properties.put("PLAINTABLE", "0");
			}
			if (!this.properties.containsKey("RANKINGTABLE") || this.properties.get("RANKINGTABLE") == "") {
				this.properties.put("RANKINGTABLE", "0");
			}
			if (!this.properties.containsKey("TOTALAGG") || this.properties.get("TOTALAGG") == "") {
				this.properties.put("TOTALAGG", "0");
			}
			if (!this.properties.containsKey("VECTORSUPPORT") || this.properties.get("VECTORSUPPORT") == "") {
				this.properties.put("VECTORSUPPORT", "0");
			}
			if (!this.properties.containsKey("JOINABLE") || this.properties.get("JOINABLE") == "") {
				this.properties.put("JOINABLE", "");
			}
			if (!this.properties.containsKey("DELTACALCSUPPORT") || this.properties.get("DELTACALCSUPPORT") == "") {
				this.properties.put("DELTACALCSUPPORT", "0");
			}
		}
		this.properties.put("DATAFORMATSUPPORT", "1");

	}

	String getTypeClassID(TableImpl tableImpl) {
		tableImpl.typeClassID = tableImpl.versionID + ":" + tableImpl.versionID.split("\\:")[0] + "_"
				+ tableImpl.universeClass;
		return tableImpl.typeClassID;
	}

	

	public Map<String, Object> populateRepDbDicts(TableImpl tableImpl) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (this.tableType.equals("Measurement")) {
			// MeasurementTypeClass
			Map<String, Object> measurementTypeClass = tableImpl.properties;
			measurementTypeClass = new LinkedHashMap<String, Object>();
		//	measurementTypeClass.put("VERSIONID", tableImpl.versionID);
			measurementTypeClass.put("DESCRIPTION", tableImpl.universeClass);
			measurementTypeClass.put("TYPECLASSID", tableImpl.getTypeClassID(tableImpl));

			int DELTACALCSUPPORT = 0;
			// Measurementdeltacalcsupport
			Map<String, Object> deltacalcSupport = new LinkedHashMap<String, Object>();
			if (tableImpl.properties.containsKey("DELTACALCSUPPORT")) {
				if (tableImpl.properties.get("DELTACALCSUPPORT") != null
						&& !(String.valueOf(tableImpl.properties.get("DELTACALCSUPPORT")).trim().equals("0"))) {
					deltacalcSupport.put("TYPEID", tableImpl.versionID + ":" + tableImpl.name);
					deltacalcSupport.put("VENDORRELEASE", tableImpl.properties.get("DELTACALCSUPPOR"));
					//measurementTypeClass.put("VERSIONID", tableImpl.versionID);
					DELTACALCSUPPORT = 1;
				}
			}

			// MeasurementType
			Map<String, Object> measurementType = tableImpl.properties;
			measurementType.put("TYPEID", tableImpl.versionID + ":" + tableImpl.name);
			measurementType.put("TYPENAME", tableImpl.name);
			measurementType.put("VENDORID", tableImpl.versionID.split("\\:")[0]);
			measurementType.put("FOLDERNAME", tableImpl.name);
			measurementType.put("OBJECTID", tableImpl.versionID + ":" + tableImpl.name);
			measurementType.put("OBJECTNAME", tableImpl.name);
			measurementType.put("DELTACALCSUPPORT", DELTACALCSUPPORT);
			measurementType.put("TYPECLASSID", getTypeClassID(tableImpl));
		//	measurementType.put("VERSIONID", tableImpl.versionID);

			map.put("MeasTypeClass", measurementTypeClass);
			map.put("deltacalcSupport", deltacalcSupport);
			map.put("MeasType", measurementType);

		}
		if (this.tableType.equals("Reference")) {
			Map<String, Object> refTable = null;
			refTable = new LinkedHashMap<String, Object>();
			//refTable.put("VERSIONID", tableImpl.versionID);
			refTable.put("TYPEID", tableImpl.versionID + ":" + tableImpl.name);
			refTable.put("TYPENAME", tableImpl.name);
			refTable.put("OBJECTID", tableImpl.versionID + ":" + tableImpl.name);
			refTable.put("OBJECTNAME", tableImpl.name);
			refTable.put("BASEDEF", 0);
			return refTable;
		}
		return map;
	}

	public String toJSON(TableImpl tableImpl) {
		String dq = "\"";
		String s0 = "\t";
		String s1 = "\n" + s0;
		String s2 = s1 + s0;
		String s3 = s2 + s0;

		String outputJSON = s1 + "{" + s1 + dq + "name" + dq + ":" + dq + tableImpl.name + dq + "," + s1 + dq
				+ "tableType" + dq + ":" + dq + tableImpl.tableType + dq + "," + s1 + dq + "universeClass" + dq + ":"
				+ dq + Utils.escape(tableImpl.universeClass) + dq + ",";
		outputJSON += s1 + dq + "Properties" + dq + ":{";
		for (Entry<String, Object> prop : tableImpl.properties.entrySet()) {
			outputJSON += s3 + dq + prop.getKey() + dq + ":" + dq + Utils.escape(String.valueOf(prop.getValue())) + dq
					+ ",";
		}
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		outputJSON += s1 + "},";
		
		outputJSON += s1 + dq + "Attributes" + dq + ":" + "[";
		for (Map.Entry<String, Object> entry : tableImpl.attributes.entrySet()) {
			AttributeImpl attributeImpl = (AttributeImpl) entry.getValue();
			 outputJSON += attributeImpl.toJSON(attributeImpl);
		}
		if( tableImpl.attributes.size()>0)
		{
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		}
		outputJSON += s1 + "],";

		outputJSON += s1 + dq + "Parsers" + dq + ":" + "[";
		for (Map.Entry<String, Object> entry : tableImpl.parsers.entrySet()) {
			ParserImpl parserImpl = (ParserImpl) entry.getValue();
			outputJSON += parserImpl.toJSON(parserImpl);
		}
		if(tableImpl.parsers.size()>0)
		{
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		}
		outputJSON += s1 + "],";
		outputJSON += s1 + "},";

		return outputJSON;
	}

}
