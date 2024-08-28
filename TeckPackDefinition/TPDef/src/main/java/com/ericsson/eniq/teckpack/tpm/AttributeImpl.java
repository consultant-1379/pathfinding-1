package com.ericsson.eniq.teckpack.tpm;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import com.ericsson.eniq.teckpack.util.Utils;

public class AttributeImpl {
	public String typeid = null;
	public String attributeType = null;
	public String name = null;
	public String parentTableName = null;
	public Map<String, Object> properties = null;
	public Map<String, Object> vectors = null;

	public AttributeImpl(String name, String typeid, String attributeType) {
		this.typeid = typeid;
		this.attributeType = attributeType;
		this.name = name;
		this.properties = new HashMap<String, Object>();
		this.vectors = new HashMap<String, Object>();
		this.parentTableName = "";
	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		for (Map.Entry<String, Object> entry : xlsDict.entrySet()) {
			String Name = entry.getKey();
			if (Name.equals("vectors")) {
				Map<String, Object> Value = (Map<String, Object>) entry.getValue();
				for (Map.Entry<String, Object> val1 : Value.entrySet()) {
					String qtyLabel = val1.getKey();
					Map<String, Object> qtyVal = (Map<String, Object>) val1.getValue();
					for (Map.Entry<String, Object> val2 : qtyVal.entrySet()) {
						String qty = val2.getKey();
						Map<String, Object> Val3 = (Map<String, Object>) val2.getValue();
						for (Map.Entry<String, Object> valEntry3 : Val3.entrySet()) {
							String vendRel = valEntry3.getKey();
							Map<String, Object> indices = (Map<String, Object>) valEntry3.getValue();

							if (!this.vectors.containsKey(qty)) {
								Map<String, Object> list = new HashMap<>();
								this.vectors.put(qty, list);
							}
							Map<String, Object> qtyTemp = (Map<String, Object>) this.vectors.get(qty);
							if (!this.vectors.containsKey(vendRel)) {
								Map<String, Object> list = new HashMap<>();
								qtyTemp.put(vendRel, list);
								this.vectors.put(qty, list);
							}

							for (Map.Entry<String, Object> indicesEntry : indices.entrySet()) {
								String index = indicesEntry.getKey();
								Map<String, Object> properties = (Map<String, Object>) indicesEntry.getValue();
								VectorImpl vector = new VectorImpl(this.typeid, this.name, index, vendRel, qty);
								vector.getPropertiesFromXls(properties);
								qtyTemp = (Map<String, Object>) this.vectors.get(qty);
								Map<String, Object> vendRelTemp = (Map<String, Object>) qtyTemp.get(vendRel);
								vendRelTemp.put(index, vector);
								qtyTemp.put(vendRel, vendRelTemp);
								this.vectors.put(qty, qtyTemp);
							}
						}
					}
				}
			}

			else {
				this.properties.put(Name, entry.getValue());
			}
		}

		completeModel();
	}

	private void completeModel() {
		if (this.attributeType.equals("measurementCounter")) {
			if (!this.properties.containsKey("INCLUDESQL") || this.properties.get("INCLUDESQL").equals("")) {
				this.properties.put("INCLUDESQL", "0");
			}
		} else if (this.attributeType.equals("measurementKey")) {
			if (!this.properties.containsKey("ISELEMENT") || this.properties.get("ISELEMENT").equals("")) {
				this.properties.put("ISELEMENT", "0");
			}
			if (!this.properties.containsKey("UNIQUEKEY") || this.properties.get("UNIQUEKEY").equals("")) {
				this.properties.put("UNIQUEKEY", "0");
			}
			if (!this.properties.containsKey("INCLUDESQL") || this.properties.get("INCLUDESQL").equals("")) {
				this.properties.put("INCLUDESQL", "0");
			}
			if (!this.properties.containsKey("NULLABLE") || this.properties.get("NULLABLE").equals("")) {
				this.properties.put("NULLABLE", "0");
			}
			if (!this.properties.containsKey("JOINABLE") || this.properties.get("JOINABLE").equals("")) {
				this.properties.put("JOINABLE", "0");
			}
		} else if (this.attributeType.equals("referenceKey"))

		{
			if (!this.properties.containsKey("UNIQUEKEY") || this.properties.get("UNIQUEKEY").equals("")) {
				this.properties.put("UNIQUEKEY", "0");
			}
			if (!this.properties.containsKey("NULLABLE") || this.properties.get("NULLABLE").equals("")) {
				this.properties.put("NULLABLE", "0");
			}
		}

		if (!this.properties.containsKey("DATAID") || this.properties.get("DATAID").equals("")) {
			this.properties.put("DATAID", "");
		}

	}

	public Map<String, Object> populateRepDbDicts(AttributeImpl attributeImpl) {
		Map<String, Object> repDbDict = attributeImpl.properties;
		repDbDict.put("DATANAME", attributeImpl.name);

		if (attributeImpl.attributeType.equals("measurementCounter")) {
			repDbDict.put("COUNTERPROCESS", attributeImpl.properties.get("COUNTERTYPE"));
		}

		else if (attributeImpl.attributeType.equals("measurementKey")) {
			repDbDict.put("UNIQUEVALUE", 255);
			repDbDict.put("ROPGRPCELL", 0);
		} else if (attributeImpl.attributeType.equals("referenceKey")) {
			repDbDict.put("UNIQUEVALUE", 255);
			repDbDict.put("INDEXES", "HG");
			repDbDict.put("COLTYPE", "COLUMN");
			repDbDict.put("BASEDEF", 0);
		}
		return repDbDict;
	}

	public String toJSON(AttributeImpl attributeImpl) {
		String offset = "\t";
		String s1 = "\n" + offset;
		String s2 = s1 + offset;
		String s3 = s2 + offset;
		String dq = "\"";
		String outputJSON = s1 + "{";
		outputJSON += s2 + dq + "name" + dq + ":" + this.name + dq + ",";
		outputJSON += s2 + dq + "attributeType" + dq + ":" + dq + this.attributeType + dq + ",";
		outputJSON += s2 + dq + "Properties" + dq + ":{";
		for (Map.Entry<String, Object> attr : attributeImpl.properties.entrySet()) {
			outputJSON += s3 + dq + attr.getKey() + dq + ":" + dq + Utils.escape("" + attr.getValue()) + dq + ",";
		}
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		outputJSON += s2 + "},";

		outputJSON += s2 + dq + "Vectors" + dq + ":[";
		for (Map.Entry<String, Object> vector : attributeImpl.vectors.entrySet()) {
			for (Map.Entry<String, Object> entry : ((Map<String, Object>) vector.getValue()).entrySet()) {
				VectorImpl vectorImpl = (VectorImpl) entry.getValue();
				outputJSON += vectorImpl.toJSON(vectorImpl);
			}
		}if(attributeImpl.vectors.size()>0)
		{
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		}
		outputJSON += s2 + "],";

		outputJSON += s1 + "},";
		return outputJSON;
	}
}