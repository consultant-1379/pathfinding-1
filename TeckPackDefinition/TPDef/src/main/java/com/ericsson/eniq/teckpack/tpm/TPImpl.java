package com.ericsson.eniq.teckpack.tpm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eniq.teckpack.util.Utils;

public class TPImpl {
	private static final Logger logger = LoggerFactory.getLogger(TPImpl.class);
	public String versionID = null;
	public String tpname = null;
	public String name = null;
	public String versionNumber = null;
	public String versionNo = null;
	public LinkedHashMap<String, Object> versioning = null;
	public List<String> supportedVendorReleases = null;
	public Map<String, Object> measurementTables = null;
	public Map<String, Object> referenceTables = null;
	public Map<String, Object> dependencyTechPacks = null;
	public Map<String, Object> busyHours = null;
	public Map<String, Object> externalStatements = null;
	public Map<String, Object> universes = null;
	public Map<String, Object> manmods = null;
	public Boolean isVector;

	public TPImpl() {
		this.versionID = null;
		this.tpname = null;
		this.name = null;
		this.versionNumber = null;
		this.versionNo = null;
		this.intialiseVersion(versionID);
		this.versioning = new LinkedHashMap<String, Object>();
		this.supportedVendorReleases = new ArrayList<String>();
		this.measurementTables = new LinkedHashMap<String, Object>();
		this.referenceTables = new LinkedHashMap<String, Object>();
		this.dependencyTechPacks = new LinkedHashMap<String, Object>();
		this.busyHours = new LinkedHashMap<String, Object>();
		this.externalStatements = new LinkedHashMap<String, Object>();
		this.universes = new LinkedHashMap<String, Object>();
		this.manmods = new LinkedHashMap<String, Object>();
		this.isVector = false;
	}

	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		Map<String, Object> versioningTemp = (Map<String, Object>) xlsDict.get("Versioning");
		String versionID = (String) versioningTemp.get("TECHPACK_NAME") + ":(("
				+ Utils.strFloatToInt((String) versioningTemp.get("BUILD_NUMBER")) + "))";
		intialiseVersion(versionID);

		for (String key : versioningTemp.keySet()) {
			if (!key.equals("BUILD_NUMBER") && !key.equals("VENDORRELEASE") && !key.equals("Dependency")
					&& !key.equals("ManModsFile")) {
				this.versioning.put(key, Utils.checkNull("" + versioningTemp.get(key)));
			} else if (key.equals("ManModsFile")) {
				this.manmods.put(key, Utils.checkNull("" + versioningTemp.get(key)));
			}
		}

		// Supported Vendor Releases
		supportedVendorReleases = (List<String>) versioningTemp.get("VENDORRELEASE");
		this.supportedVendorReleases = supportedVendorReleases;
		String deps = (String) versioningTemp.get("Dependency");
		if (deps != null) {
			String entries[] = deps.split(",");
			for (String item : entries) {
				String temp[] = item.split(":");
				if (temp[0] != "") {
					this.dependencyTechPacks.put(temp[0], temp[1]);
				}
			}
		}

		List<String> elemBHsupport = null;
		// Tables and BH
		Map<String, Object> tablesTemp = (Map<String, Object>) xlsDict.get("Tables");
		for (Map.Entry<String, Object> entry : tablesTemp.entrySet()) {
			String Name = entry.getKey();
			Map<String, Object> Values = (Map<String, Object>) entry.getValue();
			String TABLETYPE = (String) Values.get("TABLETYPE");
			TableImpl tableImpl = new TableImpl(versionID, Name, TABLETYPE);
			tableImpl.getPropertiesFromXls(xlsDict);
			if (TABLETYPE != null && TABLETYPE.equals("Measurement")) {
				this.measurementTables.put(tableImpl.name, tableImpl);
			} else if (TABLETYPE != null && TABLETYPE.equals("Reference")) {
				this.referenceTables.put(tableImpl.name, tableImpl);
			}

		}

	}

	public void intialiseVersion(String versionID) {

		if (versionID == null) {
			versionID = "UNINITIALISED:((0))";
		}
		String versionNo = "";
		String tpname = versionID.split(":")[0];
		String versionNumber = versionID.split(":")[1];
		try {
			versionNo = versionID.replaceAll("[^0-9]", "");
		} catch (Exception e) {
			String logError = "Could not get versionNo from versionID " + versionID;
			logger.error(logError);
		}

		this.versionNumber = versionNumber;
		this.versionID = versionID;
		this.tpname = tpname;
		this.versionNo = versionNo;
	}

	public Map<String, Object> populateRepDbDicts(TPImpl tpImpl) {
		List<Map<String, String>> tpDependencies = new ArrayList<Map<String, String>>();
		Map<String, Object> dependencyTechPacks = tpImpl.dependencyTechPacks;
		Map<String, String> tpDependency = null;
		Map<String, Object> res = new HashMap<String, Object>();

		for (Entry<String, Object> entry : dependencyTechPacks.entrySet()) {
			tpDependency = new HashMap<String, String>();
			String tpname = entry.getKey();
			String Rstate = (String) entry.getValue();
			tpDependency.put("VERSIONID", tpImpl.versionID);
			tpDependency.put("TECHPACKNAME", tpname);
			tpDependency.put("VERSION", Rstate);
			tpDependencies.add(tpDependency);
		}
		Map<String, Object> versioning = tpImpl.versioning;
		versioning.put("TECHPACK_NAME", tpImpl.tpname);
		versioning.put("VERSIONID", tpImpl.versionID);

		res.put("versioning", versioning);
		res.put("tpDependencies", tpDependencies);
		res.put("supportedVendorReleases", tpImpl.supportedVendorReleases);

		return res;
	}

	public String toJSON() {
		String dq = "\"";
		String s0 = "\t";
		String s1 = "\n" + s0;
		String s2 = s1 + s0;
		String s3 = s2 + s0;
		String s4 = s3 + s0;
		String outputJSON = "{" + dq + "Techpack" + dq + ":{";
		outputJSON += s1 + dq + "name" + dq + ":" + dq + this.versionID + dq + ",";
		outputJSON += s1 + dq + "Versioning" + dq + ":{";
		for (String item : this.versioning.keySet()) {
			outputJSON += s2 + dq + item + dq + ":" + dq + this.versioning.get(item) + dq + ",";
		}
		outputJSON += s2 + dq + "SupportedVendorReleases" + dq + ":[";
		for (String item : this.supportedVendorReleases) {
			outputJSON += s3 + "{" + s4 + dq + "VendorRelease" + dq + ":" + dq + item + dq + s3 + "},";
		}
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		outputJSON += s2 + "],";

		outputJSON += s2 + dq + "Dependencies" + dq + ":[";
		for (String item : this.dependencyTechPacks.keySet()) {
			outputJSON += s3 + "{" + s4 + dq + item + dq + ":" + dq + this.dependencyTechPacks.get(item) + dq + s3
					+ "},";
		}
		outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		outputJSON += s2 + "]";
		outputJSON += s1 + "},";
		if (this.manmods.size() > 0) {
			outputJSON += s1 + dq + "ManModsFile" + dq + ":" + dq + this.manmods.get("ManModsFile") + dq + ",";

		}
		// Tables

		outputJSON += s1 + dq + "Tables" + dq + ":[";
		for (Map.Entry<String, Object> entry : this.measurementTables.entrySet()) {
			TableImpl tableImpl = (TableImpl) entry.getValue();
			outputJSON += tableImpl.toJSON(tableImpl);
		}

		for (Map.Entry<String, Object> entry : this.referenceTables.entrySet()) {
			TableImpl tableImpl = (TableImpl) entry.getValue();
			outputJSON += tableImpl.toJSON(tableImpl);
		}
		if (this.measurementTables.size() > 0 || this.referenceTables.size() > 0) {
			outputJSON = outputJSON.substring(0, outputJSON.length() - 1);
		}
		outputJSON += s1 + "]";
		outputJSON += "\n}";
		return outputJSON += "\n}";

	}
}
