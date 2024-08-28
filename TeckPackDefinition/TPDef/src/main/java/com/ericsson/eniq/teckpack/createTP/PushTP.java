package com.ericsson.eniq.teckpack.createTP;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ericsson.eniq.teckpack.model.Versioning;
import com.ericsson.eniq.teckpack.tpm.AttributeImpl;
import com.ericsson.eniq.teckpack.tpm.TPImpl;
import com.ericsson.eniq.teckpack.tpm.TableImpl;
import com.ericsson.eniq.teckpack.tpm.VectorImpl;
import com.ericsson.eniq.teckpack.util.GenerateJSON;
import com.ericsson.eniq.teckpack.util.Utils;
import com.google.gson.Gson;

@Component
public class PushTP {
	private static final Logger logger = LoggerFactory.getLogger(PushTP.class);
	@Autowired
	private GenerateJSON generateJSON;
	Map<String, Object> dataStore = null;

	public String pushData(TPImpl tpModel) {
		this.dataStore = new LinkedHashMap<>();
		LinkedHashMap<String, Object> formatedData = new LinkedHashMap<>();
		logger.info("Pushing Versioning");
		formatedData = pushVersioningDetails(tpModel, formatedData);
		logger.info("Pushing Fact Tables");
		formatedData = pushFactTables(tpModel, formatedData);
		logger.info("Pushing Reference Tables");
		formatedData = pushReferenceTables(tpModel, formatedData);
		return generateJSON.createJson(formatedData);

	}

	private LinkedHashMap<String, Object> pushReferenceTables(TPImpl tpModel,
			LinkedHashMap<String, Object> formatedData) {
		List<Map<String, Object>> referencetableData = new ArrayList<>();
		for (Map.Entry<String, Object> entry : tpModel.referenceTables.entrySet()) {
			TableImpl table = (TableImpl) entry.getValue();
			Map<String, Object> props = table.populateRepDbDicts(table);
			referencetableData.add(props);
		}
		formatedData.put("Referencetable", referencetableData);
		return formatedData;
	}

	private LinkedHashMap<String, Object> pushFactTables(TPImpl tpModel, LinkedHashMap<String, Object> formatedData) {
		// ####### Fact Tables #######
		List<Map<String, Object>> measurementtypeData = new ArrayList<>();
		List<Map<String, Object>> measurementtypeclassData = new ArrayList<>();
		List<Map<String, Object>> measurementdeltacalcsupportData = new ArrayList<>();

		for (Map.Entry<String, Object> entry : tpModel.measurementTables.entrySet()) {
			TableImpl table = (TableImpl) entry.getValue();
			Map<String, Object> MeasTypeClass = null, deltacalcSupport = null, MeasType = null;
			Map<String, Object> props = table.populateRepDbDicts(table);
			MeasTypeClass = (Map<String, Object>) props.get("MeasTypeClass");
			deltacalcSupport = (Map<String, Object>) props.get("deltacalcSupport");
			MeasType = (Map<String, Object>) props.get("MeasType");

			List<Object> measTypeClasses = (List<Object>) this.dataStore.get("MeasTypeClass");
			String TYPECLASSID = (String) MeasTypeClass.get("TYPECLASSID");
			if (measTypeClasses == null) {
				measTypeClasses = new ArrayList();
			}
			if (!measTypeClasses.contains(TYPECLASSID)) {
				measurementtypeclassData.add((HashMap<String, Object>) MeasTypeClass);
				measTypeClasses.add(TYPECLASSID);
				this.dataStore.put("MeasTypeClass", measTypeClasses);
			}
			measurementtypeData.add((HashMap<String, Object>) MeasType);

			if ((Integer) MeasType.get("DELTACALCSUPPORT") == 0) {
				List<String> SVR = (List<String>) this.dataStore.get("SVR");
				if (deltacalcSupport.containsKey("VENDORRELEASE")) {
					String deltaCalc = (String) deltacalcSupport.get("VENDORRELEASE");
					if (deltaCalc.contains(":")) {
						deltaCalc = deltaCalc.substring(0, deltaCalc.indexOf(":"));
					}
					String supportedReleases = deltaCalc;
					for (String VendorRelease : SVR) {
						deltacalcSupport.put("VENDORRELEASE", VendorRelease);
						if (supportedReleases.contains(VendorRelease)) {
							deltacalcSupport.put("DELTACALCSUPPORT", 1);
						} else {
							deltacalcSupport.put("DELTACALCSUPPORT", 0);
						}
						measurementdeltacalcsupportData.add(deltacalcSupport);
					}
				}
			}
		}

		formatedData.put("Measurementtype", measurementtypeData);
		formatedData.put("Measurementtypeclass", measurementtypeclassData);
		formatedData.put("Measurementdeltacalcsupport", measurementdeltacalcsupportData);

		return formatedData;

	}

	public LinkedHashMap<String, Object> pushVersioningDetails(TPImpl tpImpl,
			LinkedHashMap<String, Object> formatedData) {
		// TP Versioning
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
		String lockdate = dateFormat.format(date);

		Map<String, Object> props = tpImpl.populateRepDbDicts(tpImpl);
		Map<String, Object> versioningdict = (Map<String, Object>) props.get("versioning");
		versioningdict.put("BASEDEFINITION", "");// need to read from db
		versioningdict.put("STATUS", "1");
		versioningdict.put("ENIQ_LEVEL", ""); // need to be analyze
		versioningdict.put("LOCKEDBY", "");// need to be analyze
		versioningdict.put("LOCKDATE", lockdate);
		List<Map<String, Object>> versioningData = new ArrayList();
		versioningData.add(versioningdict);
		formatedData.put("Versioning", versioningData);
		this.dataStore.put("Versioning", versioningData);

		// Tech Pack Dependency
		List<Map<String, Object>> tpDependenciesData = (List<Map<String, Object>>) props.get("tpDependencies");
		formatedData.put("Techpackdependency", tpDependenciesData);

		// Supported Vendor Releases
		List<String> supportedVendorReleases = (List<String>) props.get("supportedVendorReleases");
		Map<String, Object> repVendorRelease = null;
		List<Map<String, Object>> supportedVendorReleasesData = new ArrayList();
		String versionID = tpImpl.versionID;
		if (supportedVendorReleases != null) {
			for (String supportedVendorRelease : supportedVendorReleases) {
				repVendorRelease = new HashMap<String, Object>();
				repVendorRelease.put("VERSIONID", versionID);
				repVendorRelease.put("VENDORRELEASE", supportedVendorRelease);
				supportedVendorReleasesData.add(repVendorRelease);
			}
		}
		formatedData.put("SupportedVendorRelease", supportedVendorReleasesData);
		this.dataStore.put("SVR", supportedVendorReleases);
		return formatedData;

	}

	public String pushDataByApproach2(TPImpl tpModel) {
		this.dataStore = new LinkedHashMap<>();
		LinkedHashMap<String, Object> formatedData = new LinkedHashMap<>();
		logger.info("Pushing Versioning");
		formatedData = pushVersioningDetailsByApproach2(tpModel, formatedData);
		return generateJSON.createJson(formatedData);

	}

	private LinkedHashMap<String, Object> pushVersioningDetailsByApproach2(TPImpl tpModel,
			LinkedHashMap<String, Object> formatedData) {
		// TP Versioning
		Map<String, Object> props = tpModel.populateRepDbDicts(tpModel);
		Map<String, Object> versioningMap = (Map<String, Object>) props.get("versioning");
		Versioning versioning = new Versioning();

		versioning.setVERSIONID(getValue((String) versioningMap.get("VERSIONID")));
		versioning.setDESCRIPTION(getValue((String) versioningMap.get("DESCRIPTION")));
		versioning.setVERSIONID(getValue((String) versioningMap.get("TECHPACK_NAME")));
		versioning.setTECHPACK_VERSION(getValue((String) versioningMap.get("TECHPACK_VERSION")));
		versioning.setTECHPACK_TYPE(getValue((String) versioningMap.get("TECHPACK_TYPE")));
		versioning.setPRODUCT_NUMBER(getValue((String) versioningMap.get("PRODUCT_NUMBER")));
		versioning.setLOCKEDBY(getValue((String) versioningMap.get("LOCKEDBY")));
		versioning.setLOCKDATE(new java.sql.Timestamp(new Date().getTime()));
		versioning.setBASEDEFINITION(getValue((String) versioningMap.get("BASEDEFINITION")));
		versioning.setBASEVERSION(getValue((String) versioningMap.get("BASEVERSION")));
		versioning.setINSTALLDESCRIPTION(getValue((String) versioningMap.get("INSTALLDESCRIPTION")));
		versioning.setUNIVERSENAME(getValue((String) versioningMap.get("UNIVERSENAME")));
		versioning.setUNIVERSEEXTENSION(getValue((String) versioningMap.get("UNIVERSEEXTENSION")));
		versioning.setBASEVERSION(getValue((String) versioningMap.get("ENIQ_LEVEL")));
		versioning.setLICENSENAME(getValue((String) versioningMap.get("LICENSENAME")));

		List<Object> versioningData = new ArrayList<Object>();
		versioningData.add(versioning);
		formatedData.put("Versioning", versioningData);
		return formatedData;

	}

	private String getValue(String value) {
		if (value == null) {
			value = "";
		}
		return value;
	}

	public String generateJson(TPImpl tpImpl) {
		this.dataStore = new LinkedHashMap<>();
		LinkedHashMap<String, Object> formatedData = new LinkedHashMap<>();

		logger.info("Versioning JSON");
		formatedData = generateVersioningJson(tpImpl, formatedData);
		logger.info("Fact Tables JSON");
		formatedData = generateFactTablesJson(tpImpl, formatedData);
		
		  logger.info("Reference Tables JSON"); formatedData =
		  generateReferenceTablesJson(tpImpl, formatedData);
		 
		logger.info("Attributes and Parsers JSON");
		formatedData = generateAttributesAndParsersJson(tpImpl, formatedData);
		return generateJSON.createJson(formatedData);
		// return generateJSON.createJsonNew(formatedData);

	}

	private LinkedHashMap<String, Object> generateAttributesAndParsersJson(TPImpl tpImpl,
			LinkedHashMap<String, Object> formatedData) {
		LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
		String mKeyRow[] = { "DATASIZE", "JOINABLE", "NULLABLE", "DATASCALE", "ISELEMENT", "UNIQUEKEY", "INCLUDESQL",
				"ROPGRPCELL", "COLNUMBER", "UNIQUEVALUE", "DESCRIPTION", "INDEXES", "DATATYPE", "UNIVOBJECT", "DATAID",
				"TYPEID", "DATANAME" };
		String mCountRow[] = { "DATASIZE", "DATASCALE", "FOLLOWJOHN", "INCLUDESQL", "COLNUMBER", "DESCRIPTION",
				"COUNTAGGREGATION", "COUNTERTYPE", "COUNTERPROCESS", "UNIVCLASS", "DATATYPE", "TIMEAGGREGATION",
				"GROUPAGGREGATION", "UNIVOBJECT", "DATAID", "TYPEID", "DATANAME" };
		String mVectorRow[] = { "VINDEX", "QUANTITY", "VTO", "VFROM", "MEASURE", "TYPEID", "DATANAME",
				"VENDORRELEASE" };

		List<Map<String, Object>> measurementcounterData = new ArrayList<>();
		List<Map<String, Object>> measurementkeyData = new ArrayList<>();
		List<Map<String, Object>> measurementvectorData = new ArrayList<>();
	

		for (Map.Entry<String, Object> entry : tpImpl.measurementTables.entrySet()) {
			TableImpl table = (TableImpl) entry.getValue();
			Map<String, Object> props = table.populateRepDbDicts(table);
			Map<String, Object> measTypeClass = (Map<String, Object>) props.get("MeasTypeClass");
			Map<String, Object> deltacalcSupport = (Map<String, Object>) props.get("deltacalcSupport");
			Map<String, Object> measType = (Map<String, Object>) props.get("MeasType");
			// ####### Attributes #######
			List<Map<String, Object>> measCounters = new ArrayList<>();
			List<Map<String, Object>> measKeys = new ArrayList<>();
			int measCounterColNumber = 1;
			int measKeyColNumber = 1;
			for (Map.Entry<String, Object> attrEntry : table.attributes.entrySet()) {
				AttributeImpl attribute = (AttributeImpl) attrEntry.getValue();
				Map<String, Object> attDict = attribute.populateRepDbDicts(attribute);
				attDict.put("TYPEID", table.typeid);
				if (attribute.attributeType.equals("measurementCounter")) {
					attDict.put("COLNUMBER", measCounterColNumber);
					attDict.put("COUNTAGGREGATION",
							CreateTPUtils.evaluteCountAggreation(measType, deltacalcSupport, this.dataStore));
					// HashMap<String, Object> counterRes = Utils.convertAndPush(attDict,mCountRow);
					// measurementcounterData.add(counterRes);

					if (Utils.isValidJson(attDict)) {
						 HashMap<String, Object> counterRes = Utils.convertAndPush(attDict,mCountRow);
						measurementcounterData.add(counterRes);
					}
					measCounterColNumber = measCounterColNumber + 1;

					for (Map.Entry<String, Object> quant : attribute.vectors.entrySet()) {
						Map<String, Object> values = (Map<String, Object>) quant.getValue();
						for (Map.Entry<String, Object> indices : values.entrySet()) {
							Map<String, Object> vectors = (Map<String, Object>) indices.getValue();
							for (Map.Entry<String, Object> vectorEntry : vectors.entrySet()) {
								VectorImpl vector = (VectorImpl) vectorEntry.getValue();
								for (String venRel : vector.VendorRelease.split(",")) {
									Map<String, Object> vectorRow = vector.populateRepDbDicts(vector);
									vectorRow.put("VENDORRELEASE", venRel);
									HashMap<String, Object> vestorRes = Utils.convertAndPush(attDict, mVectorRow);
									measurementvectorData.add(vestorRes);
								}
							}
						}
					}

				} else if (attribute.attributeType.equals("measurementKey")) {
					attDict.put("COLNUMBER", measKeyColNumber);
					// HashMap<String, Object> measKeyRes = Utils.convertAndPush(attDict, mKeyRow);
					// measurementkeyData.add(measKeyRes);
					if (Utils.isValidJson(attDict)) {
						HashMap<String, Object> measKeyRes = Utils.convertAndPush(attDict, mKeyRow);
						measurementkeyData.add(measKeyRes);
					}
					measKeyColNumber = measKeyColNumber + 1;
				}
			}

			// ####### Parsers #######
			// pushParsersJSON(table);
		}
		this.dataStore.put("counters", measurementcounterData);
		this.dataStore.put("keys", measurementkeyData);

		attributes.put("MeasurementCounter", measurementcounterData);
		attributes.put("Measurementkey", measurementkeyData);
		attributes.put("MeasurementVector", measurementvectorData);
		formatedData.put("Attributes", attributes);

		System.out.println("MeasurementCounter" + measurementcounterData.size());
		System.out.println("Measurementkey" + measurementkeyData.size());
		System.out.println("MeasurementVector" + measurementvectorData.size());

		return formatedData;
	}

	private void pushParsersJSON(TableImpl table) {

	}

	private LinkedHashMap<String, Object> generateReferenceTablesJson(TPImpl tpImpl,
			LinkedHashMap<String, Object> formatedData) {
		LinkedHashMap<String, Object> reference = new LinkedHashMap<>();
		List<Map<String, Object>> referencetableData = new ArrayList<>();
		for (Map.Entry<String, Object> entry : tpImpl.referenceTables.entrySet()) {
			TableImpl table = (TableImpl) entry.getValue();
			Map<String, Object> props = table.populateRepDbDicts(table);
			referencetableData.add(props);
		}
		reference.put("VERSIONID", tpImpl.versionID);
		reference.put("Referencetable", referencetableData);
		formatedData.put("Reference", reference);
		return formatedData;
	
	}

	private LinkedHashMap<String, Object> generateFactTablesJson(TPImpl tpImpl,
			LinkedHashMap<String, Object> formatedData) {
		LinkedHashMap<String, Object> measurement = new LinkedHashMap<>();
		List<Map<String, Object>> measurementtypeData = new ArrayList<>();
		List<Map<String, Object>> measurementtypeclassData = new ArrayList<>();
		List<Map<String, Object>> measurementdeltacalcsupportData = new ArrayList<>();
		/*
		 * if (formatedData.get("Measurement") != null) { measurement =
		 * (LinkedHashMap<String, Object>) formatedData.get("Measurement"); }
		 */
		for (Map.Entry<String, Object> entry : tpImpl.measurementTables.entrySet()) {
			TableImpl table = (TableImpl) entry.getValue();
			Map<String, Object> props = table.populateRepDbDicts(table);
			Map<String, Object> measTypeClass = (Map<String, Object>) props.get("MeasTypeClass");
			Map<String, Object> deltacalcSupport = (Map<String, Object>) props.get("deltacalcSupport");
			Map<String, Object> measType = (Map<String, Object>) props.get("MeasType");

			List<Object> measTypeClasses = (List<Object>) this.dataStore.get("MeasTypeClass");
			String typeClassId = (String) measTypeClass.get("TYPECLASSID");
			if (measTypeClasses == null) {
				measTypeClasses = new ArrayList();
			}
			if (!measTypeClasses.contains(typeClassId)) {
				measurementtypeclassData.add(measTypeClass);
				measTypeClasses.add(typeClassId);
				this.dataStore.put("MeasTypeClass", measTypeClasses);
			}
			measurementtypeData.add(measType);

			Map<String, Object> versioning = (Map<String, Object>) this.dataStore.get("Versioning");
			if ((Integer) measType.get("DELTACALCSUPPORT") == 0) {
				List<String> sVR = (List<String>) versioning.get("SupportedVendorRelease");
				if (deltacalcSupport.containsKey("VENDORRELEASE")) {
					String deltaCalc = (String) deltacalcSupport.get("VENDORRELEASE");
					if (deltaCalc.contains(":")) {
						deltaCalc = deltaCalc.substring(0, deltaCalc.indexOf(":"));
					}
					String supportedReleases = deltaCalc;
					for (String vendorRelease : sVR) {
						deltacalcSupport.put("VENDORRELEASE", vendorRelease);
						if (supportedReleases.contains(vendorRelease)) {
							deltacalcSupport.put("DELTACALCSUPPORT", 1);
						} else {
							deltacalcSupport.put("DELTACALCSUPPORT", 0);
						}
						measurementdeltacalcsupportData.add(deltacalcSupport);
					}
				}
			}
		}

		measurement.put("VERSIONID", tpImpl.versionID);
		measurement.put("Measurementtype", measurementtypeData);
		measurement.put("Measurementtypeclass", measurementtypeclassData);
		measurement.put("Measurementdeltacalcsupport", measurementdeltacalcsupportData);
		formatedData.put("Measurement", measurement);
		return formatedData;

	}

	private LinkedHashMap<String, Object> generateVersioningJson(TPImpl tpImpl,
			LinkedHashMap<String, Object> formatedData) {
		// TP Versioning
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
		String lockdate = dateFormat.format(date);

		LinkedHashMap<String, Object> versioningData = tpImpl.versioning;
		versioningData.put("VERSIONID", tpImpl.versionID);
		versioningData.put("TECHPACK_NAME", tpImpl.tpname);
		versioningData.put("BASEDEFINITION", "");// need to read from db
		versioningData.put("STATUS", "1");
		versioningData.put("ENIQ_LEVEL", ""); // need to be analyze
		versioningData.put("LOCKEDBY", "");// need to be analyze
		versioningData.put("LOCKDATE", lockdate);

		HashMap<String, String> tpDependency = null;
		List<Map<String, String>> tpDependenciesData = new ArrayList<>();
		for (Entry<String, Object> entry : tpImpl.dependencyTechPacks.entrySet()) {
			tpDependency = new HashMap<String, String>();
			String tpName = entry.getKey();
			String rState = (String) entry.getValue();
			tpDependency.put("TECHPACKNAME", tpName);
			tpDependency.put("VERSION", rState);
			tpDependenciesData.add(tpDependency);
		}

		// Tech Pack Dependency
		versioningData.put("Techpackdependency", tpDependenciesData);

		// Supported Vendor Releases
		List<String> supportedVendorReleases = tpImpl.supportedVendorReleases;
		Map<String, String> repVendorRelease = null;
		List<Map<String, String>> supportedVendorReleasesData = new ArrayList<Map<String, String>>();
		if (supportedVendorReleases != null) {
			for (String supportedVendorRelease : supportedVendorReleases) {
				repVendorRelease = new HashMap<String, String>();
				repVendorRelease.put("VENDORRELEASE", supportedVendorRelease);
				supportedVendorReleasesData.add(repVendorRelease);
			}
		}
		versioningData.put("SupportedVendorRelease", supportedVendorReleasesData);
		this.dataStore.put("Versioning", versioningData);
		formatedData.put("Versioning", versioningData);
		return formatedData;
	}

}
