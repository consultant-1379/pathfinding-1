package com.ericsson.eniq.teckpack.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class XlsDict {
	private static final Logger logger = LoggerFactory.getLogger(XlsDict.class);
	String updatePolicylist[] = { "Static", "Predefined", "Dynamic", "Timed Dynamic", "History Dynamic" };
	String fileName = "";

	public Map<String, Object> parse(File file) {
		Map<String, String> versioningDict = new HashMap<String, String>();
		versioningDict.put("Name", "TECHPACK_NAME");
		versioningDict.put("Description", "DESCRIPTION");
		versioningDict.put("Release", "TECHPACK_VERSION");
		versioningDict.put("Product number", "PRODUCT_NUMBER");
		versioningDict.put("License", "LICENSENAME");
		versioningDict.put("Type", "TECHPACK_TYPE");
		versioningDict.put("Supported Versions", "VENDORRELEASE");
		versioningDict.put("Build Number", "BUILD_NUMBER");
		versioningDict.put("Dependency TechPack", "Dependency");
		versioningDict.put("ManModsFile", "ManModsFile");
		versioningDict.put("Universe Delivered", "UNIVERSE_DELIVERED");
		versioningDict.put("Supported Node Types", "SUPPORTED_NODE_TYPES");

		Map<String, String> interfaceDict = new HashMap<String, String>();
		interfaceDict.put("Interface R-State", "RSTATE");
		interfaceDict.put("Interface Type", "INTERFACETYPE");
		interfaceDict.put("Description", "DESCRIPTION");
		interfaceDict.put("Tech Pack", "intfTechpacks");
		interfaceDict.put("Dependencies", "dependencies");
		interfaceDict.put("Parser Name", "DATAFORMATTYPE");
		interfaceDict.put("Element Type", "ELEMTYPE");

		Map<String, String> factTableDict = new HashMap<String, String>();
		factTableDict.put("Fact Table Description", "DESCRIPTION");
		factTableDict.put("Universe Class", "CLASSIFICATION");
		factTableDict.put("Table Sizing", "SIZING");
		factTableDict.put("Total aggregation", "TOTALAGG");
		factTableDict.put("Object BHs", "OBJECTBH");
		factTableDict.put("Element BHs", "ELEMENTBHSUPPORT");
		factTableDict.put("Rank Table", "RANKINGTABLE");
		factTableDict.put("Count Table", "DELTACALCSUPPORT");
		factTableDict.put("Vector Table", "VECTORSUPPORT");
		factTableDict.put("Plain Table", "PLAINTABLE");
		factTableDict.put("Universe Extension", "UNIVERSEEXTENSION");
		factTableDict.put("Joinable", "JOINABLE");
		factTableDict.put("FOLLOWJOHN", "FOLLOWJOHN");

		Map<String, String> ftKeysDict = new HashMap<String, String>();
		ftKeysDict.put("Key Description", "DESCRIPTION");
		ftKeysDict.put("Data type", "DATATYPE");
		ftKeysDict.put("Duplicate Constraint", "UNIQUEKEY");
		ftKeysDict.put("Nullable", "NULLABLE");
		ftKeysDict.put("IQ Index", "INDEXES");
		ftKeysDict.put("Universe object", "UNIVOBJECT");
		ftKeysDict.put("Element Column", "ISELEMENT");
		ftKeysDict.put("IncludeSQL", "INCLUDESQL");
		Map<String, String> ftCountersDict = new HashMap<String, String>();
		ftCountersDict.put("Counter Description", "DESCRIPTION");
		ftCountersDict.put("Data type", "DATATYPE");
		ftCountersDict.put("Time Aggregation", "TIMEAGGREGATION");
		ftCountersDict.put("Group Aggregation", "GROUPAGGREGATION");
		ftCountersDict.put("Universe Object", "UNIVOBJECT");
		ftCountersDict.put("Universe Class", "UNIVCLASS");
		ftCountersDict.put("Counter Type", "COUNTERTYPE");
		ftCountersDict.put("IncludeSQL", "INCLUDESQL");
		ftCountersDict.put("FOLLOWJOHN", "FOLLOWJOHN");
		// Removed "QUANTITY" as part of fix EQEV-57773
		Map<String, String> vectorsDict = new HashMap<String, String>();
		vectorsDict.put("From", "VFROM");
		vectorsDict.put("To", "VTO");
		vectorsDict.put("Vector Description", "MEASURE");
		Map<String, String> topTableDict = new HashMap<String, String>();
		topTableDict.put("Topology Table Description", "DESCRIPTION");
		topTableDict.put("Source Type", "UPDATE_POLICY");

		Map<String, String> topKeysDict = new HashMap<String, String>();
		topKeysDict.put("Key Description", "DESCRIPTION");
		topKeysDict.put("Data type", "DATATYPE");
		topKeysDict.put("Duplicate Constraint", "UNIQUEKEY");
		topKeysDict.put("Nullable", "NULLABLE");
		topKeysDict.put("Universe Class", "UNIVERSECLASS");
		topKeysDict.put("Universe Object", "UNIVERSEOBJECT");
		topKeysDict.put("Universe Condition", "UNIVERSECONDITION");
		topKeysDict.put("IncludeSQL", "INCLUDESQL");
		topKeysDict.put("Include Update", "INCLUDEUPD");

		Map<String, String> transDict = new HashMap<String, String>();
		transDict.put("Transformation Type", "TYPE");
		transDict.put("Transformation Source", "SOURCE");
		transDict.put("Transformation Target", "TARGET");
		transDict.put("Transformation Config", "CONFIG");

		Map<String, String> bhDict = new HashMap<String, String>();
		bhDict.put("Description", "DESCRIPTION");
		bhDict.put("Where Clause", "WHERECLAUSE");
		bhDict.put("Criteria", "BHCRITERIA");
		bhDict.put("Aggregation Type", "AGGREGATIONTYPE");
		bhDict.put("Loopback", "LOOKBACK");
		bhDict.put("P Threshold", "P_THRESHOLD");
		bhDict.put("N Threshold", "N_THRESHOLD");
		bhDict.put("SupportedTables", "SupportedTables");

		Map<String, String> esDict = new HashMap<String, String>();
		esDict.put("Database Name", "DBCONNECTION");
		esDict.put("Definition", "STATEMENT");

		Map<String, String> roDict = new HashMap<String, String>();
		roDict.put("Fact Table", "MEASTYPE");
		roDict.put("Level", "MEASLEVEL");
		roDict.put("Object Class", "OBJECTCLASS");
		roDict.put("Object Name", "OBJECTNAME");

		Map<String, String> rcDict = new HashMap<String, String>();
		rcDict.put("Fact Table", "FACTTABLE");
		rcDict.put("Level", "VERLEVEL");
		rcDict.put("Condition Class", "CONDITIONCLASS");
		rcDict.put("Condition", "VERCONDITION");
		rcDict.put("Prompt Name (1)", "PROMPTNAME1");
		rcDict.put("Prompt Value (1)", "PROMPTVALUE1");
		rcDict.put("Prompt Name (2)", "PROMPTNAME2");
		rcDict.put("Prompt Value (2)", "PROMPTVALUE2");
		rcDict.put("Object Condition", "OBJECTCONDITION");
		rcDict.put("Prompt Name (3)", "PROMPTNAME3");
		rcDict.put("Prompt Value (3)", "PROMPTVALUE3");

		Map<String, String> uniExtDict = new HashMap<String, String>();
		uniExtDict.put("Universe Ext Name", "UNIVERSEEXTENSIONNAME");
		Map<String, String> uniTableDict = new HashMap<String, String>();
		uniTableDict.put("Topology Table Owner", "OWNER");
		uniTableDict.put("Table Alias", "ALIAS");
		uniTableDict.put("Universe Extension", "UNIVERSEEXTENSION");
		Map<String, String> uniClassDict = new HashMap<String, String>();
		uniClassDict.put("Class Description", "DESCRIPTION");
		uniClassDict.put("Parent Class Name", "PARENT");
		uniClassDict.put("Universe Extension", "UNIVERSEEXTENSION");
		Map<String, String> uniObjDict = new HashMap<String, String>();
		uniObjDict.put("Unv. Class", "UniverseClass");
		uniObjDict.put("Unv. Description", "DESCRIPTION");
		uniObjDict.put("Unv. Type", "OBJECTTYPE");
		uniObjDict.put("Unv. Qualification", "QUALIFICATION");
		uniObjDict.put("Unv. Aggregation", "AGGREGATION");
		uniObjDict.put("Select statement", "OBJSELECT");
		uniObjDict.put("Where Clause", "OBJWHERE");
		uniObjDict.put("Prompt Hierarchy", "PROMPTHIERARCHY");
		uniObjDict.put("Universe Extension", "UNIVERSEEXTENSION");
		Map<String, String> uniConDict = new HashMap<String, String>();
		uniConDict.put("Condition Description", "DESCRIPTION");
		uniConDict.put("Where Clause", "CONDWHERE");
		uniConDict.put("Auto generate", "AUTOGENERATE");
		uniConDict.put("Condition object class", "CONDOBJCLASS");
		uniConDict.put("Condition object", "CONDOBJECT");
		uniConDict.put("Prompt Text", "PROMPTTEXT");
		uniConDict.put("Multi selection", "MULTISELECTION");
		uniConDict.put("Free text", "FREETEXT");
		uniConDict.put("Universe Extension", "UNIVERSEEXTENSION");
		Map<String, String> uniJoinsDict = new HashMap<String, String>();
		uniJoinsDict.put("Source Table", "SOURCETABLE");
		uniJoinsDict.put("Source Level", "SOURCELEVEL");
		uniJoinsDict.put("Source Columns", "SOURCECOLUMN");
		uniJoinsDict.put("Target Table", "TARGETTABLE");
		uniJoinsDict.put("Target Level", "TARGETLEVEL");
		uniJoinsDict.put("Target Columns", "TARGETCOLUMN");
		uniJoinsDict.put("Join Cardinality", "CARDINALITY");
		uniJoinsDict.put("Contexts", "CONTEXT");
		uniJoinsDict.put("Excluded contexts", "EXCLUDEDCONTEXTS");
		uniJoinsDict.put("Universe Extension", "UNIVERSEEXTENSION");

		Map<String, String> verObjDict = new HashMap<String, String>();
		verObjDict.put("Fact Table", "MEASTYPE");
		verObjDict.put("Level", "MEASLEVEL");
		verObjDict.put("Object Class", "OBJECTCLASS");
		verObjDict.put("Object Name", "OBJECTNAME");

		Map<String, String> verConDict = new HashMap<String, String>();
		verConDict.put("Fact Table", "FACTTABLE");
		verConDict.put("Level", "VERLEVEL");
		verConDict.put("Condition Class", "CONDITIONCLASS");
		verConDict.put("Condition", "VERCONDITION");
		verConDict.put("Prompt Name (1)", "PROMPTNAME1");
		verConDict.put("Prompt Value (1)", "PROMPTVALUE1");
		verConDict.put("Prompt Name (2)", "PROMPTNAME2");
		verConDict.put("Prompt Value (2)", "PROMPTVALUE2");
		verConDict.put("Object Condition", "OBJECTCONDITION");
		verConDict.put("Prompt Name (3)", "PROMPTNAME3");
		verConDict.put("Prompt Value (3)", "PROMPTVALUE3");

		XSSFWorkbook workbook = null;
		XSSFSheet sheet = null;
		Map<String, Object> xlsxdict = null;
		fileName = file.getName();
		try {
			xlsxdict = new HashMap<String, Object>();
			workbook = new XSSFWorkbook(file);
			// CoverSheet and Versioning information
			logger.info("Coversheet");
			sheet = workbook.getSheet("Coversheet");
			xlsxdict = versioning(sheet, versioningDict, xlsxdict);
			// Fact Tables
			logger.info("Fact Tables");
			sheet = workbook.getSheet("Fact Tables");
			xlsxdict = factTables(sheet, factTableDict, xlsxdict);
			// Fact Table Keys
			logger.info("Fact Tables Keys");
			sheet = workbook.getSheet("Keys");
			xlsxdict = factTablesKeys(sheet, ftKeysDict, xlsxdict);
			// Fact table Counters
			logger.info("Fact Tables Counters");
			sheet = workbook.getSheet("Counters");
			xlsxdict = factTablesCounters(sheet, ftCountersDict, xlsxdict);
			// Vectors
			logger.info("Vectors");
			sheet = workbook.getSheet("Vectors");
			xlsxdict = vectors(sheet, vectorsDict, xlsxdict);
			// Topology Table
			logger.info("Topology Table");
			sheet = workbook.getSheet("Topology Tables");
			xlsxdict = topologyTable(sheet, topTableDict, xlsxdict);
			// Topology Keys
			logger.info("Topology Keys");
			sheet = workbook.getSheet("Topology Keys");
			xlsxdict = topologyKeys(sheet, topKeysDict, xlsxdict);
			// Transformations
			logger.info("Transformations");
			sheet = workbook.getSheet("Transformations");
			xlsxdict = transformations(sheet, transDict, xlsxdict);
			// DataFormat
			logger.info("DataFormat");
			sheet = workbook.getSheet("Data Format");
			xlsxdict = dataFormat(sheet, null, xlsxdict);
			// BH
			logger.info("BH");
			sheet = workbook.getSheet("BH");
			xlsxdict = busyHour(sheet, bhDict, xlsxdict);
			// BH Rank Keys
			logger.info("BH Rank Keys");
			sheet = workbook.getSheet("BH Rank Keys");
			xlsxdict = bhRankKeys(sheet, bhDict, xlsxdict);
			// External Statments
			logger.info("External Statements");
			sheet = workbook.getSheet("External Statement");
			xlsxdict = externalStatement(sheet, esDict, xlsxdict);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return xlsxdict;
	}

	private Map<String, Object> externalStatement(XSSFSheet sheet, Map<String, String> esDict,
			Map<String, Object> xlsxdict) throws Exception {
		String fileName = "";// need to be find the path
		String estxtFileName = "" + ".txt";// path.splitext(filename)[0] + ".txt";
		Map<String, Object> esCollection = Utils.loadEStxtFile(estxtFileName);
		Map<String, Object> externalStatementsTemp = new HashMap<String, Object>();
		Map<String, Object> esNameTemp = null;
		xlsxdict.put("ExternalStatements", externalStatementsTemp);

		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			if (sheet.getRow(rowNum).getCell(0) != null) {
				externalStatementsTemp = (Map<String, Object>) xlsxdict.get("ExternalStatements");
				String esName = sheet.getRow(rowNum).getCell(0).toString();
				if (esName != "") {
					esNameTemp = new HashMap<>();
					esNameTemp.put("EXECUTIONORDER", rowNum);
					externalStatementsTemp.put(esName, esNameTemp);
					for (Map.Entry<String, String> entry : esDict.entrySet()) {
						String FDColumn = entry.getKey();
						String Parameter = entry.getValue();
						int cellNum = findValue(sheet, FDColumn).getColumnIndex();
						String value = "";
						try {
							value = sheet.getRow(rowNum).getCell(cellNum).toString();
						} catch (Exception e) {
							value = "";
						}

						if (Parameter.equals("STATEMENT")) {
							if (estxtFileName.contains(value)) {
								if (esCollection != null) {
									value = esCollection.get(esName).toString();
								} else {
									throw new Exception(fileName + " not found");
								}
							}

						}

						esNameTemp = (Map<String, Object>) externalStatementsTemp.get(esName);
						esNameTemp.put(Parameter, encodeValue(value));
						externalStatementsTemp.put(esName, esNameTemp);
						xlsxdict.put("ExternalStatements", externalStatementsTemp);
					}
				}
			}
		}

		return xlsxdict;
	}

	private Map<String, Object> bhRankKeys(XSSFSheet sheet, Map<String, String> bhDict, Map<String, Object> xlsxdict) {
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String phName = sheet.getRow(rowNum).getCell(1).toString();
			String bhName = sheet.getRow(rowNum).getCell(0).toString();
			String keyName = "";
			String keyValue = "";
			if (bhName != "") {
				Map<String, Object> BHOBJECTTemp = (Map<String, Object>) xlsxdict.get("BHOBJECT");
				Map<String, Object> BHNameTemp = (Map<String, Object>) BHOBJECTTemp.get(bhName);
				Map<String, Object> PHNameTemp = (Map<String, Object>) BHNameTemp.get(phName);
				Map<String, Object> RANKINGKEYSTemp = null;
				List<Object> TYPENAMETemp = null;
				String sourceTable = null;
				if (!PHNameTemp.containsKey("RANKINGKEYS")) {
					RANKINGKEYSTemp = new HashMap<>();
					PHNameTemp.put("RANKINGKEYS", RANKINGKEYSTemp);
					BHNameTemp.put(phName, PHNameTemp);
				}
				if (!PHNameTemp.containsKey("TYPENAME")) {
					TYPENAMETemp = new ArrayList<Object>();
					PHNameTemp.put("TYPENAME", TYPENAMETemp);
					BHNameTemp.put(phName, PHNameTemp);
				}

				try {
					keyName = sheet.getRow(rowNum).getCell(2).toString();
					keyValue = sheet.getRow(rowNum).getCell(3).toString().strip();
				} catch (Exception e) {
					keyName = "";
					keyValue = "";
				}

				try {
					sourceTable = sheet.getRow(rowNum).getCell(4).toString().strip();
				} catch (Exception e) {
				}

				PHNameTemp = (Map<String, Object>) BHNameTemp.get(phName);
				RANKINGKEYSTemp = (Map<String, Object>) PHNameTemp.get("RANKINGKEYS");
				RANKINGKEYSTemp.put(keyName, keyValue);
				PHNameTemp.put("RANKINGKEYS", RANKINGKEYSTemp);
				if (sourceTable != null && sourceTable != "") {
					List<Object> tables = (List<Object>) PHNameTemp.get("TYPENAME");
					if (sourceTable.contains(",")) {
						for (String table : sourceTable.split(",")) {
							if (!tables.contains(table)) {
								tables.add(table);
							}
						}
					} else {
						if (!tables.contains(sourceTable)) {
							tables.add(sourceTable);
						}
					}
					PHNameTemp.put("TYPENAME", tables);
				}
				BHNameTemp.put("PHName", PHNameTemp);
				BHOBJECTTemp.put("BHName", BHNameTemp);
				xlsxdict.put("BHOBJECT", BHOBJECTTemp);
			}
		}

		return xlsxdict;
	}

	private Map<String, Object> busyHour(XSSFSheet sheet, Map<String, String> bhDict, Map<String, Object> xlsxdict) {
		Map<String, Object> BHOBJECTTemp = new HashMap<String, Object>();
		xlsxdict.put("BHOBJECT", BHOBJECTTemp);
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			if (sheet.getRow(rowNum) != null) {
				BHOBJECTTemp = (Map<String, Object>) xlsxdict.get("BHOBJECT");
				String PHName = sheet.getRow(rowNum).getCell(1).toString();
				String BHName = sheet.getRow(rowNum).getCell(0).toString();
				Map<String, Object> BHNameTemp = null;
				Map<String, Object> PHNameTemp = null;
				if (BHName != "") {
					if (!BHOBJECTTemp.containsKey(BHName)) {
						BHNameTemp = new HashMap<String, Object>();
						BHOBJECTTemp.put(BHName, BHNameTemp);
					}
					BHNameTemp = (Map<String, Object>) BHOBJECTTemp.get(BHName);
					PHNameTemp = new HashMap<String, Object>();
					BHNameTemp.put(PHName, PHNameTemp);
					Map<String, Object> tempDict = parseFDRows(bhDict, sheet, BHNameTemp, PHName, false, rowNum);
					BHOBJECTTemp.put(BHName, tempDict);
					xlsxdict.put("BHOBJECT", BHOBJECTTemp);
				}
			}
		}

		return xlsxdict;

	}

	private Map<String, Object> dataFormat(XSSFSheet sheet, Object object, Map<String, Object> xlsxdict) {
		String attrList[] = { "measurementCounter", "measurementKey", "referenceKey" };
		Iterator<Cell> cellIterator = sheet.getRow(0).cellIterator();
		Cell cell = null;
		while (cellIterator.hasNext()) {
			cell = cellIterator.next();
			String parserName = cell.toString();
			int cellNum = cell.getColumnIndex();

			if (!parserName.equals("Table Name") && !parserName.equals("Counter/key Name")) {
				Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
				for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
					String format = getCellValue(sheet, rowNum, cellNum);
					String tableName = getCellValue(sheet, rowNum, 0).strip();
					String attrName = getCellValue(sheet, rowNum, 1).strip();
					if (tableName != "" && attrName != "") {
						Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
						Map<Object, Object> parserTemp = null;
						Map<Object, Object> parserNameTemp = null;
						Map<Object, Object> ATTRTAGSTemp = null;
						if (!tableNameTemp.containsKey("Parser")) {
							parserTemp = new HashMap<Object, Object>();
							tableNameTemp.put("Parser", parserTemp);
						}
						parserTemp = (Map<Object, Object>) tableNameTemp.get("Parser");
						if (!parserTemp.containsKey(parserName)) {
							parserNameTemp = new HashMap<Object, Object>();
							parserTemp.put(parserName, parserNameTemp);
							tableNameTemp.put("Parser", parserTemp);
						}
						parserTemp = (Map<Object, Object>) tableNameTemp.get("Parser");
						parserNameTemp = (Map<Object, Object>) parserTemp.get(parserName);

						if (!parserNameTemp.containsKey("ATTRTAGS")) {
							ATTRTAGSTemp = new HashMap<Object, Object>();
							parserNameTemp.put("ATTRTAGS", ATTRTAGSTemp);
							parserTemp.put(parserName, parserNameTemp);
							tableNameTemp.put("Parser", parserTemp);
						}
						parserTemp = (Map<Object, Object>) tableNameTemp.get("Parser");
						parserNameTemp = (Map<Object, Object>) parserTemp.get(parserName);
						ATTRTAGSTemp = (Map<Object, Object>) parserNameTemp.get("ATTRTAGS");
						ATTRTAGSTemp.put(attrName, encodeValue(format));
						parserNameTemp.put("ATTRTAGS", ATTRTAGSTemp);
						parserTemp.put(parserName, parserNameTemp);
						tableNameTemp.put("Parser", parserTemp);
						tables.put(tableName, tableNameTemp);

						for (String attrtype : attrList) {
							tableNameTemp = (Map<String, Object>) tables.get(tableName);
							Map<String, Object> attrtypeTemp = null;
							Map<String, Object> attrNameTemp = null;
							if (tableNameTemp.containsKey(attrtype)) {
								attrtypeTemp = (Map<String, Object>) tableNameTemp.get(attrtype);
								if (attrtypeTemp.containsKey(attrName)) {
									attrNameTemp = (Map<String, Object>) attrtypeTemp.get(attrName);
									attrNameTemp.put("DATAID", encodeValue(format));
									attrtypeTemp.put(attrName, attrNameTemp);
									attrtypeTemp.put(attrtype, attrtypeTemp);
									tables.put(tableName, tableNameTemp);
								}
							}
						}
					}
				}
				xlsxdict.put("Tables", tables);
			}
		}
		return xlsxdict;
	}

	private Map<String, Object> transformations(XSSFSheet sheet, Map<String, String> transDict,
			Map<String, Object> xlsxdict) {
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String parserName = sheet.getRow(rowNum).getCell(0).toString();
			if (parserName != "") {
				String tableName = sheet.getRow(rowNum).getCell(1).toString();
				Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
				Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
				Map<String, Object> parserTemp = null;
				Map<Object, Object> parserNameTemp = null;
				Map<Object, Object> rowNumTemp = null;
				if (!tableNameTemp.containsKey("Parser")) {
					parserTemp = new HashMap<>();
					tableNameTemp.put("Parser", parserTemp);
				}
				parserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
				if (!parserTemp.containsKey(parserName)) {
					parserNameTemp = new HashMap<>();
					parserTemp.put(parserName, parserNameTemp);
					tableNameTemp.put("Parser", parserTemp);
				}
				parserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
				parserNameTemp = (Map<Object, Object>) parserTemp.get(parserName);
				if (!parserNameTemp.containsKey(rowNum - 1)) {
					rowNumTemp = new HashMap<>();
					parserNameTemp.put(rowNum - 1, rowNumTemp);
					parserTemp.put(parserName, parserNameTemp);
					tableNameTemp.put("Parser", parserTemp);
				}
				parserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
				parserNameTemp = (Map<Object, Object>) parserTemp.get(parserName);
				rowNumTemp = (Map<Object, Object>) parserNameTemp.get(rowNum - 1);
				for (Map.Entry<String, String> entry : transDict.entrySet()) {
					String FDColumn = entry.getKey();
					String Parameter = entry.getValue();
					int cellNum = findValue(sheet, FDColumn).getColumnIndex();
					String value = getCellValue(sheet, rowNum, cellNum);
					rowNumTemp.put(Parameter, encodeValue(value));
				}
				parserNameTemp.put(rowNum - 1, rowNumTemp);
				parserTemp.put(parserName, parserNameTemp);
				tableNameTemp.put("Parser", parserTemp);
				tables.put(tableName, tableNameTemp);
				xlsxdict.put("Tables", tables);
			}
		}
		return xlsxdict;
	}

	private Map<String, Object> topologyKeys(XSSFSheet sheet, Map<String, String> topKeysDict,
			Map<String, Object> xlsxdict) {
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String keyName = sheet.getRow(rowNum).getCell(1).toString();
			String tableName = sheet.getRow(rowNum).getCell(0).toString();
			Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
			Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
			Map<String, Object> referenceKeyTemp = null;
			Map<String, Object> keyNameTemp = null;
			if (!tableNameTemp.containsKey("referenceKey")) {
				referenceKeyTemp = new HashMap<String, Object>();
				tables.put("referenceKey", referenceKeyTemp);
			}
			referenceKeyTemp = (Map<String, Object>) tables.get("referenceKey");
			keyNameTemp = new HashMap<String, Object>();
			referenceKeyTemp.put(keyName, keyNameTemp);
			Map<String, Object> tempDict = parseFDRows(topKeysDict, sheet, referenceKeyTemp, keyName, false, rowNum);
			tableNameTemp.put("referenceKey", tempDict);
			tables.put(tableName, tableNameTemp);
			xlsxdict.put("Tables", tables);
		}
		return xlsxdict;
	}

	private Map<String, Object> topologyTable(XSSFSheet sheet, Map<String, String> topTableDict,
			Map<String, Object> xlsxdict) {
		sheet.getRow(0).removeCell(sheet.getRow(0).getCell(0));
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String tableName = sheet.getRow(rowNum).getCell(0).toString();
			Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
			Map<String, Object> tableNameTemp = null;
			if (tableName != "") {
				tableNameTemp = new HashMap<String, Object>();
				tableNameTemp.put("TABLETYPE", "Reference");
				tables.put(tableName, tableNameTemp);
			}
			Iterator<Cell> cellIterator = sheet.getRow(0).cellIterator();
			Cell cell = null;
			while (cellIterator.hasNext()) {
				cell = cellIterator.next();
				String headerValue = String.valueOf(sheet.getRow(0).getCell(cell.getColumnIndex()));
				String value = "";
				try {
					value = String.valueOf(sheet.getRow(rowNum).getCell(cell.getColumnIndex()));
					if (topTableDict.get(headerValue) != null
							&& topTableDict.get(headerValue).equals("UPDATE_POLICY")) {
						value = String.valueOf(Arrays.asList(updatePolicylist).indexOf(value));
					}
					tableNameTemp = (Map<String, Object>) tables.get(tableName);
					if (topTableDict.get(headerValue) != null) {
						tableNameTemp.put(topTableDict.get(headerValue), value);
						tables.put(tableName, tableNameTemp);
					} else {
						if (value != "") {
							tableNameTemp = (Map<String, Object>) tables.get(tableName);
							Map<String, Object> parserTemp = null;
							Map<String, Object> headerValueTemp = null;
							if (!tableNameTemp.containsKey("Parser")) {
								parserTemp = new HashMap<String, Object>();
								tableNameTemp.put("Parser", parserTemp);
							}
							parserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
							if (!parserTemp.containsKey(headerValue)) {
								headerValueTemp = new HashMap<String, Object>();
								parserTemp.put(headerValue, headerValueTemp);
								tableNameTemp.put("Parser", parserTemp);
							}
							parserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
							headerValueTemp = (Map<String, Object>) parserTemp.get(headerValue);
							headerValueTemp.put("DATATAGS", value);
							parserTemp.put(headerValue, headerValueTemp);
							tableNameTemp.put("Parser", parserTemp);
							tables.put(tableName, tableNameTemp);
						}

					}
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}

			}
			xlsxdict.put("Tables", tables);
		}

		return xlsxdict;
	}

	private Map<String, Object> vectors(XSSFSheet sheet, Map<String, String> vectorsDict,
			Map<String, Object> xlsxdict) {
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String index = getCellValue(sheet, rowNum, 3);
			if (index != "") {
				String quantity = sheet.getRow(rowNum).getCell(6).toString().strip();
				String vendRel = sheet.getRow(rowNum).getCell(2).toString().strip();
				String CounterName = sheet.getRow(rowNum).getCell(1).toString().strip();
				String tableName = sheet.getRow(rowNum).getCell(0).toString().strip();
				Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
				Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
				Map<String, Object> measurementCounterTemp = (Map<String, Object>) tableNameTemp
						.get("measurementCounter");
				Map<String, Object> counterNameTemp = (Map<String, Object>) measurementCounterTemp.get(CounterName);
				Map<String, Object> vectorsTemp = null;
				Map<String, Object> qUANTITYTemp = null;
				Map<String, Object> quantityTemp = null;
				Map<String, Object> vendRelTemp = null;
				Map<String, Object> indexTemp = null;
				if (!counterNameTemp.containsKey("Vectors")) {
					vectorsTemp = new HashMap<String, Object>();
					counterNameTemp.put("Vectors", vectorsTemp);
				}
				vectorsTemp = (Map<String, Object>) counterNameTemp.get("Vectors");
				if (!vectorsTemp.containsKey("QUANTITY")) {
					qUANTITYTemp = new HashMap<String, Object>();
					vectorsTemp.put("QUANTITY", qUANTITYTemp);
				}
				vectorsTemp = (Map<String, Object>) counterNameTemp.get("Vectors");
				qUANTITYTemp = (Map<String, Object>) vectorsTemp.get("QUANTITY");
				if (!qUANTITYTemp.containsKey(quantity)) {
					quantityTemp = new HashMap<String, Object>();
					qUANTITYTemp.put(quantity, quantityTemp);
				}
				qUANTITYTemp = (Map<String, Object>) vectorsTemp.get("QUANTITY");
				quantityTemp = (Map<String, Object>) qUANTITYTemp.get(quantity);

				if (!quantityTemp.containsKey(vendRel)) {
					vendRelTemp = new HashMap<String, Object>();
					quantityTemp.put(vendRel, vendRelTemp);
				}
				vendRelTemp = (Map<String, Object>) quantityTemp.get(vendRel);
				indexTemp = new HashMap<String, Object>();
				vendRelTemp.put(index, indexTemp);
				Map<String, Object> tempDict = parseFDRows(vectorsDict, sheet, vendRelTemp, index, false, rowNum);
				quantityTemp.put(vendRel, tempDict);
				qUANTITYTemp.put(quantity, quantityTemp);
				vectorsTemp.put("QUANTITY", qUANTITYTemp);
				counterNameTemp.put("Vectors", vectorsTemp);
				measurementCounterTemp.put(CounterName, counterNameTemp);
				tableNameTemp.put("measurementCounter", measurementCounterTemp);
				tables.put(tableName, tableNameTemp);
				xlsxdict.put("Tables", tables);
			}

		}
		return xlsxdict;
	}

	private Map<String, Object> factTablesCounters(XSSFSheet sheet, Map<String, String> ftCountersDict,
			Map<String, Object> xlsxdict) {
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String CounterName = getCellValue(sheet, rowNum, 1);
			if (CounterName != "") {
				String tableName = sheet.getRow(rowNum).getCell(0).toString().strip();
				Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
				Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
				Map<String, Object> measurementCounterTemp = null;
				if (!tableNameTemp.containsKey("measurementCounter")) {
					measurementCounterTemp = new HashMap<String, Object>();
					tableNameTemp.put("measurementCounter", measurementCounterTemp);
				}
				measurementCounterTemp = (Map<String, Object>) tableNameTemp.get("measurementCounter");
				Map<String, Object> counterNameTemp = new HashMap<String, Object>();
				measurementCounterTemp.put(CounterName, counterNameTemp);
				Map<String, Object> tempDict = parseFDRows(ftCountersDict, sheet, measurementCounterTemp, CounterName,
						false, rowNum);
				tableNameTemp.put("measurementCounter", tempDict);
				tables.put(tableName, tableNameTemp);
				xlsxdict.put("Tables", tables);
			}
		}
		return xlsxdict;
	}

	private Map<String, Object> factTablesKeys(XSSFSheet sheet, Map<String, String> ftKeysDict,
			Map<String, Object> xlsxdict) {

		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String keyName = sheet.getRow(rowNum).getCell(1).toString().strip();
			if (keyName.strip() != "") {
				String tableName = sheet.getRow(rowNum).getCell(0).toString().strip();
				Map<String, Object> tables = (Map<String, Object>) xlsxdict.get("Tables");
				Map<String, Object> tableNameTemp = (Map<String, Object>) tables.get(tableName);
				Map<String, Object> measurementKeyTemp = null;
				if (!tableNameTemp.containsKey("measurementKey")) {
					measurementKeyTemp = new HashMap<String, Object>();
					tableNameTemp.put("measurementKey", measurementKeyTemp);
				}
				measurementKeyTemp = (Map<String, Object>) tableNameTemp.get("measurementKey");
				Map<String, Object> keyNameTemp = new HashMap<String, Object>();
				measurementKeyTemp.put(keyName, keyNameTemp);
				Map<String, Object> tempDict = parseFDRows(ftKeysDict, sheet, measurementKeyTemp, keyName, false,
						rowNum);
				tableNameTemp.put("measurementKey", tempDict);
				if (tableNameTemp.get("JOINABLE") == keyName) {
					measurementKeyTemp = (Map<String, Object>) tableNameTemp.get("measurementKey");
					keyNameTemp = (Map<String, Object>) measurementKeyTemp.get(keyName);
					keyNameTemp.put("JOINABLE", 1);
					measurementKeyTemp.put(keyName, keyNameTemp);
					tableNameTemp.put("measurementKey", measurementKeyTemp);
				}
				tables.put(tableName, tableNameTemp);
				xlsxdict.put("Tables", tables);
			}
		}
		return xlsxdict;

	}

	private Map<String, Object> factTables(XSSFSheet sheet, Map<String, String> factTableDict,
			Map<String, Object> xlsxdict) {
		Map<String, Object> tables = new HashMap<String, Object>();
		sheet.getRow(0).removeCell(sheet.getRow(0).getCell(0));
		for (int rowNum = 1; rowNum < sheet.getLastRowNum() + 1; rowNum++) {
			String tableName = getCellValue(sheet, rowNum, 0);
			Map<String, Object> tableNameTemp = null;
			if (tableName != "") {
				tableNameTemp = new HashMap<String, Object>();
				tableNameTemp.put("TABLETYPE", "Measurement");
				tables.put(tableName, tableNameTemp);
				tableNameTemp = (Map<String, Object>) tables.get(tableName);
			}
			Iterator<Cell> cellIterator = sheet.getRow(0).cellIterator();
			Cell cell = null;
			while (cellIterator.hasNext()) {
				cell = cellIterator.next();
				String headerValue = cell.toString();
				String value = "";
				int valueTemp = 0;
				try {
					value = getCellValue(sheet, rowNum, cell.getColumnIndex());
					if (value.equalsIgnoreCase("Y")) {
						valueTemp = 1;
						value = "" + valueTemp;
					}
					if (headerValue.equals("FOLLOWJOHN")) {
						valueTemp = (int) Float.parseFloat(value);
						value = String.valueOf(valueTemp);
					}
					if (headerValue.equals("Count Table")) {
						if (value.contains(",")) {
							value = value.replaceAll("\\s", "").strip();
						}
					}
					if (factTableDict.get(headerValue) != null) {
						tableNameTemp.put(factTableDict.get(headerValue), encodeValue(value));
						tables.put(tableName, tableNameTemp);
					} else {

						if (value != "") {
							Map<String, Object> ParserTemp = null;
							Map<String, Object> headerValueTemp = null;
							String DATATAGSTemp = null;
							tableNameTemp = (Map<String, Object>) tables.get(tableName);
							if (!tableNameTemp.containsKey("Parser")) {
								ParserTemp = new HashMap<String, Object>();
								tableNameTemp.put("Parser", ParserTemp);
								tables.put(tableName, tableNameTemp);
							}
							tableNameTemp = (Map<String, Object>) tables.get(tableName);
							ParserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
							if (!ParserTemp.containsKey(headerValue)) {
								headerValueTemp = new HashMap<String, Object>();
								ParserTemp.put(headerValue, headerValueTemp);
								tableNameTemp.put("Parser", ParserTemp);
								tables.put(tableName, tableNameTemp);
							}
							tableNameTemp = (Map<String, Object>) tables.get(tableName);
							ParserTemp = (Map<String, Object>) tableNameTemp.get("Parser");
							headerValueTemp = (Map<String, Object>) ParserTemp.get(headerValue);
							if (!headerValueTemp.containsKey("DATATAGS")) {
								DATATAGSTemp = "";
								headerValueTemp.put("DATATAGS", DATATAGSTemp);
								ParserTemp.put(headerValue, headerValueTemp);
								tableNameTemp.put("Parser", ParserTemp);
								tables.put(tableName, tableNameTemp);
							}
							headerValueTemp.put("DATATAGS", value);
							ParserTemp.put(headerValue, headerValueTemp);
							tableNameTemp.put("Parser", ParserTemp);
							tables.put(tableName, tableNameTemp);
						}

					}

				} catch (Exception e) {
					logger.error(e.getMessage());
				}

			}

		}

		xlsxdict.put("Tables", tables);
		return xlsxdict;
	}

	private Map<String, Object> versioning(XSSFSheet sheet, Map<String, String> versioningDict,
			Map<String, Object> xlsxdict) {
		Map<String, Object> versioning = new HashMap<String, Object>();
		// Check For Manual Modifications
		var manMods = findValue(sheet, "ManModsFile");
		if (manMods != null) {
			var manModRow = manMods.getRowIndex();
			int fileIndex = fileName.lastIndexOf(".");
			String subFileName = fileName.substring(0, fileIndex);
			String value = getCellValue(sheet, manModRow, 1).strip();
			if (value != "") {
				if (value.contains(subFileName + "_SetsModifications_TPC.xml")) {
					versioning.put("ManModsFile", value);
				} else {
					logger.error(
							"Manual Modification File is not present or File Format is not correct. Please follow Manual Modification file name in the specific format. For example <Model-TName>_SetsModifications_TPC.xml");
					// raise Exception
				}
			} else {
				versioning.put("ManModsFile", "");
				logger.info(
						"Manual Modification details are not provided in the Model-T. So proceeding without Manual Modifications");
				// raise exception
			}

		} else {
			logger.error(
					"Please check 'Coversheet' contains 'ManModsFile' row or syntax is correct. Please add 'ManModsFile' row in the 'Coversheet' of Model-T.");
		}

		// check for Adding description
		Cell universeDeliveredCell = findValue(sheet, "Universe Delivered");
		if (universeDeliveredCell != null) {
			int universeDeliveredRowIndex = universeDeliveredCell.getRowIndex();
			String value = getCellValue(sheet, universeDeliveredRowIndex, 1).strip().toUpperCase();
			if (value != "" && (value.equals("YES") || value.equals("NO"))) {
				if (value.equals("YES")) {
					logger.info(
							"Universe is being delivered for this Techpack, so Counter and key description will not be added for this Techpack Document");
					versioning.put("UNIVERSE_DELIVERED", value);
				} else {
					versioning.put("UNIVERSE_DELIVERED", value);
					logger.info(
							"Universe Delivered details are not correct in the Model-T. So proceeding without adding counter and key description in Techpack Document ");
				}
			} else {
				logger.error(
						"Please check 'Coversheet' contains 'Universe Delivered' row or syntax is correct. Please add 'Universe Delivery' row in the 'Coversheet' of Model-T.");
				// raise Exception
			}
			for (Map.Entry<String, String> entry : versioningDict.entrySet()) {
				String fdColumn = entry.getKey();
				String parameter = entry.getValue();
				int rowNumber = findValue(sheet, fdColumn).getRowIndex();
				if (!parameter.equals("ManModsFile") && !parameter.equals("UNIVERSE_DELIVERED")) {
					value = "";
					Cell cell = sheet.getRow(rowNumber).getCell(1);
					if (cell != null) {
						value = cell.toString().strip();
					}
					if (parameter.equals("VENDORRELEASE")) {
						value = value.replaceAll("\\s", "").strip();
						List<String> listReleases = Arrays.asList(value.split(","));
						versioning.put(parameter, listReleases);
					} else {
						versioning.put(parameter, value);
					}
				}
			}
		}

		xlsxdict.put("Versioning", versioning);
		return xlsxdict;
	}

	private Map<String, Object> parseFDRows(Map<String, String> mappingDict, XSSFSheet FDsheet,
			Map<String, Object> destinationDict, String keyName, boolean getOrderNo, int rowNumber) {
		if (keyName != "") {
			if (getOrderNo) {
				Map<String, Object> keyNameTemp = (Map<String, Object>) destinationDict.get(keyName);
				keyNameTemp.put("ORDERNRO", rowNumber);
				destinationDict.put(keyName, keyNameTemp);
			}

			for (Map.Entry<String, String> entry : mappingDict.entrySet()) {
				String FDColumn = entry.getKey();
				String Parameter = entry.getValue();
				String value = "";

				try {
					int cellNum = findValue(FDsheet, FDColumn).getColumnIndex();
					value = FDsheet.getRow(rowNumber).getCell(cellNum).toString();
				} catch (Exception e) {
				}

				if (value.equalsIgnoreCase("Y")) {
					value = "" + 1;
				}
				if (Parameter.equals("DATATYPE")) {
					Map<String, String> map = parseDataType(value);
					value = map.get("datatype");
					Map<String, Object> keyNameTepm = (Map<String, Object>) destinationDict.get(keyName);
					keyNameTepm.put("DATASIZE", map.get("datasize"));
					keyNameTepm.put("DATASCALE", map.get("datascale"));
					destinationDict.put(keyName, keyNameTepm);
				}
				if (!Parameter.equals("DESCRIPTION")) {
					value = Utils.strFloatToInt(value.strip());
				}
				Map<String, Object> keyNameTepm = (Map<String, Object>) destinationDict.get(keyName);
				keyNameTepm.put(Parameter, encodeValue(value));
				destinationDict.put(keyName, keyNameTepm);
			}
		}

		return destinationDict;

	}

	private String encodeValue(String value) {
		try {
			value = value.strip();
		} catch (Exception e) {
			e.printStackTrace();
			return value;
		}
		return value;
	}

	private String getCellValue(XSSFSheet sheet, int rowNum, int cellNum) {
		String cellValue = "";
		try {
			cellValue = sheet.getRow(rowNum).getCell(cellNum).toString();
		} catch (Exception e) {
			return "";
		}

		return cellValue;
	}

	private Cell findValue(XSSFSheet sheet, String value) {
		Iterator<Row> rowIterator = sheet.iterator();
		Row row = null;
		while (rowIterator.hasNext()) {
			row = rowIterator.next();
			Iterator<Cell> cellIterator = row.cellIterator();
			Cell cell = null;
			while (cellIterator.hasNext()) {
				cell = cellIterator.next();
				final DataFormatter df = new DataFormatter();
				String valueAsString = df.formatCellValue(cell);
				if (valueAsString.equals(value)) {
					return cell;
				}
			}
		}
		return null;
	}

	private Map<String, String> parseDataType(String datatype) {
		String datasize = "0";
		String datascale = "0";
		Map<String, String> map = new HashMap<String, String>();
		if (datatype.contains("(")) {
			String parts[] = datatype.split("\\(");
			datatype = parts[0];
			if (parts[1].contains(",")) {
				datasize = parts[1].split(",")[0];
				datascale = parts[1].split(",")[1].replace(")", "");
			} else {
				datasize = parts[1].replace(")", "");
			}
		}
		map.put("datatype", datatype);
		map.put("datasize", datasize);
		map.put("datascale", datascale);
		return map;

	}
}
