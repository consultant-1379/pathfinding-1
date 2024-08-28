/**
 * 
 */
package com.ericsson.eniq.common;



/**
 * @author eheijun
 * 
 */
public class Constants {

  private Constants() {
    // never construct this
  }

  /**
   * Regular expression pattern for Ericsson R-States. Accepts "normal" R-States
   * according to the corporate basic standards (e.g. R1A --> R99ZZ), including
   * preliminary R-states (e.g. P1A --> P99ZZ), excluding R-states with
   * verification level (amendment level) (e.g. R1A01, P99ZZ99, R1AB123) and
   * special R-states (e.g. R1A/1, R1A/A).
   */
  public static final String RSTATEPATTERN = "[prPR]{1}[123456789]\\d?[a-zA-Z]{1,2}";

  /**
   * The latest ENIQ database version for techpacks.
   */
  public static final String CURRENT_TECHPACK_ENIQ_LEVEL = "11";
  
  /**
   * The OSS directory variable used while Set creation
   */
  public final static String OSS_DIR = "${OSS}";
  
  public static final String DIM_E_SGEH = "DIM_E_SGEH";
  
  public static final String EVENT_E = "EVENT_E";
  
  public static final String SONAGG = "SONDAYAGG";
  public static final String SON15AGG = "ROPGRP";
  public static final String[] ROPGRPSUPPORTED_TP = {"SONV_PM", "FUTURE_TP_WITH_ROPGRP_SUPPORT"};
  
  public static final String SONVISTEMPTABLE = SON15AGG+"TEMP";
  
  public static final String ENIQ_EVENT = "ENIQ_EVENT";
  
  public final static String ENIQ_EVENTS_TOPOLOGY_DIR = "eniq_events_topology";
  
  public static final String EVENT_LOADER = "EventLoader";
  
  public static final String TIMEBASE_PARTITION_LOADER = "TimeBase EventLoader";
  
//This is the ETLDATA directory for Events. Currently this is hard coded as
  // no environment property has been setup by integration for this yet.
  public static final String EVENTS_ETLDATA_DIR = "/eniq/data/etldata_";

  // Number of directories to create under the directory, /eniq/data/etldata_/
  // For example, directories would look like
  // /eniq/data/etldata_/00/...../eniq/data/etldata_/15/
  public final static int NUM_OF_DIRECTORIES = 16;

  public static final String EVENTS_BACKUP_DIR = "/eniq/backup";

  // This is the ETLDATA directory for STATS. It should be /eniq/data/eniq/. An
  // Environment property has been setup by integration for this
  public final static String STATS_ETLDATA_DIR = "${ETLDATA_DIR}";
  
  public static final String EXTRA_ZERO = "0";

  public static final String SONV = "SONV";
  /**
   * The ENIQ database version where Busy Hour Improvements were introduced.
   */
  public static final String BH_IMPROVEMENT_ENIQ_LEVEL = "11";

  /**
   * The latest ENIQ database version for interfaces.
   */
  public static final String CURRENT_INTERFACE_ENIQ_LEVEL = "2.0";

  /**
   * returns ":" separator for measurement type id usage
   */
  public static final String TYPESEPARATOR = ":";

  /**
   * returns "_" separator for measurement type name usage
   */
  public static final String TYPENAMESEPARATOR = "_";

  /**
   * returns "PLAIN"
   */
  public static final String PLAINLEVEL = "PLAIN";

  /**
   * returns "RAW"
   */
  public static final String RAWLEVEL = "RAW";

  /**
   * returns "DAY"
   */
  public static final String DAYLEVEL = "DAY";

  /**
   * returns "DAYBH"
   */
  public static final String DAYBHLEVEL = "DAYBH";

  /**
   * returns "COUNT"
   */
  public static final String COUNTLEVEL = "COUNT";

  /**
   * returns "RANKBH"
   */
  public static final String RANKBHLEVEL = "RANKBH";

  public static final String RANKBHCLASS = "RANKBHCLASS";

  public static final String MONTHRANKBHLEVEL = "MONTHRANKBH";

  public static final String WEEKRANKBHLEVEL = "WEEKRANKBH";

  // public static final String RANKBHLEVEL_TIMECONSISTENT =
  // "RANKBH_TIMECONSISTENT";
  // public static final String RANKBHLEVEL_TIMELIMITED = "RANKBH_TIMELIMITED";
  // public static final String RANKBHLEVEL_SLIDINGWINDOW =
  // "RANKBH_SLIDINGWINDOW";

  /**
   * returns "RANKBHCLASS"
   */
  public static final String RANKBHCLASSLEVEL = "RANKBHCLASS";

  /**
   * returns "DAY"
   */
  public static final String DAYSCOPE = "DAY";

  /**
   * returns scope "WEEK"
   */
  public static final String WEEKSCOPE = "WEEK";

  /**
   * returns type "BHSRC"
   */
  public static final String BHSRC = "BHSRC";

  /**
   * returns type "RANKSRC"
   */
  public static final String RANKSRC = "RANKSRC";

  /**
   * returns type "DAYBHCLASS"
   */
  public static final String DAYBHCLASS = "DAYBHCLASS";

  /**
   * returns "MONTH"
   */
  public static final String MONTHSCOPE = "MONTH";

  /**
   * returns "COUNT"
   */
  public static final String COUNTSCOPE = "COUNT";

  /**
   * returns "TOTAL"
   */
  public static final String TOTALTYPE = "TOTAL";

  /**
   * returns WEEKBH
   */
  public static final String WEEKBH = "WEEKBH";

  /**
   * returns MONTHBH
   */
  public static final String MONTHBH = "MONTHBH";

  public static final String LEV2 = "_LEV2";

  /**
   * returns table level, "RAW", for ENIQ events.
   */
  public static final String RAW = "RAW";

  /**
   * returns table level, "RAW_LEV2", for ENIQ events.
   */
  public static final String RAW_LEV2 = RAW + LEV2;

  /**
   * returns table level, "1MIN", for ENIQ events.
   */
  public static final String ONEMIN = "1MIN";

  /**
   * returns table level, "15MIN", for ENIQ events.
   */
  public static final String FIFTEENMIN = "15MIN";

  /**
   * returns table level, "DAY", for ENIQ events.
   */
  public static final String DAY = "DAY";

  /**
   * returns full list of table levels for ENIQ events.
   */
  public final static String[] LIST_OF_EVENT_TABLE_LEVELS = { RAW, ONEMIN, FIFTEENMIN, DAY };
  
  /**
   * name of the property that indicates the ENIQ/DC config directory
   */
  public static final String DC_CONFIG_DIR_PROPERTY_NAME = "dc.config.dir";
  public static final String CONF_DIR_PROPERTY_NAME = "CONF_DIR";
  
  /**
   * "varchar", "numeric", "integer", "int", "tinyint", "smallint",
   * "unsigned int", "long", "double", "datetime", "date", "char", "float",
   */
  public final static String[] DATATYPES = { "varchar", "numeric", "integer", "int", "tinyint", "smallint",
      "unsigned int", "long", "double", "datetime", "date", "char", "float" };

  /**
   * "extrasmall", "small", "medium", "large", "extralarge"
   */
  public final static String[] SIZINGITEMS = { "extrasmall", "small", "medium", "large", "extralarge" };

  /**
   * 30, 20, 10
   */
  public final static int[] PROMPTPRIORITY = { 30, 20, 10 };

  /**
   * "DEFAULT", "MEDIUM", "HIGH"
   */
  public final static String[] PROMPTPRIORITY_TEXT = { "DEFAULT", "MEDIUM", "HIGH" };

  /**
   * 10, 20, 30, 40, 50
   */
  public final static Integer[] PROMPTORDER = { 10, 20, 30, 40, 50 };

  /**
   * "", "YES", "NO"
   */
  public final static String[] PROMPTUNREFRESHABLE = { "", "YES", "NO" };

  /**
   * 0L, 1L, 2L, 3L
   */
  public final static long[] UPDATE_METHODS = { 0L, 1L, 2L, 3L };

  /**
   * "Static", "Predefined", "Dynamic", "Timed Dynamic"
   */
  public final static String[] UPDATE_METHODS_TEXT = { "Static", "Predefined", "Dynamic", "Timed Dynamic" };

  /**
   * "", "TABLE", "VIEW"
   */
  public final static String[] TABLE_TYPES = { "", "TABLE", "VIEW" };

  /**
   * "GAUGE", "PEG", "VECTOR", "UNIQUEVECTOR"
   */
  public final static String[] COUNTER_TYPES = { "GAUGE", "PEG", "VECTOR", "UNIQUEVECTOR", "CMVECTOR" };

  /**
   * "", "SUM", "AVG", "MAX", "MIN"
   */
  public final static String[] AGGREGATION_FORMULAS = { "", "NONE", "SUM", "AVG", "MAX", "MIN" };

  /**
   * Transformer_types start here
   */
  public final static String CURRENTTIME = "currenttime";

  public final static String FIXED = "fixed";

  public final static String LOOKUP = "lookup";

  public final static String COPY = "copy";

  public final static String CONDITION = "condition";

  public final static String POSTAPPENDER = "postappender";

  public final static String PROPERTYTOKENIZER = "propertytokenizer";

  public final static String PREAPPENDER = "preappender";

  public final static String DSTPARAMETERS = "dstparameters";

  public final static String DATABASELOOKUP = "databaselookup";

  public final static String CALCULATION = "calculation";

  public final static String DATEFORMAT = "dateformat";

  public final static String DEFAULTTIMEHANDLER = "defaulttimehandler";

  public final static String ALARM = "alarm";

  public final static String BITMAPLOOKUP = "bitmaplookup";

  public final static String REDUCEDATE = "reducedate";

  public final static String FIELDTOKENIZER = "fieldtokenizer";

  public final static String ROUNDTIME = "roundtime";

  public final static String SWITCH = "switch";

  public final static String RADIXCONVERTER = "radixconverter";

  public final static Object[] ACTIONTYPEITEMS = { "Aggregation", "AggregationRuleCopy", "AggRuleCacheRefresh",
      "AlarmHandler", "AlarmInterfaceUpdate", "AlarmMarkup", "AutomaticAggregation", "AutomaticREAggregati", "CreateDir",
      "DirectoryDiskmanager", "Diskmanager", "Distribute", "DuplicateCheck", "DWHMigrate", "EBSUpdate",
      "ExecutionProfiler", "GateKeeper", "JDBC Mediation", "Join", "JVMMonitor", "Load", "Loader",
      "ManualReAggregation", "Mediation", "Parse", "PartitionAction", "Partitioned Loader", "PartitionedSQLExec",
      "RefreshDBLookup", "ReloadDBLookups", "ReloadProperties", "ReloadTransformation", "SanityCheck",
      "SessionLog Loader", "SetTypeTrigger", "SMTP Mediation", "SNMP Poller", "SQL Execute", "SQL Extract",
      "SQLJoiner", "StorageTimeAction", "System Call", "System Monitor", "TableCheck", "TableCleaner",
      "TriggerScheduledSet", "Uncompress", "UnPartitioned Loader", "UpdateDimSession", "UpdateMonitoredTypes",
      "UpdateMonitoring", "UpdatePlan", "VersionUpdate" };

  public final static String[] TRANFORMER_TYPES = new String[] { CURRENTTIME, FIXED, LOOKUP, COPY, CONDITION,
      POSTAPPENDER, PROPERTYTOKENIZER, PREAPPENDER, DSTPARAMETERS, DATABASELOOKUP, CALCULATION, DATEFORMAT,
      DEFAULTTIMEHANDLER, ALARM, BITMAPLOOKUP, REDUCEDATE, FIELDTOKENIZER, ROUNDTIME, SWITCH, RADIXCONVERTER };

  public final static String[] UNIVERSEEXTENSIONTYPES = new String[] { "ALL" };

  public final static String[] UNIVERSEOBJECTTYPES = new String[] { "Character", "Number", "Date" };

  public final static String[] UNIVERSEQUALIFICATIONTYPES = new String[] { "Dimension", "Measure" };

  public final static String[] UNIVERSEOWNERTYPES = new String[] { "DC", "DWH" };

  public final static String PARSERFORMATS[] = { "alarm", "ascii", "axd", "ebs", "csexport", "ct", "eniqasn1", "mdc",
      "nascii", "nossdb", "omes", "omes2", "raml", "redback", "sasn", "separator", "spf", "stfiop", "xml", "3gpp32435",
      "CUSTOM" };

  public final static String ALLTECHPACKTYPES[] = { "PM", "CM", "BASE", "SYSTEM", "Topology", "EVENT", "CUSTOM" };

  public final static String CUSTOMTECHPACKTYPES[] = { "CUSTOM" };

  public final static String TYPES_NOBASE[] = { "BASE", "SYSTEM" };

  public static final String BOXIAUTHENTICATIONS[] = { "ENTERPRISE" };

  public static final String REPORTOBJECTLEVELS[] = { "TOTAL_RAW", "TOTAL_DAY", "DAYBH_RAW", "DAYBH_DAY", "ELEM_RAW",
      "ELEM_DAY" };

  public static final String REPORTCONDITIONLEVELS[] = { "TOTAL_RAW", "TOPOLOGY", "KEYTOPOLOGY", "TOTAL_DAY",
      "DAYBH_RAW", "DAYBH_DAY", "DAYBH", "ELEMBH_RAW", "ELEMBH_DAY" };

  /**
   * The default number of busy hour product place holder.
   */
  public static final int DEFAULT_NUMBER_OF_BH_PRODUCT_PLACE_HOLDERS = 5;

  /**
   * The default number of busy hour product place holder.
   */
  public static final int DEFAULT_NUMBER_OF_BH_CUSTOM_PLACE_HOLDERS = 5;

  /**
   * The busy hour product place holder prefix.
   */
  public static final String BH_PRODUCT_PLACE_HOLDER_PREFIX = "PP";

  /**
   * The busy hour custom place holder prefix.
   */
  public static final String BH_CUSTOM_PLACE_HOLDER_PREFIX = "CP";

  /**
   * Custom TechPack holder prefix.
   */
  public static final String BH_CUSTOM_TP_PREFIX = "CTP";

  
  /**
   * The maximum number of busy hour place holders (including both product and
   * custom place holders).
   */
  public static final int MAX_NUMBER_OF_BH_PRODUCT_PLACE_HOLDERS = 30;

  /**
   * The values for the busy hour aggregation types shown in the GUI.
   */
  public final static String[] BH_AGGREGATION_TYPPES_SHOW = { "Timelimited", "Slidingwindow",
      "Timelimited + Timeconsistent", "Slidingwindow + Timeconsistent" };

  /**
   * The values for the busy hour aggregation types stored to the database.
   */
  public final static String[] BH_AGGREGATION_TYPPES = { "RANKBH_TIMELIMITED", "RANKBH_SLIDINGWINDOW",
      "RANKBH_TIMECONSISTENT", "RANKBH_TIMECONSISTENT_SLIDINGWINDOW" };

  /**
   * The values for the busy hour grouping options.
   */
  public final static String[] BH_GROUPING_TYPES = new String[] { "None", "Time", "Node", "Time + Node" };
  
  public final static String ENGINE_DB_URL = "ENGINE_DB_URL";
  public final static String ENGINE_DB_USERNAME = "ENGINE_DB_USERNAME";
  public final static String ENGINE_DB_PASSWORD = "ENGINE_DB_PASSWORD";
  public final static String ENGINE_DB_DRIVERNAME = "ENGINE_DB_DRIVERNAME";
  
  /**
   * Status for aggregations.
   */
  public final static String AGG_BLOCKED_STATUS = "BLOCKED";
  public final static String AGG_FAILED_DEPENDENCY_STATUS = "FAILEDDEPENDENCY";
  public final static String AGG_FAILED_STATUS = "ERROR";  
  public final static String AGG_MANUAL_STATUS = "MANUAL";
  public final static String AGG_IGNORED_STATUS = "IGNORED";
  public final static String AGG_LATE_DATA_STATUS = "LATE_DATA";
  public final static String AGG_AGGREGATED_STATUS = "AGGREGATED";

public static final String ROPAGGSCOPE = "ROP";
  
// ENIQ STATS STARTER LICENSE DEFINITION
public static final String ENIQ_STARTER_LICENSE = "CXC4012419";
public static final String ENIQ_STARTER_LICENSE_DETAILS = "CXC4012419::Ericsson Network IQ Starter::FAJ8010774";



//ENIQ STATS CAPACITY LICENSE DEFINTION
public static final String ENIQ_CAPACITY_LICENSE = "CXC4012420";
public static final String ENIQ_CAPACITY_LICENSE_DETAILS = "CXC4012420::ENIQ Capacity::FAJ8010774";


public static final String TP_INSTALL_FILE = "/eniq/admin/version/eniq_status";
}

