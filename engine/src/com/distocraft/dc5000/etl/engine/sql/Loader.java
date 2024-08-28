package com.distocraft.dc5000.etl.engine.sql;

import java.io.*;
import java.rmi.NotBoundException;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.*;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.*;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.engine.system.ETLCEventHandler;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.cache.*;
import com.ericsson.eniq.backuprestore.backup.LoaderBackupHandling;
import com.ericsson.eniq.common.*;
import com.ericsson.eniq.exception.ConfigurationException;
import com.ericsson.eniq.exception.FileException;

/**
 * Common parent for all Loader classes <br>
 * <br>
 * Where-column of this action needs to a serialized properties-object which is stored in class variable whereProps. ActionContents-column shall
 * contain velocity template evaluated to get load clause <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>tablename</td>
 * <td>&nbsp;</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>taildir</td>
 * <td>&nbsp;</td>
 * <td>&nbsp;</td>
 * </tr>
 * </table>
 * 
 * Copyright Distocraft 2005 <br>
 * <br>
 * $id$
 * 
 * @author lemminkainen, savinen, melantie, melkko
 */
public abstract class Loader extends SQLOperation {

    private static final String COMMA_SEPARATOR = ", ";

    private static final String BINARY = ".binary";

    protected Logger log = null;

    protected Logger fileLog = null;

    protected Logger sqlLog = null;

    protected Logger sqlErrLog = null;
    
    protected Logger aggregatorLog;

    protected String loaderParamsASCII;

    protected String loaderParamsBIN;

    protected Properties whereProps;

    protected Long techPackId;

    protected Meta_versions version;

    protected Meta_collections set;

    protected String tablename;

    protected String tablelevel;

    protected String tpName;

    protected SetContext sctx;

    private long collectionSetId;
    
    protected FileInputStream fstream;
	
    protected BufferedReader br;

    protected File f;

    protected String logfile;
	
    final String plogdir = System.getProperty("LOG_DIR");
	
    private boolean statFlag = false;
    
    private boolean isMoveToFailedNeeded = false;

    /**
     * Default settings if no FileSystems are specified in the niq.ini. If its not set the old /eniq/data/etldata/dc_e_asbc/ structure is presumed.
     */
    public static final int NO_MOUNT_POINTS = 0;

    //	protected String failedDir;
    /**
     * Etl failed directory name. The Absolute failed path is generated at runtime and is based on the load file path, covers the ${ETLDATA_DIR}/00/
     * directory format, @see <a href="URL#getAbsoluteFailedDir">getAbsoluteFailedDir()</a>
     */
    protected String failedDirName;

    protected int maxLoadClauseLength = Integer.MAX_VALUE;

    protected String notifyTypeName = null;

    protected int totalRowCount = 0;
    
    protected boolean sqlErrorRowLocked = false;
    
    protected boolean inSufficientBuffer = false;

    private RockFactory rockFactory;

    private final boolean useROWSTATUS;

    private final DFormat dataformat;

    protected boolean fileDuplicateCheck = false;

	// For backup-restore
	protected long restoreStartEpoch;
	private boolean outOfSpace = false;
	private static final String RESTOREFLAGNAME = "flag_loaderrestore_";
	private static final String RESTOREPARENTFOLDER = "/var/tmp/";
    private static final String BINARYGZ = ".binary.gz";
    protected static final String BACKUP_DIR = StaticProperties.getProperty("BACKUP_DIR", "/eniq/flex_data_bkup");
    
    public Loader(final Meta_versions version, final Long techPackId, final Meta_collections set, final Long transferActionId,
                  final Long transferBatchId, final Long connectId, final RockFactory rockFact, final ConnectionPool connectionPool,
                  final Meta_transfer_actions trActions, final SetContext sctx, final Logger clog) throws EngineMetaDataException {

        super(version, techPackId, set, transferActionId, transferBatchId, connectId, rockFact, connectionPool, trActions);

        this.version = version;
        this.set = set;
        this.techPackId = techPackId;
        this.sctx = sctx;
        this.log = clog;
        this.rockFactory = rockFact;

        try {
            final Meta_collection_sets mcs_cond = new Meta_collection_sets(rockFact);
            mcs_cond.setCollection_set_id(techPackId);
            final Meta_collection_setsFactory mcs_fact = new Meta_collection_setsFactory(rockFact, mcs_cond);
            final List<Meta_collection_sets> tps = mcs_fact.get();
            final Meta_collection_sets tp = tps.get(0);
            tpName = tp.getCollection_set_name();
            collectionSetId = tp.getCollection_set_id();

        } catch (Exception e) {
            throw new EngineMetaDataException("Error resolving TP name", null, e, "constructor");
        }

        if (tpName == null) {
            throw new EngineMetaDataException("Unable to resolve TP name", null, (Throwable) null, "constructor");
        }

        try {
            final String where = this.getTrActions().getWhere_clause();
            whereProps = TransferActionBase.stringToProperties(where);
            tablename = whereProps.getProperty("tablename", "");
            tablelevel = whereProps.getProperty("taildir", "plain").toUpperCase();
            //			failedDir = System.getProperty("ETLDATA_DIR") + File.separator + tablename.toLowerCase().trim() + File.separator
            //					+ whereProps.getProperty("failedDir", "failed") + File.separator;
            failedDirName = whereProps.getProperty("failedDir", "failed");
        } catch (Exception e) {
            failedDirName = "failed";
            log.log(Level.WARNING, "Error while getting failed dir, default dir " + failedDirName + " used:", e);
        }

        fileDuplicateCheck = Boolean.parseBoolean(whereProps.getProperty("fileDuplicateCheck", "false"));

        final String defaultRowLogging = StaticProperties.getProperty("LoaderRowMsgLogging", "false");

        String defaultLoaderParamsASCII = getDefaultLoaderParamsAsciiOptions();
        defaultLoaderParamsASCII = defaultLoaderParamsASCII + getExtraLoadParamsIfRowLoggingIsSet(defaultRowLogging);

        String defaultLoaderParamsBIN = getDefaultLoaderParamsBinOptions();
        defaultLoaderParamsBIN = defaultLoaderParamsBIN + getExtraLoadParamsIfRowLoggingIsSet(defaultRowLogging);

        loaderParamsBIN = whereProps.getProperty("loaderParameters_BINARY", defaultLoaderParamsBIN);
        loaderParamsASCII = whereProps.getProperty("loaderParameters", defaultLoaderParamsASCII);

        boolean useRStat = false;
        try {
            final Meta_transfer_actions whereDim = new Meta_transfer_actions(rockFact);
            whereDim.setCollection_id(set.getCollection_id());
            whereDim.setCollection_set_id(set.getCollection_set_id());
            whereDim.setVersion_number(set.getVersion_number());
            whereDim.setAction_type("UpdateDimSession");

            try {
                final Meta_transfer_actions updateDIMSession = new Meta_transfer_actions(rockFact, whereDim);
                final String DIMSessionWhere = updateDIMSession.getWhere_clause();
                final Properties DIMSessionProperties = TransferActionBase.stringToProperties(DIMSessionWhere);
                useRStat = "true".equalsIgnoreCase(DIMSessionProperties.getProperty("useRAWSTATUS", "false"));
            } catch (Exception e) {
                log.finer("No UpdateDIMsession action was found from this set.");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Error while trying to fetch UpdateDimSessions ROWSTATUS property", e);
        }

        this.useROWSTATUS = useRStat;

        try {
            final String ss = StaticProperties.getProperty("maxLoadClauseLength", null);
            if (ss != null) {
                maxLoadClauseLength = Integer.parseInt(ss);
            }
        } catch (NumberFormatException nfe) {
            log.config("maxLoadClauseLength was invalid. Ignored.");
        }

        this.dataformat = setupDataformat(tablename);
        initializeLoggers();
        
        aggregatorLog = Logger.getLogger("aggregator."+log.getName().substring(4));
        
        String timeStamp = new SimpleDateFormat("yyyy_MM_dd").format(new java.util.Date());
		
		if (plogdir == null) {
		      try {
				throw new IOException("System property \"LOG_DIR\" not defined");
			} catch (Exception e) {
				log.warning("LOGDIR path not found");
			}
		    }
		logfile = plogdir + File.separator + "engine" + File.separator + "aggregator-" + timeStamp + ".log" ;
		
		f= new File(logfile);
    }

    protected String getExtraLoadParamsIfRowLoggingIsSet(final String defaultRowLogging) {
        String extraLoadParams = "";
        if (defaultRowLogging.equalsIgnoreCase("true")) {
            log.fine("IQ MESSAGE and ROW logging is enabled, this may slow down partition loading.");
            final String defaultLocaLogDir = "/eniq/local_logs/iq";
            // Use the message and row logging in the default ascii and default binary
            // templates.
            String localLogDir = System.getProperty("LOCAL_LOGS_DIR", defaultLocaLogDir);
            final File test = new File(localLogDir);
            if (!test.exists()) {
                log.warning("${LOCAL_LOGS_DIR} does not exist -> " + test.getPath() + " defaulting to " + defaultLocaLogDir);
                localLogDir = defaultLocaLogDir;
            } else if (!test.isDirectory()) {
                log.warning("${LOCAL_LOGS_DIR} is not a directory -> " + test.getPath() + " defaulting to " + defaultLocaLogDir);
                localLogDir = defaultLocaLogDir;
            }
            final String defaultMessageLog = "MESSAGE LOG '" + localLogDir + "/${TECHPACK}:${MEASTYPE}_RAW:${MEASTYPE}_${DATE}_msg.log' \n";
            final String defaultRowLog = "ROW LOG '" + localLogDir + "/${TECHPACK}:${MEASTYPE}_RAW:${MEASTYPE}_${DATE}_row.log' \n";
            final String defaultLogUnique = "ONLY LOG UNIQUE, NULL, DATA VALUE\n" + "LOG DELIMITED BY ';' \n";
            extraLoadParams = defaultMessageLog + defaultRowLog + defaultLogUnique;
        }
        return extraLoadParams;
    }

    private String getDefaultLoaderParamsBinOptions() {
        // These are the load parameters for BINARY loading. NB:
        // "IGNORE CONSTRAINT NULL" set to 0 (zero) means the limit to the number of
        // nulls allowed is infinite.
        return "ESCAPES OFF  QUOTES OFF FORMAT BINARY WITH CHECKPOINT $CHECKPOINT \n"
                + "NOTIFY $NOTIFY_ROWS ON FILE ERROR CONTINUE IGNORE CONSTRAINT NULL 0, DATA VALUE 2000000, UNIQUE 2000000 \n";
    }

    private String getDefaultLoaderParamsAsciiOptions() {
        return "NOTIFY $NOTIFY_ROWS \n" + "ESCAPES OFF \n" + "QUOTES OFF   \n" + "DELIMITED BY '\\x09' \n" + "ROW DELIMITED BY '\\x0a' \n"
                + "IGNORE CONSTRAINT UNIQUE 2000000 \n" + "IGNORE CONSTRAINT NULL 2000000 \n" + "IGNORE CONSTRAINT DATA VALUE 2000000 \n"
                + "WITH CHECKPOINT $CHECKPOINT \n";
    }

    /**
     * Sets up dataformat. Gets the dataformat using the table name. Returns null if nothing was found in the DataformatCache and logs a warning if
     * this happens.
     * 
     * @param tableName
     * @return dformat The new dataformat.
     */
    protected DFormat setupDataformat(final String tableName) {
        final DFormat dformat = getDataformatCache().getFormatWithFolderName(tableName);

        boolean ignore = false;

        // List of tech packs to ignore:
        final String[] TECH_PACKS_TO_IGNORE = { "DWH_MONITOR" };

        // Don't warn for tech packs that have no data format (like DWH_MONITOR):
        for (String techPackToIgnore : TECH_PACKS_TO_IGNORE) {
            if (tpName.startsWith(techPackToIgnore)) {
                ignore = true;
                break;
            }
        }

        // List of table names to ignore:
        final String[] TABLE_NAMES_TO_IGNORE = { "SELECT_RAN_CELL", "SELECT_LTE_CELL", "DIM_X_BSS_CELL_CURRENT_DC" };

        // Don't warn for table names that have no data format :
        for (String tableNamesToIgnore : TABLE_NAMES_TO_IGNORE) {
            if (tableName.startsWith(tableNamesToIgnore)) {
                ignore = true;
                break;
            }
        }

        if (!ignore) {
            if (dformat != null) {
                log.fine("Dataformat " + dformat.getDataFormatID() + " found " + dformat.getDItemCount() + " columns");
            } else {
                log.warning("Dataformat not found for folderName \"" + tableName + "\"");
            }
        }
        return dformat;
    }

    /**
     * This constructor is only intended for test purposes.
     */
    public Loader() {
        this.useROWSTATUS = false;
        this.dataformat = null;
    }

    @Override
    public void execute() throws EngineException {
        log.fine("Executing...");

        final ActivationCache ac = ActivationCache.getCache();

        final List<String> noTableList = new ArrayList<String>();
        final List<String> tableList = new ArrayList<String>();
        Map<String, List<String>> tableToFile = null;

        Statement statement = null;
        VelocityEngine vengine = null;

        try {

            tableToFile = getTableToFileMap();

            if (tableToFile.size() != 0) {
            	String versionFromDB = this.getTrActions().getVersion_number();
            	versionFromDB = versionFromDB.substring(versionFromDB.lastIndexOf("(") + 1, versionFromDB.indexOf("))"));
                statement = this.getStatement();
                log.info("Query timeout set for statement is :" + statement.getQueryTimeout());
                // get the load template.
                String asciiLoadTemplate = this.getTrActions().getAction_contents();
                if (asciiLoadTemplate != null) {
                    log.fine("Ascii Template Initially:" + asciiLoadTemplate);
                    log.finest("Size of Ascii Template Initially:" + asciiLoadTemplate.length());
                }

                String binLoadTemplate = null;
                log.finest("Binary Template Initially:" + binLoadTemplate);

                /*
                 * Check if the wran parsed file name ends with .txt. If so then we have parsed text files which are backlog for loading. During an
                 * upgrade, the newer tp will not have the template embedded in tp even though the ascii parsed file from older version would have
                 * existed. So for this scenario, create the ascii loader sql which will be used to load the .txt backlog after an upgrade.
                 */

                boolean tryToLoad = false; // did we try to load any files

                if (ac.isActive(tpName, tablename, tablelevel)) {
                    log.finer("Found " + tableToFile.size() + " mappings.");
                    vengine = VelocityPool.reserveEngine();
                    // Create load table commands

                    final Iterator<Map.Entry<String, List<String>>> tableToFileIterator = tableToFile.entrySet().iterator();

                    if (fileDuplicateCheck) {
                        statement.getConnection().setAutoCommit(false);
                        createTemporaryTable(statement);
                    }

                    while (tableToFileIterator.hasNext()) {

                        final Map.Entry<String, List<String>> entry = tableToFileIterator.next();

                        final String physicalTableName = entry.getKey();
                        log.finest("table = " + physicalTableName);

                        if (physicalTableName == null) {
                            noTableList.addAll(entry.getValue());
                        } else {
                            if (physicalTableName.equalsIgnoreCase("waiting")) {
                                // waiting, do not remove or try to load.
                                log.info("Files waiting a new partition ");
                            } else {
                                final List<String> files = entry.getValue();
                                log.finest("files size = " + files.size());
                                // remove files to be loaded from tableToFile-list
                                tableToFileIterator.remove();

                                List<String> asciiFileList = new ArrayList<String>();
                                List<String> binFileList = new ArrayList<String>();
                                
								// EQEV-40057 and EQEV-40058 - Changes for .gz file handling and version check
                                List<String> binOldVersionList = new ArrayList<String>();
                                List<String> asciiOldVersionList = new ArrayList<String>();
                                final String BUILDNOREGEX = ".+_b(.+)_.+";
                                String oldBuild = null;
                                for (String tempFileName : files) {
                                	String buildNoString = parseFileName(tempFileName, BUILDNOREGEX);
                                    if (tempFileName.endsWith(getEndOfBinaryFileName()) || tempFileName.endsWith(BINARYGZ)) {
                                    	if(buildNoString.equalsIgnoreCase("") && (tablename.startsWith("DIM_E_") || tablename.startsWith("DC_E_"))){
                                    		log.warning("File will be skipped for loading since it doesn't contain techpack version number appended to it.Filename along with path is: " +tempFileName);
                                    		continue;
                                    	}
                                    	else if(!buildNoString.equalsIgnoreCase(versionFromDB) && !buildNoString.equalsIgnoreCase("")){
                                    		log.finest("Version number in file - " + buildNoString);
                                    		log.finest("Version number from DB - " + versionFromDB);
                                    		binOldVersionList.add(tempFileName);
                                    		oldBuild = buildNoString;                                    	
                                    	}
                                    	else{
                                    		binFileList.add(tempFileName);
                                    	}
                                    } else {
                                    	if(buildNoString.equalsIgnoreCase("") && (tablename.startsWith("DIM_E_") || tablename.startsWith("DC_E_"))){
                                    	log.warning("File will be skipped for loading since it doesn't contain techpack version number appended to it.Filename along with path is: " +tempFileName);
                                		continue;
                                	}
                                    	else if(!buildNoString.equalsIgnoreCase(versionFromDB) && !buildNoString.equalsIgnoreCase("")){
                                    		log.finest("Version number in file - " + buildNoString);
                                    		log.finest("Version number from DB - " + versionFromDB);
                                        	asciiOldVersionList.add(tempFileName);
                                        	oldBuild = buildNoString;
                                    	}
                                        else{
                                        	asciiFileList.add(tempFileName);                                        	
                                    	}
                                    }
                                }

                                log.finest("Loading " + asciiFileList.size() + " ASCII files and " + binFileList.size() + " binary files");
                                log.finest("Loading " + asciiOldVersionList.size() + " old ASCII files and " + binOldVersionList.size() + " old binary files");
                                if (asciiFileList.size() > 0) {
                                    if (fileDuplicateCheck) {
                                        // Do duplicate check first. No point in doing anything else
                                        // if all duplicates
                                        asciiFileList = pruneDuplicates(asciiFileList, statement);
                                        if (asciiFileList.size() == 0) {
                                            continue;
                                        }
                                    }

                                    if (asciiLoadTemplate == null || asciiLoadTemplate.length() == 0) {
                                        log.finest("Generating AsciiLoadTemplate Dynamically");
                                        asciiLoadTemplate = generateLoadTemplate(false);
                                    }

                                    log.finest("Ascii Load Template:" + asciiLoadTemplate);
                                    for (String tpSubStr : Constants.ROPGRPSUPPORTED_TP) {
                                        if (physicalTableName.contains(tpSubStr) && tablelevel.equalsIgnoreCase("RAW") && isRopGrpCellEnabled()) {
                                            final String tempTableName = physicalTableName.substring(0, physicalTableName.indexOf("_RAW")) + "_"
                                                    + Constants.SONVISTEMPTABLE;
                                            loadTempTable(tempTableName, physicalTableName, asciiFileList, asciiLoadTemplate, vengine, statement,
                                                    loaderParamsASCII);
                                            break;
                                        }
                                    }
                                    loadTable(physicalTableName, asciiFileList, asciiLoadTemplate, vengine, statement, loaderParamsASCII);
                                }
								
								if(asciiOldVersionList.size() > 0){
                                	if (fileDuplicateCheck) {
                                        // Do duplicate check first. No point in doing anything else
                                        // if all duplicates
                                		asciiOldVersionList = pruneDuplicates(asciiOldVersionList, statement);
                                        if (asciiOldVersionList.size() == 0) {
                                            continue;
                                        }
                                    }
                                	String asciiOldLoadTemplate = getOldVersionLoadTemplate(oldBuild,true);
                                	
                                	if (asciiOldLoadTemplate == null || asciiOldLoadTemplate.length() == 0) {
                                        log.finest("Generating for old AsciiLoadTemplate Dynamically");
                                        asciiOldLoadTemplate = generateLoadTemplate(false);
                                    }
                                	
                                	log.finest("Ascii Old Load Template:" + asciiOldLoadTemplate);
                                    for (String tpSubStr : Constants.ROPGRPSUPPORTED_TP) {
                                        if (physicalTableName.contains(tpSubStr) && tablelevel.equalsIgnoreCase("RAW") && isRopGrpCellEnabled()) {
                                            final String tempTableName = physicalTableName.substring(0, physicalTableName.indexOf("_RAW")) + "_"
                                                    + Constants.SONVISTEMPTABLE;
                                            loadTempTable(tempTableName, physicalTableName, asciiOldVersionList, asciiOldLoadTemplate, vengine, statement,
                                                    loaderParamsASCII);
                                            break;
                                        }
                                    }
                                    log.info("Loading files generated by older version:" + asciiOldLoadTemplate);
                                    loadTable(physicalTableName, asciiOldVersionList, asciiOldLoadTemplate, vengine, statement, loaderParamsASCII);
                                }

                                if (binFileList.size() > 0) {
                                    if (fileDuplicateCheck) {
                                        // Do duplicate check first. No point in doing anything else
                                        // if all duplicates
                                        binFileList = pruneDuplicates(binFileList, statement);
                                        if (binFileList.size() == 0) {
                                            continue;
                                        }
                                    }

                                    if (binLoadTemplate == null || binLoadTemplate.length() == 0) {
                                        binLoadTemplate = generateLoadTemplate(true);
                                    }
                                    log.finest("Binary SQL Load Template: " + binLoadTemplate);
                                    for (String tpSubStr : Constants.ROPGRPSUPPORTED_TP) {
                                        if (physicalTableName.contains(tpSubStr) && tablelevel.equalsIgnoreCase("RAW") && isRopGrpCellEnabled()) {
                                            final String tempTableName = physicalTableName.substring(0, physicalTableName.indexOf("_RAW")) + "_"
                                                    + Constants.SONVISTEMPTABLE;
                                            loadTempTable(tempTableName, physicalTableName, binFileList, binLoadTemplate, vengine, statement,
                                                    loaderParamsBIN);
                                            break;
                                        }
                                    }
                                    loadTable(physicalTableName, binFileList, binLoadTemplate, vengine, statement, loaderParamsBIN);
                                }
								
								if(binOldVersionList.size() > 0){
                                	if (fileDuplicateCheck) {
                                        // Do duplicate check first. No point in doing anything else
                                        // if all duplicates
                                		binOldVersionList = pruneDuplicates(binOldVersionList, statement);
                                        if (binOldVersionList.size() == 0) {
                                            continue;
                                        }
                                    }
                                	String binOldLoadTemplate = getOldVersionLoadTemplate(oldBuild,false);
                                	
                                	if (binOldLoadTemplate == null || binOldLoadTemplate.length() == 0) {
                                		binOldLoadTemplate = generateLoadTemplate(true);
                                    }
                                	
                                	log.finest("Binary SQL Old Load Template: " + binOldLoadTemplate);
                                    for (String tpSubStr : Constants.ROPGRPSUPPORTED_TP) {
                                        if (physicalTableName.contains(tpSubStr) && tablelevel.equalsIgnoreCase("RAW") && isRopGrpCellEnabled()) {
                                            final String tempTableName = physicalTableName.substring(0, physicalTableName.indexOf("_RAW")) + "_"
                                                    + Constants.SONVISTEMPTABLE;
                                            loadTempTable(tempTableName, physicalTableName, binOldVersionList, binOldLoadTemplate, vengine, statement,
                                            		loaderParamsBIN);
                                            break;
                                        }
                                    }
                                    loadTable(physicalTableName, binOldVersionList, binOldLoadTemplate, vengine, statement, loaderParamsBIN);
                                }

                                tryToLoad = true;
                                tableList.add(physicalTableName);

                                log.finest(physicalTableName + " was added to tableList");
                            }
                        }
                    }

                    addTableListToSetContext(tableList, useROWSTATUS);

                    if (noTableList.isEmpty()) {
                        if (tryToLoad) {
                            log.fine("Succesfully loaded.");
                        } else {
                            log.fine("All loader files are waiting for new partition.");
                        }
                    } else {
                        log.warning("Found " + noTableList.size() + " files without tables. Moving to failed.");
                        if (!tryToLoad) {
                            throw new Exception("All loader files are without table.");
                        }
                    }
                } else {
                    log.log(Level.WARNING, "Measurement type " + tablename + ":" + tablelevel
                            + " is DISABLED, loader not executed, files will be deleted.");
                    addTableListToSetContext(tableList, false);

                    log.info("MeasurementType is Inactive deleting files...");
                    for (Map.Entry<String, List<String>> entry : tableToFile.entrySet()) {
                        deleteFiles(entry.getValue());
                    }
                }
            } else {
                addTableListToSetContext(tableList, false);
            }
        } 
        catch (EngineException ee) {
            throw ee;
        } 
        catch (Exception e) {
            if (e.getMessage().contains("You have run out of space")) {
                log.log(Level.WARNING, "Loader failure : No space in database ");
                outOfSpace = true;
            }
            else if(e.getMessage().contains("SQL Anywhere Error -210")){
            	sqlErrorRowLocked = true;
            }
            else if((e.getMessage().contains("SQL Anywhere Error -1009134")) || (e.getMessage().contains("Insufficient buffers for 'Sort'."))){
            	inSufficientBuffer = true;
            }
            else if(e.getMessage().contains("Error in Re-Triggering Topology set")){
            	log.log(Level.WARNING, "Re-Triggering failed due to insufficient buffers for 'sort'");
            	log.log(Level.INFO, "Moving all topology files to failed");
            }
            else  {
                log.log(Level.WARNING, "General loader failure:", e);
                log.log(Level.INFO, "Moving all files to failed:");
            }
            throw new EngineException("Error while loading files", e, this, "execute", EngineConstants.CANNOT_EXECUTE);
        } 
        finally {   
        	 cleanup(tableToFile, statement, vengine);
        }

        triggerListeners();
        log.info("Set loaded in total " + totalRowCount + " rows.");
    }

 /**
  * Cleans up 
  * 
  * @return
  */
        protected void cleanup(final Map<String, List<String>> tableToFile, final Statement statement, final VelocityEngine vengine) {
   
        VelocityPool.releaseEngine(vengine);
        final String collectionName=this.set.getCollection_name();
        
        //Fix for HU42234
        if(sqlErrorRowLocked){
        	log.info("Not moving load files, will be picked up in the next cycle");
            sqlErrorRowLocked = false;
        }
        else if(inSufficientBuffer && !collectionName.contains("Loader_DIM_")){
        	log.info("Not moving load files, will be picked up in the next cycle");
            inSufficientBuffer = false; 
        }
        else{
        	moveFilesToFailed(tableToFile);
        	outOfSpace = false;
        }
        if (statement != null) {
            try {
                statement.getConnection().commit();
            } catch (Exception e) {
                log.log(Level.FINEST, "error committing", e);
            }

            if (fileDuplicateCheck) {
                try {
                    statement.getConnection().setAutoCommit(true);
                } catch (SQLException sqle) {
                    log.log(Level.FINE, "Failed to enable autocommit", sqle);
                }
            }

            try {
                statement.close();
            } catch (Exception e) {
                log.log(Level.FINEST, "error closing statement", e);
            }
        }
    }

    /**
     * Gets the end of the binary file name. Extracted out because of eventsupport module
     * 
     * @return
     */
    protected String getEndOfBinaryFileName() {
        return BINARY;
    }

    /**
     * 20120327 eanguan efaigha :: Function to check whether ROPGRPCELL level Aggregation is supported for the current Measurement type Needed for SON
     * ROP level aggregations. Can be used for other TPs as well
     * 
     * @return True if ROPGRPCELL level aggregation is supported. false otherwise
     */
    private boolean isRopGrpCellEnabled() {
        boolean isRopGrpCellEnabled = false;
        final String collName = this.set.getCollection_name();

        if (collName.startsWith("Loader_")) {
            final String genName = collName.substring(collName.indexOf("DC_"), collName.length());
            final String ropGrpAggName = "Aggregator_" + genName + "_" + Constants.SON15AGG;

            //Search for collection_name with ropGrpAggName in Meta_Collection table
            try {
                final Meta_collections mc_cond = new Meta_collections(this.rockFactory);
                mc_cond.setCollection_name(ropGrpAggName);
                mc_cond.setCollection_set_id(collectionSetId);
                final Meta_collectionsFactory mc_fact = new Meta_collectionsFactory(this.rockFactory, mc_cond);
                final List<Meta_collections> mcsList = mc_fact.get();
                if (mcsList.size() > 0) {
                    isRopGrpCellEnabled = true;
                }
            } catch (final Exception e) {
                log.warning("Exception comes while searching Collection Name: " + ropGrpAggName + " and collection_set_id: " + collectionSetId
                        + " in table Meta_collections in etlrep.");
                isRopGrpCellEnabled = false;
            }
        }//if

        return isRopGrpCellEnabled;
    }

    /**
     * Trigger listeners where applicable such as a refresh of DBLookupCache
     */
    protected void triggerListeners() {
        try {
            if (totalRowCount > 0) {
                final Share share = Share.instance();
                final ETLCEventHandler etlcEventHandler = (ETLCEventHandler) share.get("LoadedTypes");
                etlcEventHandler.triggerListeners(this.notifyTypeName);
                log.finer("Event sent to ETLCEventHandler");
            }
        } catch (Exception e) {
            log.warning("Error occured in triggering listeners.");
        }
    }

    /**
     * Add the tableList to the SetContext Map.
     * 
     * @param tableList
     * @param rowStatus
     * @throws SQLException
     */
    protected void addTableListToSetContext(final List<String> tableList, final boolean rowStatus) throws SQLException {
        log.finest("useROWSTATUS = " + rowStatus);
        if (rowStatus) {
            List<String> tableListCreated = createTableList();
            if (tableListCreated.isEmpty()) {
                tableListCreated = tableList;
            }
            sctx.put("tableList", tableListCreated);
        } else {
            sctx.put("tableList", tableList);
        }
    }

    /**
     * Method to contruct a SQL load template - either of ASCII or BINARY loading.
     * 
     * @param boolean indicating if binary (true) or ascii (false) load template is to be generated.
     */
    public String generateLoadTemplate(final boolean binary) {

        if (dataformat != null) {
            log.fine("Dataformat " + dataformat.getDataFormatID() + " found " + dataformat.getDItemCount() + " columns");
        } else {
            log.warning("Dataformat not found for folderName \"" + tablename + "\"");
        }

        final String BWNB = (binary) ? " BINARY WITH NULL BYTE" : "";

        final StringBuilder loadTemplate = new StringBuilder("LOAD TABLE $TABLE (");

        loadTemplate.append(getColumns(BWNB));

        loadTemplate.append(") FROM $FILENAMES $LOADERPARAMETERS;\n");

        return loadTemplate.toString();
    }

    /**
     * @param BWNB
     * @param loadTemplate
     */
    private String getColumns(final String BWNB) {
        final StringBuilder columns = new StringBuilder();
        Iterator<DItem> dataItems;
		final List<DItem> prelimDataItems = dataformat.getItems();
		DItem firstItem = prelimDataItems.get(0);
		DItem secondItem = prelimDataItems.get(1);
		DItem thirdItem = prelimDataItems.get(2);
		if ((firstItem.getColNumber() != 1 && firstItem.getColNumber() != 100)
				|| (secondItem.getColNumber() != 2 && secondItem.getColNumber() != 101)
				|| (thirdItem.getColNumber() != 3 && thirdItem.getColNumber() != 102)) {
			log.warning("Columns not in right order : Columns list  = " + prelimDataItems.toString());
			Collections.sort(prelimDataItems);
			dataItems = prelimDataItems.iterator();
		} else {
			log.info("Columns in right order");
			dataItems = dataformat.getDItems();
		}
        while (dataItems.hasNext()) {
            String columnName = dataItems.next().getDataName();
            if (addColumnToLoadTemplate(columnName)) {
                columns.append(columnName).append(BWNB);
                columns.append(COMMA_SEPARATOR);
            }
        }
        return columns.toString().substring(0, columns.toString().lastIndexOf(COMMA_SEPARATOR));
    }

    protected boolean addColumnToLoadTemplate(final String columnName) {
        //Add all columns by default, change criteria in subclass if necessary
        return true;
    }

    protected void deleteFiles(final Map<String, List<String>> tableToFile) {
        for (List<String> files : tableToFile.values()) {
            deleteFiles(files);
        }
    }

    protected void deleteFiles(final List<String> fileList) {
        for (String fileName : fileList) {
            final File file = new File(fileName);
            file.delete();
            fileLog.finest(file.toString() + " deleted");
        }
    }

    protected void moveFilesToFailed(final Map<String, List<String>> filesMap) {
        for (String key : filesMap.keySet()) {
            if (key == null || !key.equalsIgnoreCase("waiting")) {
                moveFilesToFailed(filesMap.get(key));
            }
        }
    }

    protected void moveFilesToFailed(final List<String> fileList) {
        for (String fileName : fileList) {
			if(isRestoreEnabled() && fileName.startsWith(BACKUP_DIR)){
				if(parseEpochTime(fileName) < restoreStartEpoch){
					log.fine("File " + fileName + " will not be moved to failed as it is a restored file.");
					if(!outOfSpace){
						final File file = new File(fileName);
			 			file.delete();
			 			fileLog.finest(file.toString() + " deleted");
					}
					else{
						log.fine("File " + fileName + " will not be deleted as out of space, will be picked up in next cycle");
					}
		 			continue;
				}
 		   }
        	moveFileToFailed(fileName);
        }
    }

    protected void moveFileToFailed(final String fileName) {
        final File file = new File(fileName);
        final String failedDir = getAbsoluteFailedDir(fileName);
        fileLog.finest(file.toString() + " moved to failed");
        moveToDirectory(file, failedDir);
    }
    
    protected void listFailedLoaderFiles(final List<String> fileList) {
    	for (String fileName : fileList) {
    		final File file = new File(fileName);
    		final String failedDir = getAbsoluteFailedDir(fileName);
            log.info("Loader files failed for Delimiter is "+failedDir+"/"+file.getName());
    	}
    }

    /**
     * Get the failed dir for a file. Handles the etldata structure with and without mount points. e.g. /eniq/data/etldata/dc_e_abc/raw/load_file.sql
     * --> /eniq/data/etldata/dc_e_abc/failed /eniq/data/etldata/07/dc_e_abc/raw/load_file.sql --> /eniq/data/etldata/07/dc_e_abc/failed
     * 
     * @param fileToMove
     *            file being moved to the failed directory
     * @return The failed directory path for the input file
     */
    protected String getAbsoluteFailedDir(final String fileToMove) {
        File tmp = new File(fileToMove);
        
        /*In case of Topology Files, data is being loaded into table_name_current_dc tables.
         *Removing _current_dc from the Table Name to identify the Directory Name.
         */ 
        String dirName = tablename.toLowerCase().endsWith("_current_dc") ? tablename.substring(0, tablename.toLowerCase().lastIndexOf("_current_dc")) : tablename;      
        
        //work back up to where dir.name == tablename
        while (tmp != null && !tmp.getName().equalsIgnoreCase(dirName)) {
            tmp = tmp.getParentFile();
        }
        
        //EQEV-39845 - for moving files to correct failed directory for PREV tables
        if(tmp == null && dirName.toLowerCase().endsWith("_prev")){
        	tmp = new File(fileToMove);
        	String type_prev = dirName.substring(0, dirName.toLowerCase().lastIndexOf("_prev"));
        	log.finest("typename (for prev tables) - " + type_prev);
        	while (tmp != null && !tmp.getName().equalsIgnoreCase(type_prev)) {
                tmp = tmp.getParentFile();
            }
        }
        //End of fix provided for EQEV-39845
        
        final File failedDir;
        if (tmp == null) {
            //shouldn't happen?!?!
            failedDir = new File(System.getProperty("ETLDATA_DIR", "/eniq/data/etldata"), dirName.toLowerCase().trim() + File.separator
                    + failedDirName);
            log.warning("Couldn't work out failed dir based on '" + fileToMove + "', defaulting to " + failedDir.getPath());
        } else {
            //either at /eniq/data/etldata/dc_e_abc/ or /eniq/data/etldata/09/dc_e_abc/
            failedDir = new File(tmp, failedDirName);
            log.info("Failed Directory is set to: " + failedDir.getPath());
        }
        
        return failedDir.getPath();
    }

    /**
     * Loads a list of files to table defined.
     */
    protected void loadTable(final String tableName, final List<String> files, final String loadTemplate, final VelocityEngine vengine,
                             final Statement statement, final String loadParameters) throws IOException, SQLException, ParseErrorException,
            MethodInvocationException, ResourceNotFoundException {
    	
    	int rowcount;
    	final String collecName = this.set.getCollection_name();
    	log.fine("Load table " + tableName + " " + files.size() + " files");

        if ("TABLE_NOT_FOUND".equals(tableName)) {

            fileLog.info(formatFileNamesForLog(files, " not loaded, physical table not found."));
            log.warning("Physical table not found for files" + formatFileNamesForLog(files, ""));
            moveFilesToFailed(files);
        } else {

            final StringWriter pwriter = new StringWriter();
            final StringWriter writer = new StringWriter();
            final VelocityContext loaderParameterContext = new VelocityContext();
            fillVelocityContext(loaderParameterContext);

            loaderParameterContext.put("LOG_DIR", System.getProperty("LOG_DIR"));
            loaderParameterContext.put("REJECTED_DIR", System.getProperty("REJECTED_DIR"));
            vengine.evaluate(loaderParameterContext, pwriter, "", loadParameters);

            final VelocityContext context = new VelocityContext();
            context.put("TABLE", tableName);
            context.put("FILENAMES", getFileNamesInCorrectFormat(files, loadTemplate.length(), tableName));
            context.put("FILENAMES_LIST", files);

            context.put("LOADERPARAMETERS", pwriter.toString());
            context.put("LOG_DIR", System.getProperty("LOG_DIR"));
            context.put("LOCAL_LOGS_DIR", System.getProperty("LOCAL_LOGS_DIR"));
            context.put("REJECTED_DIR", System.getProperty("REJECTED_DIR"));
            log.finest("SQL velocitytemplate context values: TABLE = " + context.get("TABLE") + ", FILENAMES = " + context.get("FILENAMES")
                    + ", LOADERPARAMETERS = " + context.get("LOADERPARAMETERS"));
            fillVelocityContext(context);

            vengine.evaluate(context, writer, "", loadTemplate);
            log.info("Executing load table command for table " + tableName);
            log.fine("loadTable loadSQL = " + writer.toString());

            sqlLog.finer(writer.toString());
            try {
                final String loadSQL = writer.toString();
                if (loadSQL.length() >= (maxLoadClauseLength)) {
                    log.severe("Load clause was too long (" + loadSQL.length() + "). Something will go wrong!");
                }

                final long start = System.currentTimeMillis();
                if (!collecName.contains("Loader_DIM_") ){
                	rowcount = statement.executeUpdate(loadSQL);
                }
                else {
                	rowcount = executeTopology(statement, loadSQL, tpName, collecName);	
                }
                log.finer("Performance: Loading " + rowcount + " rows to " + tableName + " took " + (System.currentTimeMillis() - start) + " ms");

                totalRowCount += rowcount;
                log.info("Load table returned. " + rowcount + " rows loaded.");

                if (rowcount == 0) {
                    log.warning("ZERO ROWS LOADED TO TABLE " + tableName + " from file(s) " + context.get("FILENAMES") + "\nusing SQL:\n" + loadSQL);
                }

                insertDataIntoDuplicateTable(tableName, statement);
                
            } catch (SQLException e) {
                if (e.getMessage().contains("You have run out of space")) {
                    sqlErrLog.log(Level.WARNING, "Load table failed due to db space issues : files will not be moved to failed ", e);
                } else if ((e.getMessage().contains("SQL Anywhere Error -1009134")) || (e.getMessage().contains("Insufficient buffers for 'Sort'."))) {
                   sqlErrLog.log(Level.WARNING, "Load table failed due to insufficient buffers for sort issues", e);
                } else if (e.getMessage().contains("SQL Anywhere Error -210")) {
                   sqlErrLog.log(Level.WARNING, "Load table failed, because another operation is in progress on table: files will not be moved to failed ", e);
                } else if (e.getMessage().contains("Error in Re-Triggering Topology set")){
                   log.log(Level.WARNING, "Re-Triggering failed. Topology Files moved to failed directory", e);
                   isMoveToFailedNeeded = true;
                }else if (e.getMessage().contains("row delimiter")){
        			log.log(Level.WARNING,"Loader sets failed for delimiter issue");
                	listFailedLoaderFiles(files);
                	isMoveToFailedNeeded = true;      
                }  else {
                    sqlErrLog.log(Level.WARNING, "Load table failed exceptionally, files moved to failed ", e);
                    sqlErrLog.info(writer.toString());
                    isMoveToFailedNeeded = true;
                }
                statement.getConnection().rollback();
                log.finer("Explicit rollback performed");
                
                if(isMoveToFailedNeeded) {
                	// Load Table failed exceptionally, moving files to failed directory.
                	moveFilesToFailed(files);
                	isMoveToFailedNeeded = false;
                }
                
               throw e;
            }

            statement.getConnection().commit();
            log.finer("Explicit commit performed");
            fileLog.info(formatFileNamesForLog(files, " loaded to table " + tableName + "."));
           //checks if 2 week back up and restore is enabled for the measurement type or not and proceeds further otherwise deletes file as per normal flow.
            if(isBackup())
            {
            	log.info("2 Week Backup and Restore enabled.");
            	moveToProcessed(files);
            }
            else
            {
            	deleteFiles(files);
            }
            

        }

    }
//executes the executeUpdate for Topology loaders
    protected int executeTopology(Statement statement, String loadSQL,String tpName, String collecName) throws SQLException, IOException {
    	int rowcount = 0;	
    	try{
    		//First Execution
    		rowcount = statement.executeUpdate(loadSQL);
    	}
    	catch(SQLException se){
    		if ((se.getMessage().contains("SQL Anywhere Error -1009134")) || (se.getMessage().contains("Insufficient buffers for 'Sort'."))) {
    			try{
    				/*if file exists, search if the topologyLoader has executed for the day. If yes and if it has failed (due to insufficient sort for 'buffers')
					after Re-Triggering also, then move the fails to failed directory.
					Else continue with Re-Execution.*/
    				if(f.exists()){
    					try{
    						fstream = new FileInputStream(f);
    						br = new BufferedReader(new InputStreamReader(fstream));
    						String strLine;			  
    						while ((strLine = br.readLine()) != null)  {
    							if(strLine.contains( tpName + " : " + collecName )){
    					    		statFlag = true;
    					    		break;
    							}
    							else{
    	    						statFlag = false;
    							}
    						}
    					}
    					catch(Exception fe){
    						fe.getMessage();
    					}
    				}
    				
    				if(statFlag){
    					throw new SQLException("Error in Re-Triggering Topology set");
    				}
    				else{
    					//Re-executing as the Topology loader wasn't Re-Triggered before
    					log.info("Re-Triggering the " +collecName+ " and re-executing the loadSQL");
    					rowcount = statement.executeUpdate(loadSQL);
    				}
    			}
    			catch(SQLException sqle){
    				if ((sqle.getMessage().contains("SQL Anywhere Error -1009134")) || (sqle.getMessage().contains("Insufficient buffers for 'Sort'."))) {
    	    			aggregatorLog.info(tpName + " : " + collecName);
    	    				
    	    			//In Case of Topology Files, move the files to Failed Directory.
    	    			isMoveToFailedNeeded = true;
    	    				
    	    			//Throwing Exception so as to stop this set and trigger new Topology_loader set for particular collection name
    	    			//throws SQL Anywhere Error -1009134 
    	    			throw sqle;
    				}
    				else{
    					//To stop the execution as re-triggering also failed.
    					//throws "Error in Re-Triggering Topology set"	
    					throw sqle;
    				}
    			}		
    		}
    		else{
    			log.info("exception is due to "+ se.getMessage());
    		}
    	}
    	return rowcount;
	}
    
   
	

	/**
     * @param tableName
     * @param statement
     * @throws SQLException
     */
    protected void insertDataIntoDuplicateTable(final String tableName, final Statement statement) throws SQLException {
        if (fileDuplicateCheck) {
            insertToDuplicateTable(tableName, statement);
        }
    }

    /**
     * Loads a list of files to table defined.
     */
    protected void loadTempTable(final String tempTableName, final String tableName, final List<String> files, final String loadTemplate,
                                 final VelocityEngine vengine, final Statement statement, final String loadParameters) throws IOException,
            SQLException, ParseErrorException, MethodInvocationException, ResourceNotFoundException {

        //20120314 eanguan efaigha :: Run the preLoadSQL queries -- SON15AGG work   
        /*
         * String preLoadSqlQuery = "drop table if exists " + tempTableName + " ; "; preLoadSqlQuery += " SELECT * INTO " + tempTableName + " FROM " +
         * tableName + " where 1=2 ; ";
         */

        //20120706: efaigha DROP TABLE IF EXISTS query does not work in multiplex IQ environment.Jira SONV-1149
        final String preLoadSqlQuery = "IF NOT EXISTS (select * FROM sys.systable WHERE table_name = '" + tempTableName + "')"
                + "BEGIN SELECT * INTO " + tempTableName + " FROM " + tableName + " where 1=2 END ELSE  BEGIN TRUNCATE TABLE " + tempTableName
                + " END";
        boolean isTempTableCreationSucc = false;

        log.finer("Going to exectue: " + preLoadSqlQuery);
        //Making sure that temptable gets populated
        int times = 0;
        while (true) {
            times++;
            if (times > 5) {
                log.warning(" Failed to run query: " + preLoadSqlQuery + "\n Give up after trying : " + times
                        + " times. SON15AGG will not work for this ROP.");
                break;
            }
            try {
                log.info("Trying " + times + " time.");
                statement.execute(preLoadSqlQuery);
                isTempTableCreationSucc = true;
                break;
            } catch (final Exception e) {
                log.log(Level.WARNING, " Failed to run query: " + preLoadSqlQuery + "\n Trying again:", e);
                isTempTableCreationSucc = false;
                //Sleeping for 1 sec
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    // Nothing to do
                }
            }//catch
        }//while

        //statement.getConnection().commit();
        if (isTempTableCreationSucc) {
            log.fine("Load table " + tempTableName + " " + files.size() + " files");

            if (tempTableName != null && tempTableName.isEmpty()) {
                //fileLog.info(formatFileNamesForLog(files, " not loaded, physical table not found."));
                log.warning("Physical table not found for files" + formatFileNamesForLog(files, ""));
            } else {
                final StringWriter pwriter = new StringWriter();
                final StringWriter writer = new StringWriter();
                final VelocityContext loaderParameterContext = new VelocityContext();
                fillVelocityContext(loaderParameterContext);

                loaderParameterContext.put("LOG_DIR", System.getProperty("LOG_DIR"));
                loaderParameterContext.put("REJECTED_DIR", System.getProperty("REJECTED_DIR"));
                vengine.evaluate(loaderParameterContext, pwriter, "", loadParameters);

                final VelocityContext context = new VelocityContext();
                context.put("TABLE", tempTableName);
                context.put("FILENAMES", getFileNamesInCorrectFormat(files, loadTemplate.length(), tempTableName));
                context.put("FILENAMES_LIST", files);

                context.put("LOADERPARAMETERS", pwriter.toString());
                context.put("LOG_DIR", "");
                context.put("REJECTED_DIR", "");
                log.finest("SQL velocitytemplate context values: TABLE = " + context.get("TABLE") + ", FILENAMES = " + context.get("FILENAMES")
                        + ", LOADERPARAMETERS = " + context.get("LOADERPARAMETERS"));
                fillVelocityContext(context);
                vengine.evaluate(context, writer, "", loadTemplate);
                log.info("Executing load table command for table " + tempTableName);
                log.fine("loadTempTable loadSQL = " + writer.toString());
                sqlLog.finer(writer.toString());
                final int rowcount;
                try {
                    final String loadSQL = writer.toString();
                    if (loadSQL.length() >= (maxLoadClauseLength)) {
                        log.severe("Load clause was too long (" + loadSQL.length() + "). Something will go wrong!");
                    }

                    final long start = System.currentTimeMillis();

                    rowcount = statement.executeUpdate(loadSQL);
                    log.finer("Performance: Loading " + rowcount + " rows to " + tempTableName + " took " + (System.currentTimeMillis() - start)
                            + " ms");

                    totalRowCount += rowcount;
                    log.info("Load table returned. " + rowcount + " rows loaded.");
                    if (rowcount == 0) {
                        log.warning("ZERO ROWS LOADED TO TABLE " + tempTableName + " from file(s) " + context.get("FILENAMES") + "\nusing SQL:\n"
                                + loadSQL);
                    }
                } catch (SQLException e) {
                    if (e.getMessage().contains("You have run out of space")) {
                        sqlErrLog.log(Level.WARNING, "Load table failed due to db space issues : files will not be moved to failed ", e);
                    } else {
                        sqlErrLog.log(Level.WARNING, "Load table failed exceptionally. ", e);
                        sqlErrLog.info(writer.toString());
                    }
                    statement.getConnection().rollback();
                    log.finer("Explicit rollback performed");
                }//catch
            }//else

            statement.getConnection().commit();
            log.finer("Explicit commit performed");
            fileLog.info(formatFileNamesForLog(files, " loaded to table " + tempTableName + "."));

        }//if(isTempTableCreationSucc)

    }//function end

    /**
     * Formats a filename list to suitable format for SQL-clause
     * 
     * @param fileNames
     * @return
     */
    protected String getFileNamesInCorrectFormat(final List<String> fileNames, final int loadTemplateLength, final String physicalTableName)
            throws IOException {
        final StringBuffer fileNamesStr = new StringBuffer();
        int fName_ix = 0;
        for (; fileNamesStr.length() < (maxLoadClauseLength - loadTemplateLength - 1000) && fName_ix < fileNames.size(); fName_ix++) {
            final String fileName = fileNames.get(fName_ix);
            if (fName_ix > 0) {
                fileNamesStr.append(",");
            }
            fileNamesStr.append("'").append(fileName).append("'");
        }
        if (fName_ix < fileNames.size()) { // No space for all files in SQL
            log.fine(fName_ix + " files out of " + fileNames.size() + " inserted into SQL for: " + physicalTableName);
            for (; fName_ix < fileNames.size();) {
                fileNames.remove(fName_ix);
            }
        }
        return fileNamesStr.toString().replaceAll("\\\\", "/"); // convert \ -> /
    }

    /**
     * Formats a filename list to suitable format for logs
     * 
     * @param fileNames
     * @param status
     * @return
     */
    protected String formatFileNamesForLog(final List<String> fileNames, final String status) {
        final StringBuilder fileNamesStr = new StringBuilder("\n");

        for (String fileName : fileNames) {
            fileNamesStr.append(fileName).append(status).append("\n");
        }

        return fileNamesStr.toString();
    }

    /**
     * Moves a file to a directory. files is renamed, if that does not work memorycopy is used. If output directory does not exsist, it will be
     * created.
     * 
     * @return
     * @throws Exception
     */
    protected boolean moveToDirectory(final File outputFile, String destDir) {
        if (!destDir.endsWith(File.separator)) {
            destDir += File.separator;
        }
        if (!new File(destDir).exists()) {
            log.log(Level.INFO, "Creating directory " + destDir);
            new File(destDir).mkdirs();
        }
        final File targetFile = new File(destDir + outputFile.getName());
        log.finer("Moving file " + outputFile.getName() + " to " + destDir);
        boolean moveSuccess = outputFile.renameTo(targetFile);
        if (!moveSuccess) {
            log.finer("renameTo failed. Moving with memory copy");
            try {
                final InputStream in = new FileInputStream(outputFile);
                final OutputStream out = new FileOutputStream(targetFile);
                final byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                in.close();
                out.close();
                outputFile.delete();
                moveSuccess = true;
            } catch (Exception e) {
                log.log(Level.WARNING, "Move with memory copy failed", e);
            }
        }
        return moveSuccess;
    }

    /**
     * Creating a list of tables which include null values in their rowstatus column.
     * 
     * TODO: This should be reworked. Looks bad and unioning with same table twice does not make sense.
     * 
     * @return List of tables to be loaded.
     * @throws Exception
     */
    private List<String> createTableList() throws SQLException {
        final List<String> tableList = new ArrayList<String>();

        final PhysicalTableCache ptc = PhysicalTableCache.getCache();
        log.finest("ptc found");

        final String storageID = tablename.trim() + ":RAW";
        log.finest("storageID " + storageID);
        final List<String> activeTables = ptc.getActiveTables(storageID);

        final StringBuilder sqlClause = new StringBuilder();
        final String selectPart = "\n SELECT DISTINCT date_id ";
        final String fromPart = "\n FROM ";
        final String wherePartForNulls = "\n WHERE rowstatus IS NULL AND date_id IS NOT NULL ";
        final String wherePartForEmpties = "\n WHERE rowstatus = '' AND date_id IS NOT NULL ";
        final String unionPart = "\n UNION ";

        final int activeTablesSize = activeTables.size();
        log.finest("activeTablesSize = " + activeTablesSize);
        if (activeTablesSize > 0) {
            for (int i = 0; i < activeTablesSize; i++) {
                final String partitionTable = activeTables.get(i);
                log.finest("partitionTable = " + partitionTable);

                sqlClause.append(selectPart).append(fromPart).append(partitionTable).append(wherePartForNulls);
                sqlClause.append(unionPart);
                sqlClause.append(selectPart).append(fromPart).append(partitionTable).append(wherePartForEmpties);
                if (i < activeTablesSize - 1) {
                    sqlClause.append(unionPart);
                }
            }
            sqlClause.append(";");
            log.finest("sqlClause " + sqlClause.toString());
        } else {
            log.fine("No active tables found for storageID: " + storageID);
        }

        final RockFactory r = this.getConnection();

        Statement s = null;
        ResultSet resultSet = null;
        try {
            if (sqlClause.length() > 0) {
                s = r.getConnection().createStatement();
                resultSet = s.executeQuery(sqlClause.toString());
            }
            if (activeTablesSize > 0 && null != resultSet) {
                while (resultSet.next()) {
                    for (int i = 0; i < activeTablesSize; i++) {
                        final Long startTime = ptc.getStartTime(activeTables.get(i));
                        final Long endTime = ptc.getEndTime(activeTables.get(i));

                        final Date tableDate = resultSet.getDate(1);
                        log.finest("resultSet.getDate(1) " + tableDate);

                        if (tableDate.getTime() >= startTime && tableDate.getTime() < endTime) {
                            log.finest("activeTables.get(i) " + activeTables.get(i));
                            if (!tableList.contains(activeTables.get(i))) {
                                tableList.add(activeTables.get(i));
                            }
                        }
                    }

                }
            }
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (Exception e) {
                log.log(Level.FINER, "Cleanup error", e);
            }

            try {
                if (s != null) {
                    s.close();
                }
            } catch (Exception e) {
                log.log(Level.FINER, "Cleanup error", e);
            }

        }

        log.fine("Table list created " + tableList.size() + " tables discovered");

        return tableList;
    }

    /**
     * Determine prober loggernames for log, sqlLog and fileLog
     */
    protected abstract void initializeLoggers();

    /**
     * Should create Map tableName -> List of File-objects
     * 
     * @return
     * @throws Exception
     */
    protected abstract Map<String, List<String>> getTableToFileMap() throws ConfigurationException, FileException, RockException, SQLException;

    /**
     * Add needed values to velocityContext before evaluating
     */
    protected abstract void fillVelocityContext(VelocityContext context);

    /**
     * Makes implementations specific modifications to sessionLogEntry.
     */
    protected abstract void updateSessionLog();

    /**
     * Creates temporary table for duplicate checking loadfiles
     */
    protected void createTemporaryTable(final Statement statement) throws SQLException {

        final String createTempTable = "DECLARE LOCAL TEMPORARY TABLE LOADFILES_" + tablename + " (FILENAME varchar(255) NOT NULL)";
        sqlLog.finest(createTempTable);

        final long start = System.currentTimeMillis();
        statement.execute(createTempTable);

        log.finer("Performance: Temporary table creation took " + (System.currentTimeMillis() - start) + " ms");
    }

    /**
     * Removes already loaded files (duplicates) from filelist. Loadfiles are stored in temporary table LOADFILES_<tablename>.
     */
    protected List<String> pruneDuplicates(final List<String> fileList, final Statement statement) throws SQLException {
        final long start = System.currentTimeMillis();

        final StringBuilder sql = new StringBuilder();
        for (String file : fileList) {
            sql.append("INSERT INTO LOADFILES_").append(tablename).append(" VALUES ('").append(file).append("');\n");
        }
        sql.append("DELETE FROM LOADFILES_").append(tablename);
        sql.append(" WHERE FILENAME IN (SELECT lf.FILENAME FROM LOADFILES_").append(tablename);
        sql.append(" lf, ").append(tablename).append("_DUBCHECK fl WHERE lf.FILENAME = fl.FILENAME)");

        sqlLog.finest(sql.toString());

        statement.executeUpdate(sql.toString());

        log.finer("Performance: Insert into temptable and prune took " + (System.currentTimeMillis() - start) + " ms");

        final long startQuery = System.currentTimeMillis();
        final List<String> prunedList = new ArrayList<String>();
        ResultSet rs = null;

        try {
            rs = statement.executeQuery("SELECT FILENAME FROM LOADFILES_" + tablename);

            while (rs.next()) {
                prunedList.add(rs.getString(1));
            }

        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        fileList.removeAll(prunedList);

        for (String fileName : fileList) {
            log.fine("Duplicate check removing file: " + fileName);
            final File file = new File(fileName);
            file.delete();
            fileLog.finest(file.toString() + " deleted as duplicate");
        }

        log.finer("Performance: Creating filelist took " + (System.currentTimeMillis() - startQuery) + " ms");

        return prunedList;
    }

    /**
     * Insert list of loaded files into duplicate check table.
     */
    protected void insertToDuplicateTable(final String physicalTableName, final Statement statement) throws SQLException {
        final long updstart = System.currentTimeMillis();
        final StringBuilder update = new StringBuilder();
        update.append("INSERT INTO ").append(tablename).append("_DUBCHECK");
        update.append(" SELECT '").append(physicalTableName).append("' as TABLENAME, FILENAME FROM LOADFILES_").append(tablename).append(";\n");
        update.append("TRUNCATE TABLE LOADFILES_").append(tablename);

        sqlLog.finest(update.toString());

        statement.executeUpdate(update.toString());

        log.finer("Performance: Insert to duplicate table update took " + (System.currentTimeMillis() - updstart) + " ms");
    }

    /**
     * Gets the DataformatCache.
     * 
     * @return DataformatCache instance.
     */
    protected DataFormatCache getDataformatCache() {
        return DataFormatCache.getCache();
    }

    protected String getTpName() {
        return tpName;
    }

    protected void setTpName(final String tpName) {
        this.tpName = tpName;
    }

    /**
     * Wrapper for making junits easier
     * 
     * @return value from CommonUtils.getNumOfDirectories()
     */
    protected int getNumOfDirectories() {
        return CommonUtils.getNumOfDirectories(log);
    }

    /**
     * Expand and etldata(_) path to etldata(_)/00/ ... etldata(_)/NN/
     * 
     * @param linkPath
     *            Path to expand
     * @return List of etldata digit dirs
     */
    public List<File> expandEtlPaths(final File linkPath) {
        final int numDirs = getNumOfDirectories();
        return CommonUtils.expandEtlPathWithMountPoints(linkPath.getPath(), numDirs);
    }
    /**
     * Function to move file to processed directory and further to
     * backup filesytem if 2 week back up and restore is enabled.
     */
   protected void moveToProcessed(final List<String> fileList)
   {
	   for (String fileName : fileList) {
		   if(isRestoreEnabled() && fileName.startsWith(BACKUP_DIR)){
			   if(parseEpochTime(fileName) < restoreStartEpoch){
				   log.fine("File " + fileName + " will not be backed up as it is a restored file.");
				   final File file = new File(fileName);
				   file.delete();
				   fileLog.finest(file.toString() + " deleted");
				   continue;
			   }
		   }
		   final File file = new File(fileName);
		   File tmp = new File(fileName);
		   while (tmp != null && !tmp.getName().equalsIgnoreCase(tablename)) {
			   tmp = tmp.getParentFile();
		   }
		   if(tmp == null && tablename.toLowerCase().endsWith("_prev")){
	        	tmp = new File(fileName);
	        	String type_prev = tablename.substring(0, tablename.toLowerCase().lastIndexOf("_prev"));
	        	log.finest("typename (for prev tables) - " + type_prev);
	        	while (tmp != null && !tmp.getName().equalsIgnoreCase(type_prev)) {
	                tmp = tmp.getParentFile();
	            }
	        }
	       
		   File	processedDir;
	       if (tmp == null) {
	    	   //shouldn't happen?!?!
	    	   processedDir = new File(System.getProperty("ETLDATA_DIR", "/eniq/data/etldata"), tablename.toLowerCase().trim() + File.separator
	                    + "processed");
	           log.warning("Couldn't work out processed dir based on '" + fileName + "', defaulting to " + processedDir.getPath());
	        } else {
	            //either at /eniq/data/etldata/dc_e_abc/ or /eniq/data/etldata/09/dc_e_abc/
	        	processedDir = new File(tmp, "processed");
	        }
		   
		   String processed=processedDir.getPath();
		   if (!processed.endsWith(File.separator)) {
			   processed += File.separator;
		   }
		   if(moveToDirectory(file, processed))
		   {
			   LoaderBackupHandling handler=LoaderBackupHandling.getinstance() ;
			   handler.processMessage(tablename,log,processed + file.getName());
		   }
       } 
   }
    /**
     * Checks whether 2 week backup and restore 
     * is enabled for the measurement type or not.
     * 
     * Returns true if all the below are fulfilled otherwise 
     * returns false.
     * 1)2 Week Backup and Restore functionality is enabled.
     * 2)Raw backup option is enabled.
     * 3)Functionality is enabled for measurement type.
     */
    protected boolean isBackup()
    {
    	final String collecName = this.set.getCollection_name();
    	if(BackupConfigurationCache.isBackupStatus()){
    		log.finest("2 Week Backup and Restore functionality is enabled.");
    		if(BackupConfigurationCache.getCache().getBackupLevel().equalsIgnoreCase("RAW") || (BackupConfigurationCache.getCache().getBackupLevel().equalsIgnoreCase("BOTH")))
    		{
    			log.finest("Raw backup option is enabled.");
    			if((BackupConfigurationCache.getCache().isBackupActive(tablename)) && (collecName.startsWith("Loader_DC") || collecName.startsWith("Loader_PM")))
    			{
    				log.finest("Functionality is enabled for measurement type"+tablename);
    				return true;
    			}
    		}
    	}
    	return false;
    	
    }

	/**
     * Checks if restore flag is enabled, 
     * determines the restore cut-off point if enabled
     * 
     * @return true if Restore enabled
     * 		   false if restore disabled
     */
    protected boolean isRestoreEnabled() {
    	// 1. Check if restore flag exists
    	File restoreParent = new File(RESTOREPARENTFOLDER);
    	File[] restoreFlag = restoreParent.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(RESTOREFLAGNAME);
			}
		});
    	if(restoreFlag.length == 1 && restoreFlag[0].exists() && restoreFlag[0].isFile()){
    		// 2. Extract restore timestamp from flag filename
    		this.restoreStartEpoch = Long.parseLong(parseFileName(restoreFlag[0].getName(), ".*_(.+)"));
    		log.finest("restore flag name - " + restoreFlag[0].getName());
    		log.finest("restore start time - " + restoreStartEpoch);
    		sctx.put("RESTORE_FLAG", true);
    		sctx.put("RESTORE_TIMESTAMP", restoreStartEpoch);
    		return true;
    	}
    	else if (restoreFlag.length == 0){
    		log.fine("Restore is not enabled");
    	}
    	else{
    		log.warning("Multiple restore flags, please remove those not required");
    		String fileString = null;
    		for (File file : restoreFlag){
    			fileString = fileString + file.getAbsolutePath() + ",";
    		}
    		log.info("List of restore flags - " + fileString);
    	}
    	sctx.put("RESTORE_FLAG", false);
    	return false;
    }
    
    /**
     * Parses filename, extracts epoch timestamp
     * 
     * @param filename
     * @return epoch timestamp in filename
     */
    protected long parseEpochTime(String filename) {
    	String epochString = null;
    	String regEx = null;
    	String typeUnPrev = tablename.trim();
    	if (typeUnPrev.endsWith("_PREV")){
    		typeUnPrev = typeUnPrev.substring(0, typeUnPrev.indexOf("_PREV"));
    	}
    	log.finest("Typename of measurement - " + typeUnPrev);
    	if(filename.startsWith(BACKUP_DIR)){
    		regEx = "/eniq/flex_data_bkup/" + tablename.trim() + "/raw/" + typeUnPrev;
    	}
    	else{
    		regEx = typeUnPrev;
    	}
    	epochString = filename.substring(regEx.length() + 1);
		epochString = epochString.substring(0, epochString.indexOf("_"));
		epochString = removeIndex(epochString);
		log.finest("Epoch time parsed from filename - " + epochString.trim());
    	return Long.parseLong(epochString.trim());
	}

    /**
     * @param epochString
     * @return
     */
    private String removeIndex(String epochString) {
		final String currentEpoch = Long.toString(System.currentTimeMillis());
		if(epochString.length() > currentEpoch.length()){
			log.finest("Timestamp in filename has index - " + epochString);
			epochString = epochString.substring(0,currentEpoch.length());
		}
		return epochString;
	}

	/**
	 * Extracts a substring from given string based on given regExp
	 * 
	 */
	public String parseFileName(final String str, final String regExp) {
		final Pattern pattern = Pattern.compile(regExp);
		final Matcher matcher = pattern.matcher(str);
		final String BUILDNOREGEX = ".+_b(.+)_.+";
		if (matcher.matches()) {
			final String result = matcher.group(1);
			log.finest(" regExp (" + regExp + ") found from " + str + "  :" + result);
			return result;
		} else {
			if(regExp.equalsIgnoreCase(BUILDNOREGEX)){
				log.info("String " + str + " doesn't match defined REG-EXP or BUILDNOREGEX " + regExp); //will be issue if build no. not in load files (remove when new parser confirmed)
			}
			else{
				log.info("String " + str + " doesn't match defined REG-EXP  " + regExp);
			}
		}
		return "";
	}
	
	/**
	 * Generates load query when loading data for old version of TP
	 * 
	 * @return load query template
	 */
	private String getOldVersionLoadTemplate(String buildNo, boolean ASCII) {
		String oldColSeq = getColumnSeqOld(buildNo, ASCII);
		log.finest("Column sequence for " + buildNo + " - " + oldColSeq);
		if(oldColSeq == null){
			return null;
		}
        final StringBuilder loadTemplate = new StringBuilder("LOAD TABLE $TABLE (");
        loadTemplate.append(oldColSeq);
        loadTemplate.append(") FROM $FILENAMES $LOADERPARAMETERS;\n");
        return loadTemplate.toString();
	}

	/**
	 * Get the column sequence for the old version
	 * 
	 * @param buildNo
	 * @param aSCII
	 * @return column sequence string
	 */
	private String getColumnSeqOld(String buildNo, boolean aSCII) {
		final String BWNB = (aSCII) ? "" : " BINARY WITH NULL BYTE";
		final StringBuilder columns = new StringBuilder();
		Connection con = null;
		Statement stmt = null;
		ResultSet oldRSet = null;
		try{
			String dFormatID = dataformat.getDataFormatID();
			String oldDFormatID = dFormatID.substring(0, dFormatID.indexOf("(") + 2) + buildNo + dFormatID.substring(dFormatID.indexOf(")"));
			String sql = "SELECT DATANAME FROM DataItem WHERE DATAFORMATID = '" + oldDFormatID + "' ORDER BY COLNUMBER ";
			con = DatabaseConnections.getDwhRepConnection().getConnection();
			stmt =  con.createStatement();
			oldRSet = stmt.executeQuery(sql);
			while(oldRSet.next()){
				columns.append(oldRSet.getString("DATANAME")).append(BWNB);
                columns.append(COMMA_SEPARATOR);
			}
		}
		catch (Exception e){
			log.warning("Could not generate load query with old column sequence, will generate default");
			return null;
		}
		finally{
			if(oldRSet != null){
				try {
					oldRSet.close();
					stmt.close();
					con.close();
				} catch (SQLException e) {
					log.warning("Error closing connection during generation of old version load query" + e);
				}
			}
		}
		if(columns.toString() == null || columns.toString().isEmpty()){
			return null;
		}
        return columns.toString().substring(0, columns.toString().lastIndexOf(COMMA_SEPARATOR));
	}
}
