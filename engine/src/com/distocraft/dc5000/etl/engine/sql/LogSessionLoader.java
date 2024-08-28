package com.distocraft.dc5000.etl.engine.sql;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;

/**
 * Loader implementation for loading into LOG_SESSION_* tables. <br>
 * Copyright Distocraft 2005 <br>
 * 
 * @author lemminkainen
 */
public class LogSessionLoader extends Loader {

    final private String name;

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    //private static PhysicalTableCache ptc = null;

    public LogSessionLoader(final Meta_versions version, final Long collectionSetId, final Meta_collections collection, final Long transferActionId,
                            final Long transferBatchId, final Long connectId, final RockFactory rockFact, final ConnectionPool connectionPool,
                            final Meta_transfer_actions trActions, final SetContext sctx, final Logger clog) throws EngineMetaDataException {

        super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool, trActions, sctx, clog);

        /*
         * if (ptc == null) { ptc = PhysicalTableCache.getCache(); }
         */

        final String where = this.getTrActions().getWhere_clause();

        whereProps = TransferActionBase.stringToProperties(where);

        if (whereProps == null) {
            whereProps = new Properties();
            whereProps.setProperty("where", where);
        }

        this.name = whereProps.getProperty("logname");

        notifyTypeName = this.tablename;

    }

    /**
     * Determine prober loggernames for log, sqlLog and fileLog
     */
    @Override
    protected void initializeLoggers() {

        final String logname = log.getName() + ".LogSessionLoader";

        log = Logger.getLogger(logname);

        final String logname_pfx = logname.substring(logname.indexOf("."));

        fileLog = Logger.getLogger("file." + logname_pfx + ".LogSessionLoader");
        sqlLog = Logger.getLogger("sql." + logname_pfx + ".LogSessionLoader");
        sqlErrLog = Logger.getLogger("sqlerror." + logname_pfx + ".LogSessionLoader");

    }

    /**
     * Creates Map tableName -> List of filename strings
     */
    @Override
    protected Map<String, List<String>> getTableToFileMap() {

        SessionHandler.rotate(name);

        //		final String typeDir = "/session/" + name;
        final String inDir = System.getProperty("ETLDATA_DIR") + "/session/" + name;
        //		final String ETLDATA_DIR = System.getProperty("ETLDATA_DIR");
        //		final File inDir = new File(ETLDATA_DIR, typeDir);
        //		final List<File> etlSearchDirs = expandEtlPaths(inDir);
        //		
        //		if(etlSearchDirs.isEmpty()){
        //			log.warning("In directory " + inDir.getPath() + " not found or cannot be read");
        //		}

        final FilenameFilter filter = new FilenameFilter() {

            @Override
            public boolean accept(final File dir, final String fname) {
                return fname.startsWith(name) && !fname.endsWith(".unfinished");
            }
        };

        final File[] files = new File(inDir).listFiles(filter);
        final Map<String, List<String>> tableMap = new HashMap<String, List<String>>();

        //		for(File etlSub : etlSearchDirs) {		      
        //			log.finest("inDir: " + etlSub.getPath());

        //			if (!etlSub.isDirectory() || !etlSub.canRead()) {
        //				log.warning("In directory " + etlSub + " not found or cannot be read");
        //			}

        //			final File[] files = etlSub.listFiles(filter);
        if (files == null || files.length == 0) {
            return tableMap;
            //				log.info("No files found on input directory " + etlSub.getPath());
            //				continue; // Look for the next directory
        }

        final String storageID = "LOG_SESSION_" + name + ":PLAIN";

        for (File file : files) {
            handleFile(tableMap, storageID, file);
        }
        //		}
        return tableMap;
    }

    /**
     * Handles one input file and add it into tableMap
     */
    private void handleFile(final Map<String, List<String>> tableMap, final String storageID, final File file) {
        Date datadate = null;
        String datestamp = null;
        // Catch date format error in order to process files with invalid filename
        // format
        try {
            datestamp = file.getName().substring(file.getName().lastIndexOf(".") + 1);
            datadate = sdf.parse(datestamp);

            String tableName = datadate == null ? null : PhysicalTableCache.getCache().getTableName(storageID, datadate.getTime());

            if (tableName == null && datestamp != null) {
                //If tableName is null, attempt a refresh of cache and try once more
                PhysicalTableCache.getCache().revalidate();

                tableName = datadate == null ? null : PhysicalTableCache.getCache().getTableName(storageID, datadate.getTime());
                if (tableName == null) {
                    log.info("Session load failure: Can't find table for " + storageID + " date " + datestamp);
                } else {
               	 log.info("TABLENAME :: "+tableName);
                }
            }

            List<String> filelist = tableMap.get(tableName);

            if (filelist == null) {
                filelist = new ArrayList<String>();
                tableMap.put(tableName, filelist);
            }

            filelist.add(file.toString());
        } catch (Exception e) {
            log.info("Session load failure: Can't parse date from filename: " + file.getName());
        }
    }

    /**
     * Add needed values to velocityContext before evaluating
     */
    @Override
    protected void fillVelocityContext(final VelocityContext context) {
        final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());
        context.put("DATE", dateTimeFormat.format(new Date()));
        context.put("CHECKPOINT", StaticProperties.getProperty("UnPartitionedLoader.checkpoint", "OFF"));
        context.put("NOTIFY_ROWS", StaticProperties.getProperty("UnPartitionedLoader.notify_rows", "100000"));
    }

    @Override
    protected String getExtraLoadParamsIfRowLoggingIsSet(final String defaultRowLogging) {
        final StringBuilder extraLoadParams = new StringBuilder();
        if (defaultRowLogging.equalsIgnoreCase("true")) {
            final File iqDir = new File(System.getProperty("LOCAL_LOGS_DIR", "/eniq/local_logs"), "iq");
            final File msgFile = new File(iqDir, "SESSION_${DATE}_msg.log");
            final File rowFile = new File(iqDir, "SESSION_${DATE}_row.log");
            extraLoadParams.append("MESSAGE LOG '").append(msgFile.getPath()).append("' \n");
            extraLoadParams.append("ROW LOG '").append(rowFile.getPath()).append("' \n");
            extraLoadParams.append("ONLY LOG UNIQUE, NULL, DATA VALUE\n" + "LOG DELIMITED BY ';' \n");
        }
        return extraLoadParams.toString();
    }

    /**
     * Makes implementations specific modifications to sessionLogEntry.
     */
    @Override
    protected void updateSessionLog() {
    }

    /**
     * Query timeout will be set to 180 minutes [ For other loaders, query timeout will be 30 minutes by default ]. If
     * QueryTimeOutForSessionLogLoaderAdapter is defined in static properties files, it will be taken.
     */
    @Override
    public Statement getStatement() throws SQLException {

        final Statement statement = this.getConnection().getConnection().createStatement();
        // Execute with AutoCommit = false. This will be helpful to rollback in case
        // of exceptions.
        // Please see TR HP27220.
        statement.getConnection().setAutoCommit(false);
        log.finer("Setting AutoCommit to " + statement.getConnection().getAutoCommit());

        final String overrideForDefaultQueryTimeoutInMins = StaticProperties.getProperty("QueryTimeOutForSessionLogLoaderAdapter", "180");
        final int manualQueryTimeoutInMin = Integer.parseInt(overrideForDefaultQueryTimeoutInMins);
        statement.setQueryTimeout(manualQueryTimeoutInMin * 60);

        log.info("Setting query timeout to " + manualQueryTimeoutInMin * 60 + "seconds");

        return statement;
    }

}
