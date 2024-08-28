package com.distocraft.dc5000.etl.engine.sql;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.exception.ConfigurationException;
import com.ericsson.eniq.exception.FileException;

/**
 * Copyright Distocraft 2005 <br>
 * <br>
 * $id$ <br>
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
 * <td>Defines the base tablename of the loaded data. Actual tables (or
 * partitons) are retrieved just before actual load.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>techpack</td>
 * <td>Defines the teckpack of the loaded data.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>checkpoint</td>
 * <td>Parameter in loadTable command. This parameter is read first from static
 * properties (PartitionedLoader.checkpoint) then from normal action parameters
 * and if still no value default is used.</td>
 * <td>OFF</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>notify_rows</td>
 * <td>Parameter in loadTable command. This parameter is read first from static
 * properties (PartitionedLoader.notify_rows) then from normal action parameters
 * and if still no value default is used.</td>
 * <td>100000</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>filemask</td>
 * <td>Defines RegExp mask to filter files from input directory (raw dir).</td>
 * <td>.+</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>dateformat</td>
 * <td>Dafines the dateformat (simpleDateFormat) to parse the date (datadate)
 * found at the end of the loadDatafile.</td>
 * <td>yyyy-MM-dd</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>taildir</td>
 * <td>Defines the input directory in etldata/'meastype'/.</td>
 * <td>raw</td>
 * </tr>
 * <td>Load template</td>
 * <td>Action_contents column</td>
 * <td>Defines the velocity template for the actual sql clause.</td>
 * <td>&nbsp;</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * 
 * @author lemminkainen
 */
public class PartitionedLoader extends Loader {

  private Pattern patt = null;

  protected String storageID = null;

  protected Date currDate = null;

  protected String dateFormatString = null;

  public PartitionedLoader(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final SetContext sctx,
      final Logger clog) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
        trActions, sctx, clog);

    final Map<String, Object> sessionLogEntry = new HashMap<String, Object>();

    final long loadersetID;
    try {
      loadersetID = getSessionHandler();
    } catch (Exception e) {
      throw new EngineMetaDataException("Error getting loaderSetID", e, "init");
    }
    currDate = new Date();
    dateFormatString = whereProps.getProperty("dateformat", "yyyy-MM-dd");

    sessionLogEntry.put("LOADERSET_ID", String.valueOf(loadersetID));
    sessionLogEntry.put("SESSION_ID", "");
    sessionLogEntry.put("BATCH_ID", "");
    sessionLogEntry.put("TIMELEVEL", "");
    sessionLogEntry.put("DATATIME", "");
    sessionLogEntry.put("DATADATE", "");
    sessionLogEntry.put("ROWCOUNT", "");
    sessionLogEntry.put("SESSIONSTARTTIME", String.valueOf(System.currentTimeMillis()));
    sessionLogEntry.put("SESSIONENDTIME", "");
    sessionLogEntry.put("STATUS", "");
    sessionLogEntry.put("TYPENAME", "");

    sctx.put("sessionLogEntry", sessionLogEntry);

    final String where = this.getTrActions().getWhere_clause();

    whereProps = TransferActionBase.stringToProperties(where);

    if (whereProps == null) {
      whereProps = new Properties();
      whereProps.setProperty("where", where);
    }

    if (!whereProps.contains("checkpoint") || whereProps.getProperty("checkpoint").equalsIgnoreCase("")) {
      whereProps.setProperty("checkpoint", StaticProperties.getProperty("PartitionedLoader.checkpoint", "OFF"));
    }

    if (!whereProps.contains("notify_rows") || whereProps.getProperty("notify_rows").equalsIgnoreCase("")) {
      whereProps.setProperty("notify_rows", StaticProperties.getProperty("PartitionedLoader.notify_rows", "100000"));
    }

    notifyTypeName = this.tablename + "_" + this.tablelevel;

  }

  /**
   * This constructor is only intended for test purposes.
   */
  public PartitionedLoader() {
    super();
  }

  /**
   * Extracted out for unit testing
   * 
   * @return session handler id
   * @throws ConfigurationException .
   * @throws IOException .
   * @throws NoSuchFieldException .
   */
  protected long getSessionHandler() throws ConfigurationException, IOException, NoSuchFieldException {
    return SessionHandler.getSessionID("loader");
  }

  @Override
  protected void initializeLoggers() {

    final String logname = log.getName() + ".PartitionedLoader";

    log = Logger.getLogger(logname);

    final String logname_pfx = logname.substring(logname.indexOf("."));

    fileLog = Logger.getLogger("file." + logname_pfx + ".PartitionedLoader");
    sqlLog = Logger.getLogger("sql." + logname_pfx + ".PartitionedLoader");
    sqlErrLog = Logger.getLogger("sqlerror." + logname_pfx + ".PartitionedLoader");
  }

  protected Map<String, List<String>> listFiles() throws FileException {
    final String measType = whereProps.getProperty("tablename");
    patt = Pattern.compile(whereProps.getProperty("filemask", ".+"));
    sctx.put("MeasType", measType);
    final String tailDir = whereProps.getProperty("taildir", "raw");
    storageID = measType.trim() + ":RAW";

    log.finest("The MeasurementType is :" + measType.trim());
    final String typeDir = measType.toLowerCase().trim() + File.separator + tailDir;
    final FilenameFilter filter = new FilenameFilter() {

      @Override
      public boolean accept(final File dir, final String name) {
        final Matcher m = patt.matcher(name);
        return m.matches();
      }
    };

    final String ETLDATA_DIR = System.getProperty("ETLDATA_DIR");
    final File inDir = new File(ETLDATA_DIR, typeDir);
    final List<File> etlSearchDirs = expandEtlPaths(inDir);
    if (etlSearchDirs.isEmpty()) {
      log.warning("In directory " + inDir.getPath() + " not found or cannot be read");
    }

    final Map<String, List<String>> fileMap = new HashMap<String, List<String>>();
    for (File etlSub : etlSearchDirs) {
      log.finest("storageID: " + storageID);
      log.finest("inDir: " + etlSub.getPath());

      if (!etlSub.isDirectory() || !etlSub.canRead()) {
        log.warning("In directory " + etlSub + " not found or cannot be read");
      }

      final File[] files = etlSub.listFiles(filter);
      if (files == null || files.length == 0) {
        log.info("No files found on input directory " + etlSub.getPath());
      }

      for (File file : files) {
        final String dateString = parseDate(file.toString());
        log.finest("filename: " + file.toString());
        log.finest("dateString: " + dateString);

        List<String> filenames = fileMap.get(dateString);
        if (filenames == null) {
          filenames = new ArrayList<String>();
          fileMap.put(dateString, filenames);
        }
        filenames.add(file.toString());
      }
    }
    return fileMap;
  }

  /**
   * @see com.distocraft.dc5000.etl.engine.sql.Loader#getTableToFileMap()
   */
  @Override
  protected Map<String, List<String>> getTableToFileMap() throws FileException {

    final Map<String, List<String>> fileMap = listFiles();
    if(isRestoreEnabled()){
    	fileMap.putAll(getRestoreFileMap());
    }

    log.fine("Input files for " + fileMap.size() + " different dates");

    // Create database tables map mapping tables to files
    final Map<String, List<String>> tableMap = new HashMap<String, List<String>>();
    if (fileMap.size() != 0) {
      log.fine("DateFormat of file is " + dateFormatString);

      final SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);

      log.info("Reading PhysicalTable data with StorageID " + storageID);

      for (Map.Entry<String, List<String>> entry : fileMap.entrySet()) {
        final String dateString = entry.getKey();
        final List<String> filenames = entry.getValue();

        try {

          log.finer("Physical table for day " + dateString + " " + filenames.size() + " files to load.");

          final Date date = dateFormat.parse(dateString); // -> parseException
          
          String tableName = PhysicalTableCache.getCache().getTableName(storageID, date.getTime());
          log.info("Physical table: " + tableName + " date: " + date.toString() + " storageId: "+storageID);

          if (tableName == null) {

            // if no partitions hit this time, lets take all the partitions and
            // get the endtime of the last one.
            final List<String> tableList = PhysicalTableCache.getCache().getTableName(storageID, 0, currDate.getTime());

            if (tableList.isEmpty()) {
              tableName = "waiting";
            } else {
              final long lastPartitionEndTime = PhysicalTableCache.getCache().getEndTime(tableList.get(0));

              // if data is between currendate and last partitions endtime it is
              // put on wait status.
              // wating status is needed preserve (not to move to failed) files
              // from indir as invalid (not having partitions) files later.
              if (date.getTime() <= currDate.getTime() && date.getTime() >= lastPartitionEndTime) {

                log.info("Cant find table for " + storageID + " @ " + dateString
                    + ", but dateString is between current date (" + dateFormat.format(currDate)
                    + ") and last partiton endtime (" + dateFormat.format(new Date(lastPartitionEndTime))
                    + ") -> waiting for new partition to appear.");
                tableName = "waiting";

              } else {

                log.info("Cant find table for " + storageID + " @ " + dateString);
                // continue;

              }

            }
          }

          List<String> tablefilenames = tableMap.get(tableName);
          if (tablefilenames == null) {
            tablefilenames = new ArrayList<String>();
          }

          tablefilenames.addAll(filenames);

          if (!tablefilenames.isEmpty()) {
            tableMap.put(tableName, tablefilenames);
          }

          log.finer("Using table " + tableName + " for files" + formatFileNames_log(filenames, ""));

        } catch (ParseException pe) {
          log.log(Level.WARNING, "Illegal timestamp format: " + dateString, pe);
          log.finer("Failed files: " + formatFileNames_log(filenames, " moved to failed"));
          // move to failed
          moveFilesToFailed(filenames);
        }

      }

      if (tableMap.size() <= 0) {
        log.info("No physical tables found for files");
      }
    } else {
      log.fine("No data to load.");
    }
    sctx.put("tableMap", tableMap);

    return tableMap;

  }

/**
   * Add needed values to velocityContext before evaluating
   */
  @Override
  protected void fillVelocityContext(final VelocityContext context) {
    final String dateStr = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date());
    context.put("DATE", dateStr);
    context.put("MEASTYPE", whereProps.getProperty("tablename"));
    context.put("TECHPACK", whereProps.getProperty("techpack"));
    context.put("CHECKPOINT", whereProps.getProperty("checkpoint"));
    context.put("NOTIFY_ROWS", whereProps.getProperty("notify_rows"));

  }

  @Override
  public void updateSessionLog() {

  }

  /**
   * Date is found at the end of the filename between last '_' and last '.'
   * 
   * @param fileName
   *          file name
   * @return the date
   */
  protected String parseDate(final String fileName) {
    final int start = fileName.lastIndexOf('_') + 1;
    final int end = fileName.indexOf(".", start);

    try {

      final String dateString = fileName.substring(start, end);
      log.finest("parsed Date \"" + fileName + "\" -> \"" + dateString + "\"");
      return dateString;

    } catch (Exception e) {
      log.warning("Error while trying to parse date from " + fileName);
    }

    return "";

  }

  /**
   * Formats a filename list to suitable format for logs
   * 
   * @param fileNames
   *          files
   * @param status
   *          status
   * @return log format
   */
  private String formatFileNames_log(final List<String> fileNames, final String status) {
    final StringBuilder fileNamesStr = new StringBuilder("\n");

    for (String fileName : fileNames) {
      fileNamesStr.append(fileName).append(status).append("\n");
    }
    return fileNamesStr.toString();
  }
  
  /**
   * Adds restore load files to list of files that will be executed
   * as part of the load query for partitioned loader
   * Will add files to the list only if they were created before restore cut-off
   * Will add the oldest 'n' (RESTORE_LIMIT value) files if number of files exceeds RESTORE_LIMIT value
   * 
   * @return Filemap of restore load files
   */
  protected Map<String,List<String>> getRestoreFileMap() {
  	// 1. get restore loader directory
  	final int restoreThreshold = Integer.parseInt(StaticProperties.getProperty("RESTORE_LIMIT", "100"));
  	final String tailDir = whereProps.getProperty("taildir", "raw");
  	final String typeDir = tablename.trim() + File.separator + tailDir;
  	final File restoreDir = new File(BACKUP_DIR, typeDir);
  	log.info("Path to backup directory - " + restoreDir.getAbsolutePath());
  	// 2. get list of oldest n valid files arranged according to date
  	FilenameFilter filterOld = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if(parseEpochTime(name) < restoreStartEpoch){
					return true;
				}
				return false;
			}
		};
	Comparator<File> compFile = new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				long n1 = parseEpochTime(o1.getName());
              long n2 = parseEpochTime(o2.getName());
              return (int) (n1 - n2);
			}
		};
  	final Map<String, List<String>> fileMap = new HashMap<String, List<String>>();
  	if (!restoreDir.isDirectory() || !restoreDir.canRead()) {
          log.warning("In directory " + restoreDir + " not found or cannot be read");
          return fileMap;
        }
  	final File[] files = restoreDir.listFiles(filterOld);
  	Arrays.sort(files,compFile);
    if (files == null || files.length == 0) {
    	log.info("No files found on input directory " + restoreDir.getPath());
    }
    int i = 0;
    for (File file : files) {
        final String dateString = parseDate(file.toString());
        log.finest("filename: " + file.toString());
        log.finest("dateString: " + dateString);
        List<String> filenames = fileMap.get(dateString);
        if (filenames == null) {
        	filenames = new ArrayList<String>();
            fileMap.put(dateString, filenames);
        }
        filenames.add(file.toString());
        i++;
        if(i > restoreThreshold)
        {
        	break;
        }
     }
  	 return fileMap;
  }
}
