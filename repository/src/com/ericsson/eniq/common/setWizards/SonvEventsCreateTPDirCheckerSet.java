/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for creating the directory checker sets for a ENIQ EVENTS
 * SONV techpack.
 * 
 * @author epaujor
 * @since 2012
 * 
 */
public class SonvEventsCreateTPDirCheckerSet extends EventsCreateTPDirCheckerSet {

  /**
   * Constructor for SonvEventsCreateTPDirCheckerSet
   * 
   * @param name
   * @param version
   * @param versionid
   * @param dwhrepRock
   * @param rock
   * @param techPackID
   * @param techPackName
   * @throws SQLException
   */
  public SonvEventsCreateTPDirCheckerSet(final String name,final  String version, final String versionid, final RockFactory dwhrepRock,
      final RockFactory rock, final long techPackID, final String techPackName) throws SQLException {
    super(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
  }

  /*
   * Need to create directories in /eniq/data/etldata/ folder as well as
   * /eniq/data/etldata_/ folders for SONV techpacks (non-Javadoc)
   * 
   * @see com.ericsson.eniq.common.setWizards.EventsCreateTPDirCheckerSet#
   * createETLDirsAndAddToDbForReferenceTable(long, long, java.lang.String,
   * java.util.Properties)
   */
  @Override
  protected long createETLDirsAndAddToDbForReferenceTable(final long collectId, long transferActionId,
      final String folderName, final Properties prop) throws SQLException, RockException, IOException {

    // meas
    log.log(Level.INFO, "  Creating directory checker Action: " + folderName);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName,
        Constants.STATS_ETLDATA_DIR + pathSeparator + folderName + pathSeparator, propertyToString(prop)).insertToDB(
        rock);
    transferActionId++;

    // raw
    log.log(Level.INFO, "  Creating directory checker Action: raw_" + folderName);

    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName + "_raw",
        Constants.STATS_ETLDATA_DIR + pathSeparator + folderName + pathSeparator + "raw" + pathSeparator,
        propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    transferActionId = super.createETLDirsAndAddToDbForReferenceTable(collectId, transferActionId, folderName, prop);

    return transferActionId;
  }

  /*
   * Need to remove directories in /eniq/data/etldata/ folder as well as
   * /eniq/data/etldata_/ folders for SONV techpacks (non-Javadoc)
   * 
   * @see com.ericsson.eniq.common.setWizards.EventsCreateTPDirCheckerSet#
   * removeETLDirsFromDbForMeasurementTable(java.lang.String, long)
   */
  @Override
  protected void removeETLDirsFromDbForReferenceTable(final String foldername, final long setid) throws SQLException,
      RockException {
    // remove raw folder
    log.log(Level.INFO, "  Removing directory checker Action: raw_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_raw", version, techPackID, setid, rock);

    // remove measurement folder
    log.log(Level.INFO, "  Removing directory checker Action: " + foldername);
    Utils.removeAction("CreateDir_" + foldername, version, techPackID, setid, rock);

    super.removeETLDirsFromDbForReferenceTable(foldername, setid);
  }
  
  @Override
  protected long createETLDirsAndAddToDbForMeasurementTable(final long collectId, long transferActionId,
      final String folderName, final String tableLevel, final Properties prop) throws SQLException, RockException, IOException {

    // meas
    log.log(Level.INFO, "  Creating directory checker Action: " + folderName);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName,
        Constants.STATS_ETLDATA_DIR + pathSeparator + folderName + pathSeparator, propertyToString(prop)).insertToDB(
        rock);
    transferActionId++;

    // raw
    log.log(Level.INFO, "  Creating directory checker Action: raw_" + folderName);

    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName + "_raw",
        Constants.STATS_ETLDATA_DIR + pathSeparator + folderName + pathSeparator + "raw" + pathSeparator,
        propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    transferActionId = super.createETLDirsAndAddToDbForMeasurementTable(collectId, transferActionId, folderName, tableLevel, prop);

    return transferActionId;
  }
  
  @Override
  protected void removeETLDirsFromDbForMeasurementTable(final String foldername, final long setid) throws SQLException,
      RockException {
    // remove raw folder
    log.log(Level.INFO, "  Removing directory checker Action: raw_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_raw", version, techPackID, setid, rock);

    // remove measurement folder
    log.log(Level.INFO, "  Removing directory checker Action: " + foldername);
    Utils.removeAction("CreateDir_" + foldername, version, techPackID, setid, rock);

    super.removeETLDirsFromDbForMeasurementTable(foldername, setid);
  }
}
