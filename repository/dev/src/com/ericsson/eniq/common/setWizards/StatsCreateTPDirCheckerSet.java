/**------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;

/**
 * This class is used for creating the directory checker sets for a ENIQ STATS
 * techpack.
 * 
 * @author epaujor
 * 
 */
public class StatsCreateTPDirCheckerSet extends CreateTPDirCheckerSet {

  public StatsCreateTPDirCheckerSet(final String name, final String version, final String versionid,
      final RockFactory dwhrepRock, final RockFactory rock, final long techPackID, final String techPackName)
      throws SQLException {
    super(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
  }

  @Override
  protected String getBaseEtlDirectory() {
    return Constants.STATS_ETLDATA_DIR;
  }

  @Override
  protected long createETLDirsAndAddToDbForMeasurementTable(final long collectId, long transferActionId, final String folderName,
      final String tableLevel, final Properties prop) throws SQLException, RockException, IOException {

    // meas
    log.log(Level.INFO, "  Creating directory checker Action: " + folderName);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName,
        getBaseEtlDirectory() + pathSeparator + folderName + pathSeparator, propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    // raw
    log.log(Level.INFO, "  Creating directory checker Action: "+tableLevel+"_" + folderName);

    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName + "_"+tableLevel,
        getBaseEtlDirectory() + pathSeparator + folderName + pathSeparator + tableLevel + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    transferActionId++;

    // joined
    log.log(Level.INFO, "  Creating directory checker Action: joined_" + folderName);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + folderName + "_joined",
        getBaseEtlDirectory() + pathSeparator + folderName + pathSeparator + "joined" + pathSeparator,
        propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    return transferActionId;
  }

  /*
   * For ENIQ STATS, directory structure is the same for measurement tables and reference tables
   * 
   * (non-Javadoc)
   * 
   * @see com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#
   * createETLDirsAndAddToDbForReferenceTable(long, long, java.lang.String, java.util.Properties)
   */
  @Override
  protected long createETLDirsAndAddToDbForReferenceTable(final long collectId, long transferActionId,
      final String folderName, final Properties prop) throws SQLException, RockException, IOException {
    //For reference table, tableLevel will always be "raw"
    final String tableLevel = "raw";
    transferActionId = createETLDirsAndAddToDbForMeasurementTable(collectId, transferActionId, folderName, tableLevel, prop);
    return transferActionId;
  }

  /*
   * For the moment, this does not create any backup directories for STATS.
   * 
   * (non-Javadoc)
   * 
   * @see com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#
   * createBackupDirsAndAddToDbForMeasurementTable(long, long, java.lang.String, java.util.Properties)
   */
  @Override
  protected long createBackupDirsAndAddToDb(final long iSet, final long transferActionId, final String folderName,
      final Properties prop, final String tableType) throws SQLException, RockException, IOException {
    // For the moment, this just returns the transferActionId without any updates
    return transferActionId;
  }

  /*
   * For the moment, this does not create any base backup directories for STATS.
   * 
   * (non-Javadoc)
   * 
   * @see
   * com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#createBaseBackupDirAndAddToDb
   * (long, java.util.Properties, int)
   */
  @Override
  protected long createBaseBackupDirAndAddToDb(final long transferActionId, final Properties prop, final long collectionId)
      throws SQLException, RockException, IOException {
    // For the moment, this just returns the transferActionId without any updates
    return transferActionId;
  }

  /*
   * For the moment, this does not remove any backup directories for STATS.
   * 
   * (non-Javadoc)
   * 
   * @see
   * com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#removeBackupDirsForDayTable(
   * java.lang.String, long)
   */
  @Override
  protected void removeBackupDirs(final String foldername, final long setid, final String tableType) throws SQLException, RockException {
    // For the moment, this does not remove any backup directories for STATS.
  }

  /*
   * For the moment, this does not remove any base backup directories for STATS.
   * 
   * (non-Javadoc)
   * 
   * @see
   * com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#removeBaseBackupDirAndAddToDb
   * (long)
   */
  @Override
  protected void removeBaseBackupDirAndAddToDb(final long setid) throws SQLException, RockException, IOException {
    // For the moment, this does not remove any base backup directories for STATS.
  }
}
