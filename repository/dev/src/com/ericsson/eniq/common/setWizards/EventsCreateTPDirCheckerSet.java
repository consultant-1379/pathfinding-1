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
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for creating the directory checker sets for a ENIQ EVENTS
 * techpack.
 * 
 * @author epaujor
 * 
 */
public class EventsCreateTPDirCheckerSet extends CreateTPDirCheckerSet {

  private String versionDir = null;

  public EventsCreateTPDirCheckerSet(final String name, final String version, final String versionid,
      final RockFactory dwhrepRock, final RockFactory rock, final long techPackID, final String techPackName)
      throws SQLException {
    super(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
    versionDir = Utils.getEventsProperty("versiondir", "1");
  }

  @Override
  protected String getBaseEtlDirectory() {
    return Constants.EVENTS_ETLDATA_DIR;
  }

  @Override
  protected long createETLDirsAndAddToDbForMeasurementTable(final long collectId, long transferActionId,
      final String folderName, final String tableLevel, final Properties prop) throws SQLException, RockException,
      IOException {
    final int collectionId = new Long(this.maxSetID + collectId).intValue();

    for (int i = 0; i < Constants.NUM_OF_DIRECTORIES; i++) {
      final String subDirectory = getEtlSubDirectory(i);

      transferActionId = createEtlDirsAndAddToDbUpToTableLevelDir(transferActionId, folderName, tableLevel, prop,
          collectionId, subDirectory);

      // version
      log.log(Level.INFO, "  Creating directory checker Action: " + tableLevel + "_" + subDirectory + folderName
          + versionDir);
      createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
          "CreateDir_" + subDirectory + folderName + "_" + tableLevel + versionDir,
          subDirectory + folderName + pathSeparator + tableLevel + pathSeparator + versionDir + pathSeparator,
          propertyToString(prop)).insertToDB(rock);
      transferActionId++;

    }

    return transferActionId;

  }

  /*
   * For ENIQ Events a version directory needs to be created in raw directory
   * for measurement tables. This is not needed for reference tables.
   * 
   * (non-Javadoc)
   * 
   * @see com.ericsson.eniq.common.setWizards.CreateTPDirCheckerSet#
   * createETLDirsAndAddToDbForReferenceTable(long, long, java.lang.String,
   * java.util.Properties)
   */
  @Override
  protected long createETLDirsAndAddToDbForReferenceTable(final long collectId, long transferActionId,
      final String folderName, final Properties prop) throws SQLException, RockException, IOException {
    // For reference table, tableLevel will always be "raw"
    final String tableLevel = "raw";

    final int collectionId = new Long(this.maxSetID + collectId).intValue();

    for (int i = 0; i < Constants.NUM_OF_DIRECTORIES; i++) {
      final String subDirectory = getEtlSubDirectory(i);

      transferActionId = createEtlDirsAndAddToDbUpToTableLevelDir(transferActionId, folderName, tableLevel, prop,
          collectionId, subDirectory);
    }

    return transferActionId;
  }

  /**
   * Creates the directories up to /eniq/data/etldata_/00/<foldername>/raw and
   * up to /eniq/data/etldata_/00/<foldername>/joined
   * 
   * @param transferActionId
   * @param folderName
   * @param prop
   * @param collectionId
   * @param subDirectory
   * @return
   * @throws SQLException
   * @throws RockException
   * @throws IOException
   */
  private long createEtlDirsAndAddToDbUpToTableLevelDir(long transferActionId, final String folderName,
      final String tableLevel, final Properties prop, final int collectionId, final String subDirectory)
      throws SQLException, RockException, IOException {
    // directory
    log.log(Level.INFO, "  Creating directory checker Action: " + subDirectory);
    createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
        "CreateDir_" + subDirectory, subDirectory, propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    // meas
    log.log(Level.INFO, "  Creating directory checker Action: " + subDirectory + folderName);
    createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
        "CreateDir_" + subDirectory + folderName, subDirectory + folderName + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    transferActionId++;

    // raw
    log.log(Level.INFO, "  Creating directory checker Action: " + tableLevel + "_" + subDirectory + folderName);
    createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
        "CreateDir_" + subDirectory + folderName + "_" + tableLevel,
        subDirectory + folderName + pathSeparator + tableLevel + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    transferActionId++;

    // joined
    log.log(Level.INFO, "  Creating directory checker Action: joined_" + subDirectory + folderName);
    createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
        "CreateDir_" + subDirectory + folderName + "_joined",
        subDirectory + folderName + pathSeparator + "joined" + pathSeparator, propertyToString(prop)).insertToDB(rock);
    transferActionId++;
    return transferActionId;
  }

  @Override
  protected long createBackupDirsAndAddToDb(final long collectId, long transferActionId, final String folderName,
      final Properties prop, final String tableType) throws SQLException, RockException, IOException {
    final long collectionId = new Long(this.maxSetID + collectId).intValue();

    for (int i = 0; i < Constants.NUM_OF_DIRECTORIES; i++) {
      final String subDirectory = getBackupSubDirectory(i);

      // directory, /eniq/backup/dwh_00/, /eniq/backup/dwh_01/, ....
      log.log(Level.INFO, "  Creating directory checker Action: " + subDirectory);
      createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
          "CreateDir_" + subDirectory, subDirectory, propertyToString(prop)).insertToDB(rock);
      transferActionId++;

      // directory, /eniq/backup/dwh_00/<techPackName>,
      // /eniq/backup/dwh_01/<techPackName>/, ....
      final String backupTechPackDir = subDirectory + techPackName.toLowerCase() + pathSeparator;
      log.log(Level.INFO, "  Creating directory checker Action: " + backupTechPackDir);
      createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
          "CreateDir_" + backupTechPackDir, backupTechPackDir, propertyToString(prop)).insertToDB(rock);
      transferActionId++;

      // directory, /eniq/backup/dwh_00/<techPackName>/<folderName>_day/,
      // /eniq/backup/dwh_01/<techPackName>/<folderName>_day/, ....
      final String backupFolderNameDir = backupTechPackDir + folderName + "_" + tableType.toLowerCase() + pathSeparator;
      log.log(Level.INFO, "  Creating directory checker Action: " + backupFolderNameDir);
      createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
          "CreateDir_" + backupFolderNameDir, backupFolderNameDir, propertyToString(prop)).insertToDB(rock);
      transferActionId++;
    }

    return transferActionId;
  }

  @Override
  protected long createBaseBackupDirAndAddToDb(long transferActionId, final Properties prop, final long collectId)
      throws SQLException, RockException, IOException {
    final long collectionId = new Long(this.maxSetID + collectId).intValue();
    // directory, /eniq/backup/
    log.log(Level.INFO, "  Creating directory checker Action: " + Constants.EVENTS_BACKUP_DIR + pathSeparator);
    createAction(transferActionId, "CreateDir", collectionId, getTransferActionId(transferActionId),
        "CreateDir_" + Constants.EVENTS_BACKUP_DIR + pathSeparator, Constants.EVENTS_BACKUP_DIR + pathSeparator,
        propertyToString(prop)).insertToDB(rock);
    transferActionId++;
    return transferActionId;
  }

  @Override
  protected void removeETLDirsFromDbForMeasurementTable(final String foldername, final long setid) throws SQLException,
      RockException {
    for (int i = 0; i < Constants.NUM_OF_DIRECTORIES; i++) {
      final String subDirectory = getEtlSubDirectory(i);
      // Remove version folder
      log.log(Level.INFO, "  Removing directory checker Action: raw_" + subDirectory + foldername + versionDir);
      Utils.removeAction("CreateDir_" + subDirectory + foldername + "_raw" + versionDir, version, techPackID, setid,
          rock);

      // Remove the raw, joined and measurement folders
      super.removeETLDirsFromDbForMeasurementTable(subDirectory + foldername, setid);

      // Remove directory folder
      log.log(Level.INFO, "  Removing directory checker Action: " + subDirectory);
      Utils.removeAction("CreateDir_" + subDirectory, version, techPackID, setid, rock);
    }

  }

  @Override
  protected void removeBackupDirs(final String foldername, final long setid, final String tableType)
      throws SQLException, RockException {
    for (int i = 0; i < Constants.NUM_OF_DIRECTORIES; i++) {
      // directory, /eniq/backup/dwh_00/, /eniq/backup/dwh_01/, ....
      final String subDirectory = getBackupSubDirectory(i);

      // directory, /eniq/backup/dwh_00/<techPackName>,
      // /eniq/backup/dwh_01/<techPackName>/, ....
      final String backupTechPackDir = subDirectory + techPackName.toLowerCase() + pathSeparator;

      // directory, /eniq/backup/dwh_00/<techPackName>/<folderName>_day/,
      // /eniq/backup/dwh_01/<techPackName>/<folderName>_day/, ....
      final String backupFolderNameDir = backupTechPackDir + foldername + "_" + tableType.toLowerCase() + pathSeparator;

      // Remove version folder
      log.log(Level.INFO, "  Removing directory checker Action: " + backupFolderNameDir);
      Utils.removeAction("CreateDir_" + backupFolderNameDir, version, techPackID, setid, rock);

      log.log(Level.INFO, "  Removing directory checker Action: " + backupTechPackDir);
      Utils.removeAction("CreateDir_" + backupTechPackDir, version, techPackID, setid, rock);

      log.log(Level.INFO, "  Removing directory checker Action: " + subDirectory);
      Utils.removeAction("CreateDir_" + subDirectory, version, techPackID, setid, rock);
    }
  }

  @Override
  protected void removeBaseBackupDirAndAddToDb(final long setid) throws SQLException, RockException, IOException {
    // directory, /eniq/backup/
    log.log(Level.INFO, "  Removing directory checker Action: " + Constants.EVENTS_BACKUP_DIR + pathSeparator);
    Utils.removeAction("CreateDir_" + Constants.EVENTS_BACKUP_DIR + pathSeparator, version, techPackID, setid, rock);
  }

  /**
   * Gets the Transfer Action ID for META_TRANSFER_ACTIONS table.
   * 
   * @param transferActionId
   * @return
   */
  private int getTransferActionId(final long transferActionId) {
    return new Long(this.maxActionID + transferActionId).intValue();
  }

  /**
   * This method gets the ETL sub-directory name based on the value of i e.g. if
   * i=0, then ETL sub-directory would be /eniq/data/etldata_/00/ if i=5, then
   * ETL sub-directory would be /eniq/data/etldata_/05/
   * 
   * @param i
   * @return
   */
  private String getEtlSubDirectory(final int i) {
    final StringBuilder stringbuilder = new StringBuilder();
    stringbuilder.append(getBaseEtlDirectory());
    stringbuilder.append(pathSeparator);

    // If i is less than 10, then add an extra zero to sub-directory name
    if (i < 10) {
      stringbuilder.append(Constants.EXTRA_ZERO);
    }
    stringbuilder.append(Integer.toString(i));
    stringbuilder.append(pathSeparator);

    return stringbuilder.toString();
  }

  /**
   * This method gets the Backup sub-directory name based on the value of i
   * 
   * @param i
   * @return
   */
  private String getBackupSubDirectory(final int i) {
    final StringBuilder stringbuilder = new StringBuilder();
    stringbuilder.append(Constants.EVENTS_BACKUP_DIR);
    stringbuilder.append(pathSeparator);
    stringbuilder.append("dwh_");

    // If i is less than 10, then add an extra zero to sub-directory name
    if (i < 10) {
      stringbuilder.append(Constants.EXTRA_ZERO);
    }
    stringbuilder.append(Integer.toString(i));
    stringbuilder.append(pathSeparator);

    return stringbuilder.toString();
  }

}
