/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

/**
 * Create specific DIM directories for EVENT_E_TERM 
 * @author epaujor
 * @since 2012
 * 
 */
public class DimEventsCreateTPDirCheckerSet extends EventsCreateTPDirCheckerSet {

  /**
   * @param name
   * @param version
   * @param versionid
   * @param dwhrepRock
   * @param rock
   * @param techPackID
   * @param techPackName
   * @throws SQLException
   */
  public DimEventsCreateTPDirCheckerSet(final String name, final String version, final String versionid, final RockFactory dwhrepRock,
      final RockFactory rock, final long techPackID, final String techPackName) throws SQLException {
    super(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
  }

  @Override
  public void create(final boolean topology, final boolean skip) throws Exception {
    super.create(true, skip);
  }

  @Override
  protected long createETLDirsAndAddToDbForReferenceTable(final long collectId, long transferActionId,
      final String folderName, final Properties prop)  throws SQLException, RockException, IOException{

    if (folderName.equalsIgnoreCase("DIM_E_SGEH_TAC_CURRENT_DC")) {
      transferActionId = createLogCheckers(collectId, transferActionId, "DIM_E_SGEH_TAC", getDefaultProps(),
            getDefaultProps());

      transferActionId = super.createETLDirsAndAddToDbForReferenceTable(collectId, transferActionId, "dim_e_sgeh_tac", prop);
      transferActionId = createLogCheckers(collectId, transferActionId, "DIM_E_SGEH_TAC_RAW", getDefaultProps(), getDefaultProps());

      // For DIM_E_SGEH_TAC we need to backup PLAIN table
      transferActionId = createBackupDirsAndAddToDb(collectId, transferActionId, "dim_e_sgeh_tac", prop, "plain");
    }

    transferActionId = super.createETLDirsAndAddToDbForReferenceTable(collectId, transferActionId, folderName, prop);

    return transferActionId;
  }

  @Override
  protected void removeETLDirsFromDbForReferenceTable(final String foldername, final long setid) throws SQLException,
      RockException {
    
    if (foldername.equalsIgnoreCase("DIM_E_SGEH_TAC_CURRENT_DC")) {
      removeBackupDirs("dim_e_sgeh_tac", setid, "plain");
      removeLogCheckers("DIM_E_SGEH_TAC_RAW", setid);
      removeETLDirsFromDbForReferenceTable("dim_e_sgeh_tac", setid);
      removeLogCheckers("DIM_E_SGEH_TAC", setid);
    }

    super.removeETLDirsFromDbForReferenceTable(foldername, setid);
  }
}
