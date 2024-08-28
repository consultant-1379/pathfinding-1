package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for the creation of the restore sets for ENIQ EVENTS
 * techpacks through the techpackIDE
 * 
 * @author epaujor
 * 
 */
public class CreateRestoreSet extends AbstractCreateSet {

  private static final String DAY = "_DAY";

  private static final String QUEUE_TIME = "72000";

  private static final String RESTORE_COUNT = "Restore_Count_";

  private final static transient Logger LOG = Logger.getLogger(CreateRestoreSet.class.getName());

  /**
   * constructor
   * 
   * @param templateDir
   *          Directory containing all the required templates to create the
   *          aggregation sets
   * @param name
   *          The aggregator name
   * @param version
   *          The TP Version
   * @param versionid
   *          The version ID
   * @param dwhrepRock
   *          DWHREP connection
   * @param etlrep
   *          ETLREP connection
   * @param techPackID
   *          TP ID
   * @param schedulings
   *          Regenerate the schedulings
   * @throws SQLException
   * @throws Exception
   *           Any errors
   */
  public CreateRestoreSet(final String name, final String version, final String versionid,
      final RockFactory dwhrepRock,
      final RockFactory etlrep, final int techPackID, final boolean schedulings) throws SQLException {
    super(name, version, versionid, dwhrepRock, etlrep, techPackID, schedulings);
  }

  /**
   * Removes the created sets.
   * 
   * @param skiplist
   *          The list of measurement and reference types to be skipped, i.e.
   *          not removed.
   * @throws RockException
   * @throws SQLException
   * @throws Exception
   *           Any errors
   */
  @Override
  public void removeSets(final List<String> skiplist) throws SQLException, RockException {

    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        // Skip the remove in case there is a match in the skip list.
        if (skiplist != null
            && skiplist.contains(measTable.getBasetablename()
                .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
          LOG.info("Skipped set " + measType.getTypename());
          continue;

        }
        if (isCountTable(measTable)) {

          final String restoreName = RESTORE_COUNT + measType.getTypename() + DAY;
          // Remove set
          LOG.info("Removing set " + restoreName);
          final long setid = Utils.getSetId(restoreName, version, techPackID, etlrepRock);
          if (setid == -1) {
            LOG.info("No sets found, no need to remove");
            continue;
          }

          // Remove BackupLoader action
          LOG.info("Removing action " + restoreName);
          Utils.removeAction(restoreName, version, techPackID, setid, etlrepRock);

          // Remove schedule
          LOG.info("Removing Scheduling " + restoreName);
          Utils.removeScheduling(restoreName, version, techPackID, setid, etlrepRock);

          // Remove set
          Utils.removeSet(restoreName, version, techPackID, etlrepRock);
        }
      }
    }
  }

  /**
   * Create the sets.
   * 
   * @param skip
   *          already existing sets will be skipped if true.
   * @param skiplist
   *          list of sets
   * @throws RockException
   * @throws SQLException
   * @throws Exception
   */
  @Override
  public void create(final boolean skip, final List<String> skiplist) throws SQLException, RockException {
    long iSet = 0;
    long iAction = 0;
    long iScheduling = 0;
    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final String restoreSetName = RESTORE_COUNT + measType.getTypename() + DAY;

      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        if (measTable.getTablelevel().equalsIgnoreCase(Constants.FIFTEENMIN)
            || measTable.getTablelevel().equalsIgnoreCase(Constants.ONEMIN)
            || measTable.getTablelevel().equalsIgnoreCase(Constants.DAYLEVEL)) {

          int order = 0;

          if (skiplist != null
              && skiplist.contains(measTable.getBasetablename()
                  .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
            LOG.info("Skipped " + restoreSetName);
            continue;
          }

          if (!(skip && Utils.isSet(restoreSetName, version, techPackID, etlrepRock))) {

            // create set
            LOG.info("Creating set " + restoreSetName);
            createSet("Backup", (maxSetID + iSet), restoreSetName, QUEUE_TIME).insertToDB(etlrepRock);

            // Create BackupLoader
            LOG.info("  Creating action " + restoreSetName);
            final ActionGenerator restoreActGen = createAction(restoreSetName, order, "BackupLoader",
                (this.maxSetID + iSet), (maxActionID + iAction), "", "");
            restoreActGen.insertToDB(etlrepRock);
            iAction++;
            order++;

            if (doSchedulings) {
              // create scheduling
              LOG.info("  Creating Scheduling " + name);

              createWaitSchedule((this.maxSchedulingID + iScheduling), this.techPackID, (this.maxSetID + iSet),
                  restoreSetName, HOLD_FLAG).insertToDB(this.etlrepRock);
              iScheduling++;
            }

            iSet++;

          } else {
            LOG.info("Skipped existing set Restore_Count_" + measType.getTypename());
          }

        }
      }
    }
  }
}
