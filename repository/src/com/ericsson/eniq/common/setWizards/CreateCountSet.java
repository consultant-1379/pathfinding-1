package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for the creation of the count sets for ENIQ EVENTS
 * techpacks through the techpackIDE
 * 
 * @author epaujor
 * 
 */
public class CreateCountSet extends AbstractCreateSet {

  private static final String COUNTING_DAY_ACTION = "CountingDayAction";

  private static final String COUNTING_ACTION = "CountingAction";

  private static final String QUEUE_TIME = "15";

  private final static transient Logger LOG = Logger.getLogger(CreateCountSet.class.getName());

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
  public CreateCountSet(final String name, final String version, final String versionid, final RockFactory dwhrepRock,
      final RockFactory etlrep, final int techPackID, final boolean schedulings) throws SQLException {
    super(name, version, versionid, dwhrepRock, etlrep, techPackID, schedulings);
  }

  @Override
  public void removeSets(final List<String> skiplist) throws SQLException, RockException {

    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        // Skip the remove in case there is a match in the skip list.
        final String typeName = measType.getTypename();
        if (skiplist != null
            && skiplist.contains(measTable.getBasetablename()
                .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
          LOG.info("Skipped set " + typeName);
          continue;

        }
        if (isCountTable(measTable)) {
          final String countName = getCountActionName(typeName);
          final String backupCountName = "Backup_Count_" + typeName;
          cleanUpSets(countName, backupCountName);
        }
      }
    }
  }

  /**
   * 
   * @param countName
   * @param backupCountName
   * @throws SQLException
   * @throws RockException
   */
  private void cleanUpSets(final String countName, final String backupCountName) throws SQLException, RockException {
    // Remove set
    LOG.info("Removing set " + countName);
    final long setid = Utils.getSetId(countName, version, techPackID, etlrepRock);
    if (setid == -1) {
      LOG.info("No sets found, no need to remove");
    } else {
      // Remove Count action
      LOG.info("Removing action " + countName);
      Utils.removeAction(countName, version, techPackID, setid, etlrepRock);

      // Remove Backup Count action
      LOG.info("Removing action " + backupCountName);
      Utils.removeAction(backupCountName, version, techPackID, setid, etlrepRock);

      // Remove schedule
      LOG.info("Removing Scheduling " + countName);
      Utils.removeScheduling(countName, version, techPackID, setid, etlrepRock);

      // Remove set
      Utils.removeSet(countName, version, techPackID, etlrepRock);
    }
  }

  @Override
  public void create(final boolean skip, final List<String> skiplist) throws SQLException, RockException {
    long iSet = 0;
    long iAction = 0;
    long iScheduling = 0;
    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final String typeName = measType.getTypename();
      final String countActionName = getCountActionName(typeName);
      final String backupCountActionName = "Backup_Count_" + typeName;

      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        if (isCountTable(measTable)) {
          int order = 0;

          if (skiplist != null
              && skiplist.contains(measTable.getBasetablename()
                  .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
            LOG.info("Skipped " + countActionName);
            continue;
          }

          if (!(skip && Utils.isSet(countActionName, version, techPackID, etlrepRock))) {

            // create set
            LOG.info("Creating set " + countActionName);
            createSet("Count", (maxSetID + iSet), countActionName, QUEUE_TIME).insertToDB(etlrepRock);

            // Create CountingAction
            LOG.info("  Creating action " + countActionName);
            try {
              final ActionGenerator countActGenerator = createAction(countActionName, order,
                  getActionTypeName(typeName), (this.maxSetID + iSet), (maxActionID + iAction),
                  getWhereClause(typeName), "");
              countActGenerator.insertToDB(etlrepRock);
              iAction++;
              order++;
            } catch (IOException e) {
              LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
              LOG.warning("Skipped " + countActionName + " creation because of exception");
              continue;
            }

            // Create BackupTrigger Action
            LOG.info("  Creating action " + backupCountActionName);
            final ActionGenerator backupAction = createAction(backupCountActionName, order, "BackupTrigger",
                (this.maxSetID + iSet), (this.maxActionID + iAction), "", "");
            backupAction.insertToDB(etlrepRock);
            iAction++;
            order++;

            if (doSchedulings) {
              // create scheduling
              LOG.info("  Creating Scheduling " + name);

              createWaitSchedule((this.maxSchedulingID + iScheduling), this.techPackID, (this.maxSetID + iSet),
                  countActionName, HOLD_FLAG).insertToDB(this.etlrepRock);
              iScheduling++;
            }

            iSet++;

          } else {
            LOG.info("Skipped existing set Count_" + typeName);
          }
        }
      }
    }
  }

  /**
   * @param typeName
   * @return
   * @throws IOException
   */
  private String getWhereClause(final String typeName) throws IOException {
    final Properties prop = new Properties();
    prop.setProperty("targetType", typeName);

    if (isDataTiering(typeName)) {
      prop.setProperty("sourceType", typeName + ":15MIN");
    }
    return Utils.propertyToString(prop);
  }

  /**
   * @param measType
   * @return
   */
  private String getCountActionName(final String typeName) {
    String countActionName = "Count_" + typeName;

    if (isDataTiering(typeName)) {
      countActionName = countActionName + "_DAY";
    }

    return countActionName;
  }

  private String getActionTypeName(final String typeName) {
    String actionTypeName = COUNTING_ACTION;

    if (isDataTiering(typeName)) {
      actionTypeName = COUNTING_DAY_ACTION;
    }
    return actionTypeName;
  }

  /**
   * @param typeName
   * @return
   */
  private boolean isDataTiering(final String typeName) {
    boolean isDataTiering = false;
    String dataTieringTps = StaticProperties.getProperty("dataTieringTps", "EVENT_E_LTE,EVENT_E_SGEH,EVENT_E_RAN_CFA");

    for (String dataTieringTp : dataTieringTps.split(",")) {
      if (versionid.startsWith(dataTieringTp)) {
        if (versionid.startsWith("EVENT_E_LTE") || versionid.startsWith("EVENT_E_SGEH")) {
          if (typeName.endsWith("_SUC")) {
            isDataTiering = true;
          }
        } else {
          isDataTiering = true;
        }
      }
    }
    return isDataTiering;
  }
}
