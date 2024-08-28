/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;
import com.distocraft.dc5000.repository.dwhrep.ReferencetableFactory;
import com.ericsson.eniq.common.Utils;

/**
 * This class is used for creating the backup sets for a ENIQ EVENTS SONV
 * techpack through the wizard.
 *
 * @author epaujor
 * @since 2012
 *
 */
public class CreateSonvBackupSet extends AbstractCreateSet {

  // Default hour to run this set is 3
  private static final long SCHEDULE_HOUR = 3;

  // Default minute to run this set is 35
  private static final long SCHEDULE_MIN = 35;

  private static final String CURRENT_DC = "_CURRENT_DC";

  private static final String PLAIN_TABLE_LEVEL = "PLAIN";

  private static final String STORE = "Store_";

  private static final String QUEUE_TIME = "720";

  private static final String BACKUP = "Backup_";

  private final static transient Logger LOG = Logger.getLogger(CreateSonvBackupSet.class.getName());

  private long iSet = 0;

  private long iAction = 0;

  private long iScheduling = 0;

  /**
   * Constructor for CreateSonvBackupSet
   *
   * @param name
   * @param version
   * @param versionid
   * @param dwhrepRock
   * @param etlrep
   * @param techPackID
   * @param schedulings
   * @throws SQLException
   */
  public CreateSonvBackupSet(final String name, final String version, final String versionid,
      final RockFactory dwhrepRock, final RockFactory etlrep, final int techPackID, final boolean schedulings)
      throws SQLException {
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
    removeMeasTypeSets(skiplist);
    removeReferenceTableSets(skiplist);
  }

  /**
   * Remove the created sets related to reference tables
   *
   * @param skiplist
   * @throws SQLException
   * @throws RockException
   */
  private void removeReferenceTableSets(final List<String> skiplist) throws SQLException, RockException {
    final List<Referencetable> rTables = getReferenceTables();
    for (Referencetable rTable : rTables) {
      final String typename = rTable.getTypename();
      final String backupSetName = BACKUP + typename;

      // Skip the remove in case there is a match in the skip list.
      if (skiplist != null && skiplist.contains(typename.toUpperCase())) {
        LOG.info("Skipped set " + backupSetName);
        continue;
      }

      LOG.info("Removing set " + backupSetName);
      final long setid = Utils.getSetId(backupSetName, version, techPackID, etlrepRock);
      if (setid == -1) {
        LOG.info("No sets found, no need to remove");
        continue;
      }

      // Remove BackupTables action
      LOG.info("Removing action " + backupSetName);
      Utils.removeAction(backupSetName, version, techPackID, setid, etlrepRock);

      // Remove schedule
      LOG.info("Removing Scheduling " + backupSetName);
      Utils.removeScheduling(backupSetName, version, techPackID, setid, etlrepRock);

      // Remove set
      Utils.removeSet(backupSetName, version, techPackID, etlrepRock);

      // eromsza: History Dynamic: If data format support is false, don't handle _HIST and _CALC tables
      boolean dataFormatSupport = true;
      if (rTable.getDataformatsupport() != null) {
          dataFormatSupport = rTable.getDataformatsupport().intValue() == 1;
      }
      // eeoidiv,20121128:History Dynamic
      // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then delete additional History Table backup set.
      if(rTable.getUpdate_policy() == 4 && dataFormatSupport) {
      	final String histBackupSetName = backupSetName + "_HIST";
      	// Remove BackupTables action
        LOG.info("Removing action " + histBackupSetName);
        Utils.removeAction(histBackupSetName, version, techPackID, setid, etlrepRock);

        // Remove schedule
        LOG.info("Removing Scheduling " + histBackupSetName);
        Utils.removeScheduling(histBackupSetName, version, techPackID, setid, etlrepRock);

        // Remove set
        Utils.removeSet(histBackupSetName, version, techPackID, etlrepRock);
      }
    }
  }

  /**
   * Remove the created sets related to measurement types
   *
   * @param skiplist
   * @throws SQLException
   * @throws RockException
   */
  private void removeMeasTypeSets(final List<String> skiplist) throws SQLException, RockException {
    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        final String baseTableName = measTable.getBasetablename();

        // Remove _RAW, _DAY, etc. from base table name
        final String shortBaseTableName = baseTableName.substring(0, baseTableName.lastIndexOf("_"));
        final String backupSetName = BACKUP + shortBaseTableName;

        // Skip the remove in case there is a match in the skip list.
        if (skiplist != null && skiplist.contains(shortBaseTableName.toUpperCase())) {
          LOG.info("Skipped set " + backupSetName);
          continue;
        }

        final String storeActName = STORE + shortBaseTableName;
        // Remove set
        LOG.info("Removing set " + backupSetName);
        final long setid = Utils.getSetId(backupSetName, version, techPackID, etlrepRock);
        if (setid == -1) {
          LOG.info("No sets found, no need to remove");
          continue;
        }

        // Remove StoreCountingData action
        LOG.info("Removing action " + storeActName);
        Utils.removeAction(storeActName, version, techPackID, setid, etlrepRock);

        // Remove BackupTables action
        LOG.info("Removing action " + backupSetName);
        Utils.removeAction(backupSetName, version, techPackID, setid, etlrepRock);

        // Remove schedule
        LOG.info("Removing Scheduling " + backupSetName);
        Utils.removeScheduling(backupSetName, version, techPackID, setid, etlrepRock);

        // Remove set
        Utils.removeSet(backupSetName, version, techPackID, etlrepRock);
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
   * @throws IOException
   * @throws Exception
   */
  @Override
  public void create(final boolean skip, final List<String> skiplist) throws SQLException, RockException {
    iSet = 0;
    iAction = 0;
    iScheduling = 0;
    createMeasTypeSets(skip, skiplist);
    createReferenceTableSets(skip, skiplist);
  }

  /**
   * Create sets for reference tables
   *
   * @param skip
   * @param skiplist
   * @throws SQLException
   * @throws RockException
   */
  private void createReferenceTableSets(final boolean skip, final List<String> skiplist) throws SQLException,
      RockException {
    final List<Referencetable> rTables = getReferenceTables();
    for (Referencetable rTable : rTables) {
      final String typename = rTable.getTypename();
      final String tablelevel = PLAIN_TABLE_LEVEL;
      final String backupSetName = BACKUP + typename;
      int order = 0;

      if (skiplist != null && skiplist.contains(typename.toUpperCase())) {
        LOG.info("Skipped " + backupSetName);
        continue;
      }

      if (!(skip && Utils.isSet(backupSetName, version, techPackID, etlrepRock))) {
    	try {
    		createSetActionScheduling(backupSetName, order, typename, tablelevel, false);
    	} catch (Exception e) {
    		LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
	        LOG.warning("Skipped " + backupSetName + " creation because of exception");
	        continue;
  		}
        iSet++;
        // eromsza: History Dynamic: If data format support is false, don't handle _HIST and _CALC tables
        boolean dataFormatSupport = true;
        if (rTable.getDataformatsupport() != null) {
            dataFormatSupport = rTable.getDataformatsupport().intValue() == 1;
        }
        // eeoidiv,20121128:History Dynamic
        // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then create additional History Table backup set.
        if(rTable.getUpdate_policy() == 4 && dataFormatSupport) {
        	final String histBackupSetName = backupSetName + "_HIST";
        	final String histTypename = typename + "_HIST_RAW";
        	try {
        		createSetActionScheduling(histBackupSetName, order, histTypename, tablelevel, false);
        	} catch (Exception e) {
        		LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
    	        LOG.warning("Skipped " + histBackupSetName + " creation because of exception");
    	        continue;
      		}
            iSet++;
        }
      } else {
        LOG.info("Skipped existing set Backup_" + typename);
      }
    }
  }

  /**
   * Create sets for measurement types
   *
   * @param skip
   * @param skiplist
   * @throws SQLException
   * @throws RockException
   */
  private void createMeasTypeSets(final boolean skip, final List<String> skiplist) throws SQLException, RockException {
    final List<Measurementtype> measTypes = getMeasurmentTypes();

    for (Measurementtype measType : measTypes) {
      final List<Measurementtable> measTables = getMeasurementTables(measType);

      for (Measurementtable measTable : measTables) {
        int order = 0;
        final String baseTableName = measTable.getBasetablename();
        // Remove _RAW, _DAY, etc. from base table name
        final String typename = baseTableName.substring(0, baseTableName.lastIndexOf("_"));
        final String backupSetName = BACKUP + typename;
        final String tablelevel = measTable.getTablelevel();

        if (skiplist != null && skiplist.contains(typename.toUpperCase())) {
          LOG.info("Skipped " + backupSetName);
          continue;
        }

        if (!(skip && Utils.isSet(backupSetName, version, techPackID, etlrepRock))) {
          final boolean isVolBasedPartition = isVolumeBasedPartition(measTable);
          try {
        	  createSetActionScheduling(backupSetName, order, typename, tablelevel, isVolBasedPartition);
          } catch (Exception e) {
    		LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
	        LOG.warning("Skipped " + backupSetName + " creation because of exception");
	        continue;
  		  }
          iSet++;
        } else {
          LOG.info("Skipped existing set " + backupSetName);
        }
      }
    }
  }

  private void createSetActionScheduling(final String backupSetName, int order, final String typename, final String tablelevel, final boolean isVolBasedPartition) throws Exception {
	  // create set
	  LOG.info("Creating set " + backupSetName);
      createSet("Backup", (maxSetID + iSet), backupSetName, QUEUE_TIME).insertToDB(etlrepRock);

      // Create BackupTables Action
      LOG.info("  Creating action " + backupSetName);
      final ActionGenerator backupTablesActGen = createAction(backupSetName, order, "BackupTables",
            (this.maxSetID + iSet), (maxActionID + iAction),
            createPropertyString(typename, tablelevel, isVolBasedPartition), "");
      backupTablesActGen.insertToDB(etlrepRock);
      iAction++;
      order++;
      // Create StoreCountingData Action, this is only needed if partition
      // plan is volume-based
      if (isVolBasedPartition) {
    	final String storeActName = STORE + typename;
        LOG.info("  Creating action " + storeActName);
        final ActionGenerator storeCountingDataActGen = createAction(storeActName, order, "StoreCountingData",
            (this.maxSetID + iSet), (maxActionID + iAction), "", "");
        storeCountingDataActGen.insertToDB(etlrepRock);
        iAction++;
        order++;
      }
      createScheduling(backupSetName);
  }

  /**
   * Checks to see if partitions are volume-based partitions or time-based partitions
   * @param measTable
   * @throws SQLException
   * @throws RockException
   */
  private boolean isVolumeBasedPartition(final Measurementtable measTable) throws SQLException, RockException {
    boolean isVolBasedPartition = false;
    final Partitionplan wherePlan = new Partitionplan(dwhrepRock);
    wherePlan.setPartitionplan(measTable.getPartitionplan());
    final PartitionplanFactory planFactory = new PartitionplanFactory(dwhrepRock, wherePlan);
    final Partitionplan plan = planFactory.get().get(0);

    if (plan.getPartitiontype() == 1) {
      isVolBasedPartition = true;
    }

    return isVolBasedPartition;
  }

  /**
   * Create a weekly schedule that will run every day at 03:35
   *
   * @param backupSetName
   * @throws SQLException
   * @throws RockException
   */
  private void createScheduling(final String backupSetName) throws SQLException, RockException {
    if (doSchedulings) {
      // create scheduling
      LOG.info("  Creating Scheduling " + backupSetName);
      createWeeklySchedule((this.maxSchedulingID + iScheduling), (this.maxSetID + iSet), backupSetName, SCHEDULE_HOUR,
          SCHEDULE_MIN).insertToDB(this.etlrepRock);
      iScheduling++;
    }
  }

  /**
   * Create a property string to add to where clause of action
   *
   * @param typeName
   * @param tableLevel
   * @return
   * @throws IOException
   */
  protected String createPropertyString(final String typeName, final String tableLevel, final boolean isVolBasedPartition)
      throws IOException {
    final Properties prop = new Properties();

    prop.setProperty("typeName", typeName + CURRENT_DC);
    prop.setProperty("tablename", typeName + ":" + tableLevel);
    prop.setProperty("isVolBasedPartition", Boolean.toString(isVolBasedPartition));
    return Utils.propertyToString(prop);
  }

  /**
   * Get all the reference tables associated with this techpack
   *
   * @return
   * @throws SQLException
   * @throws RockException
   */
  protected List<Referencetable> getReferenceTables() throws SQLException, RockException {
    final Referencetable rTable = new Referencetable(dwhrepRock);
    rTable.setVersionid(versionid);
    final ReferencetableFactory rTypeFact = new ReferencetableFactory(dwhrepRock, rTable);
    return rTypeFact.get();
  }
}
