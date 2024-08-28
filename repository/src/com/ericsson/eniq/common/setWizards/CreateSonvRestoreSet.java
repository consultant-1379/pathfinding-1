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
 * Create Restore Set for SONV
 * @author epaujor
 * @since 2012
 *
 */
public class CreateSonvRestoreSet extends AbstractCreateSet{

  private static final String CURRENT_DC = "_CURRENT_DC";

  private static final String PLAIN_TABLE_LEVEL = "PLAIN";

  private static final String STORE = "Store_";

  private static final String QUEUE_TIME = "72000";

  private static final String RESTORE_COUNT = "Restore_Count_";

  private final static transient Logger LOG = Logger.getLogger(CreateSonvRestoreSet.class.getName());

  private long iSet = 0;

  private long iAction = 0;

  /**
   * Constructor for CreateSonvRestoreSet
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
  public CreateSonvRestoreSet(final String name, final String version, final String versionid,
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
      final String typeNameWithTableLevel = typename +"_"+PLAIN_TABLE_LEVEL;

      final String restoreSetName = RESTORE_COUNT + typeNameWithTableLevel;

      // Skip the remove in case there is a match in the skip list.
      if (skiplist != null && skiplist.contains(typeNameWithTableLevel.toUpperCase())) {
        LOG.info("Skipped set " + restoreSetName);
        continue;
      }

      LOG.info("Removing set " + restoreSetName);
      final long setid = Utils.getSetId(restoreSetName, version, techPackID, etlrepRock);
      if (setid == -1) {
        LOG.info("No sets found, no need to remove");
        continue;
      }

      // Remove Restore action
      LOG.info("Removing action " + restoreSetName);
      Utils.removeAction(restoreSetName, version, techPackID, setid, etlrepRock);

      // Remove set
      Utils.removeSet(restoreSetName, version, techPackID, etlrepRock);

      // eromsza: History Dynamic: If data format support is false, don't handle _HIST and _CALC tables
      boolean dataFormatSupport = true;
      if (rTable.getDataformatsupport() != null) {
          dataFormatSupport = rTable.getDataformatsupport().intValue() == 1;
      }

      // eeoidiv,20121203:History Dynamic
      // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then delete additional History Table backup set.
      if(rTable.getUpdate_policy() == 4 && dataFormatSupport) {
    	final String histTypename = typename + "_HIST_RAW";
      	final String histSetName = RESTORE_COUNT + histTypename +"_"+PLAIN_TABLE_LEVEL;
      	// Remove BackupTables action
        LOG.info("Removing action " + histSetName);
        Utils.removeAction(histSetName, version, techPackID, setid, etlrepRock);
        // Remove set
        Utils.removeSet(histSetName, version, techPackID, etlrepRock);
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

        final String restoreSetName = RESTORE_COUNT + baseTableName;

        // Skip the remove in case there is a match in the skip list.
        if (skiplist != null && skiplist.contains(baseTableName.toUpperCase())) {
          LOG.info("Skipped set " + restoreSetName);
          continue;
        }

        final String storeActName = STORE + baseTableName;
        // Remove set
        LOG.info("Removing set " + restoreSetName);
        final long setid = Utils.getSetId(restoreSetName, version, techPackID, etlrepRock);
        if (setid == -1) {
          LOG.info("No sets found, no need to remove");
          continue;
        }

        // Remove StoreCountingData action
        LOG.info("Removing action " + storeActName);
        Utils.removeAction(storeActName, version, techPackID, setid, etlrepRock);

        // Remove Restore action
        LOG.info("Removing action " + restoreSetName);
        Utils.removeAction(restoreSetName, version, techPackID, setid, etlrepRock);

        // Remove set
        Utils.removeSet(restoreSetName, version, techPackID, etlrepRock);
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
      final String typeNameWithTableLevel = typename +"_"+PLAIN_TABLE_LEVEL;
      final String restoreSetName = RESTORE_COUNT + typeNameWithTableLevel;
      int order = 0;

      if (skiplist != null && skiplist.contains(typeNameWithTableLevel.toUpperCase())) {
        LOG.info("Skipped " + restoreSetName);
        continue;
      }

      if (!(skip && Utils.isSet(restoreSetName, version, techPackID, etlrepRock))) {
        // create set
        LOG.info("Creating set " + restoreSetName);
        createSet("Backup", (maxSetID + iSet), restoreSetName, QUEUE_TIME).insertToDB(etlrepRock);

        // Create Restore Action
        LOG.info("  Creating action " + restoreSetName);
        try {
          final ActionGenerator restoreActGen = createAction(restoreSetName, order, "Restore",
              (this.maxSetID + iSet), (maxActionID + iAction),
              createPropertyString(typename, PLAIN_TABLE_LEVEL, false), "");
          restoreActGen.insertToDB(etlrepRock);
          iAction++;
          order++;
        } catch (IOException e) {
          LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
          LOG.warning("Skipped " + restoreSetName + " creation because of exception");
          continue;
        }
        iSet++;

        // eromsza: History Dynamic: If data format support is false, don't handle _HIST and _CALC tables
        boolean dataFormatSupport = true;
        if (rTable.getDataformatsupport() != null) {
            dataFormatSupport = rTable.getDataformatsupport().intValue() == 1;
        }

        // eeoidiv,20121203:History Dynamic
        // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then create additional History Table backup set.
        if (rTable.getUpdate_policy() == 4 && dataFormatSupport) {
        	final String histTypename = typename + "_HIST_RAW";
        	final String histSetName = RESTORE_COUNT + histTypename +"_"+PLAIN_TABLE_LEVEL;
        	try {
        		// create set
                LOG.info("Creating set " + histSetName);
                createSet("Backup", (maxSetID + iSet), histSetName, QUEUE_TIME).insertToDB(etlrepRock);
                // Create Restore Action
                LOG.info("  Creating action " + histSetName);
                try {
                  final ActionGenerator histActGen = createAction(histSetName, order, "Restore",
                      (this.maxSetID + iSet), (maxActionID + iAction),
                      createPropertyString(histTypename, PLAIN_TABLE_LEVEL, false), "");
                  histActGen.insertToDB(etlrepRock);
                  iAction++;
                  order++;
                } catch (IOException e) {
                  LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
                  LOG.warning("Skipped " + histSetName + " creation because of exception");
                  continue;
                }
        	} catch (Exception e) {
        		LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
    	        LOG.warning("Skipped " + histSetName + " creation because of exception");
    	        continue;
      		}
            iSet++;
        }
      } else {
        LOG.info("Skipped existing set "+restoreSetName);
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
        final String shortBaseTableName = baseTableName.substring(0, baseTableName.lastIndexOf("_"));
        final String restoreSetName = RESTORE_COUNT + baseTableName;
        final String storeActName = STORE + baseTableName;
        final String tablelevel = measTable.getTablelevel();

        if (skiplist != null && skiplist.contains(baseTableName.toUpperCase())) {
          LOG.info("Skipped " + restoreSetName);
          continue;
        }

        if (!(skip && Utils.isSet(restoreSetName, version, techPackID, etlrepRock))) {
          // create set
          LOG.info("Creating set " + restoreSetName);
          createSet("Backup", (maxSetID + iSet), restoreSetName, QUEUE_TIME).insertToDB(etlrepRock);

         final  boolean isVolBasedPartition = isVolumeBasedPartition(measTable);

          // Create Restore Action
          LOG.info("  Creating action " + restoreSetName);
          try {
            final ActionGenerator RestoreActGen = createAction(restoreSetName, order, "Restore",
                (this.maxSetID + iSet), (maxActionID + iAction),
                createPropertyString(shortBaseTableName, tablelevel, isVolBasedPartition), "");
            RestoreActGen.insertToDB(etlrepRock);
            iAction++;
            order++;
          } catch (IOException e) {
            LOG.warning("Exception thrown when setting properties for where clause: " + e.getMessage());
            LOG.warning("Skipped " + restoreSetName + " creation because of exception");
            continue;
          }

          // Create StoreCountingData Action, this is only needed if partition
          // plan is volume-based
          if (isVolBasedPartition) {
            LOG.info("  Creating action " + storeActName);
            final ActionGenerator storeCountingDataActGen = createAction(storeActName, order, "StoreCountingData",
                (this.maxSetID + iSet), (maxActionID + iAction), "", "");
            storeCountingDataActGen.insertToDB(etlrepRock);
            iAction++;
            order++;
          }

          iSet++;

        } else {
          LOG.info("Skipped existing set "+restoreSetName);
        }
      }
    }
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
