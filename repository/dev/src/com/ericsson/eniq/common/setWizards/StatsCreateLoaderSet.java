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
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Measurementkey;
import com.distocraft.dc5000.repository.dwhrep.MeasurementkeyFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;

import com.ericsson.eniq.common.Utils;

/**
 * This class is used for creating the loader set for a ENIQ STATS techpack.
 * 
 * @author epaujor
 * 
 */
public class StatsCreateLoaderSet extends CreateLoaderSet {

  private static final String E_RBS = "_E_RBS";

  private static final String E_RAN = "_E_RAN";

  private static final String E_CPP = "_E_CPP";

  private static final String STATS_LOADER_ACTION_TYPE_NAME = "Loader";

  public StatsCreateLoaderSet(final String templateDir, final String name, final String version,
      final String versionid, final RockFactory dwhrepRock, final RockFactory rock, final int techPackID,
      final String techPackName, final boolean schedulings) throws SQLException {
    super(templateDir, name, version, versionid, dwhrepRock, rock, techPackID, techPackName, schedulings);
  }

  @Override
  protected boolean isSqlTemplateGoingToBeEmpty() {
    boolean result = false;
    if ((this.techPackName != null)
        && (this.techPackName.endsWith(E_CPP) || this.techPackName.endsWith(E_RAN) || this.techPackName.endsWith(E_RBS))) {
      result = true;
    }
    return result;
  }

  @Override
  protected String createWhereClauseForMetaTransferActions(final String measTypeName, final String tailDir)
      throws IOException {
    final Properties prop = createPropertiesForMetaTransferActions(measTypeName, tailDir);
    return Utils.propertyToString(prop);
  }

  @Override
  protected void createDuplicateCheckAction(final long iSet, final long iAction, final Measurementtype measType,
      final Measurementtable measTable, final int order) throws SQLException, RockException {
    log.info("  Creating action DuplicateCheck_" + measType.getTypename());
    final String duplicateCheckActionContext = getDuplicateCheckQuery();
    createAction("DuplicateCheck_" + measType.getTypename(), order, "DuplicateCheck",
        new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
        measTable, "", duplicateCheckActionContext).insertToDB(rock);
  }

  @Override
  protected void createUpdateDimSessionAction(final long iSet, final long iAction, final String element,
      final Measurementtype measType, final Measurementtable measTable, final int order) throws SQLException,
      RockException, IOException {
    // UpdateDimSession
    final Properties uds = getUpdateDimSession(element);
    log.info("  Creating action UpdateDimSession_" + measType.getTypename());
    createAction("UpdateDimSession_" + measType.getTypename(), order, "UpdateDimSession",
        new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
        measTable, Utils.propertyToString(uds), "").insertToDB(rock);
  }

  @Override
  protected String getLoaderActionTypeName() {
    return STATS_LOADER_ACTION_TYPE_NAME;
  }

  /**
   * Create the sets.
   * 
   * @param skip
   *          already existing sets will be skipped if true.
   * @param skiplist
   *          list of sets
   * @throws Exception
   */
  @Override
  public void create(final boolean skip, final Vector<String> skiplist) throws Exception {

    final Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    final Iterator iType = mtf.get().iterator();

    long iSet = 0;
    long iAction = 0;
    long iScheduling = 0;
    String element = "";

    while (iType.hasNext()) {

      boolean join = false;

      final Measurementtype measType = (Measurementtype) iType.next();

      if (measType.getJoinable() != null && measType.getJoinable().length() > 0) {
        join = true;
        ignoredKeys = measType.getJoinable();
      }

      final Measurementkey mk = new Measurementkey(dwhrepRock);
      mk.setTypeid(measType.getTypeid());
      final MeasurementkeyFactory mkf = new MeasurementkeyFactory(dwhrepRock, mk);
      final Iterator keys = mkf.get().iterator();

      while (keys.hasNext()) {
        final Measurementkey key = (Measurementkey) keys.next();

        if (key.getIselement() != null && key.getIselement().intValue() == 1) {
          element = key.getDataname();
        }
      }

      // Iterator iTable = types.getMeasurementTables().iterator();
      final Measurementtable mta = new Measurementtable(dwhrepRock);
      mta.setTypeid(measType.getTypeid());
      final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
      final Iterator measTableIter = mtaf.get().iterator();

      while (measTableIter.hasNext()) {

        final Measurementtable measTable = (Measurementtable) measTableIter.next();

        final String dir = "${ETLDATA_DIR}/" + measType.getTypename().toLowerCase() + this.pathSeparator + "raw"
            + this.pathSeparator;

        if (measTable.getTablelevel().equalsIgnoreCase("raw") || measTable.getTablelevel().equalsIgnoreCase("plain")) {

          int order = 0;

          if (skiplist != null
              && skiplist.contains(measTable.getBasetablename().substring(0,
                  measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
            log.info("Skipped " + measType.getTypename());
            continue;
          }

          if (!(skip && Utils.isSet("Loader_" + measType.getTypename(), version, techPackID, rock))) {

            // create set
            log.info("Creating set Loader_" + measType.getTypename());
            createSet("Loader", new Long(this.maxSetID + iSet).intValue(), measType, measTable).insertToDB(rock);

            // temporary option
            log.info("  Creating action SQL_Execute_" + measType.getTypename());
            ActionGenerator sqlActionGenerator = createAction("SQL_Execute_" + measType.getTypename(), order,
                "SQL Execute", new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction)
                    .intValue(), measType, measTable, "", getTemporaryOptions());
            sqlActionGenerator.insertToDB(rock);
            iAction++;
            order++;

            if (join) {
              // create unpartitioned loader
              log.info("  Creating action UnPartitioned_Loader_" + measType.getTypename());
              ActionGenerator unPartitionedActionGenerator = createAction("UnPartitioned_Loader_"
                  + measType.getTypename(), order, "UnPartitioned Loader", new Long(this.maxSetID + iSet).intValue(),
                  new Long(this.maxActionID + iAction).intValue(), measType, measTable,
                  createPropertyStringUnpartitioned(measType, dir), CreateSql(measTable.getMtableid()));
              unPartitionedActionGenerator.insertToDB(rock);
              iAction++;
              order++;

            } else {
          	  // create loader
          	  log.info("  Creating action Loader_" + measType.getTypename());
          	  // 20100114, eeoidiv, WRAN Counter Capicity IP: 263/159 41-FCP 103
          	  // 8147
          	  // For WRAN techpacks (CPP,RNC,RBS), do not use a template.
          	  String createSql = null;
          	  if (this.isSqlTemplateGoingToBeEmpty()) {
          		  createSql = "";
          		  log.fine ("  For action Loader_" + measType.getTypename() + ", no SQL template as techpack is : "
          				  + this.techPackName);
          	  } else {
          		  createSql = CreateSql(measTable.getMtableid());
          	  }
          	  final String actionType = getLoaderActionTypeName();
          	  ActionGenerator loaderActionGenerator = createAction("Loader_" + measType.getTypename(), order,
          			  actionType , new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID
          					  + iAction).intValue(), measType, measTable, createWhereClauseForMetaTransferActions(measType.getTypename(), "raw"), 
          					  createSql);
          	  loaderActionGenerator.insertToDB(rock);
          	  iAction++;
          	  order++;
            }

            // create joiner action
            if (join) {
              log.info("  Creating action SQLJoiner_" + measType.getTypename());
              final String sqlJoinQuery = getSQLJoinQuery(measType.getTypename(), measTable.getMtableid());
              ActionGenerator sqlJoinerActionGenerator = createAction("SQLJoiner_" + measType.getTypename(), order,
                  "SQLJoiner", new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction)
                      .intValue(), measType, measTable, createPropertyStringJoiner(measType), sqlJoinQuery);
              sqlJoinerActionGenerator.insertToDB(rock);
              iAction++;
              order++;
            }

            // Create duplicate check action. Only for version 5.2 and newer!
            if (templateDir.equalsIgnoreCase("5.0") || templateDir.equalsIgnoreCase("5.1")) {
                log.info("Action DuplicateCheck not created because of selected 5.0 or 5.1 versions.");
              } else {
                createDuplicateCheckAction(iSet, iAction, measType, measTable, order);
                iAction++;
                order++;
              }

              // UpdateDimSession
              createUpdateDimSessionAction(iSet, iAction, element, measType, measTable, order);
              iAction++;

              if (doSchedulings) {

                // create scheduling
                String holdFlag = "N";
                String name = "Loader_" + measType.getTypename();

                log.info("  Creating Scheduling " + name);

                createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
                    new Long(this.maxSetID + iSet).intValue(), name, holdFlag).insertToDB(this.rock);
                iScheduling++;
              }

              iSet++;

            } else {
              log.info("Skipped existing set Loader_" + measType.getTypename());
            }
          }
        }
      }
    } 
  }

