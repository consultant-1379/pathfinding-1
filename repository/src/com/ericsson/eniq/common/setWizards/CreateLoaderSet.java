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
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcolumn;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementkey;
import com.distocraft.dc5000.repository.dwhrep.MeasurementkeyFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 * This abstract class can be extended for creating loader sets specific to a
 * particular techpack.
 * 
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public abstract class CreateLoaderSet implements SetCreator {

  protected final Logger log;

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected String name;

  protected String version;

  protected RockFactory dwhrepRock;

  protected RockFactory rock;

  protected int techPackID;

  protected String templateName = "LoaderSQLXML.vm";

  protected String postLoadSQLTemplate = "postLoadSQL.vm";

  protected String updateDimSessionTemplate = "updateDimSession.vm";

  protected String temporaryOptionTemplate = "temporaryOption.vm";

  protected boolean son15agg = false;

  // "/" must be used as TechpackIDE runs on windows and when generating loader
  // set from TechpackIDE, "\" would be put into the directory structure which
  // would not work on unix
  protected final String pathSeparator = "/";

  protected String ignoredKeys = "";

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected long maxSchedulingID = 0;

  protected boolean doSchedulings = false;

  protected String techPackName = "";

  protected String templateDir = "";

  protected final String versionid;

  protected long iSet = 0;

  protected long iAction = 0;

  protected long iScheduling = 0;

  /**
   * constructor
   * 
   * @throws SQLException
   * 
   * 
   */
  public CreateLoaderSet(final String templateDir, final String name, final String version, final String versionid,
      final RockFactory dwhrepRock, final RockFactory rock, final int techPackID, final String techPackName,
      final boolean schedulings) throws SQLException {

    this.templateDir = templateDir;
    this.name = name;
    this.version = version;
    this.versionid = versionid;
    this.dwhrepRock = dwhrepRock;
    this.rock = rock;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;
    this.maxSchedulingID = Utils.getScheduleMaxID(rock) + 1;
    this.doSchedulings = schedulings;
    this.techPackName = techPackName;
    log = Logger.getLogger(getClass().getName());
  }

  /**
   * 
   * Merges template and context
   * 
   * @param templateName
   * @param context
   * @return string contains the output of the merge
   * @throws Exception
   */
  @Override
  public String merge(final String templateName, final VelocityContext context) throws Exception {

    final StringWriter strWriter = new StringWriter();
    final boolean isMergeOk = Velocity.mergeTemplate(templateDir + "/" + templateName, Velocity.ENCODING_DEFAULT,
        context, strWriter);
    log.finest("   Velocity Merge OK: " + isMergeOk);

    return strWriter.toString();

  }

  private Vector meastypes;

  private final Map meascolumns = new HashMap();

  private final Map meastables = new HashMap();

  protected String CreateSql(final String mTableID) throws Exception {
    final List measurementColumnList = new ArrayList();

    if (meastypes == null || meastypes.isEmpty()) {
      final Measurementtype mt = new Measurementtype(dwhrepRock);
      mt.setVersionid(versionid);
      final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);
      meastypes = mtf.get();
    }

    final Iterator iVer = meastypes.iterator();

    while (iVer.hasNext()) {

      final Measurementtype types = (Measurementtype) iVer.next();

      if (meastables == null || !meastables.containsKey(types.getTypeid())) {
        final Measurementtable mta = new Measurementtable(dwhrepRock);
        mta.setTypeid(types.getTypeid());
        final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
        meastables.put(types.getTypeid(), mtaf.get());
      }

      final Iterator iType = ((Vector) meastables.get(types.getTypeid())).iterator();

      while (iType.hasNext()) {

        final Measurementtable tables = (Measurementtable) iType.next();

        if (meascolumns == null || !meascolumns.containsKey(tables.getMtableid())) {
          final Measurementcolumn mc = new Measurementcolumn(dwhrepRock);
          mc.setMtableid(tables.getMtableid());
          final MeasurementcolumnFactory mcf = new MeasurementcolumnFactory(dwhrepRock, mc);
          meascolumns.put(tables.getMtableid(), mcf.get());
        }

        final Iterator iTable = ((Vector) meascolumns.get(tables.getMtableid())).iterator();

        while (iTable.hasNext()) {

          final Measurementcolumn columns = (Measurementcolumn) iTable.next();
          if (tables.getMtableid().equals(mTableID)) {

            final List sortList = new ArrayList();
            sortList.add(0, columns.getDataname());
            sortList.add(1, columns.getColnumber());
            measurementColumnList.add(sortList);

          }
        }
      }
    }

    class comp implements Comparator {

      public comp() {
      }

      @Override
      public int compare(final Object d1, final Object d2) {

        final List l1 = (List) d1;
        final List l2 = (List) d2;

        final Long i1 = (Long) l1.get(1);
        final Long i2 = (Long) l2.get(1);
        return i1.compareTo(i2);
      }
    }

    Collections.sort(measurementColumnList, new comp());

    final Vector vec = new Vector();
    final Iterator iColl = measurementColumnList.iterator();
    while (iColl.hasNext()) {
      vec.add(((List) iColl.next()).get(0));
    }

    final VelocityContext context = new VelocityContext();
    context.put("measurementColumn", vec);

    return merge(this.templateName, context);
  }

  /**
   * Check if the SQL template will be empty for this LoaderSet
   * 
   * @return
   */
  protected boolean isSqlTemplateGoingToBeEmpty() {
    // TODO in subclass
    return false;
  }

  protected String getSQLJoinQuery(final String typename, final String mTableID) throws Exception {

    final VelocityContext context = new VelocityContext();
    final String result = merge("JOIN.vm", context);
    log.finer("   SQL: " + result);

    return result;

  }

  @Override
  public void create(final boolean skip) throws Exception {
    create(skip, null);
  }

  /**
   * Create method for Node Version Update. Will help in creating sets for a
   * specific meas type.
   * 
   * @param typeName
   * @throws Exception
   */
  public void create(String typeName) throws Exception {
    create(false, null, typeName);
  }

  /**
   * Create set for the Node Version Update. Will create the set for the given
   * measurement type.
   * 
   * @param skip
   * @param skiplist
   * @param typeName
   * @throws Exception
   */
  public void create(boolean skip, Vector<String> skiplist, final String typeName) throws Exception {

    Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    if (typeName != null && typeName.length() > 0) {
      mt.setTypename(typeName);
    }
    MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    Iterator iType = mtf.get().iterator();

    iSet = 0;
    iAction = 0;
    iScheduling = 0;
    String element = "";

    while (iType.hasNext()) {

      boolean join = false;
      son15agg = false;

      Measurementtype measType = (Measurementtype) iType.next();

      if (measType.getJoinable() != null && measType.getJoinable().length() > 0) {
        join = true;
        ignoredKeys = measType.getJoinable();
      }

      if (measType.getSonfifteenminagg() != null && measType.getSonfifteenminagg().intValue() == 1) {
        son15agg = true;
      }

      Measurementkey mk = new Measurementkey(dwhrepRock);
      mk.setTypeid(measType.getTypeid());
      MeasurementkeyFactory mkf = new MeasurementkeyFactory(dwhrepRock, mk);
      Iterator keys = mkf.get().iterator();

      while (keys.hasNext()) {
        Measurementkey key = (Measurementkey) keys.next();

        if (key.getIselement() != null && key.getIselement().intValue() == 1)
          element = key.getDataname();
      }

      // Iterator iTable = types.getMeasurementTables().iterator();
      Measurementtable mta = new Measurementtable(dwhrepRock);
      mta.setTypeid(measType.getTypeid());
      MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
      Iterator measTableIter = mtaf.get().iterator();

      while (measTableIter.hasNext()) {

        Measurementtable measTable = (Measurementtable) measTableIter.next();

        String typename = getTypeName(measType);

        String dir = "${ETLDATA_DIR}/" + typename.toLowerCase() + this.pathSeparator + "raw" + this.pathSeparator;

        if (isValidSet(measTable)) {

          int order = 0;

          if (skiplist != null
              && skiplist.contains(measTable.getBasetablename()
                  .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
            log.info("Skipped " + typename);
            continue;
          }

          if (!(skip && Utils.isSet("Loader_" + typename, version, techPackID, rock))) {

            // create set
            log.info("Creating set Loader_" + typename);
            createSet("Loader", new Long(this.maxSetID + iSet).intValue(), measType, measTable).insertToDB(rock);

            // temporary option
            log.info("  Creating action SQL_Execute_" + typename);
            createAction("SQL_Execute_" + typename, order, "SQL Execute", new Long(this.maxSetID + iSet).intValue(),
                new Long(this.maxActionID + iAction).intValue(), measType, measTable, "", getTemporaryOptions())
                .insertToDB(rock);
            iAction++;
            order++;

            if (join) {

              // create unpartitioned loader
              log.info("  Creating action UnPartitioned_Loader_" + typename);
              createAction("UnPartitioned_Loader_" + typename, order, "UnPartitioned Loader",
                  new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
                  measTable, createPropertyStringUnpartitioned(measType, dir), CreateSql(measTable.getMtableid()))
                  .insertToDB(rock);
              iAction++;
              order++;

            } else {
              // create loader
              log.info("  Creating action Loader_" + typename);
              // 20100114, eeoidiv, WRAN Counter Capicity IP: 263/159 41-FCP 103
              // 8147
              // For WRAN techpacks (CPP,RNC,RBS), do not use a template.
              String createSql = null;
              if (this.isWranTechPack()) {
                createSql = "";
                log.finest("  For action Loader_" + typename + ", no SQL template as techpack is WRAN: "
                    + this.techPackName);
              } else {
                createSql = CreateSql(measTable.getMtableid());
              }
              createAction("Loader_" + typename, order, "Loader", new Long(this.maxSetID + iSet).intValue(),
                  new Long(this.maxActionID + iAction).intValue(), measType, measTable,
                  createWhereClauseForMetaTransferActions(typename, measTable.getTablelevel().toLowerCase()), createSql)
                  .insertToDB(rock);
              iAction++;
              order++;
            }

            // create joiner action
            if (join) {
              log.info("  Creating action SQLJoiner_" + typename);
              String sqlJoinQuery = getSQLJoinQuery(typename, measTable.getMtableid());
              createAction("SQLJoiner_" + typename, order, "SQLJoiner", new Long(this.maxSetID + iSet).intValue(),
                  new Long(this.maxActionID + iAction).intValue(), measType, measTable,
                  createPropertyStringJoiner(measType), sqlJoinQuery).insertToDB(rock);
              iAction++;
              order++;
            }

            if (son15agg) {
              // Create one more set to call aggregater set
              log.info("  Creating action Trigger_Updater_" + measType.getTypename());
              ActionGenerator triggerActionGenerator = createAction("Trigger_Aggregator_" + measType.getTypename(),
                  order, "TriggerScheduledSet", new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID
                      + iAction).intValue(), "Aggregator_" + measType.getTypename() + "_" + Constants.SON15AGG,
                  "Aggregator_" + measType.getTypename() + "_" + Constants.SON15AGG);
              triggerActionGenerator.insertToDB(rock);
              iAction++;
              order++;
            }

            // Create duplicate check action. Only for version 5.2 and newer!
            if (templateDir.equalsIgnoreCase("5.0") || templateDir.equalsIgnoreCase("5.1")) {
              log.info("Action DuplicateCheck not created because of selected 5.0 or 5.1 versions.");
            } else {
              log.info("  Creating action DuplicateCheck_" + typename);
              String duplicateCheckActionContext = getDuplicateCheckQuery();
              createAction("DuplicateCheck_" + typename, order, "DuplicateCheck",
                  new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
                  measTable, "", duplicateCheckActionContext).insertToDB(rock);
              iAction++;
              order++;
            }

            // UpdateDimSession
            Properties uds = getUpdateDimSession(element);
            log.info("  Creating action UpdateDimSession_" + typename);
            createAction("UpdateDimSession_" + typename, order, "UpdateDimSession",
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
                measTable, Utils.propertyToString(uds), "").insertToDB(rock);
            iAction++;

            if (doSchedulings) {

              // create scheduling
              String holdFlag = "N";
              String schedulingName = "Loader_" + typename;

              log.info("  Creating Scheduling " + schedulingName);

              iScheduling = createScheduling(holdFlag, schedulingName);
            }

            iSet++;

          } else {
            log.info("Skipped existing set Loader_" + typename);
          }

        }
      }
    }
  }

  /**
   * Create scheduling for loader
   * 
   * @param iSet
   * @param iScheduling
   * @param holdFlag
   * @param schedulingName
   * @return
   * @throws SQLException
   * @throws RockException
   * @throws Exception
   */
  protected long createScheduling(final String holdFlag, final String schedulingName) throws SQLException,
      RockException, Exception {
    createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
        new Long(this.maxSetID + iSet).intValue(), schedulingName, holdFlag).insertToDB(this.rock);
    iScheduling++;
    return iScheduling;
  }

  protected String getTypeName(Measurementtype measType) {
    String type = measType.getTypename();
    return type;
  }

  protected boolean isValidSet(Measurementtable measTable) {
    return (measTable.getTablelevel().equalsIgnoreCase("raw") || measTable.getTablelevel().equalsIgnoreCase("plain"));
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
  public void create(final boolean skip, final Vector<String> skiplist) throws Exception {

    final Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    final Iterator iType = mtf.get().iterator();

    iSet = 0;
    iAction = 0;
    iScheduling = 0;
    String element = "";

    while (iType.hasNext()) {

      boolean join = false;
      son15agg = false;

      final Measurementtype measType = (Measurementtype) iType.next();

      if (measType.getJoinable() != null && measType.getJoinable().length() > 0) {
        join = true;
        ignoredKeys = measType.getJoinable();
      }

      if (measType.getSonfifteenminagg() != null && measType.getSonfifteenminagg().intValue() == 1) {
        son15agg = true;
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

        String typeName = getTypeName(measType);
        final String dir = "${ETLDATA_DIR}/" + typeName.toLowerCase() + this.pathSeparator + "raw" + this.pathSeparator;

        if (isValidSet(measTable)) {

          int order = 0;

          if (skiplist != null
              && skiplist.contains(measTable.getBasetablename()
                  .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
            log.info("Skipped " + typeName);
            continue;
          }

          if (!(skip && Utils.isSet("Loader_" + typeName, version, techPackID, rock))) {

            // create set
            log.info("Creating set Loader_" + typeName);
            createSet("Loader", new Long(this.maxSetID + iSet).intValue(), measType, measTable).insertToDB(rock);

            // temporary option
            log.info("  Creating action SQL_Execute_" + typeName);
            ActionGenerator sqlActionGenerator = createAction("SQL_Execute_" + typeName, order, "SQL Execute",
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
                measTable, "", getTemporaryOptions());
            sqlActionGenerator.insertToDB(rock);
            iAction++;
            order++;

            if (join) {
              // create unpartitioned loader
              log.info("  Creating action UnPartitioned_Loader_" + typeName);
              ActionGenerator unPartitionedActionGenerator = createAction("UnPartitioned_Loader_" + typeName, order,
                  "UnPartitioned Loader", new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID
                      + iAction).intValue(), measType, measTable, createPropertyStringUnpartitioned(measType, dir),
                  CreateSql(measTable.getMtableid()));
              unPartitionedActionGenerator.insertToDB(rock);
              iAction++;
              order++;

            } else {
              // create loader
              log.info("  Creating action Loader_" + typeName);
              // 20100114, eeoidiv, WRAN Counter Capicity IP: 263/159 41-FCP 103
              // 8147
              // For WRAN techpacks (CPP,RNC,RBS), do not use a template.
              String createSql = null;
              if (this.isSqlTemplateGoingToBeEmpty()) {
                createSql = "";
                log.finest("  For action Loader_" + typeName + ", no SQL template as techpack is : "
                    + this.techPackName);
              } else {
                createSql = CreateSql(measTable.getMtableid());
              }
              ActionGenerator loaderActionGenerator = createActionType(measType, measTable, typeName, order, createSql);
              loaderActionGenerator.insertToDB(rock);
              iAction++;
              order++;
            }

            // create joiner action
            if (join) {
              log.info("  Creating action SQLJoiner_" + typeName);
              final String sqlJoinQuery = getSQLJoinQuery(typeName, measTable.getMtableid());
              ActionGenerator sqlJoinerActionGenerator = createAction("SQLJoiner_" + typeName, order, "SQLJoiner",
                  new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType,
                  measTable, createPropertyStringJoiner(measType), sqlJoinQuery);
              sqlJoinerActionGenerator.insertToDB(rock);
              iAction++;
              order++;
            }

            // For SONVIS ROPGRP aggregations
            if (son15agg) {

              // Create one more set to call aggregater set
              log.info("  Creating action Trigger_Updater_" + measType.getTypename());
              ActionGenerator triggerActionGenerator = createAction("Trigger_Aggregator_" + measType.getTypename(),
                  order, "TriggerScheduledSet", new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID
                      + iAction).intValue(), "Aggregator_" + measType.getTypename() + "_" + Constants.SON15AGG,
                  "Aggregator_" + measType.getTypename() + "_" + Constants.SON15AGG);
              triggerActionGenerator.insertToDB(rock);
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

            order = createExtraActions(order, typeName, measTable);

            if (doSchedulings) {

              // create scheduling
              String holdFlag = "N";
              String name = "Loader_" + typeName;

              log.info("  Creating Scheduling " + name);

              iScheduling = createScheduling(holdFlag, name);
            }
            iSet++;
          } else {
            log.info("Skipped existing set Loader_" + typeName);
          }
        }
      }
    }
  }

  protected ActionGenerator createActionType(final Measurementtype measType, final Measurementtable measTable,
      String typeName, int order, String createSql) throws Exception {
    final String actionType = getLoaderActionTypeName();
    ActionGenerator loaderActionGenerator = createAction("Loader_" + typeName, order, actionType, new Long(
        this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), measType, measTable,
        createPropertyString(measType, "raw"), createSql);
    return loaderActionGenerator;
  }

  protected String createPropertyString(Measurementtype types, String tailDir) throws Exception {
    Properties prop = new Properties();

    prop.setProperty("tablename", getTypeName(types));
    prop.setProperty("techpack", this.techPackName);
    prop.setProperty("taildir", tailDir);
    prop.setProperty("dateformat", "yyyy-MM-dd");

    return Utils.propertyToString(prop);
  }

  /**
   * WRAN if CPP/RAN(=RNC)/RBS
   * 
   * @return
   */
  private boolean isWranTechPack() {
    boolean result = false;
    if ((this.techPackName != null)
        && (this.techPackName.endsWith("_E_CPP") || this.techPackName.endsWith("_E_RAN") || this.techPackName
            .endsWith("_E_RBS"))) {
      result = true;
    }
    return result;
  }

  /**
   * Removes the created sets.
   * 
   * @throws Exception
   */
  @Override
  public void removeSets() throws Exception {
    removeSets(null);
  }

  /**
   * Removes the created sets.
   * 
   * @param skiplist
   *          The list of measurement and reference types to be skipped, i.e.
   *          not removed.
   * @throws Exception
   */
  public void removeSets(final Vector<String> skiplist) throws Exception {

    final Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    final Iterator iType = mtf.get().iterator();

    while (iType.hasNext()) {

      boolean join = false;

      final Measurementtype measType = (Measurementtype) iType.next();

      if (measType.getJoinable() != null && measType.getJoinable().length() > 0) {
        join = true;
        ignoredKeys = measType.getJoinable();
      }

      final Measurementtable mta = new Measurementtable(dwhrepRock);
      mta.setTypeid(measType.getTypeid());
      final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
      final Iterator measTableIter = mtaf.get().iterator();

      while (measTableIter.hasNext()) {

        final Measurementtable measTable = (Measurementtable) measTableIter.next();

        // Skip the remove in case there is a match in the skip list.
        String typeName = getTypeName(measType);
        if (skiplist != null
            && skiplist.contains(measTable.getBasetablename()
                .substring(0, measTable.getBasetablename().lastIndexOf("_")).toUpperCase())) {
          log.info("Skipped set " + typeName);
          continue;

        }
        if (isValidSet(measTable)) {

          // Remove set
          log.info("Removing set Loader_" + typeName);
          final long setid = Utils.getSetId("Loader" + "_" + typeName, version, techPackID, rock);
          if (setid == -1) {
            log.info("No sets found, no need to remove");
            continue;
          }

          // Temporary option
          log.info("  Removing action SQL_Execute_" + typeName);
          Utils.removeAction("SQL_Execute_" + typeName, version, techPackID, setid, rock);

          // Remove loader, based on joinable value
          if (join) {

            // Remove unpartitioned loader
            log.info("  Removing action UnPartitioned_Loader_" + typeName);
            Utils.removeAction("UnPartitioned_Loader_" + typeName, version, techPackID, setid, rock);

          } else {

            // Remove loader
            log.info("  Removing action Loader_" + typeName);
            Utils.removeAction("Loader_" + typeName, version, techPackID, setid, rock);
          }

          // Remove joiner action
          if (join) {
            log.info("  Removing action SQLJoiner_" + typeName);
            Utils.removeAction("SQLJoiner_" + typeName, version, techPackID, setid, rock);

          }

          // Remove duplicate check action. Only for version 5.2 and newer!
          if (templateDir.equalsIgnoreCase("5.0") || templateDir.equalsIgnoreCase("5.1")) {
            log.info("Action DuplicateCheck not Removed because of selected 5.0 or 5.1 versions.");
          } else {
            log.info("  Removing action DuplicateCheck_" + typeName);
            Utils.removeAction("DuplicateCheck_" + typeName, version, techPackID, setid, rock);
          }

          // Remove UpdateDimSession
          log.info("  Removing action UpdateDimSession_" + typeName);
          Utils.removeAction("UpdateDimSession_" + typeName, version, techPackID, setid, rock);

          // Remove scheduling
          final String name = "Loader_" + typeName;
          log.info("  Removing Scheduling " + name);
          Utils.removeScheduling(name, version, techPackID, setid, rock);

          // Remove set
          Utils.removeSet("Loader" + "_" + typeName, version, techPackID, rock);
        }
      }
    }
  }

  protected String getPostLoadSQL(final String baseTableName) {

    try {

      final VelocityContext context = new VelocityContext();
      context.put("basetablename", baseTableName);

      context.toString();

      return merge(postLoadSQLTemplate, context);

    } catch (final Exception e) {
      log.log(Level.WARNING, "Template " + postLoadSQLTemplate + " not found.", e);
    }

    return "";
  }

  protected String getTemporaryOptions() {

    try {

      final VelocityContext context = new VelocityContext();
      return merge(temporaryOptionTemplate, context);

    } catch (final Exception e) {
      log.log(Level.WARNING, "Template " + temporaryOptionTemplate + " not found.", e);
    }

    return "";
  }

  protected Properties getUpdateDimSession(final String element) {

    try {

      final VelocityContext context = new VelocityContext();
      context.put("element", element);
      return Utils.createProperty(merge(updateDimSessionTemplate, context));

    } catch (final Exception e) {
      log.log(Level.WARNING, "Template " + updateDimSessionTemplate + " not found.", e);
    }

    return null;
  }

  protected SetGenerator createSet(final String type, final long iSet, final Measurementtype types,
      final Measurementtable mTable) throws Exception {

    final SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME(type + "_" + getTypeName(types));
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(version);
    set.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    set.setPRIORITY("0");
    set.setQUEUE_TIME_LIMIT("30");
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

  }

  protected ActionGenerator createAction(final String name, final long order, final String type, final long iSet,
      final long iAct, final Measurementtype types, final Measurementtable mTable, final String whereClause,
      final String actionContents) {

    final ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(name);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(whereClause);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }

  protected ActionGenerator createAction(final String name, final long order, final String type, final long iSet,
      final long iAct, final Measurementtype types, final Measurementtable mTable, final String whereClause,
      final String actionContents, final String conID) throws Exception {

    final ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(name);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(whereClause);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID(conID);

    return loadAction;

  }

  protected ActionGenerator createAction(String name, long order, String type, long iSet, long iAct,
      String whereClause, String actionContents) throws Exception {

    ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(name);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(whereClause);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }

  protected ScheduleGenerator createWaitSchedule(final long scheduleID, final long techpackID, final long setID,
      final String schedulingName, final String holdFlag) throws Exception {

    final ScheduleGenerator schedule = new ScheduleGenerator();

    schedule.setVERSION_NUMBER(this.version);
    schedule.setID(new Long(scheduleID));
    schedule.setEXECUTION_TYPE("wait");
    schedule.setCOLLECTION_SET_ID(new Long(techpackID));
    schedule.setCOLLECTION_ID(new Long(setID));
    schedule.setNAME(schedulingName);
    schedule.setHOLD_FLAG(holdFlag);

    return schedule;
  }

  protected String createPropertyStringJoiner(final Measurementtype types) throws Exception {
    final Properties prop = new Properties();

    String typeName = getTypeName(types);
    prop.setProperty("objName", typeName);
    prop.setProperty("typeName", typeName + "_RAW");
    prop.setProperty("versionid", versionid);
    prop.setProperty("ignoredKeys", ignoredKeys);
    prop.setProperty("prevTableName", typeName + "_PREV");

    return Utils.propertyToString(prop);
  }

  protected String createPropertyStringUnpartitioned(final Measurementtype types, final String dir) throws Exception {
    final Properties prop = new Properties();

    prop.setProperty("tablename", getTypeName(types) + "_PREV");
    prop.setProperty("versionid", version);
    prop.setProperty("dir", dir);
    prop.setProperty("techpack", techPackName);
    prop.setProperty("pattern", ".+");

    return Utils.propertyToString(prop);
  }

  /**
   * Each Loader is saved into the table, META_TRANSFER_ACTIONS. In the
   * ACTION_TYPE column, a name for this particular loader is inserted. This
   * method will return the name that will go into this column
   * 
   * @return
   */
  protected abstract String getLoaderActionTypeName();

  /*
   * { // TODO in subclass return null ; }
   */

  /**
   * This creates an action which checks for duplicates in the data that is
   * being loaded
   * 
   * @param iSet
   * @param iAction
   * @param measType
   * @param measTable
   * @param order
   * @throws SQLException
   * @throws RockException
   */
  protected void createDuplicateCheckAction(final long iSet, final long iAction, final Measurementtype measType,
      final Measurementtable measTable, final int order) throws SQLException, RockException {
    // TODO in sub class
  }

  /**
   * This creates an action that updates which updates dimensioning session data
   * that is being loaded
   * 
   * @param iSet
   * @param iAction
   * @param element
   * @param measType
   * @param measTable
   * @param order
   * @throws SQLException
   * @throws RockException
   * @throws IOException
   */
  protected void createUpdateDimSessionAction(final long iSet, final long iAction, final String element,
      final Measurementtype measType, final Measurementtable measTable, final int order) throws SQLException,
      RockException, IOException {
    // TODO in subclass
  }

  /**
   * Create extra actions that need to be created
   */
  protected int createExtraActions(final int order, final String typeName, final Measurementtable measTable)
      throws Exception {
    // This will be over-written in sub-classes where necessary.
    return order;
  }

  /**
   * Generates a String which will be inserted into the WHERE_CLAUSE column in
   * the META_TRANSFER_ACTIONS table
   * 
   * @param measTypeName
   * @param tailDir
   * @return
   * @throws Exception
   */
  protected String createWhereClauseForMetaTransferActions(final String measTypeName, final String tailDir)
      throws Exception {
    // TODO in subclass
    return null;
  }

  protected Properties createPropertiesForMetaTransferActions(final String measTypeName, final String tailDir) {
    final Properties prop = new Properties();
    prop.setProperty("tablename", measTypeName);
    prop.setProperty("techpack", this.techPackName);
    prop.setProperty("taildir", tailDir);
    prop.setProperty("dateformat", "yyyy-MM-dd");
    return prop;
  }

  /**
   * This function returns the SQL query template (in string format) used in
   * duplicate checking.
   * 
   * @return String containing velocity context template.
   */
  protected String getDuplicateCheckQuery() {
    return "";
  }

  /**
   * Method to update the loader action for a given measurement type. Used by
   * Node Version Update in creating loader action for new counters in existing
   * measurement types.
   * 
   * @param mType
   * @return
   * @throws Exception
   */
  public void updateLoaderAction(final String mType) throws Exception {

    /*
     * 1.) Search for the meas type in the MeasurementType table to get the
     * joinable field. 2.) Check if the joinable is null or empty - implies
     * Loader else UnPartitioned_Loader. 3.) Get the setId from Utils.getSetId
     * 4.) Create the loader sql query. 5.) Update the meta_transfer_actions
     * record with the new loader query.
     */

    final Measurementtype measTypeWhere = new Measurementtype(dwhrepRock);
    measTypeWhere.setTypeid(mType);
    final MeasurementtypeFactory measTypeFactory = new MeasurementtypeFactory(dwhrepRock, measTypeWhere);
    final Vector<Measurementtype> measTypeList = measTypeFactory.get();
    final Measurementtype measTypeValue = measTypeList.get(0);

    final String joinable = measTypeValue.getJoinable();
    String loaderType = null;
    if (joinable != null && joinable.length() > 0) {
      loaderType = "UnPartitioned_Loader";
    } else {
      loaderType = "Loader";
    }

    log.info("Joinable:" + joinable + " loaderType:" + loaderType);
    final long setid = Utils.getSetId("Loader" + "_" + getTypeName(measTypeValue), version, techPackID, rock);

    if (setid == -1) {
      log.info("We have a problem");
      throw new Exception("Unable to find the set in the meta_collections_set for meas type:"
          + getTypeName(measTypeValue) + " version:" + version + " techPackID:" + techPackID);
    }
    final long tpId = techPackID;

    final Meta_transfer_actions mta = new Meta_transfer_actions(rock);
    mta.setTransfer_action_name(loaderType + "_" + getTypeName(measTypeValue));
    mta.setVersion_number(version);
    mta.setCollection_set_id(tpId);
    mta.setCollection_id(setid);
    final Meta_transfer_actionsFactory mtaF = new Meta_transfer_actionsFactory(rock, mta);
    if (mtaF.size() > 0) {
      final Meta_transfer_actions transferActionsObject = mtaF.getElementAt(0);
      log.info("Existing action_content:" + transferActionsObject.getAction_contents());
      final String sqlQuery = CreateSql(mType + ":RAW"); // Need to change this
                                                         // call.
      log.info("New action_content:" + sqlQuery);
      transferActionsObject.setAction_contents(sqlQuery);
      final int updateValue = transferActionsObject.updateDB();
      log.info("Update Value:" + updateValue);
      if (updateValue == 1) {
        log.info("Updated the loader sql with updateValue:" + updateValue + " for set:" + setid);
      }
    } else {
      log.info("Unable to find the meta_tranfer_actions record for tpId:" + techPackID + " setId:" + setid
          + " version:" + version);
      throw new Exception("Unable to update the loader sql. Not able to find record in meta_transfer_actions");
    }
  }
}
