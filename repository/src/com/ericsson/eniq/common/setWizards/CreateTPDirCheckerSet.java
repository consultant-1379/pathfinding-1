package com.ericsson.eniq.common.setWizards;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;
import com.distocraft.dc5000.repository.dwhrep.ReferencetableFactory;
import com.ericsson.eniq.common.Utils;

/**
 *
 * This abstract class can be extended for creating directory checker sets
 * specific to a particular techpack.
 *
 * @author ejarsav
 * @authore eheitur
 */
public abstract class CreateTPDirCheckerSet {

  protected final Logger log = Logger.getLogger(CreateTPDirCheckerSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected String name;

  protected String version;

  protected String versionid;

  // For events added:exuexie
  protected String type;

  protected final static String etldataDir = "${ETLDATA_DIR}";

  protected RockFactory dwhrepRock;

  protected RockFactory rock;

  protected long techPackID;

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected String topologyBaseDir = "${REFERENCE_DIR}";

  protected String loaderlogDir = "${LOG_DIR}/iqloader";

  protected String rejectedDir = "${REJECTED_DIR}";

  protected final static String baseLoaderlogDir = "${LOG_DIR}/iqloader";

  protected final static String baseRejectedDir = "${REJECTED_DIR}";

  protected final static String pathSeparator = "/";

  protected String techPackName = "";

  protected final List<String> RAW_PARTITIONS_TO_BACKUP = Arrays.asList("DC_E_LTE_SONV_CM");

  protected final List<String> PLAIN_TABLES_TO_BACKUP = Arrays.asList("DC_E_LTE_SONV_CM", "DIM_E_GSN",
		  "DIM_E_IMSI_IMEI","DIM_E_IMSI_MSISDN","DIM_E_LTE","DIM_E_MSS","DIM_E_RAN","DIM_E_SGEH", "DIM_Z_SGEH");

  /**
   *
   * Constructs the directory checker sets for a particular techpack.
   *
   * @param setName
   * @param version
   * @param versionId
   * @param dwhRepRock
   *          Connection to dwhrep database
   * @param etlRepRock
   *          Connection to etlrep database
   * @param techPackID
   * @param techPackName
   *          Name of the techpack
   * @throws SQLException
   * @throws Exception
   */
  public CreateTPDirCheckerSet(final String setName, final String version, final String versionId,
      final RockFactory dwhRepRock, final RockFactory etlRepRock, final long techPackID, final String techPackName)
      throws SQLException {

    this.name = setName;
    this.version = version;
    this.versionid = versionId;

    this.dwhrepRock = dwhRepRock;
    this.rock = etlRepRock;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(etlRepRock) + 1;
    this.maxActionID = Utils.getActionMaxID(etlRepRock) + 1;

    this.techPackName = techPackName;

    loaderlogDir += pathSeparator + techPackName;
    rejectedDir += pathSeparator + techPackName;

  }

  /**
   * Create the directory checker set and the actions for Node Version Update
   *
   * @param topology
   *          If true, then topology sets are created.
   * @param skip
   *          If true, the existing sets are skipped, instead of overwritten.
   * @throws Exception
   */
  public void createForAFJ(final String mType, final long setId) throws Exception {

    long iAction = 0;

    final Properties prop = new Properties();
    prop.put("permission", "750");

    final Properties loaderBaseProp = new Properties();
    loaderBaseProp.put("permission", "770");
    loaderBaseProp.put("owner", "dcuser");

    final Properties rejectedBaseProp = new Properties();
    rejectedBaseProp.put("permission", "770");
    rejectedBaseProp.put("owner", "dcuser");

    final Properties loaderProp = new Properties();
    loaderProp.put("permission", "750");
    loaderProp.put("owner", "dcuser");

    final Properties rejectedProp = new Properties();
    rejectedProp.put("permission", "750");
    rejectedProp.put("owner", "dcuser");

    final Measurementtype whereObject = new Measurementtype(dwhrepRock);
    whereObject.setTypename(mType);
    final MeasurementtypeFactory measTypeFactory = new MeasurementtypeFactory(dwhrepRock, whereObject);
    final List<Measurementtype> typeList = measTypeFactory.get();
    if (typeList.isEmpty()) {
      throw new Exception("Measurementtype empty for typename:" + mType);
    }
    final Measurementtype type = typeList.get(0);

    iAction = createETLCheckersForAFJ(setId, iAction, type.getFoldername().toLowerCase(), prop);
    iAction = createLogCheckersForAFJ(setId, iAction, type.getFoldername() + "_RAW", rejectedProp, loaderProp);

    if (type.getJoinable() != null && type.getJoinable().length() > 0) {
      iAction = createLogCheckers(setId, iAction, type.getFoldername() + "_PREV_RAW", rejectedProp, loaderProp);
    }

  }

  /**
   * Remove the directory checker set and the actions for Node Version Update
   *
   * @param mType
   * @param setId
   * @throws Exception
   */
  public void removeForAFJ(final String mType, final long setId) throws Exception {

    final Measurementtype whereObject = new Measurementtype(dwhrepRock);
    whereObject.setTypename(mType);
    final MeasurementtypeFactory measTypeFactory = new MeasurementtypeFactory(dwhrepRock, whereObject);
    final List<Measurementtype> typeList = measTypeFactory.get();
    if (typeList.isEmpty()) {
      throw new Exception("Measurementtype empty for typename:" + mType);
    }
    final Measurementtype type = typeList.get(0);

    removeETLCheckers(type.getFoldername().toLowerCase(), setId);
    removeLogCheckers(type.getFoldername().toLowerCase() + "_RAW", setId);

    if (type.getJoinable() != null && type.getJoinable().length() > 0) {
      removeLogCheckers(type.getFoldername() + "_PREV_RAW", setId);
    }

  }

  private void removeETLCheckers(final String foldername, final long setid) throws Exception {

    // meas
    log.log(Level.INFO, "  Removing directory checker Action: " + foldername);
    Utils.removeAction("CreateDir_" + foldername, version, techPackID, setid, rock);

    // raw
    log.log(Level.INFO, "  Removing directory checker Action: raw_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_raw", version, techPackID, setid, rock);

    // joined
    log.log(Level.INFO, "  Removing directory checker Action: joined_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_joined", version, techPackID, setid, rock);

  }

  /**
   * @param iSet
   * @param iAction
   * @param foldername
   * @param prop
   * @return
   * @throws Exception
   *           Method for Node Version Update. Creates the directory checker
   *           actions for a specific set id.
   */
  private long createETLCheckersForAFJ(final long iSet, long iAction, final String foldername, final Properties prop) throws Exception {

    // meas
    log.log(Level.INFO, "  Creating directory checker Action: " + foldername);
    createAction(iAction, "CreateDir", new Long(iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername, etldataDir + pathSeparator + foldername + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    iAction++;

    // raw
    log.log(Level.INFO, "  Creating directory checker Action: raw_" + foldername);
    createAction(iAction, "CreateDir", new Long(iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername + "_raw",
        etldataDir + pathSeparator + foldername + pathSeparator + "raw" + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    iAction++;

    // joined
    log.log(Level.INFO, "  Creating directory checker Action: joined_" + foldername);
    createAction(iAction, "CreateDir", new Long(iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername + "_joined",
        etldataDir + pathSeparator + foldername + pathSeparator + "joined" + pathSeparator, propertyToString(prop))
        .insertToDB(rock);
    iAction++;

    return iAction;

  }

  /**
   * @param iSet
   * @param iAction
   * @param foldername
   * @param rejProp
   * @param loadProp
   * @return
   * @throws Exception
   *           Method for Node Version Update. Creates the directory checker
   *           actions for a specific set id.
   */
  private long createLogCheckersForAFJ(final long iSet, long iAction, final String foldername, final Properties rejProp,
		  final Properties loadProp) throws Exception {

    // loaderLog
    log.log(Level.INFO, "  Creating directory checker Action: loaderLog_" + foldername);
    createAction(iAction, "CreateDir", new Long(iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername + "_loader", loaderlogDir + pathSeparator + foldername + pathSeparator,
        propertyToString(rejProp)).insertToDB(rock);
    iAction++;

    // rejected
    log.log(Level.INFO, "  Creating directory checker Action: rejected_" + foldername);
    createAction(iAction, "CreateDir", new Long(iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername + "_rejected", rejectedDir + pathSeparator + foldername + pathSeparator,
        propertyToString(loadProp)).insertToDB(rock);
    iAction++;

    return iAction;

  }

  /**
   * Create the directory checker set and the actions for it.
   *
   * @param topology
   *          If true, then topology sets are created.
   * @param skip
   *          If true, the existing sets are skipped, instead of overwritten.
   * @throws Exception
   */
  public void create(final boolean topology, final boolean skip) throws Exception {

    if (skip && Utils.isSet("Directory_Checker_" + name, version, techPackID, rock)) {
      log.log(Level.INFO, "Skipped directory checker Set: " + name);
      return;
    }

    final long collectionId = 0;
    // transferActionId used in META_TRANSFER_ACTIONS table. Incremented by 1
    // for each action and stored into DB
    long transferActionId = 0;

    final Properties prop = new Properties();
    prop.put("permission", "750");

    final Properties loaderBaseProp = new Properties();
    loaderBaseProp.put("permission", "770");
    loaderBaseProp.put("owner", "dcuser");

    final Properties rejectedBaseProp = new Properties();
    rejectedBaseProp.put("permission", "770");
    rejectedBaseProp.put("owner", "dcuser");

    final Properties loaderProp = getDefaultProps();

    final Properties rejectedProp = getDefaultProps();

    log.log(Level.INFO, "Creating directory checker Set: " + name);
    createSet("Install", new Long(this.maxSetID + collectionId).intValue(), name).insertToDB(rock);

    // basedir
    log.log(Level.INFO, "  Creating directory checker Action: base_" + getBaseEtlDirectory());
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_base_" + getBaseEtlDirectory(),
        getBaseEtlDirectory() + pathSeparator, propertyToString(prop)).insertToDB(rock);
    transferActionId++;

    // basedir for backup
    transferActionId = createBaseBackupDirAndAddToDb(transferActionId, prop, collectionId);

    // iqloaderBase
    log.log(Level.INFO, "  Creating directory checker Action: CreateDir_iqloaderBase_" + baseLoaderlogDir);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_iqloaderBase_" + baseLoaderlogDir,
        baseLoaderlogDir + pathSeparator, propertyToString(loaderBaseProp)).insertToDB(rock);
    transferActionId++;

    // rejectedBAse
    log.log(Level.INFO, "  Creating directory checker Action: CreateDir_rejectedBase_" + baseRejectedDir);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_rejectedBase_" + baseRejectedDir,
        baseRejectedDir + pathSeparator, propertyToString(rejectedBaseProp)).insertToDB(rock);
    transferActionId++;

    // iqloader+techpack
    log.log(Level.INFO, "  Creating directory checker Action: CreateDir_iqloaderBase_Techpack_" + loaderlogDir);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_iqloaderBase_Techpack_" + loaderlogDir,
        loaderlogDir + pathSeparator, propertyToString(loaderBaseProp)).insertToDB(rock);
    transferActionId++;

    // rejected+techpack
    log.log(Level.INFO, "  Creating directory checker Action: CreateDir_rejectedBase_Techpack_" + rejectedDir);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_rejectedBase_Techpack_" + rejectedDir,
        rejectedDir + pathSeparator, propertyToString(rejectedBaseProp)).insertToDB(rock);
    transferActionId++;

    if (topology) {

      // basedir
      log.log(Level.INFO, "  Creating directory checker Action: base_" + topologyBaseDir);
      createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
          new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_base_" + topologyBaseDir,
          topologyBaseDir + pathSeparator, propertyToString(prop)).insertToDB(rock);
      transferActionId++;

      // meas
      log.log(Level.INFO, "  Creating directory checker Action: base_" + topologyBaseDir + pathSeparator + techPackName
          + pathSeparator);
      createAction(transferActionId, "CreateDir", new Long(this.maxSetID + collectionId).intValue(),
          new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_base_" + techPackName,
          topologyBaseDir + pathSeparator + techPackName + pathSeparator, propertyToString(prop)).insertToDB(rock);
      transferActionId++;

      // Query q = session.createQuery("from ReferenceTable where VERSIONID=?");
      // q.setString(0,version.getVersionid());

      final Referencetable rt = new Referencetable(dwhrepRock);
      rt.setVersionid(versionid);
      final ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

      final Iterator<Referencetable> iTable = rtf.get().iterator();

      while (iTable.hasNext()) {

        int uPolicy = 0;
        final Referencetable rtable = iTable.next();

        if (rtable.getUpdate_policy() != null) {
          uPolicy = rtable.getUpdate_policy().intValue();
        }

        if (uPolicy == 1) {

          transferActionId = createLogCheckers(collectionId, transferActionId, rtable.getTypename(), rejectedProp,
              loaderProp);
          // 20110830 EANGUAN :: Adding comparison for policy number 4 for
          // History Dynamic (for SON)
        } else if ((uPolicy == 2) || (uPolicy == 3) || (uPolicy == 4)) {

          transferActionId = createLogCheckers(collectionId, transferActionId, rtable.getTypename(), rejectedProp,
              loaderProp);

          // The update policy is dynamic(2) or timed dynamic(3) or history
          // dynamic (4). Create the
          // directory checker
          // actions also for the "CURRENT_DC".
          transferActionId = createETLDirsAndAddToDbForReferenceTable(collectionId, transferActionId, rtable
              .getTypename().toLowerCase() + "_current_dc", prop);
          transferActionId = createLogCheckers(collectionId, transferActionId,
              rtable.getTypename() + "_CURRENT_DC_RAW", rejectedProp, loaderProp);

        } else {

        }
      }
    }

    // Query qm = session.createQuery("from MeasurementType where VERSIONID=?");
    // qm.setString(0,version.getVersionid());

    final Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    final Iterator<Measurementtype> iVer = mtf.get().iterator();

    // measurementtypes
    while (iVer.hasNext()) {

      final Measurementtype types = iVer.next();

      // Iterator iTable = types.getMeasurementTables().iterator();

      final Measurementtable mta = new Measurementtable(dwhrepRock);
      mta.setTypeid(types.getTypeid());
      final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
      final Iterator<Measurementtable> iTable = mtaf.get().iterator();

      // measurementtypes
      while (iTable.hasNext()) {

        final Measurementtable table = iTable.next();

        if (table.getTablelevel().equalsIgnoreCase("raw")) {
          transferActionId = createETLDirsAndAddToDbForMeasurementTable(collectionId, transferActionId, types
              .getFoldername().toLowerCase(), table.getTablelevel().toLowerCase(), prop);
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_RAW",
              rejectedProp, loaderProp);

          // Check if we need to backup RAW volume-based partitions e.g. DC_E_LTE_SONV_CM
          if (needToBackUpRaw(table.getBasetablename())) {
            transferActionId = createBackupDirsAndAddToDb(collectionId, transferActionId, types.getFoldername()
                .toLowerCase(), prop, "raw");
          }
        }

        if (table.getTablelevel().equalsIgnoreCase("15min") && isDataTiering(table.getTypeid())) {
          transferActionId = createETLDirsAndAddToDbForMeasurementTable(collectionId, transferActionId, types
              .getFoldername().toLowerCase(), table.getTablelevel().toLowerCase(), prop);
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_15MIN",
              rejectedProp, loaderProp);
        }

        if (table.getTablelevel().equalsIgnoreCase("day")) {
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_DAY",
              rejectedProp, loaderProp);
          transferActionId = createBackupDirsAndAddToDb(collectionId, transferActionId, types.getFoldername()
              .toLowerCase(), prop, "day");
        }
        if (table.getTablelevel().equalsIgnoreCase("daybh")) {
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_DAYBH",
              rejectedProp, loaderProp);
        }
        if (table.getTablelevel().equalsIgnoreCase("rankbh")) {
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_RANKBH",
              rejectedProp, loaderProp);
        }

        if (types.getJoinable() != null && types.getJoinable().length() > 0) {
          transferActionId = createLogCheckers(collectionId, transferActionId, types.getFoldername() + "_PREV_RAW",
              rejectedProp, loaderProp);
        }

      }
    }

    // qm = session.createQuery("from ReferenceTable where VERSIONID=?");
    // qm.setString(0,version.getVersionid());

    final Referencetable rt = new Referencetable(dwhrepRock);
    rt.setVersionid(versionid);
    final ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

    final Iterator<Referencetable> iReferencetable = rtf.get().iterator();

    // ReferenceTables
    while (iReferencetable.hasNext()) {
      final Referencetable rTable = iReferencetable.next();

      // Ignore the SELECT_XXX_AGGLEVEL reference type from the base techpack.
      if (rTable.getObjectname().contains("SELECT_") && rTable.getObjectname().contains("_AGGLEVEL")) {
        log.log(Level.INFO, "  Ignoring directory checker actions for reference type: " + rTable.getObjectname());
        continue;
      }

      // Create the actions.
      transferActionId = createETLDirsAndAddToDbForReferenceTable(collectionId, transferActionId, rTable.getTypename()
          .toLowerCase(), prop);
      transferActionId = createLogCheckers(collectionId, transferActionId, rTable.getTypename() + "_RAW", rejectedProp,
          loaderProp);

      // Check if we need to backup PLAIN tables e.g. DC_E_LTE_SONV_CM
      if (needToBackUpPlain(rTable.getTypename())) {
        transferActionId = createBackupDirsAndAddToDb(collectionId, transferActionId, rTable.getTypename()
            .toLowerCase(), prop, "plain");

        // eromsza: History Dynamic: If data format support is false, don't handle _HIST and _CALC tables
        boolean dataFormatSupport = true;
        if (rTable.getDataformatsupport() != null) {
            dataFormatSupport = rTable.getDataformatsupport().intValue() == 1;
        }

        // eeoidiv,20121128:History Dynamic. If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then create additional History backup dir.
        if(rTable.getUpdate_policy() == 4 && dataFormatSupport) {
        	transferActionId = createBackupDirsAndAddToDb(collectionId, transferActionId, rTable.getTypename().toLowerCase()+"_hist_raw", prop, "plain");
        	transferActionId = createLogCheckers(collectionId, transferActionId, rTable.getTypename() +"_HIST_RAW" + "_RAW", rejectedProp, loaderProp);
        }
      }

    }
  }

  /**
   * @return
   */
  protected Properties getDefaultProps() {
    final Properties defaultProps = new Properties();
    defaultProps.put("permission", "750");
    defaultProps.put("owner", "dcuser");
    return defaultProps;
  }

  /**
   * Gets the base ETL data directory for this particular techpack
   *
   * @return
   */
  protected abstract String getBaseEtlDirectory();

  /**
   * Removes the directory check set and its actions.
   *
   * @param topology
   * @throws Exception
   */
  public void removeSets(final boolean topology) throws Exception {

    log.log(Level.INFO, "Removing directory checker Set: " + name);
    final long setId = Utils.getSetId("Directory_Checker_" + name, version, techPackID, rock);
    if (setId == -1) {
      log.log(Level.INFO, "No sets found, no need to remove");
      return;
    }
    // basedir
    log.log(Level.INFO, "  Removing directory checker Action: base_" + getBaseEtlDirectory());
    Utils.removeAction("CreateDir_base_" + getBaseEtlDirectory(), version, techPackID, setId, rock);

    // basedir for backup
    removeBaseBackupDirAndAddToDb(setId);

    // iqloaderBase
    log.log(Level.INFO, "  Removing directory checker Action: CreateDir_iqloaderBase_" + baseLoaderlogDir);
    Utils.removeAction("CreateDir_iqloaderBase_" + baseLoaderlogDir, version, techPackID, setId, rock);

    // rejectedBAse
    log.log(Level.INFO, "  Removing directory checker Action: CreateDir_rejectedBase_" + baseRejectedDir);
    Utils.removeAction("CreateDir_rejectedBase_" + baseRejectedDir, version, techPackID, setId, rock);

    // iqloader+techpack
    log.log(Level.INFO, "  Removing directory checker Action: CreateDir_iqloaderBase_Techpack_" + loaderlogDir);
    Utils.removeAction("CreateDir_iqloaderBase_Techpack_" + loaderlogDir, version, techPackID, setId, rock);

    // rejected+techpack
    log.log(Level.INFO, "  Removing directory checker Action: CreateDir_rejectedBase_Techpack_" + rejectedDir);
    Utils.removeAction("CreateDir_rejectedBase_Techpack_" + rejectedDir, version, techPackID, setId, rock);

    if (topology) {

      // basedir
      log.log(Level.INFO, "  Removing directory checker Action: base_" + topologyBaseDir);
      Utils.removeAction("CreateDir_base_" + topologyBaseDir, version, techPackID, setId, rock);

      // meas
      log.log(Level.INFO, "  Removing directory checker Action: base_" + topologyBaseDir + pathSeparator + techPackName
          + pathSeparator);
      Utils.removeAction("CreateDir_base_" + techPackName, version, techPackID, setId, rock);

      final Referencetable rt = new Referencetable(dwhrepRock);
      rt.setVersionid(versionid);
      final ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

      final Iterator<Referencetable> iTable = rtf.get().iterator();

      while (iTable.hasNext()) {

        int uPolicy = 0;
        final Referencetable rtable = iTable.next();

        if (rtable.getUpdate_policy() != null) {
          uPolicy = rtable.getUpdate_policy().intValue();
        }

        if (uPolicy == 1) {

          removeLogCheckers(rtable.getTypename(), setId);

        } else if ((uPolicy == 2) || (uPolicy == 3) || (uPolicy == 4)) {

          removeLogCheckers(rtable.getTypename(), setId);

          removeETLDirsFromDbForReferenceTable(rtable.getTypename().toLowerCase() + "_current_dc", setId);
          removeLogCheckers(rtable.getTypename() + "_CURRENT_DC_RAW", setId);

        } else {

        }
      }
    }

    // Query qm = session.createQuery("from MeasurementType where VERSIONID=?");
    // qm.setString(0,version.getVersionid());

    final Measurementtype mt = new Measurementtype(dwhrepRock);
    mt.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mt);

    final Iterator<Measurementtype> iVer = mtf.get().iterator();

    // measurementtypes
    while (iVer.hasNext()) {

      final Measurementtype types = iVer.next();

      // Iterator iTable = types.getMeasurementTables().iterator();

      final Measurementtable mta = new Measurementtable(dwhrepRock);
      mta.setTypeid(types.getTypeid());
      final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
      final Iterator<Measurementtable> iTable = mtaf.get().iterator();

      // measurementtypes
      while (iTable.hasNext()) {

        final Measurementtable table = iTable.next();

        if (table.getTablelevel().equalsIgnoreCase("raw")) {
          removeETLDirsFromDbForMeasurementTable(types.getFoldername().toLowerCase(), setId);
          removeLogCheckers(types.getFoldername() + "_RAW", setId);

          // Check if we need to backup RAW volume-based partitions e.g. DC_E_LTE_SONV_CM
          if (needToBackUpRaw(table.getBasetablename())) {
            removeBackupDirs(types.getFoldername().toLowerCase(), setId, "raw");
          }
        }

        if (table.getTablelevel().equalsIgnoreCase("15min") && isDataTiering(table.getTypeid())) {
          removeETLDirsFromDbForMeasurementTable(types.getFoldername().toLowerCase() + "_15MIN", setId);
          removeLogCheckers(types.getFoldername() + "_15MIN", setId);
        }

        if (table.getTablelevel().equalsIgnoreCase("day")) {
          removeLogCheckers(types.getFoldername() + "_DAY", setId);
          removeBackupDirs(types.getFoldername().toLowerCase(), setId, "day");
        }
        if (table.getTablelevel().equalsIgnoreCase("daybh")) {
          removeLogCheckers(types.getFoldername() + "_DAYBH", setId);
        }
        if (table.getTablelevel().equalsIgnoreCase("rankbh")) {
          removeLogCheckers(types.getFoldername() + "_RANKBH", setId);
        }

        if (types.getJoinable() != null && types.getJoinable().length() > 0) {
          removeLogCheckers(types.getFoldername() + "_PREV_RAW", setId);
        }

      }
    }

    // qm = session.createQuery("from ReferenceTable where VERSIONID=?");
    // qm.setString(0,version.getVersionid());

    final Referencetable rt = new Referencetable(dwhrepRock);
    rt.setVersionid(versionid);
    final ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

    final Iterator<Referencetable> iReferencetable = rtf.get().iterator();

    // ReferenceTables
    while (iReferencetable.hasNext()) {

      final Referencetable rTable = iReferencetable.next();
      removeETLDirsFromDbForReferenceTable(rTable.getTypename().toLowerCase(), setId);
      removeLogCheckers(rTable.getTypename() + "_RAW", setId);

      // Check if we need to backup PLAIN tables e.g. DC_E_LTE_SONV_CM
      if (needToBackUpPlain(rTable.getTypename())) {
        removeBackupDirs(rTable.getTypename().toLowerCase(), setId, "plain");
        removeLogCheckers(rTable.getTypename() +"_HIST_RAW" + "_RAW", setId);
      }
    }

    Utils.removeSet("Directory_Checker_" + name, version, techPackID, rock);

  }

  /**
   * Create the appropriate ETL directories for this measurement table in this
   * techpack.
   *
   * NOTE: order that these folders are created in is important. Please do not
   * change the order
   *
   * @param iSet
   * @param transferActionId
   * @param folderName
   * @param prop
   * @return The updated transferActionId
   * @throws SQLException
   * @throws RockException
   */
  protected abstract long createETLDirsAndAddToDbForMeasurementTable(final long iSet, long transferActionId,
      final String folderName, final String tableLevel, final Properties prop) throws SQLException, RockException,
      IOException;

  /**
   * Create the appropriate backup directories for this measurement table in
   * this techpack.
   *
   * NOTE: order that these folders are created in is important. Please do not
   * change the order
   *
   * @param iSet
   * @param transferActionId
   * @param folderName
   * @param prop
   * @param tableType
   *          (the type of table, e.g. day, raw, etc.
   * @return The updated transferActionId
   * @throws SQLException
   * @throws RockException
   */
  protected abstract long createBackupDirsAndAddToDb(final long iSet, long transferActionId, final String folderName,
      final Properties prop, final String tableType) throws SQLException, RockException, IOException;

  /**
   * Create the appropriate base backup directory for this techpack. All the
   * backup directories for the measurement tables in this techpack will be
   * sub-directories in this base.
   *
   * @param transferActionId
   * @param prop
   * @param collectionId
   * @return
   * @throws SQLException
   * @throws RockException
   * @throws IOException
   */
  protected abstract long createBaseBackupDirAndAddToDb(long transferActionId, Properties prop, final long collectionId)
      throws SQLException, RockException, IOException;

  /**
   * Remove the appropriate backup directories for this Measurement Table for
   * this techpack
   *
   * NOTE: order that these folders are removed in is important. Please do not
   * change the order
   *
   * @param folderName
   * @param setId
   * @throws RockException
   * @throws SQLException
   */
  protected abstract void removeBackupDirs(final String foldername, final long setid, final String tableType)
      throws SQLException, RockException;

  /**
   * Remove the appropriate base backup directories for this techpack
   *
   * NOTE: order that these folders are removed in is important. Please do not
   * change the order
   *
   * @param folderName
   * @param setId
   * @throws RockException
   * @throws SQLException
   */
  protected abstract void removeBaseBackupDirAndAddToDb(final long setid) throws SQLException, RockException,
      IOException;

  /**
   * Create the appropriate ETL directories for this reference table in this
   * techpack.
   *
   * NOTE: order that these folders are created in is important. Please do not
   * change the order
   *
   * @param iSet
   * @param transferActionId
   * @param folderName
   * @param prop
   * @return The updated transferActionId
   * @throws SQLException
   * @throws RockException
   */
  protected abstract long createETLDirsAndAddToDbForReferenceTable(final long iSet, long transferActionId,
      final String folderName, final Properties prop) throws SQLException, RockException, IOException;

  /**
   * Remove the appropriate ETL directories for this Measurement Table for this
   * techpack
   *
   * NOTE: order that these folders are removed in is important. Please do not
   * change the order
   *
   * @param folderName
   * @param setId
   * @throws RockException
   * @throws SQLException
   */
  protected void removeETLDirsFromDbForMeasurementTable(final String folderName, final long setId) throws SQLException,
      RockException {
    // remove raw folder
    log.log(Level.INFO, "  Removing directory checker Action: raw_" + folderName);
    Utils.removeAction("CreateDir_" + folderName + "_raw", version, techPackID, setId, rock);

    // remove joined folder
    log.log(Level.INFO, "  Removing directory checker Action: joined_" + folderName);
    Utils.removeAction("CreateDir_" + folderName + "_joined", version, techPackID, setId, rock);

    // remove measurement folder
    log.log(Level.INFO, "  Removing directory checker Action: " + folderName);
    Utils.removeAction("CreateDir_" + folderName, version, techPackID, setId, rock);
  }

  /**
   * Remove the appropriate ETL directories for this reference table for this
   * techpack
   *
   * NOTE: order that these folders are removed in is important. Please do not
   * change the order
   *
   * @param folderName
   * @param setId
   * @throws RockException
   * @throws SQLException
   */
  protected void removeETLDirsFromDbForReferenceTable(final String folderName, final long setId) throws SQLException,
      RockException {
    removeETLDirsFromDbForMeasurementTable(folderName, setId);
  }

  protected long createLogCheckers(final long iSet, long transferActionId, final String foldername,
      final Properties rejProp, final Properties loadProp) throws SQLException, RockException, IOException {

    // loaderLog
    log.log(Level.INFO, "  Creating directory checker Action: loaderLog_" + foldername);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + foldername + "_loader",
        loaderlogDir + pathSeparator + foldername + pathSeparator, propertyToString(rejProp)).insertToDB(rock);
    transferActionId++;

    // rejected
    log.log(Level.INFO, "  Creating directory checker Action: rejected_" + foldername);
    createAction(transferActionId, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + transferActionId).intValue(), "CreateDir_" + foldername + "_rejected",
        rejectedDir + pathSeparator + foldername + pathSeparator, propertyToString(loadProp)).insertToDB(rock);
    transferActionId++;

    return transferActionId;

  }

  protected void removeLogCheckers(final String foldername, final long setid) throws RockException, SQLException {

    // loaderLog
    log.log(Level.INFO, "  Removing directory checker Action: loaderLog_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_loader", version, techPackID, setid, rock);

    // rejected
    log.log(Level.INFO, "  Removing directory checker Action: rejected_" + foldername);
    Utils.removeAction("CreateDir_" + foldername + "_rejected", version, techPackID, setid, rock);
  }

  private SetGenerator createSet(final String setType, final long iSet, final String techPackName) throws Exception {

    final SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME("Directory_Checker_" + techPackName);
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(version);
    set.setCOLLECTION_SET_ID(Long.toString(techPackID));
    set.setPRIORITY("0");
    set.setQUEUE_TIME_LIMIT("30");
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(setType);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

  }

  protected ActionGenerator createAction(final long order, final String actionType, final long collectionId,
      final int transferActionId, final String foldername, final String where, final String actionContents) {

    final ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(transferActionId));
    loadAction.setCOLLECTION_ID(Long.toString(collectionId));
    loadAction.setCOLLECTION_SET_ID(Long.toString(techPackID));
    loadAction.setACTION_TYPE(actionType);
    loadAction.setTRANSFER_ACTION_NAME(foldername);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(where);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }

  /*
   * private long getSetMaxID(RockFactory rockFact) throws Exception {
   *
   * Meta_collections coll = new Meta_collections(rockFact);
   *
   * Meta_collectionsFactory dbCollections = new
   * Meta_collectionsFactory(rockFact, coll);
   *
   * long largest = 0; Vector dbVec = dbCollections.get(); for (int i = 0; i <
   * dbVec.size(); i++) { Meta_collections collection = (Meta_collections)
   * dbVec.elementAt(i); if (largest <
   * collection.getCollection_id().longValue()) largest =
   * collection.getCollection_id().longValue(); }
   *
   * return largest;
   *
   * }
   *
   * private long getActionMaxID(RockFactory rockFact) throws Exception {
   *
   * Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
   *
   * Meta_transfer_actionsFactory dbCollections = new
   * Meta_transfer_actionsFactory(rockFact, act);
   *
   * long largest = 0; Vector dbVec = dbCollections.get(); for (int i = 0; i <
   * dbVec.size(); i++) { Meta_transfer_actions action = (Meta_transfer_actions)
   * dbVec.elementAt(i); if (largest <
   * action.getTransfer_action_id().longValue()) largest =
   * action.getTransfer_action_id().longValue(); }
   *
   * return largest;
   *
   * }
   */

  /**
   * 20110830 EANGUAN :: This functionn could cause problem as prop is null and
   * we are calling load function on it. So, commenting out this function.
   */
  // protected Properties stringToProperty(final String str) throws Exception {
  //
  // final Properties prop = null;
  //
  // if (str != null && str.length() > 0) {
  // final ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes());
  // prop.load(bais);
  // bais.close();
  // }
  //
  // return prop;
  //
  // }

  /**
   * Returns the property as a string
   *
   * @param prop
   * @return
   * @throws IOException
   */
  protected String propertyToString(final Properties prop) throws IOException {

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    prop.store(baos, "");

    return baos.toString();
  }

  protected Properties createProperty(final String str) {

    final Properties prop = new Properties();
    final StringTokenizer st = new StringTokenizer(str, "=");
    final String key = st.nextToken();
    final String value = st.nextToken();
    prop.setProperty(key.trim(), value.trim());

    return prop;

  }

  private boolean isDataTiering(final String typeName) {
    boolean isDataTiering = false;
    final String dataTieringTps = StaticProperties.getProperty("dataTieringTps", "EVENT_E_LTE,EVENT_E_SGEH,EVENT_E_RAN_CFA");

    for (String dataTieringTp : dataTieringTps.split(",")) {
      if (versionid.startsWith(dataTieringTp)) {
        if (isDataTieringMeasType(typeName)) {
          isDataTiering = true;
        }
      }
    }

    return isDataTiering;
  }

  /**
   * @param measTable
   * @return
   */
  protected boolean isDataTieringMeasType(final String typeName) {
    boolean result = false;
    final String dataTieringMeasTypes = StaticProperties.getProperty("dataTieringMeasType", "_SUC,_ALLCALLS,MCC_MNC_ROAM");

    for (String dataTieringMeasType : dataTieringMeasTypes.split(",")) {
      if ((typeName != null) && (typeName.endsWith(dataTieringMeasType))) {
        result = true;
      }
    }
    return result;
  }

  /**
   * Compares checkName against items in nameList.
   * nameIsInList returns true if checkName contains any of the items in nameList.
   * nameIsInList returns false if
   * 1. either parameter is null,
   * 2. namelist is empty,
   * 3. none of the items in nameList is found in checkName.
   *
   * @param checkName
   * @param nameList
   * @return
   */
  protected boolean nameIsInList(final String checkName, final List<String> nameList) {

	  boolean foundName = false;

	  if (checkName!=null && nameList!=null && nameList.size()>0) {
		  for (String name : nameList) {
			  if (checkName.contains(name)) {
				  foundName = true;
				  break;
			  }
		  }
	  }

	  return foundName;
  }

  /**
   * @param tableName
   * @return True if raw partitions need backing up for tableName
   */
  protected boolean needToBackUpRaw(final String tableName) {
	  return nameIsInList(tableName, RAW_PARTITIONS_TO_BACKUP);
  }

  /**
   * @param typeName
   * @return True if plain tables need backing up for typeName
   */
  protected boolean needToBackUpPlain(final String typeName) {
	  return nameIsInList(typeName, PLAIN_TABLES_TO_BACKUP);
  }
}
