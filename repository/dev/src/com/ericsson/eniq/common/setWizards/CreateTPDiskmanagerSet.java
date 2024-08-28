package com.ericsson.eniq.common.setWizards;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Utils;

/**
 * This class is used for managing a disk manager set for a techpack. 
 * 
 * @author ejarsav
 * @author eheitur
 */
public class CreateTPDiskmanagerSet {

  private Logger log = Logger.getLogger(CreateTPDiskmanagerSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected String name;

  protected String version;

  protected RockFactory rock;

  protected long techPackID;

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected long day = 0;

  protected long hour = 0;

  private String loaderlogDir = "${LOG_DIR}/iqloader";

  private String rejectedDir = "${REJECTED_DIR}";

  private String TPLogDir = "${LOG_DIR}/engine/";

  private String pathSeparator = "/";

  private String timeMask = "";

  private String dateFormatInput = "";

  private String directoryDepth = "5";

  private String techPack = "";

  /**
   * Constructor.
   * 
   * @param name
   * @param version
   * @param rock
   * @param techPackID
   * @param techPackName
   * @throws Exception
   */
  public CreateTPDiskmanagerSet(String name, String version, RockFactory rock, long techPackID, String techPackName)
      throws Exception {

    this.name = name;
    this.version = version;
    this.rock = rock;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;

    this.day = 2;
    this.hour = 0;
    this.techPack = techPackName;

  }

  /**
   * Creates the diskmanager set.
   * 
   * @param skip
   *          If true, the an existing set will be skipped. If false, existing
   *          set will be overwritten.
   * @param createSchedulings
   *          If true, then the scheduling is created for the set.
   * 
   * @throws Exception
   */
  public void create(boolean skip, boolean createSchedulings) throws Exception {

    long iSet = 0;
    long iAction = 0;

    if (skip && Utils.isSet("Diskmanager_" + name, version, techPackID, rock)) {
      log.log(Level.INFO, "Skipping Diskmanager Set: " + name);
      return;
    }

    // set
    log.log(Level.INFO, "Creating Diskmanager Set: " + name);
    createSet("Service", new Long(this.maxSetID + iSet).intValue(), this.name).insertToDB(rock);

    // actions
    iAction = createDiskmanagerAction(iSet, iAction, "loaderLog" + pathSeparator + techPack, loaderlogDir
        + pathSeparator + techPack);
    iAction = createDiskmanagerAction(iSet, iAction, "rejected" + pathSeparator + techPack, rejectedDir + pathSeparator
        + techPack);
    iAction = createDiskmanagerAction(iSet, iAction, "techpack" + pathSeparator + techPack, TPLogDir + pathSeparator
        + techPack);

    if (createSchedulings) {
      log.info("  Creating scheduling for Diskmanager_" + "Diskmanager_" + name);
      createSchedule(new Long(this.maxSetID + iSet).intValue(), "Diskmanager_" + name).insertToDB(this.rock);
    }
  }

  /**
   * Removes the sets for the diskmanager.
   * 
   * @throws Exception
   */
  public void removeSets() throws Exception {

    // set
    log.log(Level.INFO, "Remove Diskmanager Set: " + name);
    long setid = Utils.getSetId("Diskmanager_" + this.name, version, techPackID, rock);
    if (setid == -1) {
      log.log(Level.INFO, "No sets found, no need to remove");
      return;
    }
    // actions
    removeDiskmanagerAction("loaderLog" + pathSeparator + techPack, setid);
    removeDiskmanagerAction("rejected" + pathSeparator + techPack, setid);
    removeDiskmanagerAction("techpack" + pathSeparator + techPack, setid);

    Utils.removeSet("Diskmanager_" + this.name, version, techPackID, rock);

    log.info("  Remove scheduling for Diskmanager_" + "Diskmanager_" + name);
    Utils.removeScheduling("Diskmanager_" + name, version, techPackID, setid, rock);

  }

  /**
   * Creates an action for the diskmanager.
   * 
   * @param iSet
   * @param iAction
   * @param name
   * @param dir
   * @return
   * @throws Exception
   */
  private long createDiskmanagerAction(long iSet, long iAction, String name, String dir) throws Exception {

    Properties prop = new Properties();

    prop.setProperty("diskManager.dir.inDir", dir + pathSeparator);
    prop.setProperty("diskManager.dir.outDir", dir + pathSeparator);
    prop.setProperty("diskManager.dir.archiveMode", "3");
    prop.setProperty("diskManager.dir.archivePrefix", "archive");
    prop.setProperty("diskManager.dir.fileAgeHour", Long.toString(this.hour));
    prop.setProperty("diskManager.dir.fileAgeDay", Long.toString(this.day));
    prop.setProperty("diskManager.dir.fileAgeMode", "0");
    prop.setProperty("diskManager.dir.timeMask", timeMask);
    prop.setProperty("diskManager.dir.fileMask", "(?!.*\\.zip$).*");
    prop.setProperty("diskManager.dir.dateFormatInput", dateFormatInput);
    prop.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
    prop.setProperty("diskManager.dir.directoryDepth", directoryDepth);

    log.log(Level.INFO, "  Creating diskmanager Action: " + name);
    createAction(iAction, "Diskmanager", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), name, Utils.propertyToString(prop)).insertToDB(rock);
    iAction++;

    return iAction;

  }

  /**
   * Removes an action for the diskmanager.
   * 
   * @param name
   * @param setid
   * @throws Exception
   */
  private void removeDiskmanagerAction(String name, long setid) throws Exception {

    log.log(Level.INFO, "  Remove diskmanager Action: " + name);
    Utils.removeAction(name, version, techPackID, setid, rock);
  }

  /**
   * Creates the diskmanager set.
   * 
   * @param type
   * @param iSet
   * @param techPackName
   * @return
   * @throws Exception
   */
  private SetGenerator createSet(String type, long iSet, String techPackName) throws Exception {

    SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME("Diskmanager_" + techPackName);
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(version);
    set.setCOLLECTION_SET_ID(Long.toString(techPackID));
    set.setPRIORITY("3");
    set.setQUEUE_TIME_LIMIT("30");
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

  }

  /**
   * Creates an action for the diskmanager set.
   * 
   * @param order
   * @param type
   * @param iSet
   * @param iAct
   * @param foldername
   * @param actionContents
   * @return
   * @throws Exception
   */
  private ActionGenerator createAction(long order, String type, long iSet, int iAct, String foldername,
      String actionContents) throws Exception {

    ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Long.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(foldername);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE("");
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }

  /**
   * Creates a scheduling for the diskmanager set. The set will be scheduled to
   * run daily on 08:00.
   * 
   * @param iSet
   * @param name
   * @return
   * @throws Exception
   */
  private ScheduleGenerator createSchedule(long iSet, String name) throws Exception {

    ScheduleGenerator schedule = new ScheduleGenerator();
    schedule.setVERSION_NUMBER(this.version);
    schedule.setID(new Long(Utils.getScheduleMaxID(this.rock) + 1));
    schedule.setEXECUTION_TYPE("weekly");
    schedule.setSCHEDULING_MONTH(new Long(1));
    schedule.setSCHEDULING_DAY(new Long(1));
    schedule.setSCHEDULING_HOUR(new Long(8));
    schedule.setSCHEDULING_MIN(new Long(0));
    schedule.setCOLLECTION_SET_ID(new Long(techPackID));
    schedule.setCOLLECTION_ID(new Long(iSet));
    schedule.setMON_FLAG("Y");
    schedule.setTUE_FLAG("Y");
    schedule.setWED_FLAG("Y");
    schedule.setTHU_FLAG("Y");
    schedule.setFRI_FLAG("Y");
    schedule.setSAT_FLAG("Y");
    schedule.setSUN_FLAG("Y");
    schedule.setHOLD_FLAG("N");
    schedule.setSCHEDULING_YEAR(new Long(2009));
    schedule.setNAME(name);

    return schedule;
  }
}
