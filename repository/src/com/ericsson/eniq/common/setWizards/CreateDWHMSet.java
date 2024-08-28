package com.ericsson.eniq.common.setWizards;

import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
//import com.distocraft.dc5000.etl.rock.Meta_schedulings;
//import com.distocraft.dc5000.etl.rock.Meta_schedulingsFactory;
//import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
//import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.ericsson.eniq.common.Utils;

/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class CreateDWHMSet {

  private Logger log = Logger.getLogger(CreateDWHMSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected RockFactory rock;

  protected long techPackID;

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected long maxSchedulingID = 0;

  private String techPack = "";

  private boolean doSchedulings = false;

  protected String name;

  protected String version;

  /**
   * constructor
   * 
   * 
   */
  public CreateDWHMSet(String name, String version, RockFactory rock, long techPackID, String techPackName,
      boolean schedulings) throws Exception {

    this.name = name;
    this.version = version;
    this.rock = rock;
    this.techPackID = techPackID;
    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;
    this.maxSchedulingID = Utils.getScheduleMaxID(rock) + 1;
    this.techPack = techPackName;
    this.doSchedulings = schedulings;

  }

  public void create(boolean skip) throws Exception {

    long iSet = 0;
    long iAction = 0;
    long iOrder = 0;

    long iScheduling = 0;

    Properties prop = new Properties();
    prop.setProperty("tablename", "DWHM");

    if (!(skip && isSet("DWHM_Partition_" + techPack, version, techPackID))) {

      // set
      log.info("Creating set DWHM_Partition_" + techPack);
      createSet("DWHM_Partition_" + techPack, "Partition", new Long(this.maxSetID + iSet).intValue(), name).insertToDB(
          rock);

      // Partition action
      log.info("  Creating action PartitionAction_" + techPack);
      createAction(iOrder, "PartitionAction", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "PartitionAction_" + techPack, Utils.propertyToString(prop))
          .insertToDB(rock);

      iAction++;

      if (doSchedulings) {

        // create scheduling
        String holdFlag = "N";
        iScheduling++;

        String name = "DWHM_Partition_" + techPack;

        log.info("  Creating Scheduling " + name);

        ScheduleGenerator sg = createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(),
            this.techPackID, new Long(this.maxSetID + iSet).intValue(), name, holdFlag);
        sg.insertToDB(this.rock);
      }

      iSet++;

    } else {
      log.info("Skipping set DWHM_Partition_" + techPack);
    }

    if (!(skip && isSet("DWHM_StorageTimeUpdate_" + techPack, version, techPackID))) {

      // set
      log.info("Creating set DWHM_StorageTimeUpdate_" + techPack);
      createSet("DWHM_StorageTimeUpdate_" + techPack, "Support", new Long(this.maxSetID + iSet).intValue(), name)
          .insertToDB(rock);

      // Storagetime action
      log.info("  Creating action StorageTimeAction_" + techPack);
      createAction(iOrder, "StorageTimeAction", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "StorageTimeAction_" + techPack,
          Utils.propertyToString(prop)).insertToDB(rock);

      iAction++;
      iOrder++;

      // Partition action
      log.info("  Creating action PartitionAction_" + techPack);
      createAction(iOrder, "PartitionAction", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "PartitionAction_" + techPack, Utils.propertyToString(prop))
          .insertToDB(rock);

      iAction++;
      iSet++;
      iOrder = 0;

    } else {
      log.info("Skipping set DWHM_StorageTimeUpdate_" + techPack);
    }

    if (!(skip && isSet("DWHM_Install_" + techPack, version, techPackID))) {

      // set
      log.info("Creating set DWHM_Install_" + techPack);
      createSet("DWHM_Install_" + techPack, "Install", new Long(this.maxSetID + iSet).intValue(), name)
          .insertToDB(rock);

      // VersionUpdate action
      log.info("  Creating action VersionUpdate_" + techPack);
      createAction(iOrder, "VersionUpdate", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "VersionUpdate_" + techPack, Utils.propertyToString(prop))
          .insertToDB(rock);

      iAction++;
      iOrder++;

      // Storagetime action
      log.info("  Creating action StorageTimeAction_" + techPack);
      createAction(iOrder, "StorageTimeAction", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "StorageTimeAction_" + techPack,
          Utils.propertyToString(prop)).insertToDB(rock);

      iAction++;
      iOrder++;

      // Partition action
      log.info("  Creating action PartitionAction_" + techPack);
      createAction(iOrder, "PartitionAction", new Long(this.maxSetID + iSet).intValue(),
          new Long(this.maxActionID + iAction).intValue(), "PartitionAction_" + techPack, Utils.propertyToString(prop))
          .insertToDB(rock);

    } else {
      log.info("Skipping set DWHM_Install_" + techPack);
    }
  }

  public void removeSets() throws Exception {

    // set
    log.info("Removing set DWHM_Partition_" + techPack);
    long setid = Utils.getSetId("DWHM_Partition_" + techPack, version, techPackID, rock);
    if (setid == -1) {
      log.info("No sets found, no need to remove");
      return;
    }
    // Partition action
    log.info("  Remove action PartitionAction_" + techPack);
    Utils.removeAction("PartitionAction_" + techPack, version, techPackID, setid, rock);

    // if (doSchedulings) {

    log.info("Removing Scheduling " + name);

    Utils.removeScheduling(name, version, techPackID, setid, rock);
    // }

    // set
    log.info("Remove set DWHM_StorageTimeUpdate_" + techPack);
    setid = Utils.getSetId("DWHM_StorageTimeUpdate_" + techPack, version, techPackID, rock);

    // Storagetime action
    log.info("  Remove action StorageTimeAction_" + techPack);
    Utils.removeAction("StorageTimeAction_" + techPack, version, techPackID, setid, rock);

    // Partition action
    log.info("  Remove action PartitionAction_" + techPack);
    Utils.removeAction("PartitionAction_" + techPack, version, techPackID, setid, rock);

    // set
    log.info("Remove set DWHM_Install_" + techPack);
    setid = Utils.getSetId("DWHM_Install_" + techPack, version, techPackID, rock);

    // VersionUpdate action
    log.info("  Creating action VersionUpdate_" + techPack);
    Utils.removeAction("VersionUpdate_" + techPack, version, techPackID, setid, rock);

    // Storagetime action
    log.info("  Remove action StorageTimeAction_" + techPack);
    Utils.removeAction("StorageTimeAction_" + techPack, version, techPackID, setid, rock);

    // Partition action
    log.info("  Remove action PartitionAction_" + techPack);
    Utils.removeAction("PartitionAction_" + techPack, version, techPackID, setid, rock);

    Utils.removeSet("DWHM_Partition_" + techPack, version, techPackID, rock);
    Utils.removeSet("DWHM_StorageTimeUpdate_" + techPack, version, techPackID, rock);
    Utils.removeSet("DWHM_Install_" + techPack, version, techPackID, rock);

  }

  private boolean isSet(String name, String version, long tpid) throws Exception {
    Meta_collections mc = new Meta_collections(rock);
    mc.setCollection_name(name);
    mc.setVersion_number(version);
    mc.setCollection_set_id(tpid);
    Meta_collectionsFactory mcF = new Meta_collectionsFactory(rock, mc);
    return ((Meta_collections) mcF.getElementAt(0)) != null;
  }

//  private boolean isAction(String name, String version, long tpid, long setid) throws Exception {
//    Meta_transfer_actions mta = new Meta_transfer_actions(rock);
//    mta.setTransfer_action_name(name);
//    mta.setVersion_number(version);
//    mta.setCollection_set_id(tpid);
//    mta.setCollection_id(setid);
//    Meta_transfer_actionsFactory mtaF = new Meta_transfer_actionsFactory(rock, mta);
//    return ((Meta_transfer_actions) mtaF.getElementAt(0)) != null;
//  }
//
//  private boolean isScheduling(String name, String version, long tpid, long setid) throws Exception {
//    Meta_schedulings ms = new Meta_schedulings(rock);
//    ms.setName(name);
//    ms.setVersion_number(version);
//    ms.setCollection_set_id(tpid);
//    ms.setCollection_id(setid);
//    Meta_schedulingsFactory msF = new Meta_schedulingsFactory(rock, ms);
//    return ((Meta_schedulings) msF.getElementAt(0)) != null;
//  }

  private SetGenerator createSet(String name, String type, long iSet, String techPackName) throws Exception {

    SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME(name);
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

  protected ScheduleGenerator createWaitSchedule(long scheduleID, long techpackID, long setID, String name,
      String holdFlag) throws Exception {

    ScheduleGenerator schedule = new ScheduleGenerator();

    schedule.setVERSION_NUMBER(this.version);
    schedule.setID(new Long(scheduleID));
    schedule.setEXECUTION_TYPE("wait");
    schedule.setCOLLECTION_SET_ID(new Long(techpackID));
    schedule.setCOLLECTION_ID(new Long(setID));
    schedule.setNAME(name);
    schedule.setHOLD_FLAG(holdFlag);

    return schedule;
  }

}
