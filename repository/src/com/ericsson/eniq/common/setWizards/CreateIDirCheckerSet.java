package com.ericsson.eniq.common.setWizards;

import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class CreateIDirCheckerSet {

  private Logger log = Logger.getLogger(CreateIDirCheckerSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected String version;

  protected RockFactory rock;

  protected long techPackID;

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  private String name = "";

  private String itype = "";

  private String name1 = "";

  private String pathSeparator = "/";
  private String parsertype="";
  /**
   * 
   * constructor
   * 
   * @param version
   * @param rock
   * @param techPackID
   * @param name
   * @throws Exception
   */
  public CreateIDirCheckerSet(String objType, String version, RockFactory rock, long techPackID, String name,
      String name1,String parsertype) throws Exception {

    if (objType.equalsIgnoreCase("measurement")) {
      itype = "pmdata";
    } else if (objType.equalsIgnoreCase("reference")) {
      itype = "referencedata";
    } else {
      itype = "pmdata";
    }
    this.rock = rock;
    this.techPackID = techPackID;
    this.version = version;
    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;

    this.name = name;
    this.name1 = name1;
    this.parsertype=parsertype;
  }

  public void create(boolean skip) throws Exception {

    long iSet = 0;
    long iAction = 0;

    if (skip && Utils.isSet("Directory_Checker_" + this.name, version, techPackID, rock)) {
      log.info("Skipping set Directory_Checker" + this.name);
      return;
    }

    log.info("Creating set Directory_Checker" + this.name);
    createSet("Install", new Long(this.maxSetID + iSet).intValue()).insertToDB(rock);

    Properties propIn = Utils.createProperty("permission=770");
    propIn.setProperty("group", "dcdata");
    Properties prop = Utils.createProperty("permission=770");

    // ossDir
    log.info("  Creating action CreateDir" + name);
    createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "CreateDir_ossDir",
        "${ARCHIVE_DIR}" + pathSeparator + getIBaseDirectory() + pathSeparator, Utils.propertyToString(prop))
        .insertToDB(rock);
    iAction++;

    // baseDir
    log.info("  Creating action CreateDir" + name);
    createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "CreateDir_base",
        "${ARCHIVE_DIR}" + pathSeparator + getIBaseDirectory() + pathSeparator + name1 + pathSeparator,
        Utils.propertyToString(prop)).insertToDB(rock);
    iAction++;
    log.info("  Creating action CreateDir_in");
    
    if((this.parsertype.equalsIgnoreCase("mlXml"))){
        createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
                new Long(this.maxActionID + iAction).intValue(), "CreateDir_in",
                "${PMDATA_SOEM_DIR}" + pathSeparator + getIBaseDirectory() + pathSeparator,
                Utils.propertyToString(prop)).insertToDB(rock);
    }
    else {
    	createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
    	        new Long(this.maxActionID + iAction).intValue(), "CreateDir_in",
    	        "${PMDATA_DIR}" + pathSeparator + getIBaseDirectory() + pathSeparator,
    	        Utils.propertyToString(prop)).insertToDB(rock);
    }
    iAction++;

    iAction = createCheckers(iSet, iAction, "archive", prop);
    iAction = createCheckers(iSet, iAction, "processed", prop);
    iAction = createCheckers(iSet, iAction, "double", prop);
    iAction = createCheckers(iSet, iAction, "failed", prop);

    // adapter_tmp
    log.info("  Creating action adapter_tmp");
    createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "CreateDir_output", "${ETLDATA_DIR}/adapter_tmp/",
        Utils.propertyToString(prop)).insertToDB(rock);
    iAction++;

    // adapter_tmp
    log.info("  Creating action adapter_tmp");
    createAction(iAction + 1, "CreateDir", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "CreateDir_output",
        "${ETLDATA_DIR}/adapter_tmp/" + name1 + pathSeparator, Utils.propertyToString(prop)).insertToDB(rock);
    iAction++;

  }

  public void removeSets() throws Exception {

    log.info("Removing set Directory_Checker" + this.name);
    long setid = Utils.getSetId("Directory_Checker_" + this.name, version, techPackID, rock);
    if (setid == -1) {
      log.info("No sets found, no need to remove");
      return;
    }

    // ossDir
    log.info("  Removing action CreateDir" + name);
    Utils.removeAction("CreateDir_ossDir", version, techPackID, setid, rock);

    // baseDir
    log.info("  Removing action CreateDir" + name);
    Utils.removeAction("CreateDir_base", version, techPackID, setid, rock);

    log.info("  Removing action CreateDir_in");
    Utils.removeAction("CreateDir_in", version, techPackID, setid, rock);

    removeCheckers("archive", setid);
    removeCheckers("processed", setid);
    removeCheckers("double", setid);
    removeCheckers("failed", setid);

    // adapter_tmp
    log.info("  Removing action adapter_tmp");
    Utils.removeAction("CreateDir_output", version, techPackID, setid, rock);

    // adapter_tmp
    log.info("  Removing action adapter_tmp");
    Utils.removeAction("CreateDir_output", version, techPackID, setid, rock);

    Utils.removeSet("Directory_Checker_" + this.name, version, techPackID, rock);

  }

  protected String getIBaseDirectory() {
    return Constants.OSS_DIR;
  }

  private long createCheckers(long iSet, long iAction, String foldername, Properties prop) throws Exception {

    // raw
    log.info("  Creating action CreateDir_" + name1 + "/" + foldername);
    createAction(
        iAction + 1,
        "CreateDir",
        new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(),
        "CreateDir_" + foldername,
        "${ARCHIVE_DIR}" + pathSeparator + getIBaseDirectory() + pathSeparator + name1 + pathSeparator + foldername
            + pathSeparator, Utils.propertyToString(prop)).insertToDB(rock);
    iAction++;

    return iAction;

  }

  private void removeCheckers(String foldername, long setid) throws Exception {

    // raw
    log.info("  Remove action CreateDir_" + name1 + "/" + foldername);
    Utils.removeAction("CreateDir_" + foldername, version, techPackID, setid, rock);

  }

  private SetGenerator createSet(String type, long iSet) throws Exception {

    SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME("Directory_Checker_" + this.name);
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
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

  }

  private ActionGenerator createAction(long order, String type, long iSet, int iAct, String foldername, String where,
      String actionContents) throws Exception {

    ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Long.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(foldername);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(where);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }
  
  public String getItype() {
		return itype ;
  }

}
