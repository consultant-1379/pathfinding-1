package com.ericsson.eniq.common.setWizards;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

/**
 *
 * @author savinen Copyright Distocraft 2005
 *
 *         $id$
 */
public class CreateIDiskmanagerSet {

    private Logger log = Logger.getLogger(CreateIDiskmanagerSet.class.getName());

    protected String loaderTemplateName;

    protected String setTemplateName;

    protected String version;

    protected RockFactory rock;

    protected long techPackID;

    protected long maxSetID = 0;

    protected long maxActionID = 0;

    protected long day = 0;

    protected long hour = 0;

    private String pathSeparator = "/";

    private String diskManagerSetName = "";

    private String tmpDiskManagerSetName = "";

    private String itype = "";

    private String timeMask = "";

    private String fileMask = "";

    private String dateFormatInput = "";

    private String directoryDepth = "2";

    private String diskManagerInterfaceName = "";

    private String interfaceLogDir = "${LOG_DIR}/engine/";

    /**
     * constructor
     *
     *
     */
    public CreateIDiskmanagerSet(String objType, String version, RockFactory rock, long techPackID,
            String diskManagerSetName, String tmpDiskManagerSetName) throws Exception {

        if (objType.equalsIgnoreCase("measurement")) {
            itype = "pmdata";
        } else if (objType.equalsIgnoreCase("reference")) {
            itype = "referencedata";
        } else {
            itype = "pmdata";
        }

        this.version = version;
        this.rock = rock;
        this.techPackID = techPackID;

        this.maxSetID = Utils.getSetMaxID(rock) + 1;
        this.maxActionID = Utils.getActionMaxID(rock) + 1;
        this.diskManagerSetName = diskManagerSetName;
        this.tmpDiskManagerSetName = tmpDiskManagerSetName;
        this.day = 15;
        this.hour = 0;
        this.fileMask = ".*";
        this.diskManagerInterfaceName = diskManagerSetName;

    }

    public void create(boolean skip, boolean createSchedulings) throws Exception {

        long iSet = 0;
        long iAction = 0;

        if (skip && Utils.isSet("Diskmanager_" + diskManagerSetName, version, techPackID, rock)) {
            log.info("Skipped set: " + diskManagerSetName);
            return;
        }

        // set
        log.info("Creating set: " + diskManagerSetName);
        createSet("Service", new Long(this.maxSetID + iSet).intValue(), "Diskmanager_" + diskManagerSetName)
                .insertToDB(rock);

        // actions
        iAction = createDiskmanager(iSet, iAction, "archive");
        iAction = createDiskmanager(iSet, iAction, "processed");
        iAction = createDiskmanager(iSet, iAction, "double");
        iAction = createDiskmanager(iSet, iAction, "failed");
        iAction = createZippedArchiveDiskmanagerAction(iSet, iAction, "interface" + pathSeparator
                + diskManagerInterfaceName, interfaceLogDir + pathSeparator + diskManagerInterfaceName + '-'
                + getIBaseDirectory());

        if (createSchedulings) {
            log.info("  Creating scheduling for Diskmanager_" + "Diskmanager_" + diskManagerSetName);
            createSchedule(new Long(this.maxSetID + iSet).intValue(), "Diskmanager_" + diskManagerSetName).insertToDB(
                    this.rock);
        }
    }

    public void removeSets(boolean createSchedulings) throws Exception {

        // set
        log.info("Remove set: " + diskManagerSetName);
        long setid = Utils.getSetId("Diskmanager_" + diskManagerSetName, version, techPackID, rock);
        if (setid == -1) {
            log.info("No sets found, no need to remove");
            return;
        }
        // actions
        removeDiskmanager("archive", setid);
        removeDiskmanager("processed", setid);
        removeDiskmanager("double", setid);
        removeDiskmanager("failed", setid);
        removeDiskmanagerAction(diskManagerInterfaceName, setid);

        // if (createSchedulings){
        log.info("  Remove scheduling for Diskmanager_" + "Diskmanager_" + diskManagerSetName);
        Utils.removeScheduling("Diskmanager_" + diskManagerSetName, version, techPackID, setid, rock);
        // }

        Utils.removeSet("Diskmanager_" + diskManagerSetName, version, techPackID, rock);
    }

    private long createDiskmanager(long iSet, long iAction, String foldername) throws Exception {

        Properties prop = new Properties();

        prop.setProperty("diskManager.dir.inDir", "${ARCHIVE_DIR}" + pathSeparator + getIBaseDirectory()
                + pathSeparator + tmpDiskManagerSetName + pathSeparator + foldername + pathSeparator);
        prop.setProperty("diskManager.dir.outDir", "${ARCHIVE_DIR}" + pathSeparator + getIBaseDirectory()
                + pathSeparator + tmpDiskManagerSetName + pathSeparator + foldername + pathSeparator);
        prop.setProperty("diskManager.dir.archiveMode", "4");
        prop.setProperty("diskManager.dir.archivePrefix", "archive");
        prop.setProperty("diskManager.dir.fileAgeHour", Long.toString(this.hour));
        prop.setProperty("diskManager.dir.fileAgeDay", Long.toString(this.day));
        prop.setProperty("diskManager.dir.fileAgeMode", "0");
        prop.setProperty("diskManager.dir.timeMask", timeMask);
        prop.setProperty("diskManager.dir.fileMask", fileMask);
        prop.setProperty("diskManager.dir.dateFormatInput", dateFormatInput);
        prop.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
        prop.setProperty("diskManager.dir.directoryDepth", directoryDepth);

        log.info("  Creating action Diskmanager_" + foldername);
        String actionName = "Diskmanager_" + foldername;
        int setId = new Long(this.maxSetID + iSet).intValue();
        createAction(iAction, "Diskmanager", setId, new Long(this.maxActionID + iAction).intValue(), actionName,
                Utils.propertyToString(prop)).insertToDB(rock);
        iAction++;

        return iAction;

    }

    private long createZippedArchiveDiskmanagerAction(long iSet, long iAction, String name, String dir)
            throws Exception {

        Properties prop = new Properties();

        prop.setProperty("diskManager.dir.inDir", dir + pathSeparator);
        prop.setProperty("diskManager.dir.outDir", dir + pathSeparator);
        prop.setProperty("diskManager.dir.archiveMode", "3");
        prop.setProperty("diskManager.dir.archivePrefix", "archive");
        prop.setProperty("diskManager.dir.fileAgeHour", "0");
        prop.setProperty("diskManager.dir.fileAgeDay", "2");
        prop.setProperty("diskManager.dir.fileAgeMode", "0");
        prop.setProperty("diskManager.dir.timeMask", timeMask);
        prop.setProperty("diskManager.dir.fileMask", "(?!.*\\.zip$).*");
        prop.setProperty("diskManager.dir.dateFormatInput", dateFormatInput);
        prop.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
        prop.setProperty("diskManager.dir.directoryDepth", "5");

        log.log(Level.INFO, "  Creating diskmanager Action: " + name);
        createAction(iAction, "Diskmanager", new Long(this.maxSetID + iSet).intValue(),
                new Long(this.maxActionID + iAction).intValue(), name, Utils.propertyToString(prop)).insertToDB(rock);
        iAction++;

        return iAction;

    }

    protected String getIBaseDirectory() {
        return Constants.OSS_DIR;
    }

    private void removeDiskmanager(String foldername, long setid) throws Exception {
        log.info("  Remove action Diskmanager_" + foldername);
        String actionName = "Diskmanager_" + foldername;
        Utils.removeAction(actionName, version, techPackID, setid, rock);
    }

    private void removeDiskmanagerAction(String interfaceName, long setid) throws Exception {
        String actionName = "interface" + pathSeparator + interfaceName;
        log.log(Level.INFO, "  Remove diskmanager Action: " + actionName);
        Utils.removeAction(actionName, version, techPackID, setid, rock);
    }

    private SetGenerator createSet(String type, long iSet, String Name) throws Exception {

        SetGenerator set = new SetGenerator();
        set.setCOLLECTION_NAME(Name);
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

    private ScheduleGenerator createSchedule(long iSet, String name) throws Exception {

        ScheduleGenerator schedule = new ScheduleGenerator();
        schedule.setVERSION_NUMBER(this.version);
        schedule.setID(new Long(Utils.getScheduleMaxID(this.rock) + 1));
        schedule.setEXECUTION_TYPE("weekly");
        schedule.setSCHEDULING_MONTH(new Long(1));
        schedule.setSCHEDULING_DAY(new Long(1));
        schedule.setSCHEDULING_HOUR(new Long(5));
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
        schedule.setSCHEDULING_YEAR(new Long(2006));
        schedule.setNAME(name);

        return schedule;
    }

    public String getItype() {
        return itype;
    }

}
