package com.ericsson.eniq.common.setWizards;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JFrame;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.ericsson.eniq.common.Utils;

/**
 * This class creates the Diskmanager set for the Alarm Interface Wizard. It
 * creates the Diskmanager for the "Alarminterfaces" package.
 * 
 * @author berggren
 * 
 */
public class CreateAlarmDiskmanager {

  private Logger log = Logger.getLogger(CreateAlarmDiskmanager.class.getName());

  private JFrame frame = null;

  private String interfaceName = null;

  private RockFactory rockFactory = null;

  private Meta_collection_sets alarmInterfacesPackage = null;

  private Meta_collections diskmanagerSet = null;

  private String baseDirectory = "${ETLDATA_DIR}";

  // private String etlDataDirectory = "${PMDATA_DIR}";

  /**
   * Constructor of this class. Initializes class variables.
   * 
   * @param interfaceName
   *          Name of the interface the wizard is creating.
   * @param rockFactory
   *          Rockfactory for database operations.
   * @param alarmInterfacesPackage
   *          The "Alarminterfaces" package where this Diskmanager is to be
   *          created.
   */
  public CreateAlarmDiskmanager(String interfaceName, RockFactory rockFactory,
      Meta_collection_sets alarmInterfacesPackage, JFrame frame) {
    this.interfaceName = interfaceName;
    this.rockFactory = rockFactory;
    this.alarmInterfacesPackage = alarmInterfacesPackage;
    this.frame = frame;
  }

  /**
   * This function creates the whole Diskmanager set and all it's actions.
   */
  public void create() {

    try {

      createDiskmanagerSet();

      int actionOrder = 0;
      Properties actionContents = new Properties();

      // Create action Diskmanager_[INTERFACENAME]_archive
      actionContents = new Properties();

      actionContents.setProperty("diskManager.dir.fileAgeDay", "0");
      actionContents.setProperty("diskManager.dir.timeMask", "");
      actionContents.setProperty("diskManager.dir.fileAgeHour", "0");
      actionContents.setProperty("diskManager.dir.fileAgeHour", "0");
      actionContents.setProperty("diskManager.dir.outDir", this.baseDirectory + this.interfaceName + "/" + "archive"
          + "/");
      actionContents.setProperty("diskManager.dir.fileMask", ".+");
      actionContents.setProperty("diskManager.dir.archiveMode", "3");
      actionContents.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
      actionContents.setProperty("diskManager.dir.fileAgeMode", "0");
      actionContents.setProperty("diskManager.dir.inDir", this.baseDirectory + this.interfaceName + "/" + "archive"
          + "/");
      actionContents.setProperty("diskManager.dir.dateFormatInput", "");
      actionContents.setProperty("diskManager.dir.archivePrefix", "archive");
      actionContents.setProperty("diskManager.dir.directoryDepth", "2");

      createAction("Diskmanager_" + this.interfaceName + "_archive", new Long(actionOrder), "", actionContents, "Y",
          new Long(2));

      // Create action Diskmanager_[INTERFACENAME]_processed
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("diskManager.dir.fileAgeDay", "0");
      actionContents.setProperty("diskManager.dir.timeMask", "");
      actionContents.setProperty("diskManager.dir.fileAgeHour", "0");
      actionContents.setProperty("diskManager.dir.outDir", this.baseDirectory + this.interfaceName + "/" + "processed"
          + "/");
      actionContents.setProperty("diskManager.dir.fileMask", ".+");
      actionContents.setProperty("diskManager.dir.archiveMode", "3");
      actionContents.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
      actionContents.setProperty("diskManager.dir.fileAgeMode", "0");
      actionContents.setProperty("diskManager.dir.inDir", this.baseDirectory + this.interfaceName + "/" + "processed"
          + "/");
      actionContents.setProperty("diskManager.dir.dateFormatInput", "");
      actionContents.setProperty("diskManager.dir.archivePrefix", "archive");
      actionContents.setProperty("diskManager.dir.directoryDepth", "2");

      createAction("Diskmanager_" + this.interfaceName + "_processed", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "processed" + "/", actionContents, "Y", new Long(2));

      // Create action Diskmanager_[INTERFACENAME]_double
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("diskManager.dir.fileAgeDay", "0");
      actionContents.setProperty("diskManager.dir.timeMask", "");
      actionContents.setProperty("diskManager.dir.fileAgeHour", "0");
      actionContents.setProperty("diskManager.dir.outDir", this.baseDirectory + this.interfaceName + "/" + "double"
          + "/");
      actionContents.setProperty("diskManager.dir.fileMask", ".+");
      actionContents.setProperty("diskManager.dir.archiveMode", "3");
      actionContents.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
      actionContents.setProperty("diskManager.dir.fileAgeMode", "0");
      actionContents.setProperty("diskManager.dir.inDir", this.baseDirectory + this.interfaceName + "/" + "double"
          + "/");
      actionContents.setProperty("diskManager.dir.dateFormatInput", "");
      actionContents.setProperty("diskManager.dir.archivePrefix", "archive");
      actionContents.setProperty("diskManager.dir.directoryDepth", "2");

      createAction("Diskmanager_" + this.interfaceName + "_double", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "double" + "/", actionContents, "Y", new Long(2));

      // Create action Diskmanager_[INTERFACENAME]_failed
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("diskManager.dir.fileAgeDay", "0");
      actionContents.setProperty("diskManager.dir.timeMask", "");
      actionContents.setProperty("diskManager.dir.fileAgeHour", "0");
      actionContents.setProperty("diskManager.dir.outDir", this.baseDirectory + this.interfaceName + "/" + "failed"
          + "/");
      actionContents.setProperty("diskManager.dir.fileMask", ".+");
      actionContents.setProperty("diskManager.dir.archiveMode", "3");
      actionContents.setProperty("diskManager.dir.dateFormatOutput", "yyyy-MM-dd");
      actionContents.setProperty("diskManager.dir.fileAgeMode", "0");
      actionContents.setProperty("diskManager.dir.inDir", this.baseDirectory + this.interfaceName + "/" + "failed"
          + "/");
      actionContents.setProperty("diskManager.dir.dateFormatInput", "");
      actionContents.setProperty("diskManager.dir.archivePrefix", "archive");
      actionContents.setProperty("diskManager.dir.directoryDepth", "2");

      createAction("Diskmanager_" + this.interfaceName + "_failed", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "failed" + "/", actionContents, "Y", new Long(2));

    } catch (Exception e) {
    	//ExceptionHandler.instance().handle(e);
    	log.warning(" Exception comes while creating the Alarm diskmanager set. Exception message: " + e.getMessage());
    	e.printStackTrace();
    }

  }

  /**
   * This function creates only the Diskmanager set.
   * 
   */
  private void createDiskmanagerSet() {
    this.log.finest("Starting to create Alarm Set Diskmanager_" + this.interfaceName);
    try {
      this.diskmanagerSet = new Meta_collections(this.rockFactory);
      this.diskmanagerSet.setCollection_name("Diskmanager_" + this.interfaceName);
      this.diskmanagerSet.setMax_errors(new Long(0));
      this.diskmanagerSet.setMax_fk_errors(new Long(0));
      this.diskmanagerSet.setMax_col_limit_errors(new Long(0));
      this.diskmanagerSet.setCheck_fk_error_flag("N");
      this.diskmanagerSet.setCheck_col_limits_flag("N");
      this.diskmanagerSet.setVersion_number(this.alarmInterfacesPackage.getVersion_number());
      this.diskmanagerSet.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
      this.diskmanagerSet.setPriority(new Long(0));
      this.diskmanagerSet.setQueue_time_limit(new Long(30));
      this.diskmanagerSet.setEnabled_flag("Y");
      this.diskmanagerSet.setSettype("Service");
      this.diskmanagerSet.setFoldable_flag("Y");
      this.diskmanagerSet.setHold_flag("N");
      // TODO
      // this.diskmanagerSet.setCollection_id(AlarmInterfaceWizardView.getNewCollectionId(this.rockFactory,this.frame));

      this.diskmanagerSet.insertDB();

      this.log.finest("Alarm Set Diskmanager_" + this.interfaceName + " created succesfully.");

    } catch (Exception e) {
      this.log.log(Level.SEVERE, "Alarm Set Diskmanager_" + this.interfaceName + " creation failed", e);
    }
  }

  /**
   * This function creates one action for the Directory Checker set.
   * 
   * @param transferActionName
   *          Name of the action.
   * @param actionOrder
   *          Execution order of the action.
   * @param whereClause
   *          WhereClause of the action.
   * @param actionContents
   *          ActionContents of the action.
   * @param enabledFlag
   *          EnabledFlag od the action.
   * @param connectionId
   *          ConnectionId of the action.
   */
  private void createAction(String transferActionName, Long actionOrder, String whereClause, Properties actionContents,
      String enabledFlag, Long connectionId) throws Exception {
    this.log.finest("Starting to create Alarm Set action " + transferActionName);

    Meta_transfer_actions targetAction = new Meta_transfer_actions(this.rockFactory);
    targetAction.setVersion_number(this.alarmInterfacesPackage.getVersion_number());
    targetAction.setCollection_id(this.diskmanagerSet.getCollection_id());
    targetAction.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
    targetAction.setAction_type("Diskmanager");
    targetAction.setTransfer_action_name(transferActionName);
    targetAction.setOrder_by_no(actionOrder);
    targetAction.setDescription("");
    targetAction.setWhere_clause(whereClause);
    targetAction.setAction_contents(Utils.propertyToString(actionContents));
    targetAction.setEnabled_flag(enabledFlag);
    targetAction.setConnection_id(connectionId);
    // TODO
    // targetAction.setTransfer_action_id(AlarmInterfaceWizardView.getNewCollectionId(this.rockFactory,this.frame));

    try {
      targetAction.insertDB();
      this.log.finest("Alarm Set action " + transferActionName + " created succesfully.");
    } catch (Exception e) {
      this.log.log(Level.SEVERE, "Alarm Set action " + transferActionName + " creation failed.", e);
    }

  }
  
  public JFrame getFrame() {
	  	return this.frame;
  }

}
