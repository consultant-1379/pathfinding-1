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
 * This class creates the Directory Checker set for the Alarm Interface Wizard.
 * It creates the Directory Checker for the "Alarminterfaces" package.
 * 
 * @author berggren
 * 
 */
public class CreateAlarmDirectoryChecker {

  private Logger log = Logger.getLogger(CreateAlarmDirectoryChecker.class.getName());

  private JFrame frame = null;

  private String interfaceName = null;

  private RockFactory rockFactory = null;

  private Meta_collection_sets alarmInterfacesPackage = null;

  private Meta_collections directoryCheckerSet = null;

  private String baseDirectory = "${ETLDATA_DIR}";

  private String etlDataDirectory = "${PMDATA_DIR}";

  /**
   * Constructor of this class. Initializes class variables.
   * 
   * @param interfaceName
   *          Name of the interface the wizard is creating.
   * @param rockFactory
   *          Rockfactory for database operations.
   * @param alarmInterfacesPackage
   *          The "Alarminterfaces" package where this Directory Checker is to
   *          be created.
   * @param frame
   *          is a JFrame object for showing error window.
   */
  public CreateAlarmDirectoryChecker(String interfaceName, RockFactory rockFactory,
      Meta_collection_sets alarmInterfacesPackage, JFrame frame) {
    this.interfaceName = interfaceName;
    this.rockFactory = rockFactory;
    this.alarmInterfacesPackage = alarmInterfacesPackage;
    this.frame = frame;
  }

  /**
   * This function creates the whole Directory Checker set and all it's actions.
   */
  public void create() {

    try {
      createDirectoryCheckerSet();

      int actionOrder = 1;
      Properties actionContents = new Properties();

      // Create action CreateDir_[BASE_DIRECTORY][INTERFACEID]/
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + baseDirectory + this.interfaceName + "/", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_in
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      actionContents.setProperty("group", "dcdata");
      createAction("CreateDir_" + this.interfaceName + "_in", new Long(actionOrder), baseDirectory + this.interfaceName
          + "/" + "in" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_archive
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_archive", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "archive" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_out
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_out", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "out" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_processed
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_processed", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "processed" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_double
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_double", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "double" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[INTERFACEID]_failed
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_failed", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "failed" + "/", actionContents, "Y", new Long(2));

      // Create action CreateDir_[ETL_DATA_DIRECTORY]adapter_tmp/[INTERFACEID]/
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + etlDataDirectory + "adapter_tmp" + "/" + this.interfaceName + "/", new Long(
          actionOrder), etlDataDirectory + "adapter_tmp" + "/" + this.interfaceName + "/", actionContents, "Y",
          new Long(2));

      // Create action CreateDir_[INTERFACEID]_alarm
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("permission", "770");
      createAction("CreateDir_" + this.interfaceName + "_alarm", new Long(actionOrder), baseDirectory
          + this.interfaceName + "/" + "alarm" + "/", actionContents, "Y", new Long(2));

    } catch (Exception e) {
    	//ExceptionHandler.instance().handle(e);
    	log.warning(" Exception comes while creating the Alarm directory checker set. Exception message: " + e.getMessage());
    	e.printStackTrace();
    }

  }

  /**
   * This function creates only the Directory Checker set.
   * 
   */
  private void createDirectoryCheckerSet() {
    this.log.finest("Starting to create Alarm Set Directory_Checker_" + this.interfaceName);
    try {
      this.directoryCheckerSet = new Meta_collections(this.rockFactory);
      this.directoryCheckerSet.setCollection_name("Directory_Checker_" + this.interfaceName);
      this.directoryCheckerSet.setMax_errors(new Long(0));
      this.directoryCheckerSet.setMax_fk_errors(new Long(0));
      this.directoryCheckerSet.setMax_col_limit_errors(new Long(0));
      this.directoryCheckerSet.setCheck_fk_error_flag("N");
      this.directoryCheckerSet.setCheck_col_limits_flag("N");
      this.directoryCheckerSet.setVersion_number(this.alarmInterfacesPackage.getVersion_number());
      this.directoryCheckerSet.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
      this.directoryCheckerSet.setPriority(new Long(0));
      this.directoryCheckerSet.setQueue_time_limit(new Long(30));
      this.directoryCheckerSet.setEnabled_flag("Y");
      this.directoryCheckerSet.setSettype("Install");
      this.directoryCheckerSet.setFoldable_flag("Y");
      this.directoryCheckerSet.setHold_flag("N");
      // TODO
      // this.directoryCheckerSet.setCollection_id(AlarmInterfaceWizardView.getNewCollectionId(this.rockFactory,this.frame));

      this.directoryCheckerSet.insertDB();

      this.log.finest("Alarm Set Directory_Checker_" + this.interfaceName + " created succesfully.");

    } catch (Exception e) {
      this.log.log(Level.SEVERE, "Alarm Set Directory_Checker_" + this.interfaceName + " creation failed", e);
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
    targetAction.setCollection_id(this.directoryCheckerSet.getCollection_id());
    targetAction.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
    targetAction.setAction_type("CreateDir");
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
