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
 * This class creates the Adapter set for the Alarm Interface Wizard. It creates
 * the Adapter for the "Alarminterfaces" package.
 * 
 * @author berggren
 * 
 */
public class CreateAlarmAdapter {

  private static final Logger log = Logger.getLogger(CreateAlarmAdapter.class.getName());

  private JFrame frame = null;

  private String interfaceName = null;

  private RockFactory rockFactory = null;

  private Meta_collection_sets alarmInterfacesPackage = null;

  private Meta_collections adapterSet = null;

  private String baseDirectory = "${ETLDATA_DIR}";

  private String etlDataDirectory = "${PMDATA_DIR}";

  private String alarmHandlerDescription = new String("");

  private String host = new String("");

  private String username = new String("");

  private String password = new String("");

  private String alarmTemplate = new String("");

  /**
   * Constructor of this class. Initializes class variables.
   * 
   * @param interfaceName
   *          Name of the interface the wizard is creating.
   * @param rockFactory
   *          Rockfactory for database operations.
   * @param alarmInterfacesPackage
   *          The "Alarminterfaces" package where this Adapter is to be created.
   * @param frame
   *          JFrame object to show error window.
   */
  public CreateAlarmAdapter(String interfaceName, RockFactory rockFactory, Meta_collection_sets alarmInterfacesPackage,
      String alarmHandlerDescription, String host, String username, String password, String alarmTemplate, JFrame frame) {
    this.interfaceName = interfaceName;
    this.rockFactory = rockFactory;
    this.alarmInterfacesPackage = alarmInterfacesPackage;
    this.alarmHandlerDescription = alarmHandlerDescription;
    this.host = host;
    this.username = username;
    this.password = password;
    this.alarmTemplate = alarmTemplate;
    this.frame = frame;
  }

  /**
   * This function creates the whole Adapter set and all it's actions.
   */
  public void create() {

    try {

      createAdapterSet();

      int actionOrder = 1;
      Properties actionContents = new Properties();

      // Create action AlarmHandler_[INTERFACENAME]
      actionContents = new Properties();
      actionContents.setProperty("outputPath", this.baseDirectory + this.interfaceName + "/" + "in");
      actionContents.setProperty("host", this.host);
      actionContents.setProperty("username", this.username);
      actionContents.setProperty("password", this.password);
      actionContents.setProperty("outputFilePrefix", "alarm_");
      actionContents.setProperty("interfaceId", this.interfaceName);

      createAction("AlarmHandler", "AlarmHandler_" + this.interfaceName, new Long(actionOrder),
          this.alarmHandlerDescription, "", actionContents, "Y", new Long(1));

      // Create action AlarmParser_[INTERFACENAME]
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("tag_id", "alarm");
      actionContents.setProperty("afterParseAction", "move");
      actionContents.setProperty("ProcessedFiles.fileNameFormat", "");
      actionContents.setProperty("AlarmTemplate", this.alarmTemplate);
      actionContents.setProperty("transformer", "");
      actionContents.setProperty("interfaceName", "INTF_DC_Z_ALARM");
      actionContents.setProperty("baseDir", this.baseDirectory + this.interfaceName + "/");
      actionContents.setProperty("ProcessedFiles.processedDir", this.baseDirectory + this.interfaceName + "/"
          + "processed" + "/");
      actionContents.setProperty("loaderDir", this.etlDataDirectory);
      actionContents.setProperty("dateformat", "yyyy-MM-dd HH:mm:ss");
      actionContents.setProperty("maxFilesPerRun", "0");
      actionContents.setProperty("minFileAge", "0");
      actionContents.setProperty("dublicateCheck", "false");
      actionContents.setProperty("outDir", this.etlDataDirectory + "adapter_tmp" + "/" + this.interfaceName + "/");
      actionContents.setProperty("parserType", "alarm");

      createAction("Parse", "AlarmParser_" + this.interfaceName, new Long(actionOrder), "", "", actionContents, "Y",
          new Long(1));

      // Create action Trigger_loaders_alarm
      actionOrder++;
      actionContents = new Properties();
      actionContents.setProperty("", "");

      createAction("TriggerScheduledSet", "Trigger_loaders_alarm", new Long(actionOrder), "", "", actionContents, "Y",
          new Long(0));

    } catch (Exception e) {
    	//ExceptionHandler.instance().handle(e);
    	log.warning(" Exception comes while creating the Alarm set. Exception message: " + e.getMessage());
    	e.printStackTrace();
    }
  }

  /**
   * This function creates only the Adapter set.
   * 
   */
  private void createAdapterSet() {
    log.finest("Starting to create Alarm Set Adapter_" + this.interfaceName + "_alarm.");
    try {
      this.adapterSet = new Meta_collections(this.rockFactory);
      this.adapterSet.setCollection_name("Adapter_" + this.interfaceName + "_alarm");
      this.adapterSet.setMax_errors(new Long(0));
      this.adapterSet.setMax_fk_errors(new Long(0));
      this.adapterSet.setMax_col_limit_errors(new Long(0));
      this.adapterSet.setCheck_fk_error_flag("N");
      this.adapterSet.setCheck_col_limits_flag("N");
      this.adapterSet.setVersion_number(this.alarmInterfacesPackage.getVersion_number());
      this.adapterSet.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
      this.adapterSet.setPriority(new Long(0));
      this.adapterSet.setQueue_time_limit(new Long(30));
      this.adapterSet.setEnabled_flag("Y");
      this.adapterSet.setSettype("Alarm");
      this.adapterSet.setFoldable_flag("Y");
      this.adapterSet.setHold_flag("N");
      // TODO
      // this.adapterSet.setCollection_id(AlarmInterfaceWizardView.getNewCollectionId(this.rockFactory,this.frame));

      this.adapterSet.insertDB();

      log.finest("Alarm Set Adapter_" + this.interfaceName + "_alarm created succesfully.");

    } catch (Exception e) {
      log.log(Level.SEVERE, "Alarm Set Adapter_" + this.interfaceName + "_alarm creation failed", e);
    }
  }

  /**
   * This function creates one action for the Directory Checker set.
   * 
   * @param actionType
   *          Type of the action.
   * @param transferActionName
   *          Name of the action.
   * @param actionOrder
   *          Execution order of the action.
   * @param description
   *          Description of the action.
   * @param whereClause
   *          WhereClause of the action.
   * @param actionContents
   *          ActionContents of the action.
   * @param enabledFlag
   *          EnabledFlag od the action.
   * @param connectionId
   *          ConnectionId of the action.
   */
  private void createAction(String actionType, String transferActionName, Long actionOrder, String description,
      String whereClause, Properties actionContents, String enabledFlag, Long connectionId) throws Exception {
    log.finest("Starting to create Alarm Set action " + transferActionName);

    Meta_transfer_actions targetAction = new Meta_transfer_actions(this.rockFactory);
    targetAction.setVersion_number(this.alarmInterfacesPackage.getVersion_number());
    targetAction.setCollection_id(this.adapterSet.getCollection_id());
    targetAction.setCollection_set_id(this.alarmInterfacesPackage.getCollection_set_id());
    targetAction.setAction_type(actionType);
    targetAction.setTransfer_action_name(transferActionName);
    targetAction.setOrder_by_no(actionOrder);
    targetAction.setDescription(description);
    targetAction.setWhere_clause(whereClause);
    if (actionContents.size() > 0) {
      targetAction.setAction_contents(Utils.propertyToString(actionContents));
    } else {
      // NOTE: Do not let the Action_contents to be null.
      targetAction.setAction_contents("");
    }
    targetAction.setEnabled_flag(enabledFlag);
    targetAction.setConnection_id(connectionId);
    // TODO
    // targetAction.setTransfer_action_id(AlarmInterfaceWizardView.getNewCollectionId(this.rockFactory,this.frame));

    try {
      targetAction.insertDB();
      log.finest("Alarm Set action " + transferActionName + " created succesfully.");
    } catch (Exception e) {
    	log.log(Level.SEVERE, "Alarm Set action " + transferActionName + " creation failed.", e);
    }

  }
  
  public JFrame getFrame() {
	  	return this.frame;
  }

}
