package com.ericsson.eniq.common.setWizards;

import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.jdesktop.application.ResourceMap;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dataformat;
import com.distocraft.dc5000.repository.dwhrep.DataformatFactory;
import com.distocraft.dc5000.repository.dwhrep.Datainterface;
import com.distocraft.dc5000.repository.dwhrep.Defaulttags;
import com.distocraft.dc5000.repository.dwhrep.DefaulttagsFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacemeasurement;
import com.distocraft.dc5000.repository.dwhrep.Interfacetechpacks;
import com.distocraft.dc5000.repository.dwhrep.Transformer;
import com.distocraft.dc5000.repository.dwhrep.TransformerFactory;
import com.ericsson.eniq.common.Utils;

/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class CreateInterfaceSet {

  private final Logger log = Logger.getLogger(CreateInterfaceSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected RockFactory dwhrepRock;

  protected RockFactory rock;

  protected long techPackID;

  protected String defaultTemplateName = "defaultAdapter";

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected String interfaceName = "";

  protected String interfaceVersion = "";

  protected String tpVersion;

  protected String versionID;

  protected String adapterType = "";

  protected String connectionID = "";

  protected String adapterName = "";

  protected String dirName = "";

  protected String itype = "";

  protected String objType = "";

  protected String templateDir = "";

  private final ResourceMap resourceMap;

  /**
   * 
   * constructor
   * 
   * @param version
   * @param rock
   * @param techPackID
   * @param name
   * @param adapterType
   * @param adapterName
   * @param connectionID
   * @throws SQLException
   */
  public CreateInterfaceSet(String objType, String templateDir, String tpVersion, String versionID,
      RockFactory dwhrepRock, RockFactory rock, long techPackID, String interfaceName, String interfaceVersion,
      String adapterType, String adapterName, String dirName, String connectionID, final ResourceMap resourceMap)
      throws SQLException {

    if (objType.equalsIgnoreCase("measurement")) {
      itype = "pmdata";
    } else if (objType.equalsIgnoreCase("reference")) {
      itype = "referencedata";
    } else {
      itype = "pmdata";
    }

    this.objType = objType;
    this.templateDir = templateDir;
    this.rock = rock;
    this.dwhrepRock = dwhrepRock;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;

    this.interfaceName = interfaceName;
    this.interfaceVersion = interfaceVersion;
    this.tpVersion = tpVersion;
    this.versionID = tpVersion;
    this.adapterType = adapterType;
    this.connectionID = connectionID;
    this.adapterName = adapterName;
    this.dirName = dirName;
    this.resourceMap = resourceMap;
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
  public String merge(String templateName, VelocityContext context) throws Exception {

    StringWriter strWriter = new StringWriter();
    boolean isMergeOk = false;
    try {
      // 20100715, eeoidiv,HL95631:Interface set wizard does not work for non-mdc parser interface
      // Check if parser specific template exists, if not use defaultAdapter.vm (avoid ResourceNotFoundException)
      if(!Velocity.resourceExists(templateDir + "/" + templateName)) {
    	  templateName = "defaultAdapter.vm";
      }
      isMergeOk = Velocity.mergeTemplate(templateDir + "/" + templateName, Velocity.ENCODING_DEFAULT, context,
          strWriter);
    } catch (Exception ex) {
        Logger.getLogger("Wizard.Generate.Merge").log(Level.WARNING, "Error while merging template ", ex);
    }

    Logger.getLogger("Wizard.Generate.Merge").log(Level.FINEST, " Velocity Merge OK: " + isMergeOk);

    return strWriter.toString();

  }

  // 20110613 eanguan :: To refactor the code so as to make them easy to test 
  protected Properties createProperty() throws Exception {
	  VelocityContext context = new VelocityContext();
	  return createProperty(context);
  }
  
  protected Properties createProperty(VelocityContext context) throws Exception {
    // 20100715, eeoidiv,CR 26/109 18-FCP 103 8147/12:Removing parser dependency from ENIQ platform
    context.put("parserType", Utils.getParserFormatPath(this.adapterType));
    context.put("interfaceName", this.interfaceName);
    context.put("transformerName", this.interfaceName + ".xml");
    context.put("directoryName", this.dirName);
    context.put("interfaceType", this.itype);

    return Utils.createProperty(merge(getTemplateName(this.adapterName), context));
  }

  protected void createInterface() throws Exception {

    List result = new ArrayList();

    Datainterface di = new Datainterface(dwhrepRock);
    di.setInterfacename(interfaceName);
    di.setInterfacetype(this.objType);
    di.setStatus(new Long(1));
    di.setDescription("");

    di.setDataformattype(getDataFormat());
    di.setInstalldescription("");

    di.saveDB();

    // Query q = session.createQuery("from DataFormat where VERSIONID=?");
    // q.setString(0,version.getVersionid());
    // Iterator vIter = q.list().iterator();

    Dataformat adf = new Dataformat(dwhrepRock);
    adf.setVersionid(versionID);
    DataformatFactory adff = new DataformatFactory(dwhrepRock, adf);

    Iterator vIter = adff.get().iterator();
    while (vIter.hasNext()) {

      Dataformat df = (Dataformat) vIter.next();

      if (df.getDataformattype().equalsIgnoreCase(getDataFormat())
          && df.getObjecttype().equalsIgnoreCase(this.objType)) {

        // Query qd = session.createQuery("from DefaultTag where
        // DATAFORMATID=?");
        // qd.setString(0,df.getDataformatid());

        Defaulttags adt = new Defaulttags(dwhrepRock);
        adt.setDataformatid(df.getDataformatid());
        DefaulttagsFactory dtf = new DefaulttagsFactory(dwhrepRock, adt);

        Iterator dIter = dtf.get().iterator();
        while (dIter.hasNext()) {
          Defaulttags dt = (Defaulttags) dIter.next();

          Interfacemeasurement im = new Interfacemeasurement(dwhrepRock);
          im.setDataformatid(dt.getDataformatid());

          Transformer t_cond = new Transformer(dwhrepRock);
          t_cond.setTransformerid(dt.getDataformatid());
          TransformerFactory tFact = new TransformerFactory(dwhrepRock, t_cond);

          Vector v = tFact.get();

          if (v != null && v.size() == 1) {
            im.setTransformerid(dt.getDataformatid());
          }

          im.setDescription(dt.getDescription());
          im.setModiftime(new Timestamp(new Date().getTime()));
          im.setStatus(new Long(1));
          im.setTagid(dt.getTagid());
          im.setInterfacename(interfaceName);

          im.saveDB();
        }
      }
    }

    // TODO ConfigTool.reloadConfig();

  }

  private String getDataFormat() {
    String dataformat = "";

    String dataformattag = resourceMap.getString(interfaceName + ".dataformattag");
    if (dataformattag == null) {
      dataformat = this.adapterType;
    } else {
      dataformat = dataformattag;
    }
    return dataformat;
  }

  protected void createInterfaceTechpacks(String[] techpacks, String dataformattype) throws Exception {

    Datainterface di = new Datainterface(dwhrepRock);
    di.setInterfacename(interfaceName);
    di.setInterfacetype(this.adapterType);
    di.setStatus(new Long(1));
    di.setDescription("");
    di.setDataformattype(dataformattype);
    di.setInstalldescription("");
    di.saveDB();

    for (int i = 0; i < techpacks.length; i++) {

      String[] tmp = techpacks[i].split(":");

      Interfacetechpacks it = new Interfacetechpacks(dwhrepRock);
      it.setInterfacename(interfaceName);
      it.setTechpackname(tmp[0]);
      it.setInterfacename(interfaceVersion);
      it.setTechpackversion(tmp[1]);
      it.saveDB();
    }

    // TODO ConfigTool.reloadConfig();
  }

  public void create(boolean skip) throws Exception {

    long iSet = 0;
    long iAction = 0;

    createInterface();

    if (skip && Utils.isSet("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock)) {
      return;
    }

    log.log(Level.INFO, "Creating Adapter Set: " + interfaceName);
    createSet("Adapter", new Long(this.maxSetID + iSet).intValue(),
        "Adapter_" + this.interfaceName + "_" + this.adapterName).insertToDB(rock);

    log.log(Level.INFO, "  Creating Parse Action: " + interfaceName);
    createAction(1, "Parse", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "", Utils.propertyToString(createProperty()), this.adapterName)
        .insertToDB(rock);
    iAction++;
    log.log(Level.INFO, "  Creating trigger loader Action: " + interfaceName);
    createAction(2, "TriggerScheduledSet", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "", "", "Trigger_loaders_" + this.adapterName)
        .insertToDB(rock);

  }

  public void removeSets() throws Exception {

    log.log(Level.INFO, "Remove Adapter Set: " + interfaceName);
    long setid = Utils.getSetId("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock);
    if (setid == -1) {
      log.log(Level.INFO, "No sets found, no need to remove");
      return;
    }

    log.log(Level.INFO, "  Removing Parse Action: " + interfaceName);
    Utils.removeAction(this.adapterName, versionID, techPackID, setid, rock);

    log.log(Level.INFO, "  Removing trigger loader Action: " + interfaceName);
    Utils.removeAction("Trigger_loaders_" + this.adapterName, versionID, techPackID, setid, rock);

    Utils.removeSet("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock);

  }

  public void createTechpacks(boolean skip, String[] techpacks, String dataformattype, boolean cretaeSchedulings)
      throws Exception {

    long iSet = 0;
    long iAction = 0;

    if (skip && Utils.isSet("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock)) {
      return;
    }

    log.log(Level.INFO, "Creating Adapter Set: " + interfaceName);
    createSet("Adapter", new Long(this.maxSetID + iSet).intValue(),
        "Adapter_" + this.interfaceName + "_" + this.adapterName).insertToDB(rock);

    log.log(Level.INFO, "  Creating scheduling for Adapter Set: " + interfaceName);

    if (cretaeSchedulings) {

      // Create the default scheduling for the adapter.
      createSchedule(new Long(this.maxSetID + iSet).intValue(),
          "TriggerAdapter_" + this.interfaceName + "_" + this.adapterName).insertToDB(this.rock);
    }

    log.log(Level.INFO, "  Creating Parse Action: " + interfaceName);
    createAction(1, "Parse", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "", Utils.propertyToString(createProperty()), this.adapterName)
        .insertToDB(rock);
    iAction++;
    log.log(Level.INFO, "  Creating trigger loader Action: " + interfaceName);
    createAction(2, "TriggerScheduledSet", new Long(this.maxSetID + iSet).intValue(),
        new Long(this.maxActionID + iAction).intValue(), "", "", "Trigger_loaders_" + this.adapterName)
        .insertToDB(rock);

  }

  public void removeTechpacks(String[] techpacks, String dataformattype, boolean cretaeSchedulings) throws Exception {

    log.log(Level.INFO, "Removing Adapter Set: " + interfaceName);
    long setid = Utils.getSetId("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock);
    if (setid == -1) {
      log.log(Level.INFO, "No sets found, no need to remove");
      return;
    }

    // if (cretaeSchedulings) {

    log.log(Level.INFO, "  Removing scheduling for Adapter Set: " + interfaceName);

    // Create the default scheduling for the adapter.
    Utils.removeScheduling("TriggerAdapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID,
        setid, rock);
    // }

    log.log(Level.INFO, "  Removing Parse Action: " + interfaceName);
    Utils.removeAction(this.adapterName, versionID, techPackID, setid, rock);

    log.log(Level.INFO, "  Removing trigger loader Action: " + interfaceName);
    Utils.removeAction("Trigger_loaders_" + this.adapterName, versionID, techPackID, setid, rock);

    Utils.removeSet("Adapter_" + this.interfaceName + "_" + this.adapterName, versionID, techPackID, rock);
  }

  private String getTemplateName(String adapterName) {

    return adapterName.toLowerCase() + ".vm";
  }

  protected SetGenerator createSet(String type, long iSet, String name) throws Exception {

    SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME(name);
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(tpVersion);
    set.setCOLLECTION_SET_ID(Long.toString(techPackID));
    set.setPRIORITY("0");
    set.setQUEUE_TIME_LIMIT("30");
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

  }

  protected ActionGenerator createAction(int order, String type, long iSet, long iAct, String whereClause,
      String actionContents, String name) throws Exception {

    ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(tpVersion);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Long.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(name);
    loadAction.setORDER_BY_NO(Integer.toString(order));
    loadAction.setWHERE_CLAUSE(whereClause);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID(connectionID);

    return loadAction;

  }

  private ScheduleGenerator createSchedule(long iSet, String name) throws Exception {

    ScheduleGenerator schedule = new ScheduleGenerator();
    schedule.setVERSION_NUMBER(this.tpVersion);
    schedule.setID(new Long(Utils.getScheduleMaxID(this.rock) + 1));
    schedule.setEXECUTION_TYPE("interval");
    schedule.setSCHEDULING_MONTH(new Long(1));
    schedule.setSCHEDULING_DAY(new Long(1));
    schedule.setSCHEDULING_HOUR(new Long(0));
    schedule.setSCHEDULING_MIN(new Long(0));
    schedule.setCOLLECTION_SET_ID(new Long(techPackID));
    schedule.setCOLLECTION_ID(new Long(iSet));
    schedule.setINTERVAL_HOUR(new Long(0));
    schedule.setINTERVAL_MIN(new Long(15));
    schedule.setHOLD_FLAG("Y");
    schedule.setSCHEDULING_YEAR(new Long(2006));
    schedule.setNAME(name);

    return schedule;
  }
}
