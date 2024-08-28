package com.ericsson.eniq.common.setWizards;

import java.io.File;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Referencecolumn;
import com.distocraft.dc5000.repository.dwhrep.ReferencecolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;
import com.distocraft.dc5000.repository.dwhrep.ReferencetableFactory;
import com.ericsson.eniq.common.Utils;

/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class CreateTopologyLoaderSet {

  private static final String CURRENT_DC = "_CURRENT_DC";

  private final Logger log = Logger.getLogger(CreateTopologyLoaderSet.class.getName());

  protected String loaderTemplateName;

  protected String setTemplateName;

  protected String name;

  protected String version;

  protected String versionid;

  protected RockFactory dwhrepRock;

  protected RockFactory rock;

  protected int techPackID;

  protected String temporaryOptionTemplate = "temporaryOption.vm";

  protected String pathSeparator = "/";

  protected long maxSetID = 0;

  protected long maxActionID = 0;

  protected String templateDir = "";

  protected boolean doSchedulings = false;

  protected long maxSchedulingID = 0;

  protected String techPackName = "";

  /**
   * constructor
   * 
   * @throws SQLException
   * 
   * 
   */
  public CreateTopologyLoaderSet(String templateDir, String name, String version, String versionid,
      RockFactory dwhrepRock, RockFactory rock, int techPackID, String techPackName, boolean schedulings)
      throws SQLException {

    this.templateDir = templateDir;
    this.name = name;
    this.version = version;
    this.versionid = versionid;
    this.dwhrepRock = dwhrepRock;
    this.rock = rock;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(rock) + 1;
    this.maxActionID = Utils.getActionMaxID(rock) + 1;

    this.doSchedulings = schedulings;
    this.techPackName = techPackName;
    this.maxSchedulingID = Utils.getScheduleMaxID(rock) + 1;

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
    boolean isMergeOk = Velocity.mergeTemplate(templateDir + "/" + templateName, Velocity.ENCODING_DEFAULT, context,
        strWriter);
    log.fine("   Velocity Merge OK: " + isMergeOk);

    return strWriter.toString();

  }

  protected String CreateSql(String template, Referencetable rtable) throws Exception {

    Vector vec = new Vector();
    VelocityContext context = new VelocityContext();

    // Iterator iter = rtable.getReferenceColumns().iterator();

    Referencecolumn rfc = new Referencecolumn(dwhrepRock);
    rfc.setTypeid(rtable.getTypeid());
    ReferencecolumnFactory rfcf = new ReferencecolumnFactory(dwhrepRock, rfc);
    Iterator iter = rfcf.get().iterator();

    while (iter.hasNext()) {

      Referencecolumn rCol = (Referencecolumn) iter.next();
      vec.add(rCol);
    }

    Collections.sort(vec, new comp());

    context.put("referencecolumn", vec);

    return merge(template, context);
  }

  protected String CreateSqlWTable(String template, Referencetable rtable) throws Exception {

    Vector vec = new Vector();
    VelocityContext context = new VelocityContext();

    // Iterator iter = rtable.getReferenceColumns().iterator();
    Referencecolumn rfc = new Referencecolumn(dwhrepRock);
    rfc.setTypeid(rtable.getTypeid());
    ReferencecolumnFactory rfcf = new ReferencecolumnFactory(dwhrepRock, rfc);
    Iterator iter = rfcf.get().iterator();

    while (iter.hasNext()) {

      Referencecolumn rCol = (Referencecolumn) iter.next();
      vec.add(rCol);
    }

    Collections.sort(vec, new comp());

    context.put("referencecolumn", vec);
    context.put("TABLE", rtable.getTypename());
    context.put("rtable", rtable);
    
    return merge(template, context);
  } //CreateSqlWTable
  
  	@SuppressWarnings("unchecked")
  	protected String CreateSqlHistory(Referencetable rtable, VelocityContext context) throws Exception {
  		if(context == null) {
	    	context = new VelocityContext();
	    	context.put("loadTableName", rtable.getTypename()+CURRENT_DC);
	        context.put("joinTableName", rtable.getTypename()+"_CALC");
	        context.put("startTime", "START_TIME");
	        context.put("endTime", "END_TIME");
	        context.put("typeOfChangeCol", "CHANGE_TYPE");//Added=0,Modified=1,Deleted=2
	    }
  		ArrayList<String> ignoreColumns = new ArrayList<String>();
  		ignoreColumns.add((String) context.get("startTime"));
  		ignoreColumns.add((String) context.get("endTime"));
  		ignoreColumns.add((String) context.get("typeOfChangeCol"));
	    Vector<Referencecolumn> vec = new Vector<Referencecolumn>();
	    ArrayList<Referencecolumn> keys = new ArrayList<Referencecolumn>();
	    ArrayList<Referencecolumn> attributes = new ArrayList<Referencecolumn>();
	    Referencecolumn rfc = new Referencecolumn(dwhrepRock);
	    rfc.setTypeid(rtable.getTypeid());
	    ReferencecolumnFactory rfcf = new ReferencecolumnFactory(dwhrepRock, rfc);
	    Iterator<Referencecolumn> iter = rfcf.get().iterator();
	    while (iter.hasNext()) {
	    	Referencecolumn rCol = iter.next();
	    	vec.add(rCol);
	    	if (rCol.getUniquekey() == 1) {
	    		keys.add(rCol);
	    	} else {
	    		if(!ignoreColumns.contains(rCol.getDataname())){
	    			attributes.add(rCol);
	    		}
	    	}
	    }
	    //For special _Gis typname add extra attribute auto_key.
	    if(rtable!=null && rtable.getTypename().endsWith("_Gis")) {
	    	Referencecolumn autoKeyCol = new Referencecolumn(dwhrepRock);
	    	autoKeyCol.setDataname("auto_key");
	    	autoKeyCol.setColnumber(new Long(999)); //Want to be last
	    	attributes.add(autoKeyCol);
	    }
	    Collections.sort(vec, new comp());
	    Collections.sort(keys, new comp());
	    Collections.sort(attributes, new comp());
	    context.put("TABLE", rtable.getTypename());
	    context.put("referencecolumn", vec);
	    context.put("rtable", rtable);
	    context.put("keys", keys);
	    context.put("attributes", attributes);
	    
	    return merge("HISTORY.vm", context);
  	} //CreateSqlHistory

  class comp implements Comparator {

    public comp() {
    }

    @Override
    public int compare(Object d1, Object d2) {

      Referencecolumn l1 = (Referencecolumn) d1;
      Referencecolumn l2 = (Referencecolumn) d2;

      Long i1 = l1.getColnumber();
      Long i2 = l2.getColnumber();
      return i1.compareTo(i2);
    }
  }

  public void create(boolean skip) throws Exception {
    create(skip, null);
  }

  public void create(boolean skip, Vector<String> skiplist) throws Exception {

    long iAction = 0;
    long iSet = 0;
    long iOrder = 0;
    int uPolicy = 0;
    long iScheduling = 0;
    String dir = "";

    Referencetable rt = new Referencetable(dwhrepRock);
    rt.setVersionid(versionid);
    ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

    Iterator rTableIter = rtf.get().iterator();

    while (rTableIter.hasNext()) {

      iOrder = 0;
      uPolicy = 0;

      Referencetable rtable = (Referencetable) rTableIter.next();

      dir = getTopologyDirectory(rtable.getTypename().toLowerCase());

      if (rtable.getUpdate_policy() != null) {
        uPolicy = rtable.getUpdate_policy().intValue();
      }

      if (uPolicy == 1) {

        // load directly
        log.info("Creating Set and Actions for direct topology loaders");

        if (skiplist != null && skiplist.contains(rtable.getTypename().toUpperCase())) {
          log.info("Skipped set " + name);
          continue;
        }

        if (!(skip && Utils.isSet("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock))) {

          // create set
          log.info("Creating set TopologyLoader_" + rtable.getTypename());
          createSet("TopologyLoader_" + rtable.getTypename(), "Topology", new Long(this.maxSetID + iSet).intValue())
              .insertToDB(rock);

          // temporary option
          log.info("  Creating action SQL_Execute_" + rtable.getTypename());
          createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              getTemporaryOptions()).insertToDB(rock);
          iAction++;
          iOrder++;

          if (canFind("DELETE.TOPOLOGY.vm")) {
            // delete topology
            log.info("  Creating action SQL_Execute_" + rtable.getTypename());
            createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
                CreateSqlWTable("DELETE.TOPOLOGY.vm", rtable)).insertToDB(rock);
            iAction++;
            iOrder++;
          }

          // create topology loader
          log.info("  Creating action UnPartitioned_Loader_" + rtable.getTypename());
          createAction("UnPartitioned_Loader_" + rtable.getTypename(), iOrder, "UnPartitioned Loader",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
              createPropertyString(rtable.getTypename(), dir), CreateSql("LOADER.TOPOLOGY.vm", rtable))
              .insertToDB(rock);
          iAction++;
          iOrder++;

          if (doSchedulings) {

            // create scheduling
            String holdFlag = "N";
            String name = "Loader_" + rtable.getTypename();

            log.info("  Creating schedule " + name);

            createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
                new Long(this.maxSetID + iSet).intValue(), name, holdFlag).insertToDB(this.rock);
            iScheduling++;
          }

          iSet++;
        } else {
          log.info("Skipping Set and Actions for direct topology loaders");
        }
      } else if (uPolicy == 2 || uPolicy == 3 || uPolicy == 4) {

        if (skiplist != null && skiplist.contains(rtable.getTypename().toUpperCase())) {
          log.info("Skipped set " + name);
          continue;
        }

        if (!(skip && Utils.isSet("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock))) {

          // use _CURRENT view
          log.info("Creating Set and Actions for indirect (use _CURRENT view) topology loaders");

          // create set
          log.info("Creating set TopologyLoader_" + rtable.getTypename());
          createSet("TopologyLoader_" + rtable.getTypename(), "Topology", new Long(this.maxSetID + iSet).intValue())
              .insertToDB(rock);

          // temporary option
          log.info("  Creating action SQL_Execute_" + rtable.getTypename());
          createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              getTemporaryOptions()).insertToDB(rock);
          iAction++;
          iOrder++;

          // create topology loader
          log.info("  Creating action UnPartitioned_Loader_" + rtable.getTypename());
          createAction("UnPartitioned_Loader_" + rtable.getTypename(), iOrder, "UnPartitioned Loader",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
              createPropertyString(rtable.getTypename() + CURRENT_DC, dir), CreateSql("LOADER.TOPOLOGY.vm", rtable))
              .insertToDB(rock);
          iAction++;
          iOrder++;

          // Trigger Scheduled Set
          log.info("  Creating action Trigger_Updater_" + rtable.getTypename());
          createAction("Trigger_Updater_" + rtable.getTypename(), iOrder, "TriggerScheduledSet",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
              "TopologyUpdater_" + rtable.getTypename(), "TopologyUpdater_" + rtable.getTypename()).insertToDB(rock);
          iAction++;
          iOrder++;

          if (doSchedulings) {

            // create scheduling
            String holdFlag = "N";
            String name = "Loader_" + rtable.getTypename();

            log.info("  Creating schedule " + name);

            createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
                new Long(this.maxSetID + iSet).intValue(), name, holdFlag).insertToDB(this.rock);
            iScheduling++;
          }

          if (canFind("TopologyUpdater.vm")) {

            iSet++;

            if (doSchedulings) {

              // create scheduling
              String holdFlag = "N";
              String name = "TopologyUpdater_" + rtable.getTypename();

              log.info("  Creating schedule " + name);

              createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
                  new Long(this.maxSetID + iSet).intValue(), name, holdFlag).insertToDB(this.rock);
              iScheduling++;
            }

            // create set
            // new update set
            log.info("  Creating set TopologyUpdater_" + rtable.getTypename());
            createSet("TopologyUpdater_" + rtable.getTypename(), "Topology", new Long(this.maxSetID + iSet).intValue())
                .insertToDB(rock);
            iOrder = 0;

          }

          // History
          // eeoidiv,20110727:History Dynamic
          // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then move older to history partitions.
          if(uPolicy == 4) {
            createAction("History SQL_Execute_" + rtable.getTypename(), iOrder, "HistorySqlExecute",
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(),
                createPropertyStringForHistSonv(rtable.getTypename()), CreateSqlHistory(rtable, null)).insertToDB(rock);
        	  iAction++;
        	  iOrder++;
          } else {
          // insert
          log.info("  Creating insert action SQL_Execute_" + rtable.getTypename());
          createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              CreateSqlWTable("INSERT.TOPOLOGY.vm", rtable)).insertToDB(rock);
          iAction++;
          iOrder++;

          // update
          Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
              "Creating update Action for  " + rtable.getTypename());
          createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              CreateSqlWTable("UPDATE.TOPOLOGY.vm", rtable)).insertToDB(rock);
          iAction++;
          iOrder++;

          if (canFind("REPLACE.TOPOLOGY.vm")) {
            // replace
            Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
                "Creating replace Action for " + rtable.getTypename());
            createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
                new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
                CreateSqlWTable("REPLACE.TOPOLOGY.vm", rtable)).insertToDB(rock);
            iAction++;
            iOrder++;
          }

          // remove
          Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO, "Creating remove Action for " + rtable.getTypename());
          // eeoidiv 20091203 : Timed Dynamic topology handling in ENIQ, WI 6.1.2, (284/159 41-FCP 103 8147) Improved WRAN Topology in ENIQ
          // If ReferenceTable.UPDATE_POLICY==3 (Timed Dynamic) then use Set STATUS='DEACTIVE' if time interval has been exceeded OR 'ACTIVE' if within.
          // Template "REMOVE.TOPOLOGY.vm" has been changed.
          createAction("SQL_Execute_" + rtable.getTypename(), iOrder, "SQL Execute",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              CreateSqlWTable("REMOVE.TOPOLOGY.vm", rtable)).insertToDB(rock);
          iAction++;
          iOrder++;

          // ReloadDBlookupsCache
          Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
              "Creating ReloadDBLookups Action for " + rtable.getTypename());
          createAction("ReloadDBLookups_" + rtable.getTypename(), iOrder, "ReloadDBLookups",
              new Long(this.maxSetID + iSet).intValue(), new Long(this.maxActionID + iAction).intValue(), "",
              Utils.propertyToString(Utils.createProperty("tableName=" + rtable.getTypename()))).insertToDB(rock);
          iAction++;
          iOrder++;
          }

          iSet++;
        } else {
          log.info("Skipping Set and Actions for indirect (use _CURRENT view) topology loaders");
        }
      } else {

        // do nothing
        Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO, "Skipping " + rtable.getTypename());

      }

    }
  }

  protected String getTopologyDirectory(String referenceTableName) {
    return "${ETLDATA_DIR}/" + referenceTableName + this.pathSeparator + "raw" + this.pathSeparator;
  }

  /**
   * Removes the created sets.
   * 
   * @param skiplist
   *          The list of measurement and reference types to be skipped, i.e.
   *          not removed.
   * @throws Exception
   */
  public void removeSets(Vector<String> skiplist) throws Exception {

    int uPolicy = 0;

    Referencetable rt = new Referencetable(dwhrepRock);
    rt.setVersionid(versionid);
    ReferencetableFactory rtf = new ReferencetableFactory(dwhrepRock, rt);

    Iterator iTable = rtf.get().iterator();

    while (iTable.hasNext()) {

      uPolicy = 0;
      Referencetable rtable = (Referencetable) iTable.next();

      // Skip the remove in case there is a match in the skip list.
      if (skiplist != null && skiplist.contains(rtable.getTypename().toUpperCase())) {
        log.info("Skipped set " + name);
        continue;
      }

      if (rtable.getUpdate_policy() != null) {
        uPolicy = rtable.getUpdate_policy().intValue();
      }

      if (uPolicy == 1) {

        // load directly
        log.info("Removing Set and Actions for direct topology loaders");

        // Remove set
        log.info("Remove set TopologyLoader_" + rtable.getTypename());
        long setId = Utils.getSetId("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock);
        if (setId == -1) {
          log.info("No sets found, no need to remove");
          continue;
        }
        // temporary option
        log.info("  Remove action SQL_Execute_" + rtable.getTypename());
        Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);

        if (canFind("DELETE.TOPOLOGY.vm")) {
          // Remove topology
          log.info("  Remove action SQL_Execute_" + rtable.getTypename());
          Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);

        }

        // Remove topology loader
        log.info("  Remove action UnPartitioned_Loader_" + rtable.getTypename());
        Utils.removeAction("UnPartitioned_Loader_" + rtable.getTypename(), version, techPackID, setId, rock);

        // Remove scheduling
        String name = "Loader_" + rtable.getTypename();

        log.info("  Remove schedule " + name);
        Utils.removeScheduling(name, version, techPackID, setId, rock);

        Utils.removeSet("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock);

      } else if (uPolicy == 2 || uPolicy == 3 || uPolicy == 4) {

        // use _CURRENT view
        log.info("Remove Set and Actions for indirect (use _CURRENT view) topology loaders");

        // Remove set
        log.info("Remove set TopologyLoader_" + rtable.getTypename());
        long setId = Utils.getSetId("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock);

        // temporary option
        log.info("  Remove action SQL_Execute_" + rtable.getTypename());
        Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);

        // Remove topology loader
        log.info("  Remove action UnPartitioned_Loader_" + rtable.getTypename());
        Utils.removeAction("UnPartitioned_Loader_" + rtable.getTypename(), version, techPackID, setId, rock);

        // Trigger Scheduled Set
        log.info("  Remove action Trigger_Updater_" + rtable.getTypename());
        Utils.removeAction("Trigger_Updater_" + rtable.getTypename(), version, techPackID, setId, rock);

        if (doSchedulings) {

          // Remove scheduling
          // String holdFlag = "N";
          String name = "Loader_" + rtable.getTypename();

          log.info("  Remove schedule " + name);

          Utils.removeScheduling(name, version, techPackID, setId, rock);

        }
        if(uPolicy == 4) {
        	// eeoidiv,20110727:History Dynamic
            // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then move older to history partitions.
        	// Remove History
	        log.info("  Remove History action SQL_Execute_" + rtable.getTypename());
	        Utils.removeAction("History SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);
        } else { // if (uPolicy == 2 || uPolicy == 3)
	        if (canFind("TopologyUpdater.vm")) {
	
	          if (doSchedulings) {
	
	            // Remove scheduling
	            // String holdFlag = "N";
	            String name = "TopologyUpdater_" + rtable.getTypename();
	
	            log.info("  Remove schedule " + name);
	            Utils.removeScheduling(name, version, techPackID, setId, rock);
	
	          }
	
	          // Remove set
	          // new update set
	          log.info("  Remove set TopologyUpdater_" + rtable.getTypename());
	          Utils.removeAction("TopologyUpdater_" + rtable.getTypename(), version, techPackID, setId, rock);
	
	        }
	
	        // Remove insert
	        log.info("  Remove insert action SQL_Execute_" + rtable.getTypename());
	        Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);
	
	        // Remove update
	        Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
	            "Remove update Action for  " + rtable.getTypename());
	        Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);
	
	        if (canFind("REPLACE.TOPOLOGY.vm")) {
	          // replace
	          Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
	              "Remove replace Action for " + rtable.getTypename());
	          Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);
	
	        }
	
	        // remove
	        Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
	            "Remove remove Action for " + rtable.getTypename());
	        Utils.removeAction("SQL_Execute_" + rtable.getTypename(), version, techPackID, setId, rock);
	
	        // ReloadDBlookupsCache
	        Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO,
	            "Remove ReloadDBLookups Action for " + rtable.getTypename());
	        Utils.removeAction("ReloadDBLookups_" + rtable.getTypename(), version, techPackID, setId, rock);
        }//	end for else of if(uPolicy == 4)
        Utils.removeSet("TopologyLoader_" + rtable.getTypename(), version, techPackID, rock);

        if (canFind("TopologyUpdater.vm")) {
          Utils.removeSet("TopologyUpdater_" + rtable.getTypename(), version, techPackID, rock);
        }
      } else {

        // do nothing
        Logger.getLogger("Wizard.CreateTopologyLoaderSet.create").log(Level.INFO, "Skipping " + rtable.getTypename());

      }
    }
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

  protected String getTemporaryOptions() {

    try {

      VelocityContext context = new VelocityContext();
      return merge(temporaryOptionTemplate, context);

    } catch (Exception e) {
      log.log(Level.WARNING, "Template " + temporaryOptionTemplate + " not found.", e);
    }

    return "";
  }

  protected boolean canFind(String filename) {

    File file = new File(filename);

    if (file.exists()) {

      return true;

    } else {

      try {

        ClassLoader cl = this.getClass().getClassLoader();
        InputStreamReader isr = new InputStreamReader(cl.getResourceAsStream(templateDir + "/" + filename));
        if (isr != null) {
          return true;
        }

      } catch (Exception e) {

      }

      return false;

    }
  }

  protected SetGenerator createSet(String name, String type, long iSet) throws Exception {

    SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME(name);
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(version);
    set.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    set.setPRIORITY("1");
    set.setQUEUE_TIME_LIMIT("30");
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;

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

  protected String createPropertyString(String tablename, String dir) throws Exception {
    Properties prop = new Properties();

    prop.setProperty("tablename", tablename);
    prop.setProperty("dir", dir);
    prop.setProperty("techpack", techPackName);
    prop.setProperty("pattern", ".+");

    return Utils.propertyToString(prop);
  }

  protected String createPropertyStringForHistSonv(final String typename) throws Exception {
    final Properties prop = new Properties();

    prop.setProperty("typename", typename);
    prop.setProperty("tablename", typename + CURRENT_DC);
    
    return Utils.propertyToString(prop);
  }
}
