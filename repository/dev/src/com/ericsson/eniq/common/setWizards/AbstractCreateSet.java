package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;
import java.util.List;

import org.apache.velocity.VelocityContext;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

public abstract class AbstractCreateSet implements SetCreator {

  protected String setTemplateName;

  protected final transient RockFactory dwhrepRock;

  protected final transient RockFactory etlrepRock;

  protected final transient int techPackID;

  protected final transient long maxSetID;

  protected final transient long maxActionID;

  protected final transient long maxSchedulingID;

  protected final transient boolean doSchedulings;

  protected final transient String name;

  protected final transient String version;

  protected final transient String versionid;

  protected final static transient String HOLD_FLAG = "N";

  /**
   * constructor
   * 
   * @param templateDir
   *          Directory containing all the required templates to create the
   *          aggregation sets
   * @param name
   *          The aggregator name
   * @param version
   *          The TP Version
   * @param versionid
   *          The version ID
   * @param dwhrepRock
   *          DWHREP connection
   * @param etlrep
   *          ETLREP connection
   * @param techPackID
   *          TP ID
   * @param schedulings
   *          Regenerate the schedulings
   * @throws SQLException
   * @throws Exception
   *           Any errors
   */
  public AbstractCreateSet(final String name, final String version, final String versionid,
      final RockFactory dwhrepRock, final RockFactory etlrep, final int techPackID, final boolean schedulings)
      throws SQLException {
    this.dwhrepRock = dwhrepRock;
    this.etlrepRock = etlrep;
    this.techPackID = techPackID;

    this.maxSetID = Utils.getSetMaxID(etlrepRock) + 1;
    this.maxActionID = Utils.getActionMaxID(etlrepRock) + 1;
    this.maxSchedulingID = Utils.getScheduleMaxID(etlrepRock) + 1;
    this.doSchedulings = schedulings;

    this.name = name;
    this.version = version;
    this.versionid = versionid;
  }

  /**
   * Removes the created sets.
   * 
   * @throws RockException
   * @throws SQLException
   * 
   * @throws Exception
   */
  @Override
  public void removeSets() throws SQLException, RockException {
    removeSets(null);
  }

  protected List<Measurementtable> getMeasurementTables(final Measurementtype measType) throws SQLException,
      RockException {
    final Measurementtable mta = new Measurementtable(dwhrepRock);
    mta.setTypeid(measType.getTypeid());
    final MeasurementtableFactory mtaf = new MeasurementtableFactory(dwhrepRock, mta);
    return mtaf.get();
  }

  /**
   * Is the measurement table a count table
   * 
   * @param measTable
   * @return
   */
  protected boolean isCountTable(final Measurementtable measTable) {
    return measTable.getTablelevel().equalsIgnoreCase(Constants.FIFTEENMIN)
        || measTable.getTablelevel().equalsIgnoreCase(Constants.ONEMIN)
        || measTable.getTablelevel().equalsIgnoreCase(Constants.DAYLEVEL);
  }

  @Override
  public void create(final boolean skip) throws SQLException, RockException {
    create(skip, null);
  }

  protected List<Measurementtype> getMeasurmentTypes() throws SQLException, RockException {
    final Measurementtype mType = new Measurementtype(dwhrepRock);
    mType.setVersionid(versionid);
    final MeasurementtypeFactory mtf = new MeasurementtypeFactory(dwhrepRock, mType);

    return mtf.get();
  }

  protected SetGenerator createSet(final String type, final long iSet, final String setName, final String queueTime) {
    final SetGenerator set = new SetGenerator();
    set.setCOLLECTION_NAME(setName);
    set.setCOLLECTION_ID(Long.toString(iSet));
    set.setMAX_ERRORS("0");
    set.setMAX_FK_ERRORS("0");
    set.setMAX_COL_LIMIT_ERRORS("0");
    set.setCHECK_FK_ERROR_FLAG("N");
    set.setCHECK_COL_LIMITS_FLAG("N");
    set.setVERSION_NUMBER(version);
    set.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    set.setPRIORITY("0");
    set.setQUEUE_TIME_LIMIT(queueTime);
    set.setENABLED_FLAG("Y");
    set.setSETTYPE(type);
    set.setFOLDABLE_FLAG("Y");
    set.setHOLD_FLAG("N");

    return set;
  }

  protected ActionGenerator createAction(final String actionName, final long order, final String type, final long iSet,
      final long iAct, final String whereClause, final String actionContents) {

    final ActionGenerator loadAction = new ActionGenerator();
    loadAction.setVERSION_NUMBER(version);
    loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
    loadAction.setCOLLECTION_ID(Long.toString(iSet));
    loadAction.setCOLLECTION_SET_ID(Integer.toString(techPackID));
    loadAction.setACTION_TYPE(type);
    loadAction.setTRANSFER_ACTION_NAME(actionName);
    loadAction.setORDER_BY_NO(Long.toString(order));
    loadAction.setWHERE_CLAUSE(whereClause);
    loadAction.setACTION_CONTENTS(actionContents);
    loadAction.setENABLED_FLAG("Y");
    loadAction.setCONNECTION_ID("2");

    return loadAction;

  }

  protected ScheduleGenerator createWaitSchedule(final long scheduleID, final long techpackID, final long setID,
      final String setName, final String holdFlag) {

    final ScheduleGenerator schedule = new ScheduleGenerator();

    schedule.setVERSION_NUMBER(this.version);
    schedule.setID(scheduleID);
    schedule.setEXECUTION_TYPE("wait");
    schedule.setCOLLECTION_SET_ID(techpackID);
    schedule.setCOLLECTION_ID(setID);
    schedule.setNAME(setName);
    schedule.setHOLD_FLAG(holdFlag);

    return schedule;
  }

  /**
   * Create a weekly schedule that will run every day at a specific time
   * 
   * @param scheduleID
   * @param iSet
   * @param setName
   * @param hour
   * @param min
   * @return
   */
  protected ScheduleGenerator createWeeklySchedule(final long scheduleID, final long iSet, final String setName,
      final long hour, final long min) {
    final ScheduleGenerator schedule = new ScheduleGenerator();
    schedule.setVERSION_NUMBER(this.version);
    schedule.setID(scheduleID);
    schedule.setEXECUTION_TYPE("weekly");
    schedule.setSCHEDULING_MONTH(new Long(1));
    schedule.setSCHEDULING_DAY(new Long(1));
    schedule.setSCHEDULING_HOUR(hour);
    schedule.setSCHEDULING_MIN(min);
    schedule.setCOLLECTION_SET_ID(new Long(techPackID));
    schedule.setCOLLECTION_ID(new Long(iSet));
    schedule.setMON_FLAG("Y");
    schedule.setTUE_FLAG("Y");
    schedule.setWED_FLAG("Y");
    schedule.setTHU_FLAG("Y");
    schedule.setFRI_FLAG("Y");
    schedule.setSAT_FLAG("Y");
    schedule.setSUN_FLAG("Y");
    schedule.setHOLD_FLAG(HOLD_FLAG);
    schedule.setSCHEDULING_YEAR(new Long(2009));
    schedule.setNAME(setName);

    return schedule;
  }

  @Override
  public String merge(final String templateName, final VelocityContext context) {
    return "";
  }

  /**
   * Create the sets.
   * 
   * @param skip
   *          already existing sets will be skipped if true.
   * @param skiplist
   *          list of sets
   * @throws RockException
   * @throws SQLException
   * @throws Exception
   */
  public abstract void create(final boolean skip, final List<String> skiplist) throws SQLException, RockException;

  /**
   * Removes the created sets.
   * 
   * @param skiplist
   *          The list of measurement and reference types to be skipped, i.e.
   *          not removed.
   * @throws RockException
   * @throws SQLException
   * @throws Exception
   *           Any errors
   */
  public abstract void removeSets(final List<String> skiplist) throws SQLException, RockException;
}