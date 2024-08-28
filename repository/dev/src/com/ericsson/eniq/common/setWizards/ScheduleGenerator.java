/**
 * 
 */
package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;
import java.sql.Timestamp;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_schedulings;

/**
 * 
 * Set
 *
 * @author savinen
 * Copyright Distocraft 2005
 * 
 * $id$
 */

public class ScheduleGenerator {

	private String VERSION_NUMBER; 
	private Long ID; 
	private String EXECUTION_TYPE; 
	private String OS_COMMAND; 
	private Long SCHEDULING_MONTH; 
	private Long SCHEDULING_DAY; 
	private Long SCHEDULING_HOUR; 
	private Long SCHEDULING_MIN; 
	private Long COLLECTION_SET_ID; 
	private Long COLLECTION_ID; 
	private String MON_FLAG; 
	private String TUE_FLAG; 
	private String WED_FLAG; 
	private String THU_FLAG; 
	private String FRI_FLAG; 
	private String SAT_FLAG; 
	private String SUN_FLAG; 
	private String STATUS; 
	private Timestamp LAST_EXECUTION_TIME; 
	private Long INTERVAL_HOUR; 
	private Long INTERVAL_MIN; 
	private String HOLD_FLAG; 
	private Long PRIORITY; 
	private Long SCHEDULING_YEAR; 
	private String TRIGGER_COMMAND; 
	private String NAME; 
 
  
	  public ScheduleGenerator(){
	  	
	  }

	
  /**
   * 
   */
  public ScheduleGenerator(String VERSION_NUMBER, Long ID, String EXECUTION_TYPE, String OS_COMMAND, Long SCHEDULING_MONTH, Long SCHEDULING_DAY, Long SCHEDULING_HOUR, Long SCHEDULING_MIN, Long COLLECTION_SET_ID, Long COLLECTION_ID, String MON_FLAG, String TUE_FLAG, String WED_FLAG, String THU_FLAG, String FRI_FLAG, String SAT_FLAG, String SUN_FLAG, String STATUS, Timestamp LAST_EXECUTION_TIME, Long INTERVAL_HOUR, Long INTERVAL_MIN, String HOLD_FLAG, Long PRIORITY, Long SCHEDULING_YEAR, String TRIGGER_COMMAND, String NAME) {
    
    super();

    this.VERSION_NUMBER = VERSION_NUMBER;
    this.ID = ID;
    this.EXECUTION_TYPE = EXECUTION_TYPE;
    this.OS_COMMAND = OS_COMMAND;
    this.SCHEDULING_MONTH = SCHEDULING_MONTH;
    this.SCHEDULING_DAY = SCHEDULING_DAY;
    this.SCHEDULING_HOUR = SCHEDULING_HOUR;
    this.SCHEDULING_MIN = SCHEDULING_MIN;
    this.COLLECTION_SET_ID = COLLECTION_SET_ID;
    this.COLLECTION_ID = COLLECTION_ID;
    this.MON_FLAG = MON_FLAG;
    this.TUE_FLAG = TUE_FLAG;
    this.WED_FLAG = WED_FLAG;
    this.THU_FLAG = THU_FLAG;
    this.FRI_FLAG = FRI_FLAG;
    this.SAT_FLAG = SAT_FLAG;
    this.SUN_FLAG = SUN_FLAG;
    this.STATUS = STATUS;
    this.LAST_EXECUTION_TIME = LAST_EXECUTION_TIME;
    this.INTERVAL_HOUR = INTERVAL_HOUR;
    this.INTERVAL_MIN = INTERVAL_MIN;
    this.HOLD_FLAG = HOLD_FLAG;
    this.PRIORITY = PRIORITY;
    this.SCHEDULING_YEAR = SCHEDULING_YEAR;
    this.TRIGGER_COMMAND = TRIGGER_COMMAND ;
    this.NAME = NAME;
    
  }


  
 
  
	/**
	 * @return Returns the cOLLECTION_ID.
	 */
	public Long getCOLLECTION_ID() {
		return COLLECTION_ID;
	}
	/**
	 * @param collection_id The cOLLECTION_ID to set.
	 */
	public void setCOLLECTION_ID(Long collection_id) {
		COLLECTION_ID = collection_id;
	}
	/**
	 * @return Returns the cOLLECTION_SET_ID.
	 */
	public Long getCOLLECTION_SET_ID() {
		return COLLECTION_SET_ID;
	}
	/**
	 * @param collection_set_id The cOLLECTION_SET_ID to set.
	 */
	public void setCOLLECTION_SET_ID(Long collection_set_id) {
		COLLECTION_SET_ID = collection_set_id;
	}
	/**
	 * @return Returns the eXECUTION_TYPE.
	 */
	public String getEXECUTION_TYPE() {
		return EXECUTION_TYPE;
	}
	/**
	 * @param execution_type The eXECUTION_TYPE to set.
	 */
	public void setEXECUTION_TYPE(String execution_type) {
		EXECUTION_TYPE = execution_type;
	}
	/**
	 * @return Returns the fRI_FLAG.
	 */
	public String getFRI_FLAG() {
		return FRI_FLAG;
	}
	/**
	 * @param fri_flag The fRI_FLAG to set.
	 */
	public void setFRI_FLAG(String fri_flag) {
		FRI_FLAG = fri_flag;
	}
	/**
	 * @return Returns the hOLD_FLAG.
	 */
	public String getHOLD_FLAG() {
		return HOLD_FLAG;
	}
	/**
	 * @param hold_flag The hOLD_FLAG to set.
	 */
	public void setHOLD_FLAG(String hold_flag) {
		HOLD_FLAG = hold_flag;
	}
	/**
	 * @return Returns the iD.
	 */
	public Long getID() {
		return ID;
	}
	/**
	 * @param id The iD to set.
	 */
	public void setID(Long id) {
		ID = id;
	}
	/**
	 * @return Returns the iNTERVAL_HOUR.
	 */
	public Long getINTERVAL_HOUR() {
		return INTERVAL_HOUR;
	}
	/**
	 * @param interval_hour The iNTERVAL_HOUR to set.
	 */
	public void setINTERVAL_HOUR(Long interval_hour) {
		INTERVAL_HOUR = interval_hour;
	}
	/**
	 * @return Returns the iNTERVAL_MIN.
	 */
	public Long getINTERVAL_MIN() {
		return INTERVAL_MIN;
	}
	/**
	 * @param interval_min The iNTERVAL_MIN to set.
	 */
	public void setINTERVAL_MIN(Long interval_min) {
		INTERVAL_MIN = interval_min;
	}
	/**
	 * @return Returns the lAST_EXECUTION_TIME.
	 */
	public Timestamp getLAST_EXECUTION_TIME() {
		return LAST_EXECUTION_TIME;
	}
	/**
	 * @param last_execution_time The lAST_EXECUTION_TIME to set.
	 */
	public void setLAST_EXECUTION_TIME(Timestamp last_execution_time) {
		LAST_EXECUTION_TIME = last_execution_time;
	}
	/**
	 * @return Returns the mON_FLAG.
	 */
	public String getMON_FLAG() {
		return MON_FLAG;
	}
	/**
	 * @param mon_flag The mON_FLAG to set.
	 */
	public void setMON_FLAG(String mon_flag) {
		MON_FLAG = mon_flag;
	}
	/**
	 * @return Returns the nAME.
	 */
	public String getNAME() {
		return NAME;
	}
	/**
	 * @param name The nAME to set.
	 */
	public void setNAME(String name) {
		NAME = name;
	}
	/**
	 * @return Returns the oS_COMMAND.
	 */
	public String getOS_COMMAND() {
		return OS_COMMAND;
	}
	/**
	 * @param os_command The oS_COMMAND to set.
	 */
	public void setOS_COMMAND(String os_command) {
		OS_COMMAND = os_command;
	}
	/**
	 * @return Returns the pRIORITY.
	 */
	public Long getPRIORITY() {
		return PRIORITY;
	}
	/**
	 * @param priority The pRIORITY to set.
	 */
	public void setPRIORITY(Long priority) {
		PRIORITY = priority;
	}
	/**
	 * @return Returns the sAT_FLAG.
	 */
	public String getSAT_FLAG() {
		return SAT_FLAG;
	}
	/**
	 * @param sat_flag The sAT_FLAG to set.
	 */
	public void setSAT_FLAG(String sat_flag) {
		SAT_FLAG = sat_flag;
	}
	/**
	 * @return Returns the sCHEDULING_DAY.
	 */
	public Long getSCHEDULING_DAY() {
		return SCHEDULING_DAY;
	}
	/**
	 * @param scheduling_day The sCHEDULING_DAY to set.
	 */
	public void setSCHEDULING_DAY(Long scheduling_day) {
		SCHEDULING_DAY = scheduling_day;
	}
	/**
	 * @return Returns the sCHEDULING_HOUR.
	 */
	public Long getSCHEDULING_HOUR() {
		return SCHEDULING_HOUR;
	}
	/**
	 * @param scheduling_hour The sCHEDULING_HOUR to set.
	 */
	public void setSCHEDULING_HOUR(Long scheduling_hour) {
		SCHEDULING_HOUR = scheduling_hour;
	}
	/**
	 * @return Returns the sCHEDULING_MIN.
	 */
	public Long getSCHEDULING_MIN() {
		return SCHEDULING_MIN;
	}
	/**
	 * @param scheduling_min The sCHEDULING_MIN to set.
	 */
	public void setSCHEDULING_MIN(Long scheduling_min) {
		SCHEDULING_MIN = scheduling_min;
	}
	/**
	 * @return Returns the sCHEDULING_MONTH.
	 */
	public Long getSCHEDULING_MONTH() {
		return SCHEDULING_MONTH;
	}
	/**
	 * @param scheduling_month The sCHEDULING_MONTH to set.
	 */
	public void setSCHEDULING_MONTH(Long scheduling_month) {
		SCHEDULING_MONTH = scheduling_month;
	}
	/**
	 * @return Returns the sCHEDULING_YEAR.
	 */
	public Long getSCHEDULING_YEAR() {
		return SCHEDULING_YEAR;
	}
	/**
	 * @param scheduling_year The sCHEDULING_YEAR to set.
	 */
	public void setSCHEDULING_YEAR(Long scheduling_year) {
		SCHEDULING_YEAR = scheduling_year;
	}
	/**
	 * @return Returns the sTATUS.
	 */
	public String getSTATUS() {
		return STATUS;
	}
	/**
	 * @param status The sTATUS to set.
	 */
	public void setSTATUS(String status) {
		STATUS = status;
	}
	/**
	 * @return Returns the sUN_FLAG.
	 */
	public String getSUN_FLAG() {
		return SUN_FLAG;
	}
	/**
	 * @param sun_flag The sUN_FLAG to set.
	 */
	public void setSUN_FLAG(String sun_flag) {
		SUN_FLAG = sun_flag;
	}
	/**
	 * @return Returns the tHU_FLAG.
	 */
	public String getTHU_FLAG() {
		return THU_FLAG;
	}
	/**
	 * @param thu_flag The tHU_FLAG to set.
	 */
	public void setTHU_FLAG(String thu_flag) {
		THU_FLAG = thu_flag;
	}
	/**
	 * @return Returns the tRIGGER_COMMAND.
	 */
	public String getTRIGGER_COMMAND() {
		return TRIGGER_COMMAND;
	}
	/**
	 * @param trigger_command The tRIGGER_COMMAND to set.
	 */
	public void setTRIGGER_COMMAND(String trigger_command) {
		TRIGGER_COMMAND = trigger_command;
	}
	/**
	 * @return Returns the tUE_FLAG.
	 */
	public String getTUE_FLAG() {
		return TUE_FLAG;
	}
	/**
	 * @param tue_flag The tUE_FLAG to set.
	 */
	public void setTUE_FLAG(String tue_flag) {
		TUE_FLAG = tue_flag;
	}
	/**
	 * @return Returns the vERSION_NUMBER.
	 */
	public String getVERSION_NUMBER() {
		return VERSION_NUMBER;
	}
	/**
	 * @param version_number The vERSION_NUMBER to set.
	 */
	public void setVERSION_NUMBER(String version_number) {
		VERSION_NUMBER = version_number;
	}
	/**
	 * @return Returns the wED_FLAG.
	 */
	public String getWED_FLAG() {
		return WED_FLAG;
	}
	/**
	 * @param wed_flag The wED_FLAG to set.
	 */
	public void setWED_FLAG(String wed_flag) {
		WED_FLAG = wed_flag;
	}
	
	
  public void insertToDB(RockFactory rockFact) throws SQLException, RockException {
	    
	    Meta_schedulings schedule = new Meta_schedulings(rockFact);
	    
	    schedule.setVersion_number(VERSION_NUMBER);   
	    schedule.setId(ID);   
	    schedule.setExecution_type(EXECUTION_TYPE);   
	    schedule.setOs_command(OS_COMMAND);   
	    schedule.setScheduling_month(SCHEDULING_MONTH);   
	    schedule.setScheduling_day(SCHEDULING_DAY);   
	    schedule.setScheduling_hour(SCHEDULING_HOUR);   
	    schedule.setScheduling_min(SCHEDULING_MIN);   
	    schedule.setCollection_set_id(COLLECTION_SET_ID);   
	    schedule.setCollection_id(COLLECTION_ID);   
	    schedule.setMon_flag(MON_FLAG);   
	    schedule.setTue_flag(TUE_FLAG);   
	    schedule.setWed_flag(WED_FLAG);   
	    schedule.setThu_flag(THU_FLAG);   
	    schedule.setFri_flag(FRI_FLAG);   
	    schedule.setSat_flag(SAT_FLAG);   
	    schedule.setSun_flag(SUN_FLAG);   
	    schedule.setStatus(STATUS);   
	    schedule.setLast_execution_time(LAST_EXECUTION_TIME);   
	    schedule.setInterval_hour(INTERVAL_HOUR);   
	    schedule.setInterval_min(INTERVAL_MIN);   
	    schedule.setName(NAME);   
	    schedule.setHold_flag(HOLD_FLAG);
	    schedule.setPriority(PRIORITY);
	    schedule.setScheduling_year(SCHEDULING_YEAR);
	    schedule.setTrigger_command(TRIGGER_COMMAND);
	    	    
	    schedule.insertDB(false,false);    

	  }
	
	
}