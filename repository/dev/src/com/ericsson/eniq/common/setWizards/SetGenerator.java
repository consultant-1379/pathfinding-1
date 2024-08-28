/**
 * 
 */
package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_collections;

/**
 * 
 * Set
 *
 * @author savinen
 * Copyright Distocraft 2005
 * 
 * $id$
 */
public class SetGenerator {


  private String COLLECTION_NAME;
  private String COLLECTION_ID;
  private String MAX_ERRORS;
  private String MAX_FK_ERRORS;
  private String MAX_COL_LIMIT_ERRORS;
  private String CHECK_FK_ERROR_FLAG;
  private String CHECK_COL_LIMITS_FLAG;
  private String VERSION_NUMBER;
  private String COLLECTION_SET_ID;
  private String PRIORITY;
  private String QUEUE_TIME_LIMIT;
  private String ENABLED_FLAG;
  private String SETTYPE;
  private String FOLDABLE_FLAG;
  private String HOLD_FLAG;
 
  
  /**
   * @param collection_name
   * @param collection_id
   * @param max_errors
   * @param max_fk_errors
   * @param max_col_limit_errors
   * @param check_fk_error_flag
   * @param check_col_limits_flag
   * @param version_number
   * @param collection_set_id
   * @param priority
   * @param queue_time_limit
   * @param enabled_flag
   * @param settype
   * @param foldable_flag
   * @param hold_flag
   */
  public SetGenerator(String collection_name, String collection_id, String max_errors, String max_fk_errors,
      String max_col_limit_errors, String check_fk_error_flag, String check_col_limits_flag, String version_number,
      String collection_set_id, String priority, String queue_time_limit, String enabled_flag, String settype,
      String foldable_flag, String hold_flag) {
    
    super();
    COLLECTION_NAME = collection_name;
    COLLECTION_ID = collection_id;
    MAX_ERRORS = max_errors;
    MAX_FK_ERRORS = max_fk_errors;
    MAX_COL_LIMIT_ERRORS = max_col_limit_errors;
    CHECK_FK_ERROR_FLAG = check_fk_error_flag;
    CHECK_COL_LIMITS_FLAG = check_col_limits_flag;
    VERSION_NUMBER = version_number;
    COLLECTION_SET_ID = collection_set_id;
    PRIORITY = priority;
    QUEUE_TIME_LIMIT = queue_time_limit;
    ENABLED_FLAG = enabled_flag;
    SETTYPE = settype;
    FOLDABLE_FLAG = foldable_flag;
    HOLD_FLAG = hold_flag;
  }

  public SetGenerator(){
    
  }
  
  /**
   * @return Returns the cHECK_COL_LIMITS_FLAG.
   */
  public String getCHECK_COL_LIMITS_FLAG() {
    return CHECK_COL_LIMITS_FLAG;
  }
  /**
   * @param check_col_limits_flag The cHECK_COL_LIMITS_FLAG to set.
   */
  public void setCHECK_COL_LIMITS_FLAG(String check_col_limits_flag) {
    CHECK_COL_LIMITS_FLAG = check_col_limits_flag;
  }
  /**
   * @return Returns the cHECK_FK_ERROR_FLAG.
   */
  public String getCHECK_FK_ERROR_FLAG() {
    return CHECK_FK_ERROR_FLAG;
  }
  /**
   * @param check_fk_error_flag The cHECK_FK_ERROR_FLAG to set.
   */
  public void setCHECK_FK_ERROR_FLAG(String check_fk_error_flag) {
    CHECK_FK_ERROR_FLAG = check_fk_error_flag;
  }
  /**
   * @return Returns the cOLLECTION_ID.
   */
  public String getCOLLECTION_ID() {
    return COLLECTION_ID;
  }
  /**
   * @param collection_id The cOLLECTION_ID to set.
   */
  public void setCOLLECTION_ID(String collection_id) {
    COLLECTION_ID = collection_id;
  }
  /**
   * @return Returns the cOLLECTION_NAME.
   */
  public String getCOLLECTION_NAME() {
    return COLLECTION_NAME;
  }
  /**
   * @param collection_name The cOLLECTION_NAME to set.
   */
  public void setCOLLECTION_NAME(String collection_name) {
    COLLECTION_NAME = collection_name;
  }
  /**
   * @return Returns the cOLLECTION_SET_ID.
   */
  public String getCOLLECTION_SET_ID() {
    return COLLECTION_SET_ID;
  }
  /**
   * @param collection_set_id The cOLLECTION_SET_ID to set.
   */
  public void setCOLLECTION_SET_ID(String collection_set_id) {
    COLLECTION_SET_ID = collection_set_id;
  }
  /**
   * @return Returns the eNABLED_FLAG.
   */
  public String getENABLED_FLAG() {
    return ENABLED_FLAG;
  }
  /**
   * @param enabled_flag The eNABLED_FLAG to set.
   */
  public void setENABLED_FLAG(String enabled_flag) {
    ENABLED_FLAG = enabled_flag;
  }
  /**
   * @return Returns the fOLDABLE_FLAG.
   */
  public String getFOLDABLE_FLAG() {
    return FOLDABLE_FLAG;
  }
  /**
   * @param foldable_flag The fOLDABLE_FLAG to set.
   */
  public void setFOLDABLE_FLAG(String foldable_flag) {
    FOLDABLE_FLAG = foldable_flag;
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
   * @return Returns the mAX_COL_LIMIT_ERRORS.
   */
  public String getMAX_COL_LIMIT_ERRORS() {
    return MAX_COL_LIMIT_ERRORS;
  }
  /**
   * @param max_col_limit_errors The mAX_COL_LIMIT_ERRORS to set.
   */
  public void setMAX_COL_LIMIT_ERRORS(String max_col_limit_errors) {
    MAX_COL_LIMIT_ERRORS = max_col_limit_errors;
  }
  /**
   * @return Returns the mAX_ERRORS.
   */
  public String getMAX_ERRORS() {
    return MAX_ERRORS;
  }
  /**
   * @param max_errors The mAX_ERRORS to set.
   */
  public void setMAX_ERRORS(String max_errors) {
    MAX_ERRORS = max_errors;
  }
  /**
   * @return Returns the mAX_FK_ERRORS.
   */
  public String getMAX_FK_ERRORS() {
    return MAX_FK_ERRORS;
  }
  /**
   * @param max_fk_errors The mAX_FK_ERRORS to set.
   */
  public void setMAX_FK_ERRORS(String max_fk_errors) {
    MAX_FK_ERRORS = max_fk_errors;
  }
  /**
   * @return Returns the pRIORITY.
   */
  public String getPRIORITY() {
    return PRIORITY;
  }
  /**
   * @param priority The pRIORITY to set.
   */
  public void setPRIORITY(String priority) {
    PRIORITY = priority;
  }
  /**
   * @return Returns the qUEUE_TIME_LIMIT.
   */
  public String getQUEUE_TIME_LIMIT() {
    return QUEUE_TIME_LIMIT;
  }
  /**
   * @param queue_time_limit The qUEUE_TIME_LIMIT to set.
   */
  public void setQUEUE_TIME_LIMIT(String queue_time_limit) {
    QUEUE_TIME_LIMIT = queue_time_limit;
  }
  /**
   * @return Returns the sETTYPE.
   */
  public String getSETTYPE() {
    return SETTYPE;
  }
  /**
   * @param settype The sETTYPE to set.
   */
  public void setSETTYPE(String settype) {
    SETTYPE = settype;
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
  

 
  
  
  public void insertToDB(RockFactory rockFact) throws SQLException, RockException {
    
    System.err.println("SetGenerator: Trying to create a set tp "+COLLECTION_SET_ID+" set "+COLLECTION_ID+" ("+COLLECTION_NAME+") ver "+VERSION_NUMBER);
  
    Meta_collections coll = new Meta_collections(rockFact);
    
    
    coll.setCollection_name(COLLECTION_NAME);   
    //coll.setCollection(COLLECTION_ID);
    coll.setCollection_id(new Long(COLLECTION_ID));
    //coll.setCollection_name(MAIL_ERROR_ADDR);
    //coll.setCollection_name(MAIL_FAIL_ADDR);
    //coll.setCollection_name(MAIL_BUG_ADDR);
    coll.setMax_errors(new Long(MAX_ERRORS));
    coll.setMax_fk_errors(new Long(MAX_FK_ERRORS));
    coll.setMax_col_limit_errors(new Long(MAX_COL_LIMIT_ERRORS));
    coll.setCheck_fk_error_flag(CHECK_FK_ERROR_FLAG);
    coll.setCheck_col_limits_flag(CHECK_COL_LIMITS_FLAG);
    //private Timestamp    LAST_TRANSFER_DATE;
    coll.setVersion_number(VERSION_NUMBER);
    coll.setCollection_set_id(new Long(COLLECTION_SET_ID));
    //coll.setUse_batch_id(USE_BATCH_ID);
    coll.setPriority(new Long(PRIORITY));
    coll.setQueue_time_limit(new Long(QUEUE_TIME_LIMIT));
    coll.setEnabled_flag(ENABLED_FLAG);
    coll.setSettype(SETTYPE);
    coll.setFoldable_flag(FOLDABLE_FLAG);
    //coll.setMeastype(MEASTYPE);
    coll.setHold_flag(HOLD_FLAG);
    //coll.setScheduling_info(SCHEDULING_INFO);
    
    coll.insertDB(false,false);    

  }
  
  
 
  
}