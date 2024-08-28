/*
 * Created on 30.3.2005 TODO To change the template for this generated file go
 * to Window - Preferences - Java - Code Style - Code Templates
 */
package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;

/**
 * 
 * action
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class ActionGenerator {

  private String VERSION_NUMBER;

  private String TRANSFER_ACTION_ID;

  private String COLLECTION_ID;

  private String COLLECTION_SET_ID;

  private String ACTION_TYPE;

  private String TRANSFER_ACTION_NAME;

  private String ORDER_BY_NO;

  private String WHERE_CLAUSE;

  private String ACTION_CONTENTS;

  private String ENABLED_FLAG;

  private String CONNECTION_ID;

  /**
   * @param version_number
   * @param transfer_action_id
   * @param collection_id
   * @param collection_set_id
   * @param action_type
   * @param transfer_action_name
   * @param order_by_no
   * @param where_clause
   * @param action_contents
   * @param enabled_flag
   * @param connection_id
   */
  public ActionGenerator(final String version_number, final String transfer_action_id, final String collection_id,
      final String collection_set_id, final String action_type, final String transfer_action_name,
      final String order_by_no, final String where_clause, final String action_contents, final String enabled_flag,
      final String connection_id) {
    super();
    VERSION_NUMBER = version_number;
    TRANSFER_ACTION_ID = transfer_action_id;
    COLLECTION_ID = collection_id;
    COLLECTION_SET_ID = collection_set_id;
    ACTION_TYPE = action_type;
    TRANSFER_ACTION_NAME = transfer_action_name;
    ORDER_BY_NO = order_by_no;
    WHERE_CLAUSE = where_clause;
    ACTION_CONTENTS = action_contents;
    ENABLED_FLAG = enabled_flag;
    CONNECTION_ID = connection_id;
  }

  public ActionGenerator() {

  }

  /**
   * @return Returns the aCTION_CONTENTS.
   */
  public String getACTION_CONTENTS() {
    return ACTION_CONTENTS;
  }

  /**
   * @param action_contents
   *          The aCTION_CONTENTS to set.
   */
  public void setACTION_CONTENTS(final String action_contents) {
    ACTION_CONTENTS = action_contents;
  }

  /**
   * @return Returns the aCTION_TYPE.
   */
  public String getACTION_TYPE() {
    return ACTION_TYPE;
  }

  /**
   * @param action_type
   *          The aCTION_TYPE to set.
   */
  public void setACTION_TYPE(final String action_type) {
    ACTION_TYPE = action_type;
  }

  /**
   * @return Returns the cOLLECTION_ID.
   */
  public String getCOLLECTION_ID() {
    return COLLECTION_ID;
  }

  /**
   * @param collection_id
   *          The cOLLECTION_ID to set.
   */
  public void setCOLLECTION_ID(final String collection_id) {
    COLLECTION_ID = collection_id;
  }

  /**
   * @return Returns the cOLLECTION_SET_ID.
   */
  public String getCOLLECTION_SET_ID() {
    return COLLECTION_SET_ID;
  }

  /**
   * @param collection_set_id
   *          The cOLLECTION_SET_ID to set.
   */
  public void setCOLLECTION_SET_ID(final String collection_set_id) {
    COLLECTION_SET_ID = collection_set_id;
  }

  /**
   * @return Returns the cONNECTION_ID.
   */
  public String getCONNECTION_ID() {
    return CONNECTION_ID;
  }

  /**
   * @param connection_id
   *          The cONNECTION_ID to set.
   */
  public void setCONNECTION_ID(final String connection_id) {
    CONNECTION_ID = connection_id;
  }

  /**
   * @return Returns the eNABLED_FLAG.
   */
  public String getENABLED_FLAG() {
    return ENABLED_FLAG;
  }

  /**
   * @param enabled_flag
   *          The eNABLED_FLAG to set.
   */
  public void setENABLED_FLAG(final String enabled_flag) {
    ENABLED_FLAG = enabled_flag;
  }

  /**
   * @return Returns the oRDER_BY_NO.
   */
  public String getORDER_BY_NO() {
    return ORDER_BY_NO;
  }

  /**
   * @param order_by_no
   *          The oRDER_BY_NO to set.
   */
  public void setORDER_BY_NO(final String order_by_no) {
    ORDER_BY_NO = order_by_no;
  }

  /**
   * @return Returns the tRANSFER_ACTION_ID.
   */
  public String getTRANSFER_ACTION_ID() {
    return TRANSFER_ACTION_ID;
  }

  /**
   * @param transfer_action_id
   *          The tRANSFER_ACTION_ID to set.
   */
  public void setTRANSFER_ACTION_ID(final String transfer_action_id) {
    TRANSFER_ACTION_ID = transfer_action_id;
  }

  /**
   * @return Returns the tRANSFER_ACTION_NAME.
   */
  public String getTRANSFER_ACTION_NAME() {
    return TRANSFER_ACTION_NAME;
  }

  /**
   * @param transfer_action_name
   *          The tRANSFER_ACTION_NAME to set.
   */
  public void setTRANSFER_ACTION_NAME(final String transfer_action_name) {
    TRANSFER_ACTION_NAME = transfer_action_name;
  }

  /**
   * @return Returns the vERSION_NUMBER.
   */
  public String getVERSION_NUMBER() {
    return VERSION_NUMBER;
  }

  /**
   * @param version_number
   *          The vERSION_NUMBER to set.
   */
  public void setVERSION_NUMBER(final String version_number) {
    VERSION_NUMBER = version_number;
  }

  /**
   * @return Returns the wHERE_CLAUSE.
   */
  public String getWHERE_CLAUSE() {
    return WHERE_CLAUSE;
  }

  /**
   * @param where_clause
   *          The wHERE_CLAUSE to set.
   */
  public void setWHERE_CLAUSE(final String where_clause) {
    WHERE_CLAUSE = where_clause;
  }

  public void insertToDB(final RockFactory rockFact) throws SQLException, RockException {

    final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);

    act.setVersion_number(this.VERSION_NUMBER);
    act.setTransfer_action_id(new Long(this.TRANSFER_ACTION_ID));
    act.setCollection_id(new Long(this.COLLECTION_ID));
    act.setCollection_set_id(new Long(this.COLLECTION_SET_ID));
    act.setAction_type(this.ACTION_TYPE);
    act.setTransfer_action_name(this.TRANSFER_ACTION_NAME);
    act.setOrder_by_no(new Long(this.ORDER_BY_NO));
    // act.setDescription(this.DESCRIPTION);
    act.setWhere_clause(this.WHERE_CLAUSE);
    act.setAction_contents(this.ACTION_CONTENTS);
    act.setEnabled_flag(this.ENABLED_FLAG);
    act.setConnection_id(new Long(this.CONNECTION_ID));

    act.insertDB(false, false);
  }
}
