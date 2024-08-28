package com.ericsson.eniq.common.setWizards;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class CreateRockFactoryHelper {

  private static Connection con = null;

  private static Statement stmt;

  private static final String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";

  private static final String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";

  private static final String ETLREP_URL = "jdbc:hsqldb:mem:etlrep";

  protected static RockFactory createDwhRepFactory() throws SQLException, RockException {
    return new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "test", true);
  }

  protected static RockFactory createEtlRepRockFactory() throws SQLException, RockException, InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    /* Creating connection for rockfactory */

    Class.forName("org.hsqldb.jdbcDriver").newInstance();
    con = DriverManager.getConnection(ETLREP_URL, "SA", "");

    stmt = con.createStatement();
    stmt
        .execute("CREATE TABLE META_COLLECTIONS (COLLECTION_ID NUMERIC(38), COLLECTION_NAME VARCHAR(128), COLLECTION VARCHAR(200), MAIL_ERROR_ADDR VARCHAR(100), MAIL_FAIL_ADDR VARCHAR(100), MAIL_BUG_ADDR VARCHAR(100), MAX_ERRORS NUMERIC(38), MAX_FK_ERRORS NUMERIC(38), MAX_COL_LIMIT_ERRORS NUMERIC(38), CHECK_FK_ERROR_FLAG VARCHAR(1), CHECK_COL_LIMITS_FLAG VARCHAR(1), LAST_TRANSFER_DATE DATETIME, VERSION_NUMBER VARCHAR(32), COLLECTION_SET_ID NUMERIC(38), USE_BATCH_ID VARCHAR(1), PRIORITY NUMERIC(3), QUEUE_TIME_LIMIT NUMERIC(38), ENABLED_FLAG VARCHAR(1), SETTYPE VARCHAR(10), FOLDABLE_FLAG VARCHAR(1), MEASTYPE VARCHAR(30), HOLD_FLAG VARCHAR(1), SCHEDULING_INFO VARCHAR(2000));");
    stmt
        .execute("CREATE TABLE Meta_transfer_actions (VERSION_NUMBER VARCHAR(32), TRANSFER_ACTION_ID NUMERIC(38), COLLECTION_ID NUMERIC(38), COLLECTION_SET_ID NUMERIC(38), ACTION_TYPE VARCHAR(20), TRANSFER_ACTION_NAME VARCHAR(128), ORDER_BY_NO NUMERIC(38), DESCRIPTION VARCHAR(32000), WHERE_CLAUSE_01 VARCHAR(32000), ACTION_CONTENTS_01 VARCHAR(32000), ENABLED_FLAG VARCHAR(1), CONNECTION_ID NUMERIC(38), WHERE_CLAUSE_02 VARCHAR(32000), WHERE_CLAUSE_03 VARCHAR(32000), ACTION_CONTENTS_02 VARCHAR(32000), ACTION_CONTENTS_03 VARCHAR(32000));");

    stmt
        .execute("CREATE TABLE Meta_schedulings (VERSION_NUMBER VARCHAR(20), ID BIGINT, EXECUTION_TYPE VARCHAR(20),"
            + "OS_COMMAND VARCHAR(20), SCHEDULING_MONTH BIGINT, SCHEDULING_DAY BIGINT, SCHEDULING_HOUR BIGINT,"
            + "SCHEDULING_MIN BIGINT, COLLECTION_SET_ID BIGINT, COLLECTION_ID BIGINT, MON_FLAG VARCHAR(20), TUE_FLAG VARCHAR(20),"
            + "WED_FLAG VARCHAR(20), THU_FLAG VARCHAR(20), FRI_FLAG VARCHAR(20), SAT_FLAG VARCHAR(20), SUN_FLAG VARCHAR(20),"
            + "STATUS VARCHAR(20), LAST_EXECUTION_TIME TIMESTAMP, INTERVAL_HOUR BIGINT, INTERVAL_MIN BIGINT, NAME VARCHAR(40),"
            + "HOLD_FLAG VARCHAR(20), PRIORITY BIGINT, SCHEDULING_YEAR BIGINT, TRIGGER_COMMAND VARCHAR(20), LAST_EXEC_TIME_MS BIGINT)");
    /* Initializing rockfactory */
    return new RockFactory(ETLREP_URL, "SA", "", TESTDB_DRIVER, "con", true);
  }

  public static void cleanUpRockFactory() throws SQLException {
    stmt.execute("DROP TABLE Meta_schedulings");
    stmt.execute("DROP TABLE Meta_transfer_actions");
    stmt.execute("DROP TABLE META_COLLECTIONS");
    con = null;
  }

  public static ResultSet executeQueryOnEtlRep(final String query) throws SQLException {
    return stmt.executeQuery(query);
  }
}
