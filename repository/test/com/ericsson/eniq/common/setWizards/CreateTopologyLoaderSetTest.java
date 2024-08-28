package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;
import utils.TestUtils;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.repository.dwhrep.Referencetable;

public class CreateTopologyLoaderSetTest {

  private final boolean isScheduling = true;

  private final int techpackId = 123;

  private static final String LOADER_SET_NAME = "Loader_Set";

  private static String TEMPLATE_DIR = "5.2";
    
  private static final String setVersion = "((12))";

  private static String techPackName = null;

  private static final String versionId = techPackName + ":" + setVersion;

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;
  private static boolean velocityInit = false;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StaticProperties.giveProperties(new Properties());
    _init_velocity();
    etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
    dwhrep = CreateRockFactoryHelper.createDwhRepFactory();
    TestUtils.loadSetup(dwhrep, "createLoaderSets");  
  }
  private static void _init_velocity() throws Exception {
      if(!velocityInit){
          Velocity.setProperty("resource.loader", "class");
          Velocity.setProperty("class.resource.loader.class",
                  "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
          Velocity.init();
          velocityInit = true;
      }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CreateRockFactoryHelper.cleanUpRockFactory();
    etlrep = null;
    dwhrep = null;
  }
  
  @Test
  public void testCreateSqlHistoryEg() throws Exception {
	  final CreateTopologyLoaderSet createLoaderSet = new CreateTopologyLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
		        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);
	  final String TABLE = "T_CURRENT";
	  final String typeId = "T:((3)):T_CURRENT";
	  final Referencetable  rtable = new Referencetable(dwhrep);
	  rtable.setTypename(TABLE);
	  rtable.setTypeid(typeId);
	  
	  final String expected = getExpectedHistoryLoaderSQLEg();
	  VelocityContext context = new VelocityContext();
	  //Hardcode for example from DIS
      context.put("loadTableName", "T_DELTA");
      context.put("joinTableName", "T_DELTA_AND_CURRENT");
      context.put("startTime", "START_TIME");
      context.put("endTime", "END_TIME");
      context.put("typeOfChangeCol", "EVENT_TYPE");//Added=0,Modified=1,Deleted=2
	  //Test
	  final String actual = createLoaderSet.CreateSqlHistory(rtable, context);
	  assertEquals(expected, actual);
  } //testCreateSqlHistoryEg
  
  @Test
  public void testCreateSqlHistory_ENodeB() throws Exception {
	  final CreateTopologyLoaderSet createLoaderSet = new CreateTopologyLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
		        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);
	  final String TABLE = "DC_E_LTE_SONV_CM_ENodeB";
	  final String typeId = "DC_E_LTE_SONV_CM:((43)):DC_E_LTE_SONV_CM_ENodeB";
	  final Referencetable  rtable = new Referencetable(dwhrep);
	  rtable.setTypename(TABLE);
	  rtable.setTypeid(typeId);
	  
	  final String expected = getExpectedHistoryLoaderSQL_ENodeB();
	  //Test
	  final String actual = createLoaderSet.CreateSqlHistory(rtable, null);
	  assertEquals(expected, actual);
  } //testCreateSqlHistory_ENodeB
  
  private String getExpectedHistoryLoaderSQLEg() {
	  StringBuilder result = new StringBuilder(); 
	  /*Comparing values from sync with data in actual table in db or in history partitions
	  Insert into the CALC table, the modified data from the loading table + the original MOs from the actual table: */
	  
	  result.append("/*Comparing values from sync with data in actual table in db or in history partitions"); result.append("\n");
	  result.append("Insert into the CALC table, the modified data from the loading table + the original MOs from the actual table: */"); result.append("\n");
	  result.append("INSERT INTO T_DELTA_AND_CURRENT (START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC) "); result.append("\n");
	  result.append("  SELECT"); result.append("\n");
	  result.append("      FIRST_VALUE(START_TIME) OVER (PARTITION BY KEY_1,KEY_2,KEY_3 ORDER BY START_TIME rows between current row AND current row) AS NEW_START_TIME,"); result.append("\n");
	  result.append("-- Take start time of following record as the end time of the current record. Last record will have NULL as the end time."); result.append("\n");
	  result.append("      FIRST_VALUE(START_TIME) OVER (PARTITION BY KEY_1,KEY_2,KEY_3 ORDER BY START_TIME rows between 1 following AND 1 following) AS END_TIME,"); result.append("\n");
	  result.append("      EVENT_TYPE ,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC"); result.append("\n");
	  result.append("  FROM (SELECT DISTINCT  LOADING1.START_TIME,LOADING1.END_TIME,LOADING1.EVENT_TYPE,LOADING1.KEY_1,LOADING1.KEY_2,LOADING1.KEY_3,LOADING1.ATT_1,LOADING1.ATT_2,LOADING1.ATT_3,LOADING1.FILE_TIME,LOADING1.IS_FULL_SYNC FROM T_DELTA LOADING1"); result.append("\n");
	  result.append("        ,(--Fetching only the most recent rows for every unique mo from the history table"); result.append("\n");
	  result.append("          SELECT DISTINCT HISTORY.START_TIME, HISTORY.END_TIME, HISTORY.EVENT_TYPE, HISTORY.KEY_1, HISTORY.KEY_2, HISTORY.KEY_3 FROM T_CURRENT_HIST_RAW HISTORY,"); result.append("\n");
	  result.append("	  (SELECT DISTINCT  KEY_1, KEY_2, KEY_3, MAX(START_TIME) AS latest_start_time FROM T_CURRENT_ACT_HIST "); result.append("\n");
	  result.append("	  GROUP BY  KEY_1,KEY_2 ,KEY_3  ) MAX_START_TIME "); result.append("\n");
	  result.append("	  WHERE HISTORY.CHANGE_TYPE <> 2"); result.append("\n");
	  result.append("	  AND HISTORY.START_TIME = MAX_START_TIME.latest_start_time 	  AND HISTORY.KEY_1 = MAX_START_TIME.KEY_1	  AND HISTORY.KEY_2 = MAX_START_TIME.KEY_2	  AND HISTORY.KEY_3 = MAX_START_TIME.KEY_3--  Code to eliminate view here"); result.append("\n");
	  result.append("  UNION "); result.append("\n");
	  result.append("		SELECT DISTINCT ACTUAL.START_TIME, ACTUAL.END_TIME, ACTUAL.EVENT_TYPE, ACTUAL.KEY_1, ACTUAL.KEY_2, ACTUAL.KEY_3 FROM T_CURRENT ACTUAL,"); result.append("\n");
	  result.append("		/*Note this is the second instance of this query but Sybase probably catches it and it is proven to be much more efficient to so this twice (the alternative/old way is view which joined 2 large tables and only after the join applied below filter)*/"); result.append("\n");
	  result.append("		(SELECT DISTINCT  KEY_1, KEY_2, KEY_3, MAX(START_TIME) AS latest_start_time FROM T_CURRENT_ACT_HIST "); result.append("\n");
	  result.append("	  GROUP BY  KEY_1,KEY_2 ,KEY_3  ) MAX_START_TIME"); result.append("\n");
	  result.append("	  WHERE ACTUAL.START_TIME = MAX_START_TIME.latest_start_time 	  AND ACTUAL.KEY_1 = MAX_START_TIME.KEY_1	  AND ACTUAL.KEY_2 = MAX_START_TIME.KEY_2	  AND ACTUAL.KEY_3 = MAX_START_TIME.KEY_3 ) MOST_RECENT	  "); result.append("\n");
	  result.append("  WHERE"); result.append("\n");
	  result.append("        (-- added or modified (non-deleted)"); result.append("\n");
	  result.append("         LOADING1.START_TIME > MOST_RECENT.START_TIME AND MOST_RECENT.CHANGE_TYPE <>2 AND MOST_RECENT.END_TIME='2099-12-31 00:00:00'"); result.append("\n");
	  result.append("         AND MOST_RECENT.KEY_1=LOADING1.KEY_1"); result.append("\n");
	  result.append(" AND MOST_RECENT.KEY_2=LOADING1.KEY_2"); result.append("\n");
	  result.append(" AND MOST_RECENT.KEY_3=LOADING1.KEY_3"); result.append("\n");
	  result.append("        )"); result.append("\n");
	  result.append("      OR"); result.append("\n");
	  result.append("        (--Looking for newer delete record exists in db (When mo gets deleted its entry would be in history)"); result.append("\n");
	  result.append("         LOADING1.START_TIME > MOST_RECENT.END_TIME AND MOST_RECENT.CHANGE_TYPE <>2 AND MOST_RECENT.END_TIME<>'2099-12-31 00:00:00'"); result.append("\n");
	  result.append("         AND MOST_RECENT.KEY_1=LOADING1.KEY_1"); result.append("\n");
	  result.append(" AND MOST_RECENT.KEY_2=LOADING1.KEY_2"); result.append("\n");
	  result.append(" AND MOST_RECENT.KEY_3=LOADING1.KEY_3"); result.append("\n");
	  result.append("        )"); result.append("\n");
	  result.append("  UNION ALL"); result.append("\n");
	  result.append("/* Get the original MOs from the actual table if they exist in the loaded data (current_dc)."); result.append("\n");
	  result.append("Combine these original MOs with the modified MOs (union all): */"); result.append("\n");
	  result.append("      (SELECT START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC FROM T_CURRENT ACTUAL WHERE EXISTS ("); result.append("\n");
	  result.append("            SELECT 1"); result.append("\n");
	  result.append("            FROM T_DELTA LOADING2"); result.append("\n");
	  result.append("            WHERE (ACTUAL.KEY_1=LOADING2.KEY_1 AND ACTUAL.KEY_2=LOADING2.KEY_2 AND ACTUAL.KEY_3=LOADING2.KEY_3)"); result.append("\n");
	  result.append("       ))"); result.append("\n");
	  result.append("  ) AS MODIFICATIONS_AND_ORIGINALS"); result.append("\n");
	  result.append("  ORDER BY KEY_1,KEY_2,KEY_3, START_TIME;"); result.append("\n");
	  result.append("/*Add new in Sync that are not already in db"); result.append("\n");
	  result.append("Insert new MOs into the calc table from loading table (MOs that are in the loading table, but not in the historical table).*/"); result.append("\n");
	  result.append("INSERT INTO T_DELTA_AND_CURRENT (START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC) "); result.append("\n");
	  result.append("	SELECT "); result.append("\n");
	  result.append("		FIRST_VALUE(START_TIME) OVER (PARTITION BY KEY_1,KEY_2,KEY_3 ORDER BY START_TIME rows between current row AND current row) AS NEW_START_TIME,"); result.append("\n");
	  result.append("    	FIRST_VALUE(START_TIME) OVER (PARTITION BY KEY_1,KEY_2,KEY_3 ORDER BY START_TIME rows between 1 following AND 1 following) AS END_TIME,"); result.append("\n");
      result.append("		EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC	FROM (SELECT LOADING.START_TIME,LOADING.END_TIME,LOADING.EVENT_TYPE,LOADING.KEY_1,LOADING.KEY_2,LOADING.KEY_3,LOADING.ATT_1,LOADING.ATT_2,LOADING.ATT_3,LOADING.FILE_TIME,LOADING.IS_FULL_SYNC FROM T_DELTA LOADING	"); result.append("\n");	
      result.append("	LEFT JOIN T_CURRENT_ACT_HIST HISTORY ON"); result.append("\n");
      result.append("				 LOADING.KEY_1=HISTORY.KEY_1 "); result.append("\n");
      result.append("								 AND LOADING.KEY_2=HISTORY.KEY_2"); result.append("\n");
      result.append("					 AND LOADING.KEY_3=HISTORY.KEY_3"); result.append("\n");
      result.append("				WHERE"); result.append("\n");
      result.append("						 HISTORY.KEY_1"); result.append("\n");
      result.append("																				 IS NULL ) as NEW_MOS;"); result.append("\n");
	  result.append(""); result.append("\n");
	  result.append("--Wipe the objects updated"); result.append("\n");
	  result.append("DELETE"); result.append("\n");
	  result.append("  FROM T_CURRENT"); result.append("\n");
	  result.append("  FROM T_CURRENT ACTUAL, T_DELTA_AND_CURRENT MODIFICATIONS_ORIGINALS_AND_NEW_MOS"); result.append("\n");
	  result.append("  WHERE"); result.append("\n");
	  result.append("    ACTUAL.KEY_1=MODIFICATIONS_ORIGINALS_AND_NEW_MOS.KEY_1"); result.append("\n");
	  result.append("     AND ACTUAL.KEY_2=MODIFICATIONS_ORIGINALS_AND_NEW_MOS.KEY_2"); result.append("\n");
	  result.append("     AND ACTUAL.KEY_3=MODIFICATIONS_ORIGINALS_AND_NEW_MOS.KEY_3"); result.append("\n");
	  result.append(";"); result.append("\n");
	  result.append("--insert historical records (all but the last one)"); result.append("\n");
	  result.append("INSERT INTO T_CURRENT_HIST_RAW (START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC) SELECT START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC FROM T_DELTA_AND_CURRENT WHERE END_TIME IS NOT NULL OR (END_TIME IS NULL AND EVENT_TYPE=2);"); result.append("\n");
	  result.append("/*move last record as well if it's a delete"); result.append("\n");
	  result.append("insert the  missing records from sync into history if full sync.*/"); result.append("\n");
	  result.append("IF (SELECT COUNT(*) FROM T_DELTA WHERE IS_FULL_SYNC=1) > 0"); result.append("\n");
	  result.append("THEN"); result.append("\n");
	  result.append("    BEGIN"); result.append("\n");
	  result.append("      --Records should be removed from actual table and moved to history if not in full sync file, where sync file time is newer."); result.append("\n");
	  result.append("      INSERT INTO T_CURRENT_HIST_RAW (START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC)"); result.append("\n");
	  result.append("        SELECT START_TIME,(SELECT MAX(FILE_TIME) FROM T_DELTA) AS END_TIME,2,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC FROM T_CURRENT ACTUAL"); result.append("\n");
	  result.append("        WHERE (SELECT MAX(FILE_TIME) FROM T_DELTA)>START_TIME AND NOT EXISTS (SELECT 1 FROM T_DELTA LOADING WHERE (ACTUAL.KEY_1=LOADING.KEY_1 AND ACTUAL.KEY_2=LOADING.KEY_2 AND ACTUAL.KEY_3=LOADING.KEY_3))"); result.append("\n");
	  result.append("      ;"); result.append("\n");
	  result.append("      --Deleting the records from table which are moved to history in above step."); result.append("\n");
	  result.append("      DELETE FROM T_CURRENT"); result.append("\n");
	  result.append("       FROM T_CURRENT ACTUAL,"); result.append("\n");
	  result.append("        WHERE (SELECT MAX(FILE_TIME) FROM T_DELTA)>START_TIME AND NOT EXISTS (SELECT 1 FROM T_DELTA LOADING WHERE (ACTUAL.KEY_1=LOADING.KEY_1 AND ACTUAL.KEY_2=LOADING.KEY_2 AND ACTUAL.KEY_3=LOADING.KEY_3))"); result.append("\n");
	  result.append("      ;"); result.append("\n");
	  result.append("    END"); result.append("\n");
	  result.append("END IF;"); result.append("\n");
	  result.append("\n");
	  result.append("--insert current records (most recent one)"); result.append("\n");
	  result.append("INSERT INTO T_CURRENT (START_TIME,END_TIME,EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC) SELECT START_TIME,'2099-12-31 00:00:00.000',EVENT_TYPE,KEY_1,KEY_2,KEY_3,ATT_1,ATT_2,ATT_3,FILE_TIME,IS_FULL_SYNC FROM T_DELTA_AND_CURRENT WHERE END_TIME IS NULL AND EVENT_TYPE <> 2; "); result.append("\n"); 
	  result.append("--deletes will go to history"); result.append("\n");
	  result.append("--clean up staging tables and commit"); result.append("\n");
	  result.append("TRUNCATE TABLE T_DELTA_AND_CURRENT;"); result.append("\n");
	  result.append("TRUNCATE TABLE T_DELTA;");result.append("\n");
	  result.append("--Delete data older than 32 days (UI can show last 30 days. 2 days margin for local time difference etc)"); result.append("\n");
	  result.append("DELETE from T_CURRENT_HIST_RAW"); result.append("\n");
	  result.append("WHERE --Expired historical records"); result.append("\n");
	  result.append("	END_TIME < DATEADD(dd,-32,GETDATE())"); result.append("\n");
	  result.append("OR (--expired deleted entries"); result.append("\n");
	  result.append("		CHANGE_TYPE=2"); result.append("\n");
	  result.append("		AND START_TIME < DATEADD(dd,-32,GETDATE()));"); result.append("\n");
	  result.append("commit;");
	  result.append("\n");
	  return result.toString();
  } //getExpectedHistoryLoaderSQL
  
  private String getExpectedHistoryLoaderSQL_ENodeB() {
	  StringBuilder result = new StringBuilder(); 
	  result.append("/*Comparing values from sync with data in actual table in db or in history partitions"); result.append("\n");
	  result.append("Insert into the CALC table, the modified data from the loading table + the original MOs from the actual table: */"); result.append("\n");
	  result.append("INSERT INTO DC_E_LTE_SONV_CM_ENodeB_CALC (START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC) "); result.append("\n"); 
	  result.append("  SELECT"); result.append("\n");
	  result.append("      FIRST_VALUE(START_TIME) OVER (PARTITION BY ENodeB_Hash ORDER BY START_TIME rows between current row AND current row) AS NEW_START_TIME,"); result.append("\n");
	  result.append("-- Take start time of following record as the end time of the current record. Last record will have NULL as the end time."); result.append("\n");
	  result.append("      FIRST_VALUE(START_TIME) OVER (PARTITION BY ENodeB_Hash ORDER BY START_TIME rows between 1 following AND 1 following) AS END_TIME,"); result.append("\n");
	  result.append("      CHANGE_TYPE ,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC"); result.append("\n");
	  result.append("  FROM (SELECT DISTINCT  LOADING1.START_TIME,LOADING1.END_TIME,LOADING1.CHANGE_TYPE,LOADING1.ENodeB_Hash,LOADING1.ENodeB_ID,LOADING1.MNC,LOADING1.MCC,LOADING1.FDN,LOADING1.StatusANR,LOADING1.StatusPCI,LOADING1.OSS_ID,LOADING1.TIMEZONE,LOADING1.ERBS_VERSION,LOADING1.FILE_TIME,LOADING1.IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC LOADING1"); result.append("\n");
	  result.append("        ,(--Fetching only the most recent rows for every unique mo from the history table"); result.append("\n");
	  result.append("          SELECT DISTINCT HISTORY.START_TIME, HISTORY.END_TIME, HISTORY.CHANGE_TYPE, HISTORY.ENodeB_Hash FROM DC_E_LTE_SONV_CM_ENodeB_HIST_RAW HISTORY,"); result.append("\n");
	  result.append("	  (SELECT DISTINCT  ENodeB_Hash, MAX(START_TIME) AS latest_start_time FROM DC_E_LTE_SONV_CM_ENodeB_ACT_HIST "); result.append("\n");
	  result.append("	  GROUP BY  ENodeB_Hash ) MAX_START_TIME "); result.append("\n");
	  result.append("	  WHERE HISTORY.CHANGE_TYPE <> 2"); result.append("\n");
	  result.append("	  AND HISTORY.START_TIME = MAX_START_TIME.latest_start_time 	  AND HISTORY.ENodeB_Hash = MAX_START_TIME.ENodeB_Hash--  Code to eliminate view here"); result.append("\n");
	  result.append("  UNION "); result.append("\n");
	  result.append("		SELECT DISTINCT ACTUAL.START_TIME, ACTUAL.END_TIME, ACTUAL.CHANGE_TYPE, ACTUAL.ENodeB_Hash FROM DC_E_LTE_SONV_CM_ENodeB ACTUAL,"); result.append("\n");
	  result.append("		/*Note this is the second instance of this query but Sybase probably catches it and it is proven to be much more efficient to so this twice (the alternative/old way is view which joined 2 large tables and only after the join applied below filter)*/"); result.append("\n");
	  result.append("		(SELECT DISTINCT  ENodeB_Hash, MAX(START_TIME) AS latest_start_time FROM DC_E_LTE_SONV_CM_ENodeB_ACT_HIST "); result.append("\n");
	  result.append("	  GROUP BY  ENodeB_Hash ) MAX_START_TIME"); result.append("\n");
	  result.append("	  WHERE ACTUAL.START_TIME = MAX_START_TIME.latest_start_time 	  AND ACTUAL.ENodeB_Hash = MAX_START_TIME.ENodeB_Hash ) MOST_RECENT	  "); result.append("\n");
	  result.append("  WHERE"); result.append("\n");
	  result.append("        (-- added or modified (non-deleted)"); result.append("\n");
	  result.append("         LOADING1.START_TIME > MOST_RECENT.START_TIME AND MOST_RECENT.CHANGE_TYPE <>2 AND MOST_RECENT.END_TIME='2099-12-31 00:00:00'"); result.append("\n");
	  result.append("         AND MOST_RECENT.ENodeB_Hash=LOADING1.ENodeB_Hash"); result.append("\n");
	  result.append("        )"); result.append("\n");
	  result.append("      OR"); result.append("\n");
	  result.append("        (--Looking for newer delete record exists in db (When mo gets deleted its entry would be in history)"); result.append("\n");
	  result.append("         LOADING1.START_TIME > MOST_RECENT.END_TIME AND MOST_RECENT.CHANGE_TYPE <>2 AND MOST_RECENT.END_TIME<>'2099-12-31 00:00:00'"); result.append("\n");
	  result.append("         AND MOST_RECENT.ENodeB_Hash=LOADING1.ENodeB_Hash"); result.append("\n");
	  result.append("        )"); result.append("\n");
	  result.append("  UNION ALL"); result.append("\n");
	  result.append("/* Get the original MOs from the actual table if they exist in the loaded data (current_dc)."); result.append("\n");
	  result.append("Combine these original MOs with the modified MOs (union all): */"); result.append("\n");
	  result.append("      (SELECT START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB ACTUAL WHERE EXISTS ("); result.append("\n");
	  result.append("            SELECT 1"); result.append("\n");
	  result.append("            FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC LOADING2"); result.append("\n");
	  result.append("            WHERE (ACTUAL.ENodeB_Hash=LOADING2.ENodeB_Hash)"); result.append("\n");
	  result.append("       ))"); result.append("\n");
	  result.append("  ) AS MODIFICATIONS_AND_ORIGINALS"); result.append("\n");
	  result.append("  ORDER BY ENodeB_Hash, START_TIME;"); result.append("\n");
	  result.append("/*Add new in Sync that are not already in db"); result.append("\n");
	  result.append("Insert new MOs into the calc table from loading table (MOs that are in the loading table, but not in the historical table).*/"); result.append("\n");
	  result.append("INSERT INTO DC_E_LTE_SONV_CM_ENodeB_CALC (START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC) "); result.append("\n");
	  result.append("	SELECT "); result.append("\n");
	  result.append("		FIRST_VALUE(START_TIME) OVER (PARTITION BY ENodeB_Hash ORDER BY START_TIME rows between current row AND current row) AS NEW_START_TIME,"); result.append("\n");
	  result.append("    	FIRST_VALUE(START_TIME) OVER (PARTITION BY ENodeB_Hash ORDER BY START_TIME rows between 1 following AND 1 following) AS END_TIME,"); result.append("\n");
	  result.append("		CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC	FROM (SELECT LOADING.START_TIME,LOADING.END_TIME,LOADING.CHANGE_TYPE,LOADING.ENodeB_Hash,LOADING.ENodeB_ID,LOADING.MNC,LOADING.MCC,LOADING.FDN,LOADING.StatusANR,LOADING.StatusPCI,LOADING.OSS_ID,LOADING.TIMEZONE,LOADING.ERBS_VERSION,LOADING.FILE_TIME,LOADING.IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC LOADING	"); result.append("\n");
	  result.append("	LEFT JOIN DC_E_LTE_SONV_CM_ENodeB_ACT_HIST HISTORY ON"); result.append("\n");
	  result.append("				 LOADING.ENodeB_Hash=HISTORY.ENodeB_Hash "); result.append("\n");
	  result.append("							WHERE"); result.append("\n");
	  result.append("						 HISTORY.ENodeB_Hash"); result.append("\n");
	  result.append("										 IS NULL ) as NEW_MOS;"); result.append("\n");	  	  
	  result.append(""); result.append("\n");
	  result.append("--Wipe the objects updated"); result.append("\n");
	  result.append("DELETE"); result.append("\n");
	  result.append("  FROM DC_E_LTE_SONV_CM_ENodeB"); result.append("\n");
	  result.append("  FROM DC_E_LTE_SONV_CM_ENodeB ACTUAL, DC_E_LTE_SONV_CM_ENodeB_CALC MODIFICATIONS_ORIGINALS_AND_NEW_MOS"); result.append("\n");
	  result.append("  WHERE"); result.append("\n");
	  result.append("    ACTUAL.ENodeB_Hash=MODIFICATIONS_ORIGINALS_AND_NEW_MOS.ENodeB_Hash"); result.append("\n");
	  result.append(";"); result.append("\n");
	  result.append("--insert historical records (all but the last one)"); result.append("\n");
	  result.append("INSERT INTO DC_E_LTE_SONV_CM_ENodeB_HIST_RAW (START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC) SELECT START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB_CALC WHERE END_TIME IS NOT NULL OR (END_TIME IS NULL AND CHANGE_TYPE=2);"); result.append("\n");
	  result.append("/*move last record as well if it's a delete"); result.append("\n");
	  result.append("insert the  missing records from sync into history if full sync.*/"); result.append("\n");
	  result.append("IF (SELECT COUNT(*) FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC WHERE IS_FULL_SYNC=1) > 0"); result.append("\n");
	  result.append("THEN"); result.append("\n");
	  result.append("    BEGIN"); result.append("\n");
	  result.append("      --Records should be removed from actual table and moved to history if not in full sync file, where sync file time is newer."); result.append("\n");
	  result.append("      INSERT INTO DC_E_LTE_SONV_CM_ENodeB_HIST_RAW (START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC)"); result.append("\n");
	  result.append("        SELECT START_TIME,(SELECT MAX(FILE_TIME) FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC) AS END_TIME,2,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB ACTUAL"); result.append("\n");
	  result.append("        WHERE (SELECT MAX(FILE_TIME) FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC)>START_TIME AND NOT EXISTS (SELECT 1 FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC LOADING WHERE (ACTUAL.ENodeB_Hash=LOADING.ENodeB_Hash))"); result.append("\n");
	  result.append("      ;"); result.append("\n");
	  result.append("      --Deleting the records from table which are moved to history in above step."); result.append("\n");
	  result.append("      DELETE FROM DC_E_LTE_SONV_CM_ENodeB"); result.append("\n");
	  result.append("       FROM DC_E_LTE_SONV_CM_ENodeB ACTUAL,"); result.append("\n");
	  result.append("        WHERE (SELECT MAX(FILE_TIME) FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC)>START_TIME AND NOT EXISTS (SELECT 1 FROM DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC LOADING WHERE (ACTUAL.ENodeB_Hash=LOADING.ENodeB_Hash))"); result.append("\n");
	  result.append("      ;"); result.append("\n");
	  result.append("    END"); result.append("\n");
	  result.append("END IF;"); result.append("\n");
	  result.append("\n");
	  result.append("--insert current records (most recent one)"); result.append("\n");
	  result.append("INSERT INTO DC_E_LTE_SONV_CM_ENodeB (START_TIME,END_TIME,CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC) SELECT START_TIME,'2099-12-31 00:00:00.000',CHANGE_TYPE,ENodeB_Hash,ENodeB_ID,MNC,MCC,FDN,StatusANR,StatusPCI,OSS_ID,TIMEZONE,ERBS_VERSION,FILE_TIME,IS_FULL_SYNC FROM DC_E_LTE_SONV_CM_ENodeB_CALC WHERE END_TIME IS NULL AND CHANGE_TYPE <> 2; "); result.append("\n");
	  result.append("--deletes will go to history"); result.append("\n");
	  result.append("--clean up staging tables and commit"); result.append("\n");
	  result.append("TRUNCATE TABLE DC_E_LTE_SONV_CM_ENodeB_CALC;"); result.append("\n");
	  result.append("TRUNCATE TABLE DC_E_LTE_SONV_CM_ENodeB_CURRENT_DC;"); result.append("\n");
	  result.append("--Delete data older than 32 days (UI can show last 30 days. 2 days margin for local time difference etc)"); result.append("\n");
	  result.append("DELETE from DC_E_LTE_SONV_CM_ENodeB_HIST_RAW"); result.append("\n");
	  result.append("WHERE --Expired historical records"); result.append("\n");
	  result.append("	END_TIME < DATEADD(dd,-32,GETDATE())"); result.append("\n");
	  result.append("OR (--expired deleted entries"); result.append("\n");
	  result.append("		CHANGE_TYPE=2"); result.append("\n");
	  result.append("		AND START_TIME < DATEADD(dd,-32,GETDATE()));"); result.append("\n");
	  result.append("commit;");
	  result.append("\n");
	  return result.toString();
  } //getExpectedHistoryLoaderSQL_ENodeB
} //CreateTopologyLoaderSetTest
