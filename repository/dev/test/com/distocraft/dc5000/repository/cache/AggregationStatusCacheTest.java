package com.distocraft.dc5000.repository.cache;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.text.SimpleDateFormat;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(JMock.class)
public class AggregationStatusCacheTest {
  private static RockFactory jUnitTestDB = null;

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static long longDate = -1;

  private Mockery concreteContext = new JUnit4Mockery() {{
	    setImposteriser(ClassImposteriser.INSTANCE);
	}};

  @BeforeClass
  public static void setUp() throws Exception {
    jUnitTestDB = DatabaseTestUtils.getTestDbConnection();
    DatabaseTestUtils.loadSetup(jUnitTestDB, "aggregationStatusCacheTest");
    AggregationStatusCache.init(
      DatabaseTestUtils.getTestDbUrl(),
      DatabaseTestUtils.getTestDbUser(), DatabaseTestUtils.getTestDbPassword(),
      DatabaseTestUtils.getTestDbDriver());
    PhysicalTableCache.initialize(jUnitTestDB);
    longDate = sdf.parse("2011-01-10 00:00:00").getTime();
  }

  @AfterClass
  public static void tearDown() {
    DatabaseTestUtils.close(jUnitTestDB);
  }

  @Test
  public void testLoopCount() {
    try {
      final AggregationStatus aggSta = AggregationStatusCache.getStatus("DC_E_CPP_VCLTP_DAYBH_VCLTP", longDate);
      assertNotNull(aggSta);
      assertEquals(aggSta.LOOPCOUNT, 1);
      assertEquals(aggSta.ROWCOUNT, 10);
    } catch (Exception e) {
      fail("TEST FAILED: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void test_init() throws Exception {
    AggregationStatusCache.init(
      DatabaseTestUtils.getTestDbUrl(),
      DatabaseTestUtils.getTestDbUser(), DatabaseTestUtils.getTestDbPassword(),
      DatabaseTestUtils.getTestDbDriver());
    final AggregationStatus aggSta = AggregationStatusCache.getStatus("DC_E_CPP_VCLTP_DAYBH_VCLTP", longDate);
    assertNotNull(aggSta);
    aggSta.DESCRIPTION = "abc123";
    AggregationStatusCache.setStatus(aggSta);
    final AggregationStatus aggSta_Updated = AggregationStatusCache.getStatus("DC_E_CPP_VCLTP_DAYBH_VCLTP", longDate);
    assertEquals("AggregationStatusCache write failed", "abc123", aggSta_Updated.DESCRIPTION);
  }

@Test
  public void testThreshold() {
    try {
      AggregationStatusCache.init(DatabaseTestUtils.getTestDbUrl(),
        DatabaseTestUtils.getTestDbUser(), DatabaseTestUtils.getTestDbPassword(),
        DatabaseTestUtils.getTestDbDriver());

      final PhysicalTableCache ptcMock = concreteContext.mock(PhysicalTableCache.class);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final long longDate = sdf.parse("2010-11-23 00:00:00").getTime();

      // Set a mock cache for the physical table cache.
      // Table name will be 'LOG_AggregationStatus':
      PhysicalTableCache.setCache(ptcMock);
      concreteContext.checking(new Expectations() {{
        oneOf(ptcMock).getTableName(with("LOG_AggregationStatus:PLAIN"), with(longDate));
        will(returnValue("LOG_AggregationStatus"));
      }});

      // Test that the threshold value is correct from the cache:
      AggregationStatus aggSta = AggregationStatusCache.getStatus("DC_E_CPP_VCLTP_DAYBH_VCLTP", longDate);
      assertNotNull("No AggregationStatus found for DC_E_CPP_VCLTP_DAYBH_VCLTP", aggSta);
      assertEquals("Threshold time value should be retrieved properly from cache", longDate, aggSta.THRESHOLD);

      // Test that the threshold value is set correctly in the cache:
      // Set a new time:
      long newTime = sdf.parse("2010-11-23 03:00:00").getTime();
      aggSta.THRESHOLD = newTime;
      AggregationStatusCache.setStatus(aggSta);
      AggregationStatusCache.getStatus("DC_E_CPP_VCLTP_DAYBH_VCLTP", newTime);
      assertEquals("Threshold time value should be updated properly in cache", newTime, aggSta.THRESHOLD);
    } catch (Exception e) {
      fail("TEST FAILED: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void testCheckThresholdReset() {
    boolean result = AggregationStatusCache.checkThresholdReset("QUEUED");
    assertEquals("QUEUED aggregation should not have its threshold value reset", false, result);

    result = AggregationStatusCache.checkThresholdReset("LOADED");
    assertEquals("LOADED aggregation should not have its threshold value reset", false, result);

    // MANUAL, IGNORED, ERROR, FAILEDDEPENDENCY, LATE DATA or AGGREGATED should have the threshold value reset:
    result = AggregationStatusCache.checkThresholdReset("MANUAL");
    assertEquals("MANUAL aggregation should have its threshold value reset", true, result);

    result = AggregationStatusCache.checkThresholdReset("IGNORED");
    assertEquals("IGNORED aggregation should have its threshold value reset", true, result);

    result = AggregationStatusCache.checkThresholdReset("ERROR");
    assertEquals("ERROR aggregation should have its threshold value reset", true, result);

    result = AggregationStatusCache.checkThresholdReset("FAILEDDEPENDENCY");
    assertEquals("FAILEDDEPENDENCY aggregation should have its threshold value reset", true, result);

    result = AggregationStatusCache.checkThresholdReset("LATE_DATA");
    assertEquals("LATE_DATA aggregation should have its threshold value reset", true, result);

    result = AggregationStatusCache.checkThresholdReset("AGGREGATED");
    assertEquals("AGGREGATED aggregation should have its threshold value reset", true, result);
  }


}
