package com.distocraft.dc5000.repository.cache;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class AggregationRuleCacheTest {
  private static RockFactory testRock = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testRock = DatabaseTestUtils.getTestDbConnection();
    DatabaseTestUtils.loadSetup(testRock, "arc");
  }

  @AfterClass
  public static void afterClass() {
    DatabaseTestUtils.close(testRock);
  }

  @Test
  public void testCache() {
    AggregationRuleCache.initialize(testRock);

    final String sourceName = "DC_E_IMS_IPW_AAA";
    final String sourceLevel = "COUNT";
    final String aggName = sourceName + "_" + sourceLevel;

    final List<AggregationRule> rules = AggregationRuleCache.getCache().getAggregationRules(aggName);
    Assert.assertNotNull("Cache failed", rules);
    Assert.assertEquals("Wrong number of rules found in cache", 1, rules.size());
    final AggregationRule ruleByAggName = rules.get(0);
    Assert.assertEquals("Cache key and value ID's mismatch", "COUNT", ruleByAggName.getTarget_level());
    Assert.assertEquals("Cache key and value ID's mismatch", "RAW", ruleByAggName.getSource_level());

    final List<AggregationRule> sRules = AggregationRuleCache.getCache().getAggregationRules(sourceName, sourceLevel);
    Assert.assertNotNull("Cache failed", sRules);
    Assert.assertEquals("Wrong number of rules found in cache", 1, sRules.size());
    final AggregationRule ruleBySourceNameLevel = sRules.get(0);
    Assert.assertEquals("Cache key and value ID's mismatch", "DAY", ruleBySourceNameLevel.getTarget_level());
    Assert.assertEquals("Cache key and value ID's mismatch", "COUNT", ruleBySourceNameLevel.getSource_level());

  }
}
