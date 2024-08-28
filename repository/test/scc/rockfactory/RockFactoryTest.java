package scc.rockfactory;

import com.distocraft.dc5000.common.StaticProperties;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class RockFactoryTest {

  @Before
  public void setUp() throws Exception {
    StaticProperties.giveProperties(new Properties());
  }

  @Test
  public void checkThatLongQueryTimeoutInSecsEquals3HoursInSeconds() throws Exception {
    assertEquals(RockFactory.LONG_QUERY_TIMEOUT_IN_SECONDS, 10800);
  }


  @Test
  public void checkThatDefaultQueryTimeoutInSecsEquals3MinsInSeconds() throws Exception {
    assertEquals(RockFactory.DEFAULT_QUERY_TIMEOUT_IN_SECONDS, 1800);
  }

  @Test
  public void checkThatUnLimitedQueryTimeoutInSecsEquals1DAYInSeconds() throws Exception {
    assertEquals(RockFactory.UNLIMITED_QUERY_TIMEOUT_IN_SECONDS, 86400);
  }

  @Ignore
  public void testInvalidLogin() throws RockException {
    final String url = "jdbc:hsqldb:mem:tesT_database";
    try {
      new RockFactory(url, "failed_login", "boo", "scc.rockfactory.TestDriver", "con", true);
      fail("Expected and SQLException to be thrown but it wasn't!");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Invalid user ID or password"));
    }
  }

}
