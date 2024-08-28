package com.ericsson.etl.engine.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

public class ConfigurationLoaderFactoryTest {

	private static final Logger TEST_LOG = Logger.getLogger("Logger");
	private static final Logger TEST_LOG2 = Logger.getLogger("Logger2");
	private static RockFactory rockFact;
	private static RockFactory rockFact2;

	@Before
	public void setUp() throws Exception {
		Class.forName("org.hsqldb.jdbcDriver");
		rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		rockFact2 = new RockFactory("jdbc:hsqldb:mem:testdb2", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);

		// clear mocked instance;
		ConfigurationLoaderFactory.setMockedInstance(null);
	}

	@After
	public void tearDown() throws Exception {
		rockFact.getConnection().close();
	}

	@Test
	public void testConfigurationLoaderFactoryReturnsCorrectClass() {
		
		final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(rockFact, TEST_LOG);
		
		assertEquals("ConfigurationLoaderFactory returned wrong class.", ConfigurationLoader.class, configLoader.getClass());
		
		final MockedConfigurationLoader mockConfigLoader = new MockedConfigurationLoader(rockFact, TEST_LOG);
		ConfigurationLoaderFactory.setMockedInstance(mockConfigLoader);
		final ConfigurationLoader mockedConfigLoader = ConfigurationLoaderFactory.getInstance(rockFact, TEST_LOG);
		
		assertEquals("ConfigurationLoaderFactory returned wrong class.", MockedConfigurationLoader.class, mockedConfigLoader.getClass());
	}

	@Test
	public void testResettingMockedInstanceWorks() {
		final MockedConfigurationLoader mockConfigLoader = new MockedConfigurationLoader(rockFact, TEST_LOG);
		ConfigurationLoaderFactory.setMockedInstance(mockConfigLoader);
		final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(rockFact, TEST_LOG);

		ConfigurationLoaderFactory.setMockedInstance(null);
		final ConfigurationLoader configLoader2 = ConfigurationLoaderFactory.getInstance(rockFact, TEST_LOG);
		
		assertNotSame("Returned instances are same - should be different.", configLoader, configLoader2);
	}
	
	@Test (expected=IllegalArgumentException .class)
	public void testGetInitialInstanceWithNullRockFactThrowsException() {
		final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(null, TEST_LOG);
		System.out.println("Call to getInstance() should have failed: " + configLoader.toString());
	}

	@Test (expected=IllegalArgumentException.class)
	public void testGetInitialInstanceWithNullParentLogThrowsException() {
		final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(rockFact, null);
		System.out.println("Call to getInstance() should have failed: " + configLoader.toString());
	}

	@Test (expected=IllegalArgumentException.class)
	public void testGetInitialInstanceWithNullRockFactAndParentLogThrowsException() {
		final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(null, null);
		System.out.println("Call to getInstance() should have failed: " + configLoader.toString());
	}

	/**
	 * Simple mocked version of ConfigurationLoader to check ConfigurationLoaderFactory
	 * allows overriding of standard version.
	 *
	 */
	class MockedConfigurationLoader extends ConfigurationLoader {

		public MockedConfigurationLoader(final RockFactory dwhrep, final Logger parentLog) {
			super(dwhrep, parentLog);
		}
		
	}
	
}

