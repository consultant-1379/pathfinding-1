package com.distocraft.dc5000.etl.engine.system;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class SetListenerManagerTest {

	private SetListenerManager sut;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		this.sut = SetListenerManager.instance();
	}

	@After
	public void tearDown() throws Exception {
	}

	@org.junit.Test
	public void testAddingAndRetrievingSetListener() {
		// First, add a new set listener
		SetListener setListener = new SetListener();
		long setListenerId = this.sut.addListener(setListener);
		
		// Assert that the SetListenerManager now contains a SetListener with
		// the returned id number.
		Assert.assertNotNull(this.sut.get(setListenerId));
	}
	
}
