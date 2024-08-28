package com.distocraft.dc5000.etl.engine.system;


import java.util.Date;
import java.util.List;
import java.util.Vector;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SetStatusTOTest {

	private SetStatusTO sut;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		String state = SetListener.NOSET;
		List<StatusEvent> statusEvents = new Vector<StatusEvent>();
		
		this.sut = new SetStatusTO(state, statusEvents);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@org.junit.Test
	public void testGetAndSetSetStatus() {
		// Assert that the initial status is SetListener.NOSET
		String status = this.sut.getSetStatus();
		Assert.assertEquals(SetListener.NOSET, status);

		// Set the status to SetListener.SUCCEEDED
		this.sut.setSetStatus(SetListener.SUCCEEDED);
		
		// Assert that the status is now SetListener.SUCCEEDED
		status = this.sut.getSetStatus();
		Assert.assertEquals(SetListener.SUCCEEDED, status);
	}

	@org.junit.Test
	public void testGetAndSetStatusEvents() {
		// Assert that initially the status event list is empty
		Assert.assertEquals(0, this.sut.getStatusEvents().size());
		
		// Create a new status events list
		List<StatusEvent> newStatusEventList = new Vector<StatusEvent>();

		// Add three status events to the list
		String dispatcher = this.getClass().getSimpleName();
		Date time = new Date(System.currentTimeMillis());
		String message = "message";
		newStatusEventList.add(new StatusEvent(dispatcher, time, message));
		newStatusEventList.add(new StatusEvent(dispatcher, time, message));
		newStatusEventList.add(new StatusEvent(dispatcher, time, message));
		
		// Set the object's status event list
		this.sut.setStatusEvents(newStatusEventList);
		
		// Assert that the object's list now contains three status events
		Assert.assertEquals(3, this.sut.getStatusEvents().size());
	}

}
