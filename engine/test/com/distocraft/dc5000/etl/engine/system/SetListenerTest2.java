package com.distocraft.dc5000.etl.engine.system;


import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SetListenerTest2 {

	private SetListener sut;
	
	private static StatusEvent[] testStatusEvents = {
		new StatusEvent("Dispatcher1", new Date(System.currentTimeMillis()), "Message1"),
		new StatusEvent("Dispatcher2", new Date(System.currentTimeMillis()), "Message2"),
		new StatusEvent("Dispatcher3", new Date(System.currentTimeMillis()), "Message3"),
		new StatusEvent("Dispatcher4", new Date(System.currentTimeMillis()), "Message4"),
		new StatusEvent("Dispatcher5", new Date(System.currentTimeMillis()), "Message5"),
		new StatusEvent("Dispatcher6", new Date(System.currentTimeMillis()), "Message6"),
		new StatusEvent("Dispatcher7", new Date(System.currentTimeMillis()), "Message7")
	};
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		this.sut = new SetListener();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@org.junit.Test
	public void testRetrievingAllStatusEvents() {
		// First, assert that the set listener does not contain status events
		Assert.assertEquals(0, this.sut.getNumberOfStatusEvents());
		
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}
		
		// Assert that the listener now contains the status evens added to it
		Assert.assertEquals(testStatusEvents.length, this.sut.getNumberOfStatusEvents());
		for(int i=0; i<testStatusEvents.length; ++i) {
			Assert.assertEquals(testStatusEvents[i], this.sut.getStatusEvent(i));
		}
	}
	
	@org.junit.Test
	public void testRetrievingSomeStatusEventsWithValidIndices() {
		// First, assert that the set listener does not contain status events
		Assert.assertEquals(0, this.sut.getNumberOfStatusEvents());
		
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}

		// Retrieve the first three status events
		List<StatusEvent> firstThreeStatusEvents = this.sut.getStatusEvents(0, 3);
		
		// Assert that the retrieved events equal the corresponding events in
		// testStatusEvents array.
		for(int i=0; i<firstThreeStatusEvents.size(); ++i) {
			Assert.assertEquals(firstThreeStatusEvents.get(i), testStatusEvents[i]);
		}
		
		// Retrieve status events with indices 3 to 6 from the SetListener.
		// This means getting the four status events following the one on index
		// 3.
		List<StatusEvent> statusEventsFrom3to7 = this.sut.getStatusEvents(3, 4);

		// Assert that the retrieved events equal the corresponding events in
		// testStatusEvents array.
		Assert.assertEquals(statusEventsFrom3to7.get(0), testStatusEvents[3]);
		Assert.assertEquals(statusEventsFrom3to7.get(1), testStatusEvents[4]);
		Assert.assertEquals(statusEventsFrom3to7.get(2), testStatusEvents[5]);
		Assert.assertEquals(statusEventsFrom3to7.get(3), testStatusEvents[6]);
	}
	
	@org.junit.Test
	public void testRetrievingSomeStatusEventsWithInvalidIndices() {
		// First, assert that the set listener does not contain status events
		Assert.assertEquals(0, this.sut.getNumberOfStatusEvents());
		
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}
		
		// Assert that calling trying to get status events with negative 
		// arguments causes an IllegalArgumentException.
		boolean illegalArgumentExceptionWasThrown = false;
		try {
			this.sut.getStatusEvents(-1, -1);
		}
		catch (IllegalArgumentException iae) {
			illegalArgumentExceptionWasThrown = true;
		}
		Assert.assertTrue(illegalArgumentExceptionWasThrown);	
	}
	
	@org.junit.Test
	public void testRetrievingSomeStatusEventsWithNonexistingIndices() {
		// First, assert that the set listener does not contain status events
		Assert.assertEquals(0, this.sut.getNumberOfStatusEvents());
		
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}
		
		List<StatusEvent> statusEvents = this.sut.getStatusEvents(0, 300);
		
		// Assert that the returned list of status events has only has the same
		// size of as the testStatusEvents array, from which the status events
		// were added to the set listener. 
		Assert.assertEquals(testStatusEvents.length, statusEvents.size());
	}
	
	@org.junit.Test
	public void testRetrievingSomeStatusEventsWithNonexistingIndicesAsTO() {
		// First, assert that the set listener does not contain status events
		Assert.assertEquals(0, this.sut.getNumberOfStatusEvents());
		
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}
		
		SetStatusTO setStatusTO = this.sut.getStatusAsTO(2, 250);
		 
		int expected = testStatusEvents.length - 2; // testStatusEvents[0] and
													// testStatusEvents[1] should
													// be missing
		int observed = setStatusTO.getStatusEvents().size();
		Assert.assertEquals(expected, observed);
	}
	
	@org.junit.Test
	public void testRetrievingSetStatusTOObject() {
		// Add status events for the set listener
		for (StatusEvent statusEvent : testStatusEvents) {
			this.sut.addStatusEvent(statusEvent);
		}
		
		SetStatusTO setStatusTO = this.sut.getStatusAsTO();
		List<StatusEvent> statusEvents = setStatusTO.getStatusEvents();
		
		for(int i=0; i<statusEvents.size(); ++i) {
			Assert.assertEquals(statusEvents.get(i), testStatusEvents[i]);
		}
	}

}
