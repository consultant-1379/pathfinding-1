/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Date;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class StartSetCommandTest extends BaseMock {

 private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    EngineAdmin mockedEngineAdmin;

    protected String coll = " a collection";

    protected String set = " a set";

    private final String[] validNumberArgumentsNoScheduleSpecified = new String[] { "startSet", coll, set };

    private final String scheduleInfo = "schedule=what";

    private final String scheduleInfoAggDateINyyyyMMddFormat = "aggDate=2011-10-20=schedule=what";
    
    private final String scheduleInfoAggDateINddMMyyyyFormat = "aggDate=20-10-2011=schedule=what";
    
    private final String scheduleInfoAggDateINImproperFormat = "aggDate=2011-10-=schedule=what";
    
    private final String[] validArgumentWithScheduleInfoAggDateInyyyyMMddFormat = new String[] { "startSet", coll, set, scheduleInfoAggDateINyyyyMMddFormat };
    
    private final String[] validArgumentWithScheduleInfoAggDateINddMMyyyyFormat = new String[] { "startSet", coll, set, scheduleInfoAggDateINddMMyyyyFormat };
    
    private final String[] validArgumentWithScheduleInfoAggDateImProperFormat = new String[] { "startSet", coll, set, scheduleInfoAggDateINImproperFormat };
    
    private final String[] validNumberArgumentsScheduleSpecified = new String[] { "startSet", coll, set, scheduleInfo };

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommandWhenSpecifyingSchedule() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetCommand(validNumberArgumentsScheduleSpecified);
    final String expectedSchedule = "#" + LINE_SEPARATOR + "#"
	+ new Date().toString() + LINE_SEPARATOR + scheduleInfo
	+ LINE_SEPARATOR;
        expectStartSetOnEngineAdmin(coll, set, expectedSchedule);
        command.validateArguments();
        command.performCommand();
    }

    @Test
    public void testValidateArgumentsWithScheduleAggDateInyyyyMMddFormat() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetCommand(validArgumentWithScheduleInfoAggDateInyyyyMMddFormat);
        try{
        command.validateArguments();
        }catch(InvalidArgumentsException e){
        	fail("Exception should not get raised as aggDate in yyyy-MM-dd format..");
        }
    }
    
    @Test
    public void testValidateArgumentsWithScheduleAggDateInImproperFormat() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetCommand(validArgumentWithScheduleInfoAggDateImProperFormat);
        try{
        command.validateArguments();
        fail("Exception should get raised as aggDate not in yyyy-MM-dd format..");
        }catch(InvalidArgumentsException e){
        	assertTrue("Exception must be raised as aggDate value passed is Improper format.", true);
        }
    }
    
    @Test
    public void testValidateArgumentsWithScheduleAggDateInddMMyyyyFormat() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetCommand(validArgumentWithScheduleInfoAggDateINddMMyyyyFormat);
        try{
        command.validateArguments();
        fail("Exception should get raised as aggDate in improper format..");
        }catch(InvalidArgumentsException e){
        	assertTrue("Exception must be raised as aggDate value passed is dd-MM-yyyy format.", true);
        }
    }
 

    @Test
    public void testPerformCommandWhenNotSpecifyingSchedule() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetCommand(validNumberArgumentsNoScheduleSpecified);
        expectStartSetOnEngineAdmin(coll, set, "");
        command.validateArguments();
        command.performCommand();
    }

    private void expectStartSetOnEngineAdmin(final String expectedCollection, final String expectedSet,
            final String expectedSchedule) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).startSet(expectedCollection, expectedSet, expectedSchedule);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new StartSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSet collection(string) set(string) or engine -e startSet collection(string) set(string) schedule(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWith5Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "anarg", "anarg", "anarg" };
        try {
            new StartSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSet collection(string) set(string) or engine -e startSet collection(string) set(string) schedule(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments3DoesntThrowException() throws InvalidArgumentsException {

        new StartSetCommand(validNumberArgumentsNoScheduleSpecified).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments4DoesntThrowException() throws InvalidArgumentsException {

        new StartSetCommand(validNumberArgumentsScheduleSpecified).validateArguments();
    }

    class StubbedStartSetCommand extends StartSetCommand {

        /**
         * @param args
         */
        public StubbedStartSetCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
