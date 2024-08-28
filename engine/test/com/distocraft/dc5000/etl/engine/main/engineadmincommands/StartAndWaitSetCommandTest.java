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
public class StartAndWaitSetCommandTest extends BaseMock {

  private static final String LINE_SEPERATOR = System
      .getProperty("line.separator");

    EngineAdmin mockedEngineAdmin;

    protected String coll = " a collection";

    protected String set = " a set";

    private final String[] validNumberArgumentsNoScheduleSpecified = new String[] { "startSet", coll, set };

    private final String scheduleInfo = "schedule=what";

    private final String[] validNumberArgumentsScheduleSpecified = new String[] { "startSet", coll, set, scheduleInfo };

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }
    @Test
    public void testPerformCommandWhenSpecifyingSchedule() throws NumberFormatException, Exception {
        final Command command = new StubbedStartAndWaitSetCommand(validNumberArgumentsScheduleSpecified);
    final String expectedSchedule = "#" + LINE_SEPERATOR + "#" + new Date().toString()
 + LINE_SEPERATOR + scheduleInfo
	+ LINE_SEPERATOR;
        expectStartAndWaitSetOnEngineAdmin(coll, set, expectedSchedule);
        command.validateArguments();
        command.performCommand();
    }   

    @Test
    public void testPerformCommandWhenNotSpecifyingSchedule() throws NumberFormatException, Exception {
        final Command command = new StubbedStartAndWaitSetCommand(validNumberArgumentsNoScheduleSpecified);
        expectStartAndWaitSetOnEngineAdmin(coll, set, "");
        command.validateArguments();
        command.performCommand();
    }

    private void expectStartAndWaitSetOnEngineAdmin(final String expectedCollection, final String expectedSet,
            final String expectedSchedule) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).startAndWaitSet(expectedCollection, expectedSet, expectedSchedule);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new StartAndWaitSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSetAndWait collection(string) set(string) or engine -e startSetAndWait collection(string) set(string) schedule(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWith5Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "anarg", "anarg", "anarg" };
        try {
            new StartAndWaitSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSetAndWait collection(string) set(string) or engine -e startSetAndWait collection(string) set(string) schedule(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments3DoesntThrowException() throws InvalidArgumentsException {

        new StartAndWaitSetCommand(validNumberArgumentsNoScheduleSpecified).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments4DoesntThrowException() throws InvalidArgumentsException {

        new StartAndWaitSetCommand(validNumberArgumentsScheduleSpecified).validateArguments();
    }

    class StubbedStartAndWaitSetCommand extends StartAndWaitSetCommand {

        /**
         * @param args
         */
        public StubbedStartAndWaitSetCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartAndWaitSetCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
