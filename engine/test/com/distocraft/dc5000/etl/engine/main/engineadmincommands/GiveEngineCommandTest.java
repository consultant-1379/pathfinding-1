/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class GiveEngineCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    private final String commandToGive = "a command";

    private final String[] validNumberArguments = new String[] { "giveEngineCommand", commandToGive };

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommandWhenSpecifyingSchedule() throws NumberFormatException, Exception {
        final Command command = new StubbedGiveEngineCommand(validNumberArguments);
        expectGiveEngineCommandOnEngineAdmin(commandToGive);
        command.validateArguments();
        command.performCommand();
    }

    private void expectGiveEngineCommandOnEngineAdmin(final String expectedCommand) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).giveEngineCommand(expectedCommand);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new GiveEngineCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e giveEngineCommand command(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "anarg" };
        try {
            new GiveEngineCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e giveEngineCommand command(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        new GiveEngineCommand(validNumberArguments).validateArguments();
    }

    class StubbedGiveEngineCommand extends GiveEngineCommand {

        /**
         * @param args
         */
        public StubbedGiveEngineCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.GiveEngineCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
