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
public class ShutdownSlowCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "status" };
        final Command command = new StubbedShutdownSlowCommand(validNumberArguments);
        expectSlowGracefulShutdownOnEngineAdmin();
        command.validateArguments();
        command.performCommand();
    }

    private void expectSlowGracefulShutdownOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).slowGracefulShutdown();
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new ShutdownSlowCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(), is("Incorrect number of arguments supplied, usage: engine -e shutdown_slow"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "status" };
        new ShutdownSlowCommand(validNumberArguments).validateArguments();
    }

    class StubbedShutdownSlowCommand extends ShutdownSlowCommand {

        /**
         * @param args
         */
        public StubbedShutdownSlowCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShutdownSlowCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
