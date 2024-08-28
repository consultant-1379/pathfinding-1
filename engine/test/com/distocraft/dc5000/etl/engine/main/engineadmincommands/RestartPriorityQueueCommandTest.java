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
public class RestartPriorityQueueCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "holdPriorityQueue" };
        final Command command = new StubbedRestartPriorityQueueCommand(validNumberArguments);
        expectRestartPriorityQueueCommandOnEngineAdmin();
        command.validateArguments();
        command.performCommand();
    }

    private void expectRestartPriorityQueueCommandOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).restartPriorityQueue();
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new RestartPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e restartPriorityQueue"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "holdPriorityQueue" };
        new RestartPriorityQueueCommand(validNumberArguments).validateArguments();
    }

    class StubbedRestartPriorityQueueCommand extends RestartPriorityQueueCommand {

        /**
         * @param args
         */
        public StubbedRestartPriorityQueueCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestartPriorityQueueCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
