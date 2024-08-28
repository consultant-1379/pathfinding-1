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
public class ChangeProfileAndWaitCommandTest extends BaseMock {
    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String profileName = "a profile";
        final String[] validNumberArguments = new String[] { "-e", profileName };
        final Command command = new StubbedChangeProfileAndWaitCommand(validNumberArguments);
        expectChangeProfileAndWaitWtextOnEngineAdmin(profileName);
        command.validateArguments();
        command.performCommand();
    }

    private void expectChangeProfileAndWaitWtextOnEngineAdmin(final String profileName) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).changeProfileAndWaitWtext(profileName);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new ChangeProfileAndWaitCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeProfileAndWait profileName(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new ChangeProfileAndWaitCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeProfileAndWait profileName(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg" };
        new ChangeProfileAndWaitCommand(validNumberArguments).validateArguments();
    }

    class StubbedChangeProfileAndWaitCommand extends ChangeProfileAndWaitCommand {

        /**
         * @param args
         */
        public StubbedChangeProfileAndWaitCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetsCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }
}
