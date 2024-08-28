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
public class ChangeProfileCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommandWithJustProfileName() throws NumberFormatException, Exception {
        final String profileName = "a new profile name";
        final String[] validNumberArguments = new String[] { "changeProfile", profileName };
        final Command command = new StubbedChangeProfileCommand(validNumberArguments);
        expectChangeProfileCommandOnEngineAdmin(profileName);
        command.validateArguments();
        command.performCommand();
    }

    @Test
    public void testPerformCommandWithJustProfileNameAndOneElementOfMessageText() throws NumberFormatException,
            Exception {
        final String profileName = "a new profile name";
        final String aMessage = "some message";
        final String[] validNumberArguments = new String[] { "changeProfile", profileName, aMessage };
        final Command command = new StubbedChangeProfileCommand(validNumberArguments);
        final String messageToExpect = aMessage + " ";
        expectChangeProfileCommandWithMessageTextOnEngineAdmin(profileName, messageToExpect);
        command.validateArguments();
        command.performCommand();
    }

    private void expectChangeProfileCommandWithMessageTextOnEngineAdmin(final String expectedProfileName,
            final String expectedMessageText) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).changeProfileWtext(expectedProfileName, expectedMessageText);
            }
        });

    }

    @Test
    public void testPerformCommandWithJustProfileNameAndLotsofElementOfMessageText() throws NumberFormatException,
            Exception {
        final String profileName = "a new profile name";
        final String firstMessage = "firstpart of message";
        final String secondMessage = "second part of message";
        final String[] validNumberArguments = new String[] { "changeProfile", profileName, firstMessage, secondMessage };
        final Command command = new StubbedChangeProfileCommand(validNumberArguments);

        final String expectedMessage = firstMessage + " " + secondMessage + " ";
        expectChangeProfileCommandWithMessageTextOnEngineAdmin(profileName, expectedMessage);
        command.validateArguments();
        command.performCommand();
    }

    private void expectChangeProfileCommandOnEngineAdmin(final String expectedProfileName) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).changeProfileWtext(expectedProfileName);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new ChangeProfileCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeProfileCommand profileName(string) or engine -e changeProfileCommand profileName(string) messageText(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "changeProfile", "profileName" };
        new ChangeProfileCommand(validNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsAndMessageTextDoesntThrowException()
            throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "changeProfile", "profileName", "some message" };
        new ChangeProfileCommand(validNumberArguments).validateArguments();
    }

    class StubbedChangeProfileCommand extends ChangeProfileCommand {

        /**
         * @param args
         */
        public StubbedChangeProfileCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeProfileCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
