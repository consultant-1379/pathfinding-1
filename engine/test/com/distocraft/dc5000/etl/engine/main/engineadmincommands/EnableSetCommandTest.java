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
public class EnableSetCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommandForTechPack() throws NumberFormatException, Exception {
        final String techPackName = "techPack";
        final String[] validNumberArguments = new String[] { "enableSet", techPackName };
        final Command command = new StubbedEnableSetCommand(validNumberArguments);
        expectEnableTechPackCommandOnEngineAdmin(techPackName);
        command.validateArguments();
        command.performCommand();
    }

    private void expectEnableTechPackCommandOnEngineAdmin(final String techpackName) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).enableTechpack(techpackName, false);
            }
        });

    }

    @Test
    public void testPerformCommandForSet() throws NumberFormatException, Exception {
        final String techPackName = "techPack";
        final String setName = "set name";
        final String[] validNumberArguments = new String[] { "enableSet", techPackName, setName };
        final Command command = new StubbedEnableSetCommand(validNumberArguments);
        expectEnableSetCommandOnEngineAdmin(techPackName, setName);
        command.validateArguments();
        command.performCommand();
    }

    @Test
    public void testPerformCommandForAction() throws NumberFormatException, Exception {
        final String techPackName = "techPack";
        final String setName = "some set";
        final int actionOrder = 4;
        final String[] validNumberArguments = new String[] { "enableSet", techPackName, setName,
                Integer.toString(actionOrder) };
        final Command command = new StubbedEnableSetCommand(validNumberArguments);
        expectEnableActionCommandOnEngineAdmin(techPackName, setName, actionOrder);
        command.validateArguments();
        command.performCommand();
    }

    private void expectEnableActionCommandOnEngineAdmin(final String techpackName, final String setName,
            final int actionOrder) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).enableAction(techpackName, setName, actionOrder, false);
            }
        });

    }

    private void expectEnableSetCommandOnEngineAdmin(final String techPackName, final String setName) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).enableSet(techPackName, setName, false);
            }
        });

    }

    @Test
    public void testCheckArgumentsWithTooManyArguments() {
        final String[] invalidNumberArguments = new String[] { "arg1", "arg2", "arg3", "1", "arg5", "arg6" };
        try {
            new EnableSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
          assertFalse("Exception should be thrown", e.getMessage() == null);
        }
    }

    @Test
    public void testCheckArgumentsWithTooFewArguments() {
        final String[] invalidNumberArguments = new String[] { "arg1" };
        try {
            new EnableSetCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
          assertFalse("Exception should be thrown", e.getMessage() == null);    
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments2DoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "enableSet", "techPackName" };
        new EnableSetCommand(validNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments3DoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "enableSet", "techPackName", "setName" };
        new EnableSetCommand(validNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments4DoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "enableSet", "techPackName", "setName", "4" };
        new EnableSetCommand(validNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments4ButTypeOfIntegerIsWrong() {

        final String[] validNumberArguments = new String[] { "enableSet", "techPackName", "setName",
                "this should be a number" };
        try {
            new EnableSetCommand(validNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
          assertFalse("Exception should be thrown", e.getMessage() == null);
        }
    }

    class StubbedEnableSetCommand extends EnableSetCommand {

        /**
         * @param args
         */
        public StubbedEnableSetCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.UnenableSetCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
