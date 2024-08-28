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
public class ReloadConfigCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "reloadConfigCommand" };
        final Command command = new StubbedReloadConfigCommand(validNumberArguments);
        expectReloadConfigCommandOnEngineAdmin();
        command.validateArguments();
        command.performCommand();
    }

    private void expectReloadConfigCommandOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).reloadPropertiesWText();
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new ReloadConfigCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e reloadConfigCommand"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "reloadConfigCommand" };
        new ReloadConfigCommand(validNumberArguments).validateArguments();
    }

    class StubbedReloadConfigCommand extends ReloadConfigCommand {

        /**
         * @param args
         */
        public StubbedReloadConfigCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadConfigCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
