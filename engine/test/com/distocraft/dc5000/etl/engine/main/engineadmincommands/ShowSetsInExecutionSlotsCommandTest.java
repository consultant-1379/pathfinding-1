/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class ShowSetsInExecutionSlotsCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);        
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "showSetsInExecutionSlots" };
        final Command command = new StubbedShowSetsInExecutionSlotsCommand(validNumberArguments);
        expectShowSetsInExecutionSlotsCommandOnEngineAdmin();
        command.validateArguments();
        command.performCommand();
    }

    private void expectShowSetsInExecutionSlotsCommandOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).showSetsInExecutionSlots(with(any(List.class)));
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() throws InvalidArgumentsException {
        final String[] invalidNumberArguments = new String[] { "showSetsInExecutionSlots", "DC_E_MTAS", "DC_E_SGSN", "DC_E_CPP"};      
        new ShowSetsInExecutionSlotsCommand(invalidNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "showSetsInExecutionSlots" };
        new ShowSetsInExecutionSlotsCommand(validNumberArguments).validateArguments();
    }

    class StubbedShowSetsInExecutionSlotsCommand extends ShowSetsInExecutionSlotsCommand {

        /**
         * @param args
         */
        public StubbedShowSetsInExecutionSlotsCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowSetsInExecutionSlotsCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
