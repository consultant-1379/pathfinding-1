/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
public class ChangeAggregationStatusCommandTest extends BaseMock {

    private static final String DATE_FORMATTER = "yyyy-MM-dd";

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String newStatus = "LOADED";
        final String aggregation = "some aggregation";
        final String dataDate = "2009-12-01";
        final String[] validNumberArguments = new String[] { "changeAggregationStatus", newStatus, aggregation,
                dataDate };
        final Command command = new StubbedChangeAggregationStatusCommand(validNumberArguments);
        final long dateAsLong = convertDateToLong(dataDate);
        expectChangeAggregationStatusOnEngineAdmin(newStatus, aggregation, dateAsLong);
        command.validateArguments();
        command.performCommand();
    }

    private long convertDateToLong(final String dataDate) throws ParseException {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMATTER);
        final Date formattedDate = simpleDateFormat.parse(dataDate);
        return formattedDate.getTime();
    }

    private void expectChangeAggregationStatusOnEngineAdmin(final String status, final String aggregation,
            final long dataDate) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).changeAggregationStatus(status, aggregation, dataDate);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith5Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg", "fourarg", "fivearg" };
        try {
            new ChangeAggregationStatusCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeAggregationStatus newStatus(string) aggregation(string) dataDate(date in format "
                            + DATE_FORMATTER + ")"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new ChangeAggregationStatusCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeAggregationStatus newStatus(string) aggregation(string) dataDate(date in format "
                            + DATE_FORMATTER + ")"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButOfWrongType() {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg", "threearg", "this should be a long" };
        try {
            new ChangeAggregationStatusCommand(validNumberArguments).validateArguments();
            fail("Should have thrown an exception");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Date should be in format "
                            + DATE_FORMATTER
                            + ". Usage: engine -e changeAggregationStatus newStatus(string) aggregation(string) dataDate(date in format "
                            + DATE_FORMATTER + ")"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg", "threearg", "2008-12-01" };
        new ChangeAggregationStatusCommand(validNumberArguments).validateArguments();
    }

    class StubbedChangeAggregationStatusCommand extends ChangeAggregationStatusCommand {

        /**
         * @param args
         */
        public StubbedChangeAggregationStatusCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ActivateSetInPriorityQueue#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
