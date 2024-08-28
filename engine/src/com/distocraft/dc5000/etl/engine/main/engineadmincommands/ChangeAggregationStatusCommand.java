/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class ChangeAggregationStatusCommand extends Command {

    private static final String DATE_FORMATTER = "yyyy-MM-dd";

    private String newStatus;

    private String aggregation;

    private long dataDate;

    /**
     * @param args
     */
    public ChangeAggregationStatusCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        newStatus = arguments[1];
        aggregation = arguments[2];
        final SimpleDateFormat simpleDataFormat = new SimpleDateFormat(DATE_FORMATTER);
        try {
            dataDate = simpleDataFormat.parse(arguments[3]).getTime();
        } catch (final ParseException e) {
            throw new InvalidArgumentsException("Date should be in format " + DATE_FORMATTER + ". Usage: engine -e "
                    + getUsageMessage());
        }

    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 4;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "changeAggregationStatus newStatus(string) aggregation(string) dataDate(date in format "
                + DATE_FORMATTER + ")";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        admin.changeAggregationStatus(newStatus, aggregation, dataDate);
    }

}
