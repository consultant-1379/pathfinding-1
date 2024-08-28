/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

/**
 * @author eemecoy
 *
 */
public class InvalidArgumentsException extends Exception {

    /**
     * @param details
     */
    public InvalidArgumentsException(final String details) {
        super(details);
    }

}
