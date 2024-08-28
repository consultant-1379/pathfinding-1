/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.exceptions;

/**
 * @author eemecoy
 *
 */
public class NoSuchCommandException extends Exception {

    /**
     * @param details
     */
    public NoSuchCommandException(final String details) {
        super(details);
    }

}
