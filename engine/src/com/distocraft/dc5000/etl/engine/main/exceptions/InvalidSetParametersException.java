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
public class InvalidSetParametersException extends Exception {

    /**
     * @param string
     */
    public InvalidSetParametersException(final String details) {
        super(details);
    }

}
