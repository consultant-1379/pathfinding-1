/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */

package com.distocraft.dc5000.etl.engine.main.exceptions;

import java.rmi.RemoteException;

/**
 * Exception thrown if set cannot be started by engine eg if 
 * set details are invalid, set doesn't exist or is disabled
 * 
 * @author eemecoy
 *
 */
public class InvalidSetParametersRemoteException extends RemoteException {

    public InvalidSetParametersRemoteException(final String exceptionDetail) {
        super(exceptionDetail);
    }

}
