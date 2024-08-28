/**
 * 
 */
package com.ericsson.eniq.enminterworking;

import java.rmi.RemoteException;

/**
 * @author xnagdas
 *
 */
public class InvalidSetParametersRemoteException extends RemoteException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2542329019339385624L;

	public InvalidSetParametersRemoteException(final String exceptionDetail) {
        super(exceptionDetail);
    }

}
