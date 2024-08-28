/**
 * 
 */
package com.ericsson.eniq.enminterworking;

import java.rmi.RMISecurityManager;
import java.security.Permission;

/**
 * @author xnagdas
 *
 */
/**
 * A simple security manager which just gives all permissions.
 */
public class ETLCSecurityManager extends RMISecurityManager {
	/**
	 * Check the permission.
	 * 
	 * @param perm
	 *          the permission
	 */
	public void checkPermission(final Permission perm) {
	}

}
