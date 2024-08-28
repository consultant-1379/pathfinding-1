/**
 * 
 */
package com.ericsson.eniq.enminterworking;

/**
 * @author xnagdas
 *
 */
public class NoSuchCommandException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7058630103689294545L;

	/**
     * @param details
     */
    public NoSuchCommandException(final String details) {
        super(details);
    }

}
