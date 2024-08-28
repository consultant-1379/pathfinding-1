/**
 * 
 */
package com.ericsson.eniq.enminterworking;

/**
 * @author xnagdas
 *
 */
public class InvalidArgumentsException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 8437234413318561188L;

	/**
     * @param details
     */
    public InvalidArgumentsException(final String details) {
        super(details);
    }

}
