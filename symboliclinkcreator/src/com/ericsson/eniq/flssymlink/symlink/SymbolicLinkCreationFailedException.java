package com.ericsson.eniq.flssymlink.symlink;

public class SymbolicLinkCreationFailedException extends Exception{
	
	public SymbolicLinkCreationFailedException(final String errorMessage) {
		super(errorMessage);
		System.out.println("exception ::: " + errorMessage);
	}
	
}
