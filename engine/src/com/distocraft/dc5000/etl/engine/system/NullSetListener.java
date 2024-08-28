package com.distocraft.dc5000.etl.engine.system;

import java.util.logging.Logger;

public class NullSetListener extends SetListener {

  private static final Logger log = Logger.getLogger(NullSetListener.class.getName());
  private static final NullSetListener instance = new NullSetListener();

	public static NullSetListener instance() {
		return instance;
	}
	
	private NullSetListener() {
		
	}
	
	@Override
	public String listen() {
		return SetListener.NOSET;
	}
	
	@Override
	public String getStatus() {
		return SetListener.NOSET;
	}
	
	@Override
	public void dropped() {
		// Do nothing
	}

	@Override
	public void failed() {
		// Do nothing
	}
	
	@Override
	public void succeeded() {
		// Do nothing
	}
	
	@Override
	public boolean addStatusEvent(final StatusEvent se) {
		log.finest("NullSetListener.addStatusEvent(statusEvent) called with statusEvent="+se.toString());
	  return true;
	}
	
}