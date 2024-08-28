package com.distocraft.dc5000.etl.engine.system;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;

public class TestScheduler implements ISchedulerRMI {
	
	private final List<String> triggered = new ArrayList<String>();
	private boolean toThrow = false;
	
  public void reload() {
  	
  }
  
  public void reloadLoggingProperties() {
  	
  }
  
  public void hold() {
  	
  }
  
  public void shutdown() {
  	
  }
  
  public List<String> status() {
  	return new ArrayList<String>();
  }
  
  public void trigger(final String name) throws RemoteException {
  	if(this.toThrow) {
  		this.toThrow = false;
  		throw new RemoteException();
  	}
  	
  	this.triggered.add(name);
  }
  
  public void trigger(final List<String> list) {
  	this.triggered.addAll(list);
  }
  
  public void trigger(final List<String> list, final Map<String, String> map) {
  	this.triggered.addAll(list);
  }
  
  public void trigger(final String name, final String command) {
  	this.triggered.add(name);
  }

  public void clear() {
  	this.triggered.clear();
  }
  
  public boolean isTriggered(final String name) {
  	return this.triggered.contains(name);
  }
  
  public Integer size() {
  	return Integer.valueOf(this.triggered.size());
  }
  
  public void triggerThrow(boolean toThrow) {
  	this.toThrow = toThrow;
  }
  
}
