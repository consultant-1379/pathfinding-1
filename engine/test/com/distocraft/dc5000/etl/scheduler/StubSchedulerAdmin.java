package com.distocraft.dc5000.etl.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test class to imposterise SchedulerAdmin
 * 
 * @author etuolem
 */
public class StubSchedulerAdmin {

	private static List<String> triggered = new ArrayList<String>();
	private static Map<String,String> map = new HashMap<String,String>();
	
	private static boolean triggerThrows = false;

  public StubSchedulerAdmin() {

	}

	public void trigger(final String trigger) throws Exception {
		if (triggerThrows) {
			throw new Exception();
		} else {
			triggered.add(trigger);
		}
	}

	public void trigger(final List<String> list, final Map<String,String> map) {
		triggered = list;
		
		for(String l: list) {
			System.out.println(l);
		}
	}

	// ----- UTILITY -----
	
	public static boolean isTriggered(final String trigger) {
		return triggered.contains(trigger);
	}

	public static void triggerThrow(final boolean _triggerThrows) {
		triggerThrows = _triggerThrows;
	}

	public static void clear() {
		triggered.clear();
		triggerThrows = false;
	}
	
	public static Integer size() {
		return Integer.valueOf(triggered.size());
	}
	
	public static Map<String,String> getMap() {
		return map;
	}
	
}
