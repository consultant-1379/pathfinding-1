/*
 * Created on 6th of May 2009
 *
 */
package com.distocraft.dc5000.etl.engine.executionslots;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author etogust
 */
public class ExecutionMemoryConsumption {

	private final List<Object> list;
	private final Logger log;

	/**/
	private static ExecutionMemoryConsumption singletonExecutionMemoryConsumption = null;

	/**
	 * In the first call of this method new share is created and returned. After
	 * the first call the same share is returned.
	 * 
	 * @return share
	 */
	public synchronized static ExecutionMemoryConsumption instance() {

		if (singletonExecutionMemoryConsumption == null) {
			singletonExecutionMemoryConsumption = new ExecutionMemoryConsumption();
		}

		return singletonExecutionMemoryConsumption;

	}

	/**
	 * constructor
	 * 
	 */
	private ExecutionMemoryConsumption() {
		log = Logger.getLogger("etlengine.ExecutionMemoryConsumption");
		list = new ArrayList<Object>();
		log.info("Singleton ExecutionMemoryConsumption constructed");
	}

	/**
	 * 
	 * Add a object to the share.
	 * 
	 * @param obj
	 */
	public synchronized void add(final Object obj) {
		if (list != null && !list.contains(obj)) {
			list.add(obj);
			log.info("New object added to ExecutionMemoryConsumption");
			final int size = calculate();
			final int listSize = size();
			log.info("ExecutionMemoryConsumption is now " + size + " MB");
			log.info("ExecutionMemoryConsumption list size is now " + listSize);
		}
	}

	/**
	 * Remove object from the share.
	 * 
	 * @param key
	 * @return
	 */
	public synchronized Object remove(final Object obj) {

		if (list != null) {
			final Object o = list.remove(obj);
			if (o != null) {
				log.info("Object removed from ExecutionMemoryConsumption");
				final int size = calculate();
				final int listSize = size();
				log.info("ExecutionMemoryConsumption is now " + size + " MB");
				log.info("ExecutionMemoryConsumption list size is now "
						+ listSize);
			}
			return o;
		}

		return null;
	}

	/**
	 * 
	 * Returns the size of the map.
	 * 
	 * @param key
	 * @return
	 */
	public synchronized int size() {

		if (list != null) {
			return list.size();
		}

		return -1;

	}

	public synchronized int calculate() {
		int returnValue = 0;

		if (list != null) {
			for(Object obj : list) {
				final Class<?> clas = obj.getClass();
				
				try {
					final Method met = clas.getMethod("memoryConsumptionMB");
					final Integer ret = (Integer)met.invoke(obj);					
					returnValue += ret;
				} catch(Exception e) {
					// Not MemoryRestrictedParser
				}
				
			} // while
		}

		return returnValue;
	}

}
