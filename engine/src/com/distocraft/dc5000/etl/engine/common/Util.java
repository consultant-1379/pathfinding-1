package com.distocraft.dc5000.etl.engine.common;

import java.util.Calendar;

public class Util {

	public static final String DATE_FORMAT_SEPERATOR = ":";

	protected Util() {
		
	}
	
	/**
	 * Gets the millisecond value of the formatted date.
	 * 
	 * @param date
	 * @return
	 */
	public static long dateToMilli(final String date) {
		final int year = Integer.valueOf((date.substring(0, date.indexOf(DATE_FORMAT_SEPERATOR))));
		final int month = Integer
				.valueOf((date.substring(date.indexOf(DATE_FORMAT_SEPERATOR) + DATE_FORMAT_SEPERATOR.length(),
						date.lastIndexOf(DATE_FORMAT_SEPERATOR))));
		final int day = Integer.valueOf(date.substring(
				date.lastIndexOf(DATE_FORMAT_SEPERATOR) + DATE_FORMAT_SEPERATOR.length(), date.length()));

		final int hour = 0;
		final int minute = 0;
		final int second = 0;

		final Calendar longDate = Calendar.getInstance();
		longDate.set(Calendar.MILLISECOND, 0);
		longDate.set(year, (month - 1), day, hour, minute, second);

		return longDate.getTimeInMillis();
	}

}
