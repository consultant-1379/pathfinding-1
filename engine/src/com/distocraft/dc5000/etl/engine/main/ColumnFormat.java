/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved.  *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for formatting column outputs of EngineAdmin
 * 
 * @author etuolem
 */
public final class ColumnFormat {

	private ColumnFormat() {
		// Bogus constructor to fool PMD
	}
	
	public static String format(final Map<String,String> header, final List<Map<String,String>> data) {
		final StringBuilder builder = new StringBuilder();
		
		final Map<String, Integer> lengths = new HashMap<String, Integer>();
		
		for(String key : header.keySet()) {
			int length = header.get(key).length();
			for(Map<String,String> row : data) {
        final String value = row.get(key);
        if (value != null) {
          final int rowLength = value.length();
          if (rowLength > length) {
            length = rowLength;
          }
        }
			}
			lengths.put(key, length);
		}
		
		builder.append(getSpacer(header, lengths));
		builder.append('\n');
		
		final StringBuilder headerStr = new StringBuilder();
		headerStr.append('|');
		for(String key : header.keySet()) {
      final String columnName = header.get(key);
			headerStr.append(columnName);
			headerStr.append(getChars((lengths.get(key) - columnName.length()), ' '));
			headerStr.append('|');
		}
		builder.append(headerStr);
		builder.append('\n');
		builder.append(getSpacer(header, lengths));
		builder.append('\n');
		
		for(Map<String,String> row : data) {
			final StringBuilder dataRow = new StringBuilder(); //NOPMD
			dataRow.append('|');
			for(String key : header.keySet()) {
        final String cellValue = row.get(key);
        int valueLength = 0;
        if (cellValue != null) {
          dataRow.append(cellValue);
          valueLength = cellValue.length();
        }
        dataRow.append(getChars((lengths.get(key) - valueLength), ' '));
        dataRow.append('|');
			}
			builder.append(dataRow);
			builder.append('\n');
		}

		builder.append(getSpacer(header, lengths));
		
		return builder.toString();
	}
	
	/**
	 * Returns spacer row according to header and length definitions
	 */
	private static StringBuilder getSpacer(final Map<String,String> header, final Map<String,Integer> lengths) {
		final StringBuilder spacer = new StringBuilder();
		spacer.append('+');
		for(String key : header.keySet()) {
			spacer.append(getChars(lengths.get(key), '-'));
			spacer.append('+');
		}
		return spacer;
	}

	/**
	 * Returns StringBuilder containing defined amount of defined characters
	 */
	private static StringBuilder getChars(final int amount, final char character) {
		final StringBuilder builder = new StringBuilder();
		for(int i = 0 ; i < amount ; i++) {
			builder.append(character);
		}
		return builder;
	}
	
}
