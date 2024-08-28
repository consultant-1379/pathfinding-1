package com.ericsson.eniq.teckpack.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class Utils {

	public static String checkNull(String inputStr) {
		if (inputStr == null || inputStr == "" || inputStr == "null")
			return "";
		else
			return inputStr;
	}

	public static String strFloatToInt(String value) {
		String valueTemp = null;
		try {
			if (value != null && value.trim() != "") {
				value = value.trim();
				valueTemp = value;
				value = value.split("\\.")[0];
				if (value.contains("e+")) {
					value = value.split("e+")[0];
				}
				value = String.valueOf((int) Float.parseFloat(value));
				return value;
			} else {
				value = "";
			}
			return value;
		} catch (Exception e) {
			return valueTemp;
		}

	}

	// Common method to read external statements from text file

	public static Map<String, Object> loadEStxtFile(String fileName) {
		Map<String, Object> esCollection = null;
		try {
			File file = new File(fileName);
			if (file.exists()) {
				esCollection = new HashMap<>();
				String filecontent = Files.readString(Path.of(fileName));
				String content[] = filecontent.split("@@");
				for (String es : content) {
					if (es != "") {
						String info[] = es.split("==");
						esCollection.put(info[0], info[1]);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return esCollection;
	}

	public static String escape(String text) {
		// '''return text that is safe to use in an XML string'''
		return text != null ? text.replaceAll("&", "&amp;").replaceAll("\"", "&quot;").replaceAll(">", "&gt;")
				.replaceAll("<", "&lt;") : "";
	}

	public static HashMap<String, Object> convertAndPush(Map<String, Object> attDict, String[] cols) {
		HashMap<String, Object> row = new HashMap<>();
		for (String col : cols) {
			row.put(col, attDict.get(col));
		}
		return row;
	}

	public static boolean isValidJson(Map<String, Object> attDict) {
		Gson gson=new Gson();
		try {
			gson.toJson(attDict);
		} catch (Throwable e) {
			// e.getMessage();
			return false;

		}
		return true;
	}

}
