package com.ericsson.eniq.teckpack.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Component
public class GenerateJSON {

	public String createJson(LinkedHashMap<String, Object> data) {
		try {
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String output = gson.toJson(data);
			return output;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String createJsonNew(LinkedHashMap<String, Object> data) {
		String output = "";
		List<Map<String, Object>> measCounters = (List<Map<String, Object>>) data.get("MeasurementCounter");
		List<Map<String, Object>> measKeys = (List<Map<String, Object>>) data.get("Measurementkey");
		String json = "";
		int count = 0;
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		for (Map<String, Object> counter : measKeys)
			try {
				count++;
				json = gson.toJson(counter);
				output += json;
			} catch (Throwable e) {
				System.out.println(counter);
				e.printStackTrace();
				break;
			}

		return output;
	}
}
