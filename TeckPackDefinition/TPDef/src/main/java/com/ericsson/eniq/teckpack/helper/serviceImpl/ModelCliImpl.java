package com.ericsson.eniq.teckpack.helper.serviceImpl;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ericsson.eniq.teckpack.util.XlsDict;

@Component
public class ModelCliImpl {
	private static final Logger logger = LoggerFactory.getLogger(ModelCliImpl.class);
	@Autowired
	private XlsDict xlsDict;

	public Map<String, Object> load(String inputDir) {
		File modelTPath = new File(inputDir + "/modelT/");
		File filesList[] = modelTPath.listFiles();
		Map<String, Object> xlsdict = null;
		if (filesList != null && filesList.length > 0) {

			String inputFile = filesList[0].getPath();
			xlsdict = xlsDict.parse(new File(inputFile));
		}
		return xlsdict;
	}

}