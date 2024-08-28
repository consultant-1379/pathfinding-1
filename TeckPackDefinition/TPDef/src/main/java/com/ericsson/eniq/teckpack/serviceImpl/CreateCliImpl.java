package com.ericsson.eniq.teckpack.serviceImpl;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.teckpack.createTP.PushTP;
import com.ericsson.eniq.teckpack.helper.serviceImpl.ModelCliImpl;
import com.ericsson.eniq.teckpack.tpm.TPImpl;

@Service
public class CreateCliImpl {
	private static final Logger logger = LoggerFactory.getLogger(CreateCliImpl.class);
	@Autowired
	private ModelCliImpl modelCliImpl;
	@Autowired
	private PushTP pushTP;

	public String convertByApproach1() {

		// '''Convert the model from one format to another '''
		logger.info("** LOADING THE MODEL **");
		File inputDir = new File(".");
		String tpInputDir = Paths.get(inputDir.getAbsolutePath(), "../TpInput/").normalize().toString();
		Map<String, Object> xlxsDict = modelCliImpl.load(tpInputDir);
		TPImpl tpImpl = new TPImpl();
		tpImpl.getPropertiesFromXls(xlxsDict);
		logger.info("** LOADED THE MODEL **");
		return pushTP.pushData(tpImpl);

	}

	public String convertByApproach2() {
		// '''Convert the model from one format to another '''
		logger.info("** LOADING THE MODEL **");
		File inputDir = new File(".");
		String tpInputDir = Paths.get(inputDir.getAbsolutePath(), "../TpInput/").normalize().toString();
		Map<String, Object> xlxsDict = modelCliImpl.load(tpInputDir);
		TPImpl tpImpl = new TPImpl();
		tpImpl.getPropertiesFromXls(xlxsDict);
		logger.info("** LOADED THE MODEL **");
		return pushTP.pushDataByApproach2(tpImpl);
	}

	public String tpmToJson() {
		// '''Convert the model from one format to another '''
		logger.info("** LOADING THE MODEL **");
		File inputDir = new File(".");
		String tpInputDir = Paths.get(inputDir.getAbsolutePath(), "../TpInput/").normalize().toString();
		Map<String, Object> xlxsDict = modelCliImpl.load(tpInputDir);
		TPImpl tpImpl = new TPImpl();
		tpImpl.getPropertiesFromXls(xlxsDict);
		logger.info("** LOADED THE MODEL **");
		return tpImpl.toJSON();
	}

	public String generateJson() {
		// '''Convert the model from one format to another '''
		logger.info("** LOADING THE MODEL **");
		File inputDir = new File(".");
		String tpInputDir = Paths.get(inputDir.getAbsolutePath(), "../TpInput/").normalize().toString();
		Map<String, Object> xlxsDict = modelCliImpl.load(tpInputDir);
		TPImpl tpImpl = new TPImpl();
		tpImpl.getPropertiesFromXls(xlxsDict);
		logger.info("** LOADED THE MODEL **");
		return pushTP.generateJson(tpImpl);
	}

}
