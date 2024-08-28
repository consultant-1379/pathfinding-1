package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.globalexception.TechPackExceptions;
import com.ericsson.oss.eniq.techpack.service.interfaces.FeatureDecryptUnzipService;
import com.ericsson.oss.eniq.techpack.utils.ZipCrypter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class FeatureDecryptUnzipServiceImpl implements FeatureDecryptUnzipService {
    @Value("${featurefile.filelocation.url}")
    private String decryptFeatureURL;

    @Autowired
    private ZipCrypter zipCrypter;

    /**
     * The Constant logger.
     */
    private static final Logger log = LoggerFactory.getLogger(FeatureDecryptUnzipServiceImpl.class);

    @Override
    public String getDecryptFeatureFile() {

        try {
            log.info("** Decryption Start **");
            zipCrypter.execute(new File(decryptFeatureURL));
            log.info("** Decryption End **");
        } catch (Exception e) {
            throw new TechPackExceptions("The Feature zip file does not exist.");
        }
        return "Successfully decrypted the Feature file";
    }

}
