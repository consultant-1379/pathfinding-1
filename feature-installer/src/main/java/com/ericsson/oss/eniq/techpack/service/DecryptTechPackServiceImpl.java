package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.response.ResponseData;
import com.ericsson.oss.eniq.techpack.service.interfaces.DecryptTechPackService;
import com.ericsson.oss.eniq.techpack.utils.ZipCrypter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class DecryptTechPackServiceImpl implements DecryptTechPackService {
    private static final Logger logger = LoggerFactory.getLogger(DecryptTechPackServiceImpl.class);

    @Value("${techpack.decrypt.tpi.filelocation.url}")
    private String decryptTPIFileURL;

    @Autowired
    private ZipCrypter zipCrypter;

    @Override
    public ResponseData getDecryptFile() {
        var response = new ResponseData();
        try {
            File fileTarget = new File(decryptTPIFileURL);
            zipCrypter.executeDecryptFile(fileTarget);
            response.setCode(200);
            response.setMessage("Success");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
        return response;
    }


}
