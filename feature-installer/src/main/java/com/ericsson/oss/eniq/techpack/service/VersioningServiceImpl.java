package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.constants.TechPackAPIConstant;
import com.ericsson.oss.eniq.techpack.model.Versioning;
import com.ericsson.oss.eniq.techpack.service.interfaces.VersioningService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@Qualifier("versioningServiceImpl")
public class VersioningServiceImpl implements VersioningService {
    private Logger logger = LoggerFactory.getLogger(VersioningServiceImpl.class);
    //Dummy data for API
    /*private static final String ST = "{\n" +
            "        \"versionid\": \"DWH_MONITOR:((147))\",\n" +
            "        \"description\": \"Ericsson Utran Topology\",\n" +
            "        \"status\": 1,\n" +
            "        \"techpack_name\": \"DWH_MONITOR\",\n" +
            "        \"techpack_version\": \"R2D\",\n" +
            "        \"techpack_type\": \"Topology\",\n" +
            "        \"product_number\": \"COA 252 121\",\n" +
            "        \"lockedby\": \"TPC-R1A30\",\n" +
            "        \"lockdate\": \"2019-08-30 14:08:46\",\n" +
            "        \"basedefinition\": \"TP_BASE:BASE_TP_20160518\",\n" +
            "        \"eniq_level\": \"19.2.13.EU7\",\n" +
            "        \"licensename\": \"CXC4010586,CXC4010641,CXC4010649,CXC4010777,CXC4010585,CXC4010886\"\n" +
            "    }";

     */
    @Autowired
    private ObjectMapper mapper;

    @Autowired
    RestTemplate restTemplate;

    @Value("${cmsBaseUrl}")
    private String cmsBaseUrl;
    /**
     * This function is consuming the CMS "/v1/versioning/{techpackName}" API and in Response its will return Versioning data of the techpack
     * @param tpName  TechPackName
     * @return Versioning Object for particular Techpack
     */
    @Override
    public Versioning getTargetVersioning(String tpName) {
        logger.info("inside getTargetVersioning() method");
        ResponseEntity<Versioning> response = restTemplate.getForEntity(cmsBaseUrl + TechPackAPIConstant.TP_ACTIVATION,
                Versioning.class, tpName);
        //Dummy data to verify the api call
       /*Versioning v = mapper.readValue(ST, Versioning.class);
        if (v.getTechpack_name().equals(tpName)) return v;
        else return null;*/
        logger.info("Exit from getTargetVersioning() method");
        return response.getBody();
    }
}
