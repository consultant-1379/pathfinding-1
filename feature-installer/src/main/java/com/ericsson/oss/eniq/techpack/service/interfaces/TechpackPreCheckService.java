package com.ericsson.oss.eniq.techpack.service.interfaces;

import java.io.IOException;
import java.util.Map;

public interface TechpackPreCheckService {
    Map<String,String> readVersionPropertyFile() throws IOException;

    void checkRequiredTechPackInstallations(Map<String, String> requiredDependencies) throws Exception;
}
