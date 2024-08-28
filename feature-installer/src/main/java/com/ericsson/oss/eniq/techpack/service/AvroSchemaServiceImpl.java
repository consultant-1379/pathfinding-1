package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.constants.TechPackAPIConstant;
import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;
import com.ericsson.oss.eniq.techpack.service.interfaces.AvroSchemaService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@Component
public class AvroSchemaServiceImpl implements AvroSchemaService {

    @Value("${schema.url}")
    private String avroSchemaBaseUrl;

    @Value("${schema.filelocation.url}")
    private String avroFileLocation;

    @Override
    @Autowired
    public List<String> createAvroSchemas() {
    	// creating test cases
        List<String> avrSchemas = null;
        avrSchemas = new ArrayList<>();
        Gson gson = new Gson();
        JsonObject fromJson = null;
        try (Reader read = new FileReader(avroFileLocation)) {
            fromJson = gson.fromJson(read, JsonObject.class);
            log.info("Json Data : " + fromJson);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        
        String avroTabelName = fromJson.get("name").getAsString();
        log.info("avroTabelName : " + avroTabelName);
        String subjectName = TechPackConstant.SUBJECT_NAME + "-" + avroTabelName;
        JsonObject schemaJson = new JsonObject();
        schemaJson.addProperty("schema", fromJson.toString());
        log.info("Register new schema for subject name,data : {} ", schemaJson);
        ResponseEntity<String> response = addSchema(subjectName, schemaJson.toString());
        log.info("Response from Register schema : " + response);

        avrSchemas.add(response.getBody());
        return avrSchemas;
    }

    private ResponseEntity<String> addSchema(String subjectName, String avrSchema) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        HttpEntity<String> request = new HttpEntity<>(avrSchema, headers);
        return restTemplate.postForEntity(avroSchemaBaseUrl + TechPackAPIConstant.ADD_SCHEMA, request,
                String.class, subjectName);
    }
}
