package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.CoreApplication;
import com.ericsson.oss.eniq.techpack.service.interfaces.AvroMetricsService;
import com.ericsson.oss.eniq.techpack.service.interfaces.AvroSchemaService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/v1")
@Slf4j


public class AvroSchemaController {

    @Autowired
    private AvroSchemaService avroSchemaService;
    @Autowired
    private AvroMetricsService avroMetricsService;
    Logger log = LoggerFactory.getLogger(CoreApplication.class);

    @PostMapping("/create-schema")
    @ApiOperation("Avro schema registry")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Retrieve schema Id"),@ApiResponse(code = 404, message = "The resource is not found")})
    
    public List<String> createAvroSchemas() {
        log.trace("Inside createAvroSchema() method");
        List<String> schemas = avroSchemaService.createAvroSchemas();
        if (schemas != null) {
            String schemaId = schemas.get(0);
            avroMetricsService.increment(schemaId);
            avroMetricsService.getTimer(schemaId).record(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    log.info(e.getMessage());
                    Thread.currentThread().interrupt();
                }
            });
        }

        return schemas;
    }
    
    


}
