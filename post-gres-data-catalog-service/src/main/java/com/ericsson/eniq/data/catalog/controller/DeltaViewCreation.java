package com.ericsson.eniq.data.catalog.controller;

import com.ericsson.eniq.data.catalog.error.TechPackVersionMissingException;
import com.ericsson.eniq.data.catalog.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/delta-views")
public class DeltaViewCreation {

    @Autowired
    DatabaseService databaseService;

    @GetMapping
    public ResponseEntity<List<Map<String,Object>>> getDeltaViews(
            @RequestParam(name = "techPackVersion") String versionId){

        if (versionId == null || versionId.isEmpty())
            throw  new TechPackVersionMissingException();

        return ResponseEntity.ok().body(databaseService.getDeltaViews(versionId));
    }
}
