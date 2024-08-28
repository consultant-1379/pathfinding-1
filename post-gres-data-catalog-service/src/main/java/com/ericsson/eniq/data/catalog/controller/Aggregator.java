package com.ericsson.eniq.data.catalog.controller;

import com.ericsson.eniq.data.catalog.error.TechPackNamesMissingException;
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
@RequestMapping("aggregations")
public class Aggregator {

    @Autowired
    DatabaseService databaseService;


    @GetMapping
    public ResponseEntity<List<Map<String,Object>>> getAggregations(
            @RequestParam(name = "techPackName") String techPackName){


        if (techPackName == null || techPackName.isEmpty())
            throw new TechPackNamesMissingException();

        return ResponseEntity.ok(databaseService.getAggregations(techPackName));
    }
}
