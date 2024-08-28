package com.ericsson.eniq.data.catalog.controller;

import com.ericsson.eniq.data.catalog.error.TechPackVersionMissingException;
import com.ericsson.eniq.data.catalog.error.UnknownFieldException;
import com.ericsson.eniq.data.catalog.service.DatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("busy-hour")
public class BusyHour {

    @Autowired
    DatabaseService databaseService;

    static final String ACTIVE_VERSIONINGS  = "active-versionings";
    static final String ACTIVE_PM_VERSIONINGS = "active-pm-versionings";
    static final String BUSY_HOUR_SRC_TABLES= "busy-hour-src-tables";
    static final String BUSY_HOUR_REF_TABLES= "busy-hour-ref-tables";
    static final String BUSY_HOURS="busy-hours";
    static final String BUSY_HOUR_PLACEHOLDERS="busy-hour-placeholders";

    Logger logger = LoggerFactory.getLogger(BusyHour.class);

//    @GetMapping(ACTIVE_VERSIONINGS)
//    public ResponseEntity<List<Map<String,Object>>> getActiveVersionings(){
//
//        return ResponseEntity.ok(databaseService.getActiveVersionings());
//    }
//
//    @GetMapping(ACTIVE_PM_VERSIONINGS)
//    public ResponseEntity<List<Map<String,Object>>> getActivePMTypeVersionings(){
//
//        return ResponseEntity.ok(databaseService.getActivePMTypeVersionings());
//    }
//
//    @GetMapping(BUSY_HOUR_SRC_TABLES)
//    public ResponseEntity<List<Map<String,Object>>> getBusyHourSrcTable(
//            @RequestParam(name = "typeId") Optional<String> typeId){
//
//
//        if (typeId.isEmpty() ){
//            return ResponseEntity.badRequest().build();
//        }else {
//            return ResponseEntity.ok(
//                    databaseService.getBusyHourSrcTable(typeId.get()));
//        }
//    }
//
//    @GetMapping(BUSY_HOUR_REF_TABLES)
//    public ResponseEntity<List<Map<String,Object>>> getBusyHourRefTable(
//            @RequestParam(name = "versionId") Optional<String> versionId){
//
//
//        if (versionId.isEmpty() ){
//            return ResponseEntity.badRequest().build();
//        }else {
//            return ResponseEntity.ok(
//                    databaseService.getBusyHourRefTable(versionId.get()));
//        }
//    }
//
//    @GetMapping(BUSY_HOURS)
//    public ResponseEntity<List<Map<String,Object>>> getBusyHours(
//            @RequestParam(name = "versionId") Optional<String> versionId){
//
//
//        if (versionId.isEmpty() ){
//            return ResponseEntity.badRequest().build();
//        }else {
//            return ResponseEntity.ok(
//                    databaseService.getBusyHours(versionId.get()));
//        }
//    }
//
//    @GetMapping(BUSY_HOUR_PLACEHOLDERS)
//    public ResponseEntity<List<Map<String,Object>>> getBusyHourPlaceHolders(
//            @RequestParam(name = "versionId") Optional<String> versionId){
//
//
//        if (versionId.isEmpty() ){
//            return ResponseEntity.badRequest().build();
//        }else {
//            return ResponseEntity.ok(
//                    databaseService.getBusyHourPlaceHolders(versionId.get()));
//        }
//    }


    @GetMapping(value="fields")
    public ResponseEntity<List<String>> getFields(){
        List<String> resp = Arrays.asList(BUSY_HOUR_PLACEHOLDERS, BUSY_HOUR_REF_TABLES, BUSY_HOUR_SRC_TABLES,
                BUSY_HOURS, ACTIVE_PM_VERSIONINGS, ACTIVE_VERSIONINGS);
        return ResponseEntity.ok().body(resp);
    }

    @GetMapping
    public ResponseEntity<List<Map<String,Object>>> getAll(
            @RequestParam(name="techPackVersion") Optional<String> versionIdOptional,
            @RequestParam(name="fields") String fieldsStr){

        if (fieldsStr.isEmpty() ){
            return ResponseEntity.badRequest().build();
        }

        String[] versionRequiredList = {BUSY_HOUR_PLACEHOLDERS,BUSY_HOURS,
                BUSY_HOUR_REF_TABLES,BUSY_HOUR_SRC_TABLES};

        boolean isVersionRequired = Arrays.stream(versionRequiredList).anyMatch(fieldsStr::contains);
        if (isVersionRequired && versionIdOptional.isEmpty())
            throw new TechPackVersionMissingException();


        List<Map<String,Object>> resp = new ArrayList<>();

        String[] fields = fieldsStr.split(",");

        String versionId = versionIdOptional.orElse("");

        for (String field : fields){
            switch (field){
                case BUSY_HOURS:
                    resp.addAll(databaseService.getBusyHours(versionId));
                    continue;
                case BUSY_HOUR_PLACEHOLDERS:
                    resp.addAll(databaseService.getBusyHourPlaceHolders(versionId));
                    continue;
                case BUSY_HOUR_REF_TABLES:
                    resp.addAll(databaseService.getBusyHourRefTable(versionId));
                    continue;
                case BUSY_HOUR_SRC_TABLES:
                    resp.addAll(databaseService.getBusyHourSrcTable(versionId));
                    continue;
                case ACTIVE_VERSIONINGS:
                    resp.addAll(databaseService.getActiveVersionings());
                    continue;
                case ACTIVE_PM_VERSIONINGS:
                    resp.addAll(databaseService.getActivePMTypeVersionings());
                    continue;
                default:
                    throw  new UnknownFieldException(field);
            }
        }
        return ResponseEntity.ok().body(resp);

    }


}
