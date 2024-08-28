package com.ericsson.eniq.data.catalog.controller;

import com.ericsson.eniq.data.catalog.error.TechPackNamesMissingException;
import com.ericsson.eniq.data.catalog.error.UnknownFieldException;
import com.ericsson.eniq.data.catalog.service.DatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("parser")
public class Parser {


    @Autowired
	DatabaseService databaseService;

    Logger logger = LoggerFactory.getLogger(Parser.class);

    static final String DATA_FORMATS = "data-formats";
    static final String DATA_ITEMS = "data-items";
    static final String TRANSFORMATIONS = "transformations";


//    @GetMapping(DATA_FORMATS)
//    public ResponseEntity<List<Map<String,Object>>> loadDataFormats(){
//
//		return ResponseEntity.ok(databaseService.loadDataFormats());
//    }
//
//    @GetMapping(DATA_ITEMS)
//    public ResponseEntity<List<Map<String,Object>>>loadDataItems(){
//
//    	return ResponseEntity.ok(databaseService.loadDataItems());
//    }
//
//    @GetMapping(TRANSFORMATIONS)
//    public ResponseEntity<List<Map<String,Object>>> getTransformations(
//            @RequestParam(name = "techpacks") Optional<String> techpacknames){
//
//
//        if (techpacknames.isEmpty()){
//            return ResponseEntity.badRequest().build();
//        }else {
//            return ResponseEntity.ok(databaseService.getTransformations(techpacknames.get()));
//        }
//	}

	@GetMapping(value="fields")
    public ResponseEntity<List<String>> getFields(){
        List<String> resp = Arrays.asList(DATA_FORMATS,DATA_ITEMS,TRANSFORMATIONS);
        return ResponseEntity.ok().body(resp);
    }



    @GetMapping
    public ResponseEntity<List<Map<String,Object>>> getAll(
            @RequestParam(name="techPackNames") String techPackNames,
            @RequestParam(name="fields") String fieldsStr){

        if (fieldsStr.isEmpty() )
            return ResponseEntity.badRequest().build();

        if (techPackNames.isEmpty())
            throw new TechPackNamesMissingException();

        List<Map<String,Object>> resp = new ArrayList<>();


        String[] fields = fieldsStr.split(",");

        for (String field : fields){

            if (field.equals(DATA_FORMATS)){
                resp.addAll(databaseService.loadDataFormats(techPackNames));
            }else if (field.equals(DATA_ITEMS)){
                resp.addAll(databaseService.loadDataItems(techPackNames));
            }else if (field.equals(TRANSFORMATIONS)){
                resp.addAll(databaseService.getTransformations(techPackNames));
            }else{
                throw  new UnknownFieldException(field);
            }

        }
        return ResponseEntity.ok().body(resp);

    }


}