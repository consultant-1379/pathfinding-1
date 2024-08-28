package com.ericsson.eniq.Controller;

import java.io.File;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import freemarker.template.Template;

@Controller
public class ViewController {
	
	private static final Logger logger = LoggerFactory.getLogger(ViewController.class);
	
	@Autowired
	private FreeMarkerConfigurer freeMarkerConfigurer;
	
	
    @RequestMapping(value="/createview", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createView(Model model, @RequestParam(value="baseTableName", required=true) String baseTableName,
    		@RequestParam(value="partitions", required=true) String partitions,
    		@RequestParam(value="part", required=true) String part) {
        model.addAttribute("baseTableName", baseTableName);
        model.addAttribute("partitions", partitions);
        model.addAttribute("part", part);
        File page = new File(baseTableName + ".html");
        StringWriter pageWriter = new StringWriter();
        Template template = null;
        
        try {
        //	pageWriter = new OutputStreamWriter(new FileOutputStream(page), StandardCharsets.UTF_8);
        //	template = freeMarkerConfigurer.getConfiguration().getTemplate("welcome.ftl");
        	template = freeMarkerConfigurer.getConfiguration().getTemplate("createview.ftl");
        	//instraller.ftl
        	template.process(model, pageWriter);
        	logger.info("Writing of html file completed successfully!");
        	}catch(Exception e) {
        	logger.error("File writing using the Freemarker template engine failed!", e);
        	}
  
        return ResponseEntity.ok(pageWriter.toString());
    }
}
