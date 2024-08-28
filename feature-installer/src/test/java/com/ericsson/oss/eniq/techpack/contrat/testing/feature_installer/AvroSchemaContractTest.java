package com.ericsson.oss.eniq.techpack.contrat.testing.feature_installer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
//import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.mockito.ArgumentMatchers.anyString;


import java.util.ArrayList;
import java.util.List;
//import java.util.Timer;

//import static org.springframework.test.web.servlet.result.MockMatchers.status;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJsonTesters;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.stubrunner.StubFinder;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;
import com.ericsson.oss.eniq.techpack.controller.AvroSchemaController;
import com.ericsson.oss.eniq.techpack.controller.example.SampleApiControllerImpl;
import com.ericsson.oss.eniq.techpack.service.AvroMetricsServiceImpl;
import com.ericsson.oss.eniq.techpack.service.AvroSchemaServiceImpl;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import io.micrometer.core.instrument.Timer;

//static imports: MockMvcRequestBuilders.*

//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;



import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.restassured.module.mockmvc.response.MockMvcResponse;

//@RunWith(SpringRunner.class)
@RunWith(MockitoJUnitRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@AutoConfigureJsonTesters
@AutoConfigureStubRunner(stubsMode = StubRunnerProperties.StubsMode.LOCAL, ids = "com.ericsson.oss.eniq.techpack:eric-oss-eniq-feature-installer:+:stubs:8080")
public class AvroSchemaContractTest {
	
	private MockClock clock = new MockClock();
	private CompositeMeterRegistry composite = new CompositeMeterRegistry();
	private SimpleMeterRegistry simple = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
	@InjectMocks
	AvroSchemaController avroSchemaController;

     @Autowired
     private MockMvc mockMvc;
     
    @Autowired
 	StubFinder stubFinder;
    
    @Mock
	private AvroMetricsServiceImpl avroMetricsService;
    
    @Mock
	private AvroSchemaServiceImpl avroSchemaService;

    String baseURL = null;
    @Before
 	public void setup() {
 		//then(stubFinder.findStubUrl("com.ericsson.oss.eniq.techpack", "eric-oss-eniq-feature-installer")).isNotNull();
 		//then(stubFinder.findAllRunningStubs().isPresent("eric-oss-eniq-feature-installer")).isTrue();
 		//System.out.println(stubFinder.findStubUrl("com.ericsson.oss.eniq.techpack", "eric-oss-eniq-feature-installer"));
 		//baseURL = stubFinder.findStubUrl("eric-oss-eniq-feature-installer").toString() + "/v1/sample";
 		mockMvc=MockMvcBuilders.standaloneSetup(avroSchemaController).build();
 	}
     
     @Test
     public void AvroSchemaControllerContractConsumerTest() throws Exception {
    	 	
    	 //JsonObject schemaJson = new JsonObject();
    	 //schemaJson = {"type": "record", "namespace": "com.ericsson.eniq.data", "name": "DC_E_MRS_BGF_DAY", "fields": [{"type": "string", "name": "OSS"}, {"type": "string", "name": "SM"}, {"type": "string", "name": "NODE_NAME"}]};
    	 
    	 JSONObject jsonObject = new JSONObject("{" + System.lineSeparator() +
					" \"type\": \"record\"," +System.lineSeparator() +
					" \"namespace\": \"com.ericsson.eniq.data\"," + System.lineSeparator() +
					" \"name\": \"DC_E_MRS_BGF_DAY\"," + System.lineSeparator() + 
					" \"fields\": [ " + System.lineSeparator() +
					 	"{" + System.lineSeparator() +
					 		" \"type\": \"string\"," + System.lineSeparator()+ 
					        " \"name\": \"OSS\"" + System.lineSeparator() +
					    "}," + System.lineSeparator() + 
					    "{" + System.lineSeparator() +
					        "  \"type\": \"string\"," +System.lineSeparator() +
					        "  \"name\": \"SM\"" + System.lineSeparator() +
					    "}," + System.lineSeparator() +
					    "{" + System.lineSeparator() +
					        "  \"type\": \"string\"," + System.lineSeparator() + 
					        "  \"name\": \"NODE_NAME\"" + System.lineSeparator() +
					    "}" + System.lineSeparator() +
					    "]" + System.lineSeparator() +
					"}");
    	 
    	 
    	 Timer timer = Timer.builder(TechPackConstant.AVRO_TIMER_NAME)
 				.tags(TechPackConstant.AVRO_TIMER_TAG_NAME, "counter").register(simple);

 		List<String> inputlist = new ArrayList<>();
 		inputlist.add("A");
 		inputlist.add("B");
 		Mockito.when(avroSchemaService.createAvroSchemas()).thenReturn(inputlist);
 		Mockito.when(avroMetricsService.getTimer(anyString())).thenReturn(timer);
    	 

    			 
    	 this.mockMvc.perform( post("/v1/create-schema"))
    	//assertThat( mvcResult.getResponse().getStatus()).isEqualTo(HttpStatus.OK);
           .andExpect(status().isOk());
           //.andExpect(content().json(jsonObject.toString())));
        	/*.contentType("application/json"))*/
           //.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
           //.content(jsonObject));
           //.andExpect(status().isOk());
           //.andExpect((ResultMatcher) content().string("Even"));
            
     }
}