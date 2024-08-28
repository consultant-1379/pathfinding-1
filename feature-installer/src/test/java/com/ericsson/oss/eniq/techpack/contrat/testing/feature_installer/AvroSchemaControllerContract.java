package com.ericsson.oss.eniq.techpack.contrat.testing.feature_installer;


import static com.toomuchcoding.jsonassert.JsonAssertion.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.stubrunner.StubFinder;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.web.client.RestTemplate;
import com.ericsson.oss.eniq.techpack.constants.TechPackAPIConstant;
import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;
import com.ericsson.oss.eniq.techpack.controller.AvroSchemaController;
import com.ericsson.oss.eniq.techpack.service.AvroMetricsServiceImpl;
import com.ericsson.oss.eniq.techpack.service.AvroSchemaServiceImpl;
import com.ericsson.oss.eniq.techpack.service.interfaces.AvroSchemaService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
//import com.ericsson.oss.eniq.techpack.parser.common.cache.parserconfig.ApplicationConfigPOJO;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.restassured.module.mockmvc.RestAssuredMockMvc;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AvroSchemaControllerContract.AutoConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration
@AutoConfigureStubRunner(ids = {
		"com.ericsson.oss.eniq.techpack:eric-oss-eniq-feature-installer:+:stubs:8080" }, stubsMode = StubRunnerProperties.StubsMode.LOCAL)



public class AvroSchemaControllerContract {
	private String endPoint = "/v1/create-schema";
	
	@Value("${schema.url}")
    private String avroSchemaBaseUrl;
	
    @Value("${schema.filelocation.url}")
    private String avroFileLocation;
	
	//@Autowired
	private AvroSchemaController avroSchemaService;
	
	String subjectName = TechPackConstant.SUBJECT_NAME + "-" + "DC_E_MRS_BGF_DAY";
	
	@Autowired
	StubFinder stubFinder;
	
	private String baseURL = null;
	
	@Before
	public void setup() {
		then(stubFinder.findStubUrl("com.ericsson.oss.eniq.techpack", "eric-oss-eniq-feature-installer")).isNotNull();
		then(stubFinder.findAllRunningStubs().isPresent("eric-oss-eniq-feature-installer")).isTrue();
		//System.out.println(stubFinder.findStubUrl("com.ericsson.oss.eniq.techpack", "eric-oss-eniq-feature-installer"));
		baseURL = stubFinder.findStubUrl("eric-oss-eniq-feature-installer").toString() + endPoint;
	}
	
	@Test
	public void testApplicationConfigurationService() {

		//List<String> avrSchemas = null;
        
        HttpHeaders headers = new HttpHeaders();
		headers.set("Content-Type", "application/json");
		//AvroSchemaServiceImpl avroSchemaService = new AvroSchemaServiceImpl();
		
		
		//System.out.println(avroSchemaService.createAvroSchemas());
		
		//avrSchemas = avroSchemaService.createAvroSchemas();
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<String> request = new HttpEntity<>(null, headers);
        

		//System.out.println(stubFinder.findStubUrl("com.ericsson.oss.eniq.techpack", "eric-oss-eniq-feature-installer"));
        System.out.println(baseURL);
		
		ResponseEntity<String> response = restTemplate.exchange(baseURL , HttpMethod.POST ,null,
                String.class);
		/*ResponseEntity<AvroSchemaController> response = new RestTemplate().exchange(
				baseURL + TECHPACKNAME + "/" + PARSERKNAME, GET, new HttpEntity<>(null, headers),
				AvroSchemaController.class);*/
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(response.getHeaders().get("Content-Type").contains("application/json.*")).isTrue();
		DocumentContext parsedJson = JsonPath.parse(response.getBody());
		assertThatJson(parsedJson).field("['collection_set_name']").matches("[\\p{L}]*");
		
		assertThatJson(parsedJson).field("['id']").matches("19");
		assertThatJson(parsedJson).field("['OSS']").matches("[\\p{L}]*");
		assertThatJson(parsedJson).field("['SM']").matches("[\\p{L}]*");
		assertThatJson(parsedJson).field("['NODE_NAME']").matches("[\\p{L}]*");
	}
	@SpringBootConfiguration
	static class AutoConfig {
	}
}