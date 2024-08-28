package com.ericsson.oss.eniq.techpack.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.ericsson.oss.eniq.techpack.controller.ReadAvroModelController;
import com.ericsson.oss.eniq.techpack.model.AvroModel;
import com.ericsson.oss.eniq.techpack.model.FieldsModel;
import com.ericsson.oss.eniq.techpack.service.ReadAvroModelService;
import com.ericsson.oss.eniq.techpack.service.ReadAvroModelServiceImpl;
/**
 * 
 * The class ReadAvroModelControllerTest
 */
@RunWith(MockitoJUnitRunner.class)
public class ReadAvroModelControllerTest {
	@InjectMocks
	ReadAvroModelController readAvroModelController;

	@Mock
	ReadAvroModelService service;
	@Mock
	ReadAvroModelServiceImpl avroService;
	/** The mock mvc. */

	@Autowired
	private MockMvc mockMvc;

	@Before
	public void setUp() {
		mockMvc = MockMvcBuilders.standaloneSetup(readAvroModelController).build();
	}

	/**
	 * Test for getting mock avro model.
	 *
	 * 
	 */
	@Test
	public void testReadAvroModel() throws Exception {
		AvroModel avroModel = new AvroModel();
		List<FieldsModel> fields = new ArrayList<>();
		FieldsModel model = new FieldsModel();
		avroModel.setType("record");
		avroModel.setNamespace("com.ericsson.eniq.data");
		avroModel.setName("DC_E_ERBS_ADMISSIONCONTROL_V_DAY");
		model.setName("ERBS");
		model.setType("String");
		avroModel.setFields(fields);
		avroModel.add(fields);
		Mockito.when(avroService.readAvroModel()).thenReturn(avroModel);
		mockMvc.perform(get("/readAvroModel")).andExpect(status().isOk());
		MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/readAvroModel")).andExpect(status().isOk())
				.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
		MockHttpServletResponse httpServletResponse = mvcResult.getResponse();
		Assert.assertNotNull(httpServletResponse);
		Assert.assertEquals(200, httpServletResponse.getStatus());

	}

}
