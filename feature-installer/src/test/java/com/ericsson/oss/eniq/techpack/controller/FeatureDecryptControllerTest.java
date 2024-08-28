package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.response.ResponseData;
import com.ericsson.oss.eniq.techpack.service.interfaces.DecryptTechPackService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(MockitoJUnitRunner.class)
public class FeatureDecryptControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Mock
    DecryptTechPackService decryptTechPackService;

    @InjectMocks
    private FeatureDecryptController featureDecryptController;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(featureDecryptController).build();
    }

    @Test
    public void decryptTechPackTest() throws Exception {
        var responseData = new ResponseData();
        responseData.setCode(200);
        responseData.setMessage("Success");
        Mockito.when(decryptTechPackService.getDecryptFile()).thenReturn(responseData);
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders
                .get("/feature-installer/api/v1/decrypt-techpack"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
        MockHttpServletResponse httpServletResponse = mvcResult.getResponse();
        Assert.assertNotNull(httpServletResponse);
        Assert.assertEquals(200, httpServletResponse.getStatus());
    }

}
