package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.service.interfaces.TechpackPreCheckService;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(MockitoJUnitRunner.class)
public class TechpackControllerTest {

    @InjectMocks
    TechpackController techpackController;

    @Mock
    ObjectMapper mapper;

    @Autowired
    MockMvc mockMvc;

    @Mock
    private TechpackPreCheckService techpackPreCheckService;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(techpackController).build();
    }

    @Test
    public void testTpDependenciesPrecheck() throws Exception {
        Map<String, String> requiredTech = new HashMap<>() {{
            put("DWH_MONITOR","R2D");put("DWH_BASE","R2E");

        }};

        Mockito.when(techpackPreCheckService.readVersionPropertyFile()).thenReturn(requiredTech);
        Mockito.doNothing().when(techpackPreCheckService).checkRequiredTechPackInstallations(requiredTech);
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/v1/techpack/tp-dependencies-precheck/true")).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
        MockHttpServletResponse httpServletResponse = mvcResult.getResponse();
        Assert.assertNotNull(httpServletResponse);
        Assert.assertEquals(200, httpServletResponse.getStatus());

    }

    @Test
    public void testTpDependenciesPrecheck_skip() throws Exception{

        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/v1/techpack/tp-dependencies-precheck/false")).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
        MockHttpServletResponse httpServletResponse = mvcResult.getResponse();
        Assert.assertNotNull(httpServletResponse);
        Assert.assertEquals(200, httpServletResponse.getStatus());
    }

}
