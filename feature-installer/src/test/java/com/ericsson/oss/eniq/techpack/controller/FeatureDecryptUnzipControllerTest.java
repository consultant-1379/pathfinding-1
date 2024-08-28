package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.CoreApplication;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.ws.rs.core.MediaType;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CoreApplication.class, FeatureDecryptUnzipController.class})
public class FeatureDecryptUnzipControllerTest {

    @Autowired
    private WebApplicationContext webApplicationContext;
    private MockMvc mvc;

    @Before
    public void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void readFeatureFileTest() throws Exception {
        mvc.perform(MockMvcRequestBuilders.get("/feature-installer/api/v1/install").contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn();
    }
}
