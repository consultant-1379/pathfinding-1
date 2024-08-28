/*******************************************************************************
 * COPYRIGHT Ericsson 2020
 *
 *
 *
 * The copyright to the computer program(s) herein is the property of
 *
 * Ericsson Inc. The programs may be used and/or copied only with written
 *
 * permission from Ericsson Inc. or in accordance with the terms and
 *
 * conditions stipulated in the agreement/contract under which the
 *
 * program(s) have been supplied.
 ******************************************************************************/

package com.ericsson.oss.eniq.techpack.api.contract;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;

import com.ericsson.oss.eniq.techpack.controller.example.SampleApiControllerImpl;

import io.restassured.module.mockmvc.RestAssuredMockMvc;

@RunWith(MockitoJUnitRunner.class)
public class SampleApiBase {
    @InjectMocks
    private SampleApiControllerImpl sampleApiController;

    @Before
    public void setup() {
        final StandaloneMockMvcBuilder standaloneMockMvcBuilder = MockMvcBuilders.standaloneSetup(sampleApiController);
        RestAssuredMockMvc.standaloneSetup(standaloneMockMvcBuilder);
    }

    @Test
    public void test(){
        Assert.assertTrue(true);
    }
}
