package com.ericsson.oss.eniq.techpack.controller;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FeatureApplicationControllerTest {

    @InjectMocks
    private FeatureApplicationController featureApplicationController;

    @Test
    public void testfeatureApplicationRunningStatus() {
        assertEquals("Feature Application Deployed", featureApplicationController.featureManagerRunningStatus());
    }

    @Test
    public void testloggingApplicationCheckLogs() {
        assertEquals("SAMPLE_LOGS_LEVELS_LOGGED", featureApplicationController.loggingApplicationCheckLogs());
    }

}
