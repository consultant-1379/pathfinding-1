package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.model.Versioning;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@RunWith(MockitoJUnitRunner.class)
public class VersioningServiceImplTest {
    @Mock
    Logger log;
    @Mock
    ObjectMapper mapper;
    @Mock
    RestTemplate restTemplate;
    @InjectMocks
    VersioningServiceImpl versioningServiceImpl;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetTargetVersioning() {
        Versioning versioning = new Versioning();
        Mockito.when(restTemplate.getForEntity(Mockito.anyString(),
                Mockito.eq(Versioning.class), Mockito.anyString())).thenReturn(new ResponseEntity<>(versioning, HttpStatus.OK));

        Versioning result = null;
        result = versioningServiceImpl.getTargetVersioning("tpName");
        Assert.assertEquals(new Versioning(), result);
    }
}
