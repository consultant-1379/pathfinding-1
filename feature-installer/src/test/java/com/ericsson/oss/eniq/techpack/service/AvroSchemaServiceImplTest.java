package com.ericsson.oss.eniq.techpack.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@RunWith(MockitoJUnitRunner.class)
public class AvroSchemaServiceImplTest {

    @Mock
    RestTemplate restTemplate;

    @Test
    public void shouldRegisterSchema() {
        ResponseEntity<String> responseEntity = new ResponseEntity<>(HttpStatus.OK);
        Mockito.lenient().when(restTemplate.postForEntity(Mockito.anyString(),
                Mockito.any(), Mockito.eq(String.class), Mockito.anyString())).thenReturn(responseEntity);
        Assert.assertEquals(200, responseEntity.getStatusCodeValue());
    }

}
