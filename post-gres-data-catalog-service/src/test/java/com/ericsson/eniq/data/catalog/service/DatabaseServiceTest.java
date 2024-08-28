//package com.ericsson.eniq.data.catalog.service;
//
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mock;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.test.context.ActiveProfiles;
//import org.springframework.test.util.ReflectionTestUtils;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.hasSize;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.when;
//
//@SpringBootTest
//@ActiveProfiles("test")
//class DatabaseServiceTest {
//
//    @Mock
//    JdbcTemplate dwhDBTemplate;
//
//    @Autowired
//    DatabaseService databaseService;
//
//
//    @BeforeEach
//    void init(){
//        ReflectionTestUtils.setField(databaseService,"dwhDBTemplate", dwhDBTemplate);
//        Map<String,Object> map = new HashMap<>();
//        List<Map<String,Object>> resp = new ArrayList<>();
//        resp.add(map);
//        when(dwhDBTemplate.queryForList(anyString())).thenReturn(resp);
//        when(dwhDBTemplate.queryForList(anyString(),anyString())).thenReturn(resp);
//    }
//
//    @Test
//    void loadDataFormats() {
//        List<Map<String,Object>> resp = databaseService.loadDataFormats();
//        assertNotNull(resp);
//        assertThat(resp, hasSize(1));
//    }
//
//    @Test
//    void loadDataItems() {
//        List<Map<String,Object>> resp = databaseService.loadDataItems();
//        assertNotNull(resp);
//        assertThat(resp, hasSize(1));
//    }
//
//    @Test
//    void  veryify_getTransformations() {
//        String techpacknames="DC_E_GGSN";
//        List<Map<String,Object>> resp = databaseService.getTransformations( techpacknames);
//        assertNotNull(resp);
//
//    }
//}