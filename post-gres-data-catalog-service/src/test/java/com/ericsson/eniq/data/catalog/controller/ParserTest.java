//package com.ericsson.eniq.data.catalog.controller;
//
//import com.ericsson.eniq.data.catalog.service.DatabaseService;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.mock.mockito.MockBean;
//import org.springframework.test.context.ActiveProfiles;
//import org.springframework.test.web.servlet.MockMvc;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.when;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
//
//
//@SpringBootTest
//@AutoConfigureMockMvc
//@ActiveProfiles("test")
//class ParserTest {
//
//
//
//    @MockBean
//    DatabaseService databaseService;
//
//    @Autowired
//    MockMvc mockMvc;
//
//    @BeforeEach
//    void setup(){
//
//        List<Map<String,Object>> resp = new ArrayList<>();
//        resp.add(new HashMap<>());
//        when(databaseService.loadDataFormats()).thenReturn(resp);
//        when(databaseService.loadDataItems()).thenReturn(resp);
//        when(databaseService.getTransformations(anyString())).thenReturn(resp);
//
//    }
//
//    @Test
//    void loadDataFormats() throws Exception {
//        mockMvc.perform(get("/data-formats"))
//                .andExpect(status().isOk());
//    }
//
//    @Test
//    void loadDataItems() throws Exception {
//        mockMvc.perform(get("/data-items"))
//                .andExpect(status().isOk());
//    }
//
//    @Test
//    void verify_400_on_get_transformations() throws Exception {
//
//        mockMvc.perform(get("/transformations"))
//                .andExpect(status().isBadRequest());
//
//    }
//
//    @Test
//    void verify_200_on_get_transformations_with_techpacks() throws Exception {
//
//        String techpack = "DC_E_GGSN";
//        mockMvc.perform(get("/transformations?techpacks="+techpack))
//                .andExpect(status().isOk());
//
//    }
//
//    @Test
//    void verify_400_on_getTransferActions() throws Exception {
//
//        mockMvc.perform(get("/transfer-actions"))
//                .andExpect(status().isBadRequest());
//
//    }
//
//    @Test
//    void verify_200_on_getTransferActions_with_techpacks() throws Exception {
//
//        String techpack = "DC_E_WMG";
//        mockMvc.perform(get("/transfer-actions?collectionSetName="+techpack))
//                .andExpect(status().isOk());
//
//    }
//
//
//}