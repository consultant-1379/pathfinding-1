package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.globalexception.TechPackExceptions;
import org.junit.After;
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
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.*;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
@TestPropertySource(locations = "classpath:test.properties")
public class TechpackPreCheckServiceImplReadVersioningFileTest {
    @Mock
    Logger log;
    @Mock
    ResourceLoader resourceLoader;
    @Mock
    Resource mockResource;
    @InjectMocks
    TechpackPreCheckServiceImpl techpackPreCheckServiceImpl;
    private File file;

    @Before
    public void setUp() throws IOException {
        file = writeVersionFile();
        ReflectionTestUtils.setField(techpackPreCheckServiceImpl, "resourceLoader", resourceLoader);
        techpackPreCheckServiceImpl = Mockito.spy(new TechpackPreCheckServiceImpl());
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = TechPackExceptions.class)
    public void testTechPackNameNull() throws IOException {
        Mockito.when(resourceLoader.getResource(Mockito.anyString())).thenReturn(mockResource);
        modifyFile(file, "tech_pack.name");
        Mockito.when(mockResource.getFile()).thenReturn(file);
        techpackPreCheckServiceImpl.readVersionPropertyFile();
    }

    @Test(expected = TechPackExceptions.class)
    public void testTech_PackVersionIsNull() throws IOException {
        Mockito.when(resourceLoader.getResource(Mockito.anyString())).thenReturn(mockResource);
        modifyFile(file, "tech_pack.version");
        Mockito.when(mockResource.getFile()).thenReturn(file);
        techpackPreCheckServiceImpl.readVersionPropertyFile();
    }

    @Test(expected = TechPackExceptions.class)
    public void testTech_PackBuildNumberIsNull() throws IOException {
        Mockito.when(resourceLoader.getResource(Mockito.anyString())).thenReturn(mockResource);
        modifyFile(file, "build.number");
        Mockito.when(mockResource.getFile()).thenReturn(file);
        techpackPreCheckServiceImpl.readVersionPropertyFile();

    }

    @Test
    public void testReadVersionPropertyFile() throws IOException {

        Mockito.when(resourceLoader.getResource(Mockito.anyString())).thenReturn(mockResource);
        Mockito.when(mockResource.getFile()).thenReturn(file);
        Map<String, String> result = techpackPreCheckServiceImpl.readVersionPropertyFile();
        Assert.assertNotEquals(null, result);
    }

    @After
    public void tearDown() throws IOException {
        file = writeVersionFile();
    }

    private File writeVersionFile() throws IOException {
        Properties props = getPropertyObject();
        File file = Paths.get("src", "test", "resources", "version.properties").toFile();
        OutputStream outputstream = new FileOutputStream(file);
        props.store(outputstream, null);
        outputstream.close();
        return file;
    }

    private void modifyFile(File file, String key) throws IOException {
        Properties props = new Properties();
        InputStream is = new FileInputStream(file);
        props.load(is);
        is.close();
        if (!key.equals(""))
            props.remove(key);

        OutputStream outputstream = new FileOutputStream(file);
        props.store(outputstream, null);
        outputstream.close();
    }

    private Properties getPropertyObject() {
        Properties props = new Properties();
        props.put("tech_pack.metadata_version", "3");
        props.put("required_tech_packs.DWH_MONITOR", "R2D");
        props.put("required_tech_packs.DWH_BASE", "R2E_b");
        props.put("tech_pack.name", "DIM_E_LTE");
        props.put("author", "TPC-R1A42");
        props.put("tech_pack.version", "R37A");
        props.put("build.number", "303");
        props.put("build.tag", "");
        props.put("license.name", "CXC4010777");
        return props;

    }

}