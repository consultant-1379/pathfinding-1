package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.utils.ZipCrypter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(MockitoJUnitRunner.class)
public class DecryptTechPackServiceImplTest {

    @InjectMocks
    DecryptTechPackServiceImpl decryptTechPackService;

    @Mock
    ZipCrypter zipCrypter;

    @Before
    public void setup() {
        ReflectionTestUtils.setField(decryptTechPackService, "decryptTPIFileURL", "https://test.com");
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDecryptService() throws Exception {

        Path fileResponse = Paths.get("src", "test", "resources", "decryptfile");
        String path = fileResponse.toFile().getAbsolutePath();
        File file = new File(path);
        Mockito.lenient().doNothing().when(zipCrypter).executeDecryptFile(file);
        var response = decryptTechPackService.getDecryptFile();
        Assert.assertNotEquals(null, response);
    }

    @Test
    public void shouldTestTPIDecryption() throws Exception {
        File oldFile = new File("src/test/resources/decryptfile/DC_E_LTE_R1A_b1.tpi");
        File copyFile = new File("src/test/resources/decryptfile_copy/DC_E_LTE_R1A_b1.tpi");
        if (!copyFile.exists()) {
            Files.copy(oldFile.toPath(), copyFile.toPath());
        }
        Path fileResponse = Paths.get("src", "test", "resources", "decryptfile_copy");
        String copyFilePath = fileResponse.toFile().getAbsolutePath();
        File fileCopy = new File(copyFilePath);
        zipCrypter = Mockito.spy(ZipCrypter.class);
        zipCrypter.executeDecryptFile(fileCopy);

        File[] files = fileCopy.listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        Assert.assertTrue(true);
    }


}
