package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.globalexception.TechPackExceptions;
import com.ericsson.oss.eniq.techpack.model.Versioning;
import com.ericsson.oss.eniq.techpack.service.interfaces.VersioningService;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TechpackPreCheckServiceImplRequiredTechPackInstallationsTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    @InjectMocks
    TechpackPreCheckServiceImpl techpackPreCheckServiceImpl;
    @Mock
    Logger log;
    @Mock
    VersioningService versioningService;
    private boolean mockInitialized = false;

    @Before
    public void setUp() {
        if (!mockInitialized) {
            MockitoAnnotations.initMocks(this);
            mockInitialized = true;
        }
    }

    public List<Object[]> oldProductAndInvalidRStateCheck() {
        return new ArrayList<>() {{
            Map<String, String> requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/2:R2D/2");
            }};
            add(new Object[]{null, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2D/2");
            }};
            Versioning version = new Versioning();
            version.setProduct_number("COA 252 121");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2D");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/3");
            version.setTechpack_version("R1D_b3");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2E");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/3");
            version.setTechpack_version("R2D_b3");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/2:RRR");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/2");
            version.setTechpack_version("RRR");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/2:RRR");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/2");
            version.setTechpack_version("R2D_b3");
            add(new Object[]{version, requiredDependency});
        }};
    }

    @Test(expected = TechPackExceptions.class)
    @Parameters(method = "oldProductAndInvalidRStateCheck")
    public void testRequiredTechPackHasOldProductNum(Versioning version, Map<String, String> requiredDependency) {
        when(versioningService.getTargetVersioning(anyString())).thenReturn(version);
        techpackPreCheckServiceImpl.checkRequiredTechPackInstallations(requiredDependency);
    }

    public List<Object[]> newProductCheck() {
        return new ArrayList<>() {{

            Versioning version = new Versioning();
            Map<String, String> requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D");
            }};
            version.setProduct_number("COA 252 121/2");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D:R2D");
            }};
            version = new Versioning();
            version.setProduct_number("COA 252 121/2");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/1:R2D/2");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/2");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2D");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/3");
            version.setTechpack_version("R3D_b3");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2C");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/3");
            version.setTechpack_version("R2D_b3");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D/3:R2D");
            }};
            version = new Versioning();
            version.setProduct_number("R2D/3");
            version.setTechpack_version("R2D_b3");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D");
            }};
            version = new Versioning();
            version.setProduct_number("COA 252 121");
            version.setTechpack_version("R2D_b2");
            add(new Object[]{version, requiredDependency});

            requiredDependency = new HashMap<>() {{
                put("DWH_MONITOR", "R2D:R2E");
            }};
            version = new Versioning();
            version.setProduct_number("R2D");
            version.setTechpack_version("R2E_b3");
            add(new Object[]{version, requiredDependency});

        }};
    }

    @Test
    @Parameters(method = "newProductCheck")
    public void testRequiredTechPackHasNewProduct_1(Versioning version, Map<String, String> requiredDependency) {
        when(versioningService.getTargetVersioning(anyString())).thenReturn(version);
        techpackPreCheckServiceImpl.checkRequiredTechPackInstallations(requiredDependency);
    }

}