package com.ericsson.eniq.common.setWizards;

import java.util.Properties;

import junit.framework.TestCase;

import org.jdesktop.application.ResourceMap;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

public class CreateInterfaceSetTest extends TestCase {
	
	private int mockCount = 0;
	protected Mockery context = new JUnit4Mockery();

	private VelocityContext velocityContext;
	private static boolean velocityInit = false;
	private CreateInterfaceSet testInstance;
	private static final String TEMPLATE_DIR = "5.2";
	private final String objType = "measurement";
	private final String tpVersion = "((39))"; 
	private final String versionID = ""; 
	private RockFactory dwhrepRock; 
	private RockFactory rock; 
	private final long techPackID = 31; 
	private final String interfaceName = "INTF_DC_E_RBS"; 
	private final String interfaceVersion = "((39)"; 
	private final String adapterType = "mdc"; 
	private final String adapterName = "mdc"; 
	private final String dirName = "mdc"; 
	private final String connectionID = "2"; 
	private ResourceMap mockResMap ;

    {
        // we need to mock classes, not just interfaces.
        context.setImposteriser(ClassImposteriser.INSTANCE);
    }
    
    @Test
	public void testCreatePropertyWithNormalParser() throws Exception {
    	String expected = "mdc";
    	// Check we start with tests expected adapterType
    	assertEquals(expected, testInstance.adapterType);
	    Properties conf = testInstance.createProperty(velocityContext);
	    String actual = conf.getProperty("parserType");
	    // Check rsulting parserType
	    assertEquals(expected, actual);
	}

	@Test
	public void testMergeWithExistingMDCTemplate() throws Exception {
		String templateName = "mdc.vm";
	    String expected = "MDCParser.HashData=true"; // Know that mdc.vm adds MDCParser.HashData property 
	    String actual = testInstance.merge(templateName, velocityContext);
	    assertTrue(actual.contains(expected));
	}
	
	@Test
	public void testMergeWithExistingStfiopTemplate() throws Exception {
		String templateName = "stfiop.vm";
	    String expected = "maxFilesPerRun=0"; // Know that stfiop.vm has this string 
	    String actual = testInstance.merge(templateName, velocityContext);
	    assertTrue(actual.contains(expected));
	}
	
	@Test
	public void testMergeWithNonExistingTemplate() throws Exception {
		String templateName = "doesntexist.vm"; // Should default to duplicateCheck.vm
	    String expected = "maxFilesPerRun=32765"; // Know that duplicateCheck.vm has this string 
	    String actual = testInstance.merge(templateName, velocityContext);
	    assertTrue(actual.contains(expected));
	}
	
	
	@Test
	public void testConstructor() {
		assertNotNull(testInstance);
	}

	@Override @Before
    protected void setUp() throws Exception {
        _init_velocity();
        rock = context.mock(RockFactory.class, "rockFactory"+mockCount++); //naming the mock objects makes them unique
        mockResMap = context.mock(ResourceMap.class, "mockResMap");
        context.checking(new Expectations() {
            {
                allowing(rock).getConnection();
            }
        });
        
        testInstance = new CreateInterfaceSet(objType, TEMPLATE_DIR, tpVersion, versionID,
        	      dwhrepRock, rock, techPackID, interfaceName, interfaceVersion,
        	      adapterType, adapterName, dirName, connectionID, mockResMap);

		velocityContext = new VelocityContext();
		velocityContext.put("parserType", testInstance.adapterType);
	    velocityContext.put("interfaceName", testInstance.interfaceName);
	    velocityContext.put("transformerName", testInstance.interfaceName + ".xml");
	    velocityContext.put("directoryName", testInstance.dirName);
	    velocityContext.put("interfaceType", testInstance.itype);
	    velocityContext.put("ARCHIVE_DIR", "");
	    velocityContext.put("OSS", "");
	    velocityContext.put("PMDATA_DIR", "");
	    velocityContext.put("ETLDATA_DIR", "");
	}

	@Override @After
    protected void tearDown() throws Exception {
		testInstance = null;
		velocityContext = null;
	}
	private void _init_velocity() throws Exception {
        if(!velocityInit){
            Velocity.setProperty("resource.loader", "class");
            Velocity.setProperty("class.resource.loader.class",
                    "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            Velocity.init();
            velocityInit = true;
        }
    }
}
