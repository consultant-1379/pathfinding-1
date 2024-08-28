/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/

package com.ericsson.eniq.repository.dbusers;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
@Ignore
public class AddServiceUserTest extends UnitDatabaseTestCase {

    private AddServiceUser testInstance = null;

    @BeforeClass
    public static void beforeClass() throws IOException {
        setup(TestType.unit);
    }

    @Before
    public void before() {
        testInstance = new AddServiceUser();

    }

    @After
    public void after() {
        testInstance = null;
        truncateSchemaTables(Schema.etlrep, true);
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNullUser() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-u", null, "-p", "ddd", "-s", "sssssss" });
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNoUser() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-p", "ddd", "-s", "sssssss" });
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNullPassword() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-u", "uuu", "-p", null, "-s", "sssssss" });
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNoPassword() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-u", "ddd", "-s", "sssssss" });
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNullServicename() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-u", "uuu", "-p", "pp", "-s", null });
    }

    @Test(expected = AddServiceUser.TmpException.class)
    public void testNoServicename() throws RockException, SQLException {
        testInstance.addServiceUsers(new String[] { "-u", "ddd", "-p", "pppp" });
    }

    @Test
    public void testAddAllFromServicenames() throws RockException, SQLException, IOException {
        final List<Meta_databases> pre = getCurrentMetaDatabases();
        testInstance.addServiceUsers(new String[] { "-all" });
        final List<Meta_databases> post = getCurrentMetaDatabases();
        assertFalse("Pre and Post Meta_databases size should be different!", pre.size() == post.size());
        final List<String> expectedList = getExpectedList();

        printExpectedAndDatabaseList(post, expectedList);

        assertEquals("Wrong number of entried added!", expectedList.size(), post.size() - pre.size());

        for (final String expected : expectedList) {
            Meta_databases found = null;
            for (final Meta_databases mdb : post) {
                final String key = mdb.getConnection_name() + ":" + mdb.getType_name();
                if (key.equals(expected)) {
                    if (found != null) {
                        fail("Duplicate entry for service " + expected + " found!");
                    }
                    found = mdb;
                }
            }
            if (found == null) {
                fail("No entry for service " + expected + " found!");
            }
        }
    }

    /**
     * @param post
     * @param expectedList
     */
    private void printExpectedAndDatabaseList(final List<Meta_databases> post, final List<String> expectedList) {
        System.out.println("Expected List:");
        for (final String e : expectedList) {
            System.out.format("\t%s\n", e);
        }
        System.out.println("Database List:");
        for (final Meta_databases p : post) {
            System.out.format("\t%s:%s\n", p.getConnection_name(), p.getType_name());
        }
    }

    /**
     * @return
     * @throws IOException
     */
    private List<String> getExpectedList() throws IOException {
        final List<String> expectedList = new ArrayList<String>();
        final List<String> serviceNames = getServiceNames();
        for (final String serviceName : serviceNames) {
            final String name = serviceName.substring(serviceName.lastIndexOf(':') + 1);
            expectedList.add(name + ":dcuser");
            if (name.startsWith("dwh_reader_")) {
                expectedList.add(name + ":DBA");
                expectedList.add(name + ":USER");
            }
        }
        return expectedList;
    }

    @Test
    public void testDuplicateEntry() throws RockException, SQLException {
        final String user = "test-user";
        final String passwd = "test-password";
        final String service = "test-service";

        final List<Meta_databases> firstTime = addEntry(user, passwd, service, true, true);
        final List<Meta_databases> secondTime = addEntry(user, passwd, service, true, false);
        assertTrue("firstTime and secondTime sizes should be the same", firstTime.size() == secondTime.size());
        assertEntryExists(secondTime, user, passwd, service);
    }

    @Test
    public void testAddSingleEntry() throws RockException, SQLException {
        final String user = "test-user";
        final String passwd = "test-password";
        final String service = "test-service";
        addEntry(user, passwd, service, true, true);
    }

    @Test
    public void testNoArgs() {
        try {
            testInstance.addServiceUsers(new String[] {});
            fail("Exception should have been thrown as no arguements were passed!");
        } catch (final AddServiceUser.TmpException e) {
            //ok
        }
    }

    public List<String> getServiceNames() throws IOException {
        final List<String> serviceNames = new ArrayList<String>();

        serviceNames.add("127.0.0.1::localhost::controlzone");
        serviceNames.add("127.0.0.1::localhost::dwh_reader_1");
        serviceNames.add("127.0.0.1::localhost::dwhdb");
        serviceNames.add("127.0.0.1::localhost::ec_1");
        serviceNames.add("127.0.0.1::localhost::engine");
        serviceNames.add("127.0.0.1::localhost::glassfish");
        serviceNames.add("127.0.0.1::localhost::ldapserver");
        serviceNames.add("127.0.0.1::localhost::licenceservice");
        serviceNames.add("127.0.0.1::localhost::repdb");
        serviceNames.add("127.0.0.1::localhost::webserver");
        return serviceNames;
    }

    private List<Meta_databases> addEntry(final String username, final String password, final String service_name, final boolean assertOk,
                                          final boolean checkSizes) throws RockException, SQLException {
        final List<Meta_databases> pre = getCurrentMetaDatabases();
        testInstance.addServiceUsers(new String[] { "-u", username, "-p", password, "-s", service_name });
        final List<Meta_databases> post = getCurrentMetaDatabases();
        if (assertOk && checkSizes) {
            assertFalse("Pre and Post Meta_databases size should be different!", pre.size() == post.size());
        }
        assertEntryExists(post, username, password, service_name);
        return post;
    }

    private void assertEntryExists(final List<Meta_databases> list, final String username, final String password, final String service_name) {
        Meta_databases _new = null;
        for (final Meta_databases mdb : list) {
            if (mdb.getConnection_name().equals(service_name)) {
                assertNull("Duplicate entries found for service_name " + service_name, _new);
                _new = mdb;
            }
        }
        assertNotNull("No entry for service " + service_name + " found", _new);
        assertEquals("Wrong username stored!", username, _new.getUsername());
        assertEquals("Wrong password stored!", password, _new.getPassword());
    }

    private List<Meta_databases> getCurrentMetaDatabases() throws RockException, SQLException {
        final RockFactory etlrep = getRockFactory(Schema.etlrep);
        final Meta_databases where = new Meta_databases(etlrep);
        final Meta_databasesFactory mFac = new Meta_databasesFactory(etlrep, where);
        return mFac.get();
    }

}
