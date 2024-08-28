/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.distocraft.dc5000.etl.symboliclinkcreator;

import static org.junit.Assert.*;
import java.io.File;
import java.lang.reflect.Field;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class symboliclinkcreatorTest {

    private static Field mainParserObject;

    private static Field techPack;

    private static Field setType;

    private static Field setName;

    private static Field status;

    private static Field workerName;

    private static Field key;

    private static Field seqKey;

    private static Field seqCount;

    private static Field seqContainer;

    private static Field oldObjectClass;

    private static Field measData;

    private static Field cloneData;

    private static Field readVendorIDFrom;

    private static Field objectMask;

    private static Field fdn;

    private static Field objectClass;

    private static Field sourceFile;

    private static Field charValue;

    private static Field measFile;

    private static Field headerCount;

    private static Field fdnList;

    private static Field headerData;

    private static Field seqKeys;

    @BeforeClass
    public static void init() {
        try {
            mainParserObject = symboliclinkcreator.class
                    .getDeclaredField("mainParserObject");
            techPack = symboliclinkcreator.class.getDeclaredField("techPack");
            setType = symboliclinkcreator.class.getDeclaredField("setType");
            setName = symboliclinkcreator.class.getDeclaredField("setName");
            status = symboliclinkcreator.class.getDeclaredField("status");
            workerName = symboliclinkcreator.class.getDeclaredField("workerName");
            key = symboliclinkcreator.class.getDeclaredField("key");
            seqKey = symboliclinkcreator.class.getDeclaredField("seqKey");
            seqCount = symboliclinkcreator.class.getDeclaredField("seqCount");
            seqContainer = symboliclinkcreator.class
                    .getDeclaredField("seqContainer");
            oldObjectClass = symboliclinkcreator.class
                    .getDeclaredField("oldObjectClass");
            measData = symboliclinkcreator.class.getDeclaredField("measData");
            cloneData = symboliclinkcreator.class.getDeclaredField("cloneData");
            readVendorIDFrom = symboliclinkcreator.class
                    .getDeclaredField("readVendorIDFrom");
            objectMask = symboliclinkcreator.class.getDeclaredField("objectMask");
            fdn = symboliclinkcreator.class.getDeclaredField("fdn");
            objectClass = symboliclinkcreator.class.getDeclaredField("objectClass");
            sourceFile = symboliclinkcreator.class.getDeclaredField("sourceFile");
            charValue = symboliclinkcreator.class.getDeclaredField("charValue");
            measFile = symboliclinkcreator.class.getDeclaredField("measFile");
            headerCount = symboliclinkcreator.class.getDeclaredField("headerCount");
            fdnList = symboliclinkcreator.class.getDeclaredField("fdnList");
            headerData = symboliclinkcreator.class.getDeclaredField("headerData");
            seqKeys = symboliclinkcreator.class.getDeclaredField("seqKeys");

            mainParserObject.setAccessible(true);
            techPack.setAccessible(true);
            setType.setAccessible(true);
            setName.setAccessible(true);
            status.setAccessible(true);
            workerName.setAccessible(true);
            key.setAccessible(true);
            seqKey.setAccessible(true);
            seqCount.setAccessible(true);
            seqContainer.setAccessible(true);
            oldObjectClass.setAccessible(true);
            measData.setAccessible(true);
            cloneData.setAccessible(true);
            readVendorIDFrom.setAccessible(true);
            objectMask.setAccessible(true);
            fdn.setAccessible(true);
            objectClass.setAccessible(true);
            sourceFile.setAccessible(true);
            charValue.setAccessible(true);
            measFile.setAccessible(true);
            headerCount.setAccessible(true);
            fdnList.setAccessible(true);
            headerData.setAccessible(true);
            seqKeys.setAccessible(true);

        } catch (Exception e) {
            e.printStackTrace();
            fail("init() failed");
        }
    }

    @Test
    public void testInit() {
        symboliclinkcreator ce = new symboliclinkcreator();

        ce.init(null, "techPack", "setType", "setName", "workerName");

        try {

            String expected = "null,techPack,setType,setName,1,workerName";
            String actual = mainParserObject.get(ce) + "," + techPack.get(ce)
                    + "," + setType.get(ce) + "," + setName.get(ce) + ","
                    + status.get(ce) + "," + workerName.get(ce);

            assertEquals(expected, actual);

        } catch (Exception e) {
            e.printStackTrace();
            fail("testInit() failed");
        }

    }

    @Test
    public void testStatus() {
        symboliclinkcreator ce = new symboliclinkcreator();
        assertEquals(0, ce.status());
    }

    @Test
    public void testStartDocument() {
        symboliclinkcreator ce = new symboliclinkcreator();
        ce.init(null, null, null, null, "worker");

        try {

            ce.startDocument();

            assertNotNull(fdnList.get(ce));

            assertNotNull(headerData.get(ce));

            assertNotNull(cloneData.get(ce));

            String actual = headerCount.get(ce) + ","
                    + (String) oldObjectClass.get(ce);

            String expected = "0,";

            assertEquals(expected, actual);

        } catch (Exception e) {
            e.printStackTrace();
            fail("testStartDocument() failed");
        }
    }

    @AfterClass
    public static void clean() {
        File i = new File(System.getProperty("user.home"), "storageFile");
        i.deleteOnExit();
    }
}
