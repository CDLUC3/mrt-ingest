package org.cdlib.mrt.ingest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import com.ibm.icu.util.Calendar;

import java.util.Arrays;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerAccept;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.TException;
import org.apache.commons.io.FileUtils;
import org.cdlib.mrt.core.Identifier;

/*
 * Sample inner class with constructor
 * 
 *  class Foo {
 *      String s;
 *      Foo(String s) {
 *         this.s = s;
 *      }
 *      public String toString() {
 *          return s;
 *      }
 * 
 *      //mock cannot be made on final method, therefore a new one is shown here
 *      public int length() {
 *          return s.length();
 *      }
 *  }
 *
 * Sample Mock with constructor
 * 
 *   Foo mockedString = mock(Foo.class, "ABC");
 *   when(mockedString.length()).thenReturn(777);
 *   System.out.println(mockedString.length());
 */

public class IngestTest {
    class Foo {
        String s;
        Foo(String s) {
            this.s = s;
        }
        public String toString() {
            return s;
        }
        public int length() {
            return s.length();
        }
    }

    public Date jan1_2022() {
        Calendar cal = Calendar.getInstance();
        cal.set(122, 1, 1);
        return cal.getTime();
    }

    public static final String RESOURCES = "src/test/resources/";

    public ProfileState getProfileState() throws TException {
        File f = Paths.get(RESOURCES, "profile/merritt_test_content").toFile();
        Identifier id = new Identifier("profile");
        return ProfileUtil.getProfile(id, f);
    }

    public IngestRequest getIngestRequest(File f) {
        IngestRequest ir = new IngestRequest();
        // where processing will happen
        ir.setQueuePath(f);
        return ir;
    }

    Path tempdir;

    @Before 
    public void createTestDirectory() throws IOException {
        tempdir = Files.createTempDirectory("ingestTest");
        System.out.println("Creating " + tempdir);
        Files.createDirectory(tempdir.resolve("producer"));
        Files.createDirectory(tempdir.resolve("system"));
    }

    @After 
    public void clearTestDirectory() throws IOException {
        System.out.println("Deleting " + tempdir);
        FileUtils.deleteDirectory(tempdir.toFile());
    }

    @Test
    public void ReadProfileFile() throws TException {
        ProfileState ps = getProfileState();
        assertEquals("merritt_test_content", ps.getProfileID().getValue());
        assertEquals("Merritt Test", ps.getProfileDescription());
        assertEquals(Identifier.Namespace.ARK.name(), ps.getIdentifierScheme().name());
        assertEquals("99999", ps.getIdentifierNamespace());
        assertEquals("ark:/99999/m5000000", ps.getCollection().firstElement());
        //assertContains(new ProfileState().OBJECTTYPE, ps.getObjectType());
        assertTrue(Arrays.asList(new ProfileState().OBJECTTYPE).contains(ps.getObjectType()));
        assertTrue(Arrays.asList(new ProfileState().OBJECTROLE).contains(ps.getObjectRole()));
        assertEquals("", ps.getAggregateType());
        assertFalse(Arrays.asList(new ProfileState().AGGREGATETYPE).contains(ps.getAggregateType()));
        assertEquals("ark:/99999/j2000000", ps.getOwner());
        assertEquals(1, ps.getContactsEmail().size());
        assertEquals("test.email@test.edu", ps.getContactsEmail().get(0).getContactEmail());
        assertEquals(15, ps.getIngestHandlers().size());
        assertEquals(3, ps.getQueueHandlers().size());
        assertEquals(9999, ps.getTargetStorage().getNodeID());
        assertEquals("03", ps.getPriority());
        assertTrue(ps.getCreationDate().getDate().after(jan1_2022()));
        assertTrue(ps.getModificationDate().getDate().after(jan1_2022()));
        assertEquals("https://ezid.cdlib.org/shoulder/ark:/99999/fk4", ps.getObjectMinterURL().toString());
        assertEquals("additional", ps.getNotificationType());
        assertEquals("merritt_test", ps.getContext());
        assertNull(ps.getAccessURL());
        assertNull(ps.getLocalIDURL());
        assertNull(ps.getPURL());
    }

    @Test
    public void JobStateTest() throws TException, IOException {
        JobState js = new JobState(
            "user", 
            "package", 
            "sha256", 
            "value", 
            "primaryID",
            "objectCreator", 
            "objectTitle", 
            "2022-01-01", 
            "note"
        );

        ProfileState ps = getProfileState();
        
        String testfile = "test.txt";

        Path input = Paths.get(RESOURCES, "data", testfile);
        Path copyloc = tempdir.resolve(testfile);
        Path output = tempdir.resolve("producer").resolve(testfile);

        Files.copy(input, copyloc);

        assertTrue(copyloc.toFile().exists());
        assertEquals(input.toFile().length(), copyloc.toFile().length());

        IngestRequest ir = getIngestRequest(tempdir.toFile());

        Handler<JobState> h = new HandlerAccept();
        HandlerResult hr = h.handle(ps, ir, js);

        assertEquals(0, hr.getReturnCode());

        assertFalse(copyloc.toFile().exists());
        assertTrue(output.toFile().exists());
        assertEquals(input.toFile().length(), output.toFile().length());
    }
}
