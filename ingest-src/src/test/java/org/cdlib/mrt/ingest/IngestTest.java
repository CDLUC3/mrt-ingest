package org.cdlib.mrt.ingest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import java.util.Calendar;

import java.util.Arrays;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerAccept;
import org.cdlib.mrt.ingest.handlers.HandlerCharacterize;
import org.cdlib.mrt.ingest.handlers.HandlerInitialize;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.handlers.HandlerRetrieve;
import org.cdlib.mrt.ingest.handlers.HandlerVerify;
import org.cdlib.mrt.ingest.handlers.HandlerCorroborate;
import org.cdlib.mrt.ingest.handlers.HandlerDescribe;
import org.cdlib.mrt.ingest.handlers.HandlerDigest;
import org.cdlib.mrt.ingest.handlers.HandlerDocument;
import org.cdlib.mrt.ingest.handlers.HandlerMinter;
import org.cdlib.mrt.ingest.handlers.HandlerTransfer;
import org.cdlib.mrt.ingest.handlers.HandlerCleanup;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.TException;
import org.json.JSONException;
import org.apache.commons.io.FileUtils;
import org.cdlib.mrt.core.DateState;
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

    public IngestRequest getIngestRequest(File f) throws TException {
        IngestRequest ir = new IngestRequest();
        // where processing will happen
        ir.setQueuePath(f);
        ir.setServiceState(im.getServiceState());
        ir.setPackageType(PackageTypeEnum.file.getValue());
        return ir;
    }

    public JobState getJobState() throws TException {
        //Blank aglorithm and value will be skipped
        return getJobStateWithChecksum("test.txt", "", "");
    }

    public JobState getJobStateHello() throws TException {
        //md5 for "Hello"
        //md5sum src/test/resources/data/test.txt
        return getJobStateWithChecksum("test.txt", "md5", "8b1a9953c4611296a827abf8c47804d7");
    }

    public JobState getJobStateHelloInvalid() throws TException {
        //md5 for "Hello"
        return getJobStateWithChecksum("test.txt", "md5", "8b1a9953c4611296a827abf8c47804d8");
    }

    public JobState getJobStateWithChecksum(String fname, String alg, String digest) throws TException {
        JobState js = new JobState(
            "user", 
            fname, 
            alg,
            digest, //no digest value 
            this.ark,
            "objectCreator", 
            "objectTitle", 
            "2022-01-01", 
            "note"
        );
        js.setSubmissionDate(new DateState());
        js.setBatchID(new Identifier("batchid"));
        js.setJobID(new Identifier("jobid"));
        
        return js;
    }

    Path tempdir;
    ProfileState ps;
    JobState js;

    String ark = "ark:/99999/ab12345678";
    String testfile = "test.txt";
    Path input = Paths.get(RESOURCES, "data", testfile);

    Path copyloc;
    Path system;
    Path producer;

    IngestRequest ir;

    IngestConfig ingestConfig;
    IngestManager im;

    public IngestTest() throws TException {
        ingestConfig = IngestConfig.useYaml();
        im = new IngestManager(ingestConfig.getLogger(), ingestConfig.getStoreConf(), ingestConfig.getIngestConf(), ingestConfig.getQueueConf()); 
        im.init(ingestConfig.getStoreConf(), ingestConfig.getIngestConf(), ingestConfig.getQueueConf());
    }

    @Before 
    public void createTestDirectory() throws IOException, TException {

        tempdir = Files.createTempDirectory("ingestTest");
        System.out.println("Creating " + tempdir);
        Files.createDirectory(tempdir.resolve("producer"));
        Files.createDirectory(tempdir.resolve("system"));

        ingestConfig.setIngestQueuePath(tempdir.toAbsolutePath().toString());

        copyloc = tempdir.resolve(testfile);
        system = tempdir.resolve("system");
        producer = tempdir.resolve("producer");

        ps = getProfileState();
        
        js = getJobState();
        js.setBatchID(new Identifier("batch"));        

        Files.copy(input, copyloc);
        assertTrue(copyloc.toFile().exists());
        assertEquals(input.toFile().length(), copyloc.toFile().length());

        ir = getIngestRequest(tempdir.toFile());
        ir.setIngestQueuePath(tempdir.toString());
        ir.setJob(js);
    }

    @After 
    public void clearTestDirectory() throws IOException {
        System.out.println("Deleting " + tempdir);
        FileUtils.deleteDirectory(tempdir.toFile());
    }

    @Test
    public void ReadProfileFile() throws TException {
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

    public HandlerResult runHandler(Handler<JobState> h) throws TException {
        HandlerResult hr = h.handle(ps, ir, js);

        assertTrue(hr.getSuccess());
        return hr;        
    }

    public HandlerResult runHandlerFail(Handler<JobState> h) throws TException {
        HandlerResult hr = h.handle(ps, ir, js);

        assertFalse(hr.getSuccess());
        return hr;        
    }

    public boolean sysFileExists(String name) {
        File f = system.resolve(name).toFile();
        return f.exists();
    }

    public Properties sysFileProperties(String name) throws FileNotFoundException, IOException {
        Properties p = new Properties();
        File f = system.resolve(name).toFile();
        if (f.exists()) {
            p.load(new FileReader(f));
        }
        return p;
    }

    public String fileContent(Path p) throws IOException {
        return new String(Files.readAllBytes(p)).trim();
    }

    public String sysFileContent(String name) throws FileNotFoundException, IOException {
        return sysFileExists(name) ? fileContent(system.resolve(name)) : "";
    }

    public List<String> sysFileLines(String name) throws FileNotFoundException, IOException {
        return Arrays.asList(sysFileContent(name).split("\n"));
    }

    @Test
    public void HandlerInitializeTest() throws TException, IOException {    
        runHandler(new HandlerInitialize());   
        assertTrue(sysFileExists("mrt-ingest.txt"));
        Properties p = sysFileProperties("mrt-ingest.txt");
        assertEquals("Unit Test Ingest", p.getProperty("ingest"));
        assertFalse(p.containsKey("handlers"));
        
        assertTrue(sysFileExists("mrt-membership.txt"));
        assertEquals(ps.getCollection().firstElement(), sysFileContent("mrt-membership.txt"));
        
        assertTrue(sysFileExists("mrt-mom.txt"));
        p = sysFileProperties("mrt-mom.txt");
        assertEquals(js.getPrimaryID().getValue(), p.getProperty("primaryIdentifier"));
        assertEquals(ps.getObjectType(), p.getProperty("type"));
        assertEquals(ps.getObjectRole(), p.getProperty("role"));
        assertEquals(ps.getAggregateType(), p.getProperty("aggregate"));

        assertTrue(sysFileExists("mrt-object-map.ttl"));
        
        assertTrue(sysFileExists("mrt-owner.txt"));
        assertEquals(ps.getOwner(), sysFileContent("mrt-owner.txt"));
    }

    @Test
    public void HandlerAcceptTest() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
 
        assertFalse(copyloc.toFile().exists());
        assertTrue(producer.resolve(testfile).toFile().exists());
        assertEquals(input.toFile().length(), producer.resolve(testfile).toFile().length());
    }

    @Test
    public void HandlerVerifyTest() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        runHandler(new HandlerVerify());   
    }

    @Test
    public void HandlerVerifyTestWithDigest() throws TException, IOException {
        js = getJobStateHello();
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        runHandler(new HandlerVerify());   
    }

    @Test
    public void HandlerVerifyTestWithInvalidDigest() throws TException, IOException {
        js = getJobStateHelloInvalid();
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        runHandlerFail(new HandlerVerify());   
    }

    @Test
    public void HandlerRetrieveTest() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        //No retrieval for a simple file... need to retrieve a real file
        runHandler(new HandlerRetrieve());   
    }

    //@Test
    public void HandlerRetrieveTestWithRetrieve() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        //No retrieval for a simple file... need to retrieve a real file
        runHandler(new HandlerRetrieve());   
    }

    @Test
    public void HandlerCorroborate() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        //no corroborate for a single file, need to process a real manifest
        runHandler(new HandlerCorroborate());   
    }

    @Test
    public void HandlerCharacterize() throws TException, IOException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        //Code seems disabled in Merritt
        //[warn] HandlerCharacterize: URL has not been set.  Skipping characterization. 
        runHandler(new HandlerCharacterize());   
    }

    //Not a unit test, this calls out to EZID
    //@Test
    public void HandlerMinter() throws TException, IOException, JSONException {
        runHandler(new HandlerInitialize());   
        runHandler(new HandlerAccept());   
        runHandler(new HandlerMinter());   
     }

     @Test
     public void HandlerDescribe() throws TException, IOException {
         runHandler(new HandlerInitialize());   
         runHandler(new HandlerAccept());   
         runHandler(new HandlerDescribe());   
         
         assertTrue(sysFileExists("mrt-dc.xml"));

         assertTrue(sysFileExists("mrt-erc.txt"));
         Properties p = sysFileProperties("mrt-erc.txt");
         assertEquals("objectCreator", p.getProperty("who"));
         assertEquals("objectTitle", p.getProperty("what"));
         //where element may exist more than once and cannot be read as a property
         assertTrue(sysFileLines("mrt-erc.txt").contains("where: "+this.ark));
     }

     @Test
     public void HandlerDocument() throws TException, IOException {
         runHandler(new HandlerInitialize());   
         runHandler(new HandlerAccept());   
         runHandler(new HandlerDescribe());   
         Properties p = sysFileProperties("mrt-ingest.txt");
         assertFalse(p.containsKey("handlers"));
         runHandler(new HandlerDocument());   
         p = sysFileProperties("mrt-ingest.txt");
         assertTrue(p.containsKey("handlers"));
       }

       @Test
       public void HandlerDigest() throws TException, IOException {
           runHandler(new HandlerInitialize());   
           runHandler(new HandlerAccept());   
           runHandler(new HandlerDescribe());   
           runHandler(new HandlerDigest());
           assertTrue(sysFileExists("mrt-manifest.txt"));
           assertEquals(15, sysFileLines("mrt-manifest.txt").size());
        }
  
        //@Test
        public void HandlerTransfer() throws TException, IOException {
            runHandler(new HandlerInitialize());   
            runHandler(new HandlerAccept());   
            runHandler(new HandlerDescribe()); 
            //Requires a storage service  
            runHandler(new HandlerTransfer());
         }

        @Test
        public void HandlerCleanup() throws TException, IOException {
            runHandler(new HandlerInitialize());   
            runHandler(new HandlerAccept());   
            runHandler(new HandlerDescribe()); 
            assertTrue(producer.toFile().exists());
            runHandler(new HandlerCleanup());
            assertFalse(producer.toFile().exists());
        }
    
}
