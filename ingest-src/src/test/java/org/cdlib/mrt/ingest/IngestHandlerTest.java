package org.cdlib.mrt.ingest;

import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
//import static org.mockito.Mockito.*;
//import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import java.util.ArrayList;
import java.util.Arrays;

import org.cdlib.mrt.ingest.handlers.HandlerInitialize;
import org.cdlib.mrt.ingest.handlers.HandlerAccept;
import org.cdlib.mrt.ingest.handlers.HandlerDescribe;
import org.cdlib.mrt.ingest.handlers.HandlerCharacterize;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.handlers.HandlerRetrieve;
import org.cdlib.mrt.ingest.handlers.HandlerVerify;
import org.cdlib.mrt.ingest.handlers.HandlerCorroborate;
import org.cdlib.mrt.ingest.handlers.HandlerDigest;
import org.cdlib.mrt.ingest.handlers.HandlerDisaggregate;
import org.cdlib.mrt.ingest.handlers.HandlerDocument;
import org.cdlib.mrt.ingest.handlers.HandlerCleanup;

import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.TException;
import org.apache.commons.io.FileUtils;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;

public class IngestHandlerTest extends IngestTestCore {

        public static final String ARK = "ark:/99999/ab12345678";
        public static final String SAMPLES = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/";
        public static final String JOBID = "jobID";
        public static final String BATCHID = "batchid";

        public enum SampleFile {
                SingleFileNoDigest("test.txt", PackageTypeEnum.file, ""),
                SingleFileWithDigest("test.txt", PackageTypeEnum.file, "") {
                        public String getAlg() {
                                return "md5";
                        }

                        public String getDigest() {
                                return "8b1a9953c4611296a827abf8c47804d7";
                        }
                },
                SingleFileBadDigest("test.txt", PackageTypeEnum.file, "") {
                        public String getAlg() {
                                return "md5";
                        }

                        public String getDigest() {
                                return "8b1a9953c4611296a827abf8c47804d8";
                        }
                },
                ZipFileAsFile("test.zip", PackageTypeEnum.file, ""),
                ZipFileAsContainer("test.zip", PackageTypeEnum.container, "test.txt,foo.txt"),
                FourBlocks(SAMPLES + "4blocks.checkm", PackageTypeEnum.manifest, "4blocks.jpg,4blocks.txt"),
                BigHunt(SAMPLES + "bigHunt.checkm", PackageTypeEnum.manifest, "bigHunt.txt,bigHunt2.jpg,bigHunt3.jpg"),
                Call911(SAMPLES + "call911.checkm", PackageTypeEnum.manifest, "call911.txt,call911.jpg"),
                BatchContainers(SAMPLES + "sampleBatchOfContainers.checkm", PackageTypeEnum.batchManifestContainer,
                                "huskyChicken.zip,souvenirs.zip,outdoorStore.zip"),
                BatchFiles(SAMPLES + "sampleBatchOfFiles.checkm", PackageTypeEnum.batchManifestFile,
                                "tumbleBug.jpg,goldenDragon.jpg,generalDrapery.jpg"),
                BatchManifests(SAMPLES + "sampleBatchOfManifests.checkm", PackageTypeEnum.batchManifest,
                                "bigHunt.checkm,call911.checkm,4blocks.checkm");

                private PackageTypeEnum type = PackageTypeEnum.file;
                public PackageTypeEnum type() {
                        return type;
                }
                private String path;
                private String alg = "";
                private String digest = "";
                private URL url;
                private ArrayList<String> files = new ArrayList<>();
                public ArrayList<String> files(){
                        return files;
                }

                SampleFile(String path, PackageTypeEnum type, String list) {
                        this.type = type;
                        if (path.startsWith("http")) {
                                try {
                                        this.url = new URL(path);
                                        Path p = Paths.get(this.url.getFile());
                                        this.path = p.getFileName().toString();
                                } catch (MalformedURLException e) {
                                        System.err.println(e);
                                }
                        } else {
                                this.path = path;
                        }
                        if (list.isEmpty()) {
                                files.add(path);
                        } else {
                                for (String s : list.split(",")) {
                                        files.add(s);
                                }
                        }
                }

                public boolean isManifest() {
                        if (type == PackageTypeEnum.file || type == PackageTypeEnum.container) {
                                return false;
                        }
                        return true;
                }

                public boolean isBatch() {
                        if (type == PackageTypeEnum.batchManifest || type == PackageTypeEnum.batchManifestContainer
                                        || type == PackageTypeEnum.batchManifestFile) {
                                return true;
                        }
                        return false;
                }

                public Path getPath() {
                        return Paths.get(RESOURCES, "data", path);
                }

                public File getFile() {
                        return getPath().toFile();
                }

                public String getAlg() {
                        return alg;
                }

                public String getDigest() {
                        return digest;
                }

                public URL getUrl() {
                        return url;
                }

                public int getListSizeCount() {
                        return 13 + files.size() + (isManifest() ? 1 : 0);
                }
        }

        public class InputFile {
                SampleFile sampleFile;
                Path tempdir;
                private BatchState batch;
                private JobState js;

                public InputFile(SampleFile inputType, Path tempdir) throws TException {
                        this.sampleFile = inputType;
                        this.tempdir = tempdir;
                }

                public IngestRequest getIngestRequest(IngestManager im, JobState js) throws TException {
                        IngestRequest ir = new IngestRequest();
                        ir.setQueuePath(this.tempdir.toFile());
                        ir.setServiceState(im.getServiceState());
                        ir.setPackageType(sampleFile.type.getValue());
                        ir.setIngestQueuePath(this.tempdir.toString());
                        ir.setJob(js);
                        return ir;
                }

                public SampleFile sampleFile() {
                        return sampleFile;
                }

                public Path tempdir() {
                        return tempdir;
                }

                public Path getCopyPath() {
                        return this.tempdir.resolve(this.sampleFile.path);
                }

                public Path getProducerPath() {
                        return this.tempdir.resolve("producer").resolve(this.sampleFile.path);
                }

                public void moveToIngestDir() throws IOException {
                        Files.copy(this.sampleFile.getPath(), getCopyPath());
                }

                public BatchState getBatchState() throws TException {
                        if (this.batch == null) {
                                this.batch = new BatchState(new Identifier(BATCHID));
                        }
                        return this.batch;
                }

                public JobState getJobState() throws TException {
                        if (this.js == null) {
                                this.js = new JobState(
                                                "user",
                                                sampleFile.path,
                                                sampleFile.getAlg(),
                                                sampleFile.getDigest(),
                                                ARK,
                                                "objectCreator",
                                                "objectTitle",
                                                "2022-01-01",
                                                "note");
                                js.setSubmissionDate(new DateState());
                        }
                        BatchState b = this.getBatchState();
                        js.setJobID(new Identifier(JOBID));
                        b.addJob(js.getJobID().getValue(), js);
                        js.setBatchID(b.getBatchID());
                        return this.js;
                }
        }

        public enum SystemFile {
                mrt_ingest("mrt-ingest.txt"),
                mrt_membership("mrt-membership.txt"),
                mrt_mom("mrt-mom.txt"),
                mrt_owner("mrt-owner.txt"),
                mrt_dc("mrt-dc.xml"),
                mrt_erc("mrt-erc.txt"),
                mrt_manifest("mrt-manifest.txt"),
                mrt_submission_manifest("mrt-submission-manifest.txt");

                String path;

                SystemFile(String path) {
                        this.path = path;
                }
        }

        public class SystemFileInstance {
                SystemFile sysfile;

                SystemFileInstance(SystemFile sysfile) {
                        this.sysfile = sysfile;
                }

                public Path getPath() {
                        return IngestHandlerTest.this.getSystemPath().resolve(sysfile.path);
                }

                public File getFile() {
                        return getPath().toFile();
                }

                public boolean exists() {
                        return getFile().exists();
                }

                public Properties sysFileProperties() throws FileNotFoundException, IOException {
                        Properties p = new Properties();
                        if (exists()) {
                                p.load(new FileReader(getFile()));
                        }
                        return p;
                }

                public boolean hasProperty(String name) throws FileNotFoundException, IOException {
                        return sysFileProperties().containsKey(name);
                }

                public String getProperty(String name) throws FileNotFoundException, IOException {
                        return sysFileProperties().getProperty(name);
                }

                public String getContent() throws IOException {
                        return exists() ? new String(Files.readAllBytes(getPath())).trim() : "";
                }

                public List<String> sysFileLines() throws FileNotFoundException, IOException {
                        return Arrays.asList(getContent().split("\n"));
                }

                public boolean contains(String val) throws FileNotFoundException, IOException {
                        return sysFileLines().contains(val);
                }
        }

        Path tempdir;
        ProfileState ps;

        IngestConfig ingestConfig;
        IngestManager im;

        public IngestHandlerTest() throws TException {
                ingestConfig = IngestConfig.useYaml();
                im = new IngestManager(ingestConfig.getLogger(), ingestConfig.getStoreConf(),
                                ingestConfig.getIngestConf(), ingestConfig.getQueueConf());
                im.init(ingestConfig.getStoreConf(), ingestConfig.getIngestConf(), ingestConfig.getQueueConf());
        }

        public Path getSystemPath() {
                return tempdir.resolve("system");
        }

        public Path getProducerPath() {
                return tempdir.resolve("producer");
        }

        @Before
        public void createTestDirectory() throws IOException, TException {

                tempdir = Files.createTempDirectory("ingestTest");
                System.out.println("Creating " + tempdir);
                Files.createDirectory(getProducerPath());
                Files.createDirectory(getSystemPath());

                ingestConfig.setIngestQueuePath(tempdir.toAbsolutePath().toString());

                ps = getProfileState();
        }

        @After
        public void clearTestDirectory() throws IOException {
                System.out.println("Deleting " + tempdir);
                FileUtils.deleteDirectory(tempdir.toFile());
        }

        public Path createBatchDir() throws IOException {
                Path bdir = tempdir.resolve("batch");
                Files.createDirectories(bdir);
                return bdir;
        }

        public void runQueueHandlerTests(InputFile ingestInput, IngestRequest ir, PackageTypeEnum newtype)
                        throws TException {
                BatchState batch = ingestInput.getBatchState();
                assertEquals(1, batch.getJobStates().size());
                org.cdlib.mrt.ingest.handlers.queue.HandlerResult hr = new org.cdlib.mrt.ingest.handlers.queue.HandlerDisaggregate()
                                .handle(
                                                ps,
                                                ir,
                                                batch);
                assertTrue(hr.getSuccess());
                assertEquals(ingestInput.sampleFile.files.size() + 1, batch.getJobStates().size());
                for (JobState tjs : batch.getJobStates().values()) {
                        if (tjs.getJobID().getValue().equals(JOBID)) {
                                continue;
                        }
                        assertTrue(ingestInput.sampleFile.files.contains(tjs.getPackageName()));
                        assertEquals(newtype.getValue(), tjs.getObjectType());
                }
        }

        public void runHandlerInitializeTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {

                assertTrue(ingestInput.getCopyPath().toFile().exists());
                HandlerResult hr = new HandlerInitialize().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                SystemFileInstance sfi = new SystemFileInstance(SystemFile.mrt_ingest);
                assertTrue(sfi.exists());
                assertEquals("Unit Test Ingest", sfi.getProperty("ingest"));
                assertFalse(sfi.hasProperty("handlers"));

                sfi = new SystemFileInstance(SystemFile.mrt_membership);
                assertTrue(sfi.exists());
                assertEquals(ps.getCollection().firstElement(), sfi.getContent());

                sfi = new SystemFileInstance(SystemFile.mrt_mom);
                assertTrue(sfi.exists());
                Identifier pi = ingestInput.getJobState().getPrimaryID();
                assertEquals(pi == null ? null : pi.getValue(), sfi.getProperty("primaryIdentifier"));
                assertEquals(ps.getObjectType(), sfi.getProperty("type"));
                assertEquals(ps.getObjectRole(), sfi.getProperty("role"));
                assertEquals(ps.getAggregateType(), sfi.getProperty("aggregate"));

                sfi = new SystemFileInstance(SystemFile.mrt_owner);
                assertTrue(sfi.exists());
                assertEquals(ps.getOwner(), sfi.getContent());
        }

        public void runHandlerAcceptTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerAccept().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                assertFalse(ingestInput.getCopyPath().toFile().exists());
                assertTrue(ingestInput.getProducerPath().toFile().exists());
        }

        public void runHandlerDescribeTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerDescribe().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                SystemFileInstance sfi = new SystemFileInstance(SystemFile.mrt_dc);
                assertTrue(sfi.exists());

                sfi = new SystemFileInstance(SystemFile.mrt_erc);
                assertTrue(sfi.exists());
                assertEquals("objectCreator", sfi.getProperty("who"));
                assertEquals("objectTitle", sfi.getProperty("what"));
                // where element may exist more than once and cannot be read as a property
                assertTrue(sfi.contains("where: " + ARK));
        }

        public void runHandlerVerifyTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerVerify().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

        public void failHandlerVerifyTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerVerify().handle(ps, ir, ingestInput.getJobState());
                assertFalse(hr.getSuccess());
        }

        public void runHandlerDisaggregateTests(InputFile ingestInput, IngestRequest ir)
                        throws TException, IOException {
                HandlerResult hr = new HandlerDisaggregate().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                if (ingestInput.sampleFile().type() == PackageTypeEnum.container) {
                        assertFalse(ingestInput.getProducerPath().toFile().exists());
                        for (String s : ingestInput.sampleFile().files()) {
                                assertTrue(getProducerPath().resolve(s).toFile().exists());
                        }
                } else {
                        assertTrue(ingestInput.getProducerPath().toFile().exists());
                }
        }

        public void runHandlerRetrieveTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerRetrieve().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                if (!ingestInput.sampleFile().isBatch()) {
                        for (String s : ingestInput.sampleFile().files()) {
                                assertTrue(getProducerPath().resolve(s).toFile().exists());
                        }
                }
        }

        public void runHandlerCorroborateTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerCorroborate().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

        public void runHandlerCharacterizeTests(InputFile ingestInput, IngestRequest ir)
                        throws TException, IOException {
                HandlerResult hr = new HandlerCharacterize().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

        public void runHandlerDocumentTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                SystemFileInstance sfi = new SystemFileInstance(SystemFile.mrt_ingest);
                assertFalse(sfi.hasProperty("handlers"));
                HandlerResult hr = new HandlerDocument().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                assertTrue(sfi.hasProperty("handlers"));
        }

        public void runHandlerDigestTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerDigest().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                SystemFileInstance sfi = new SystemFileInstance(SystemFile.mrt_manifest);
                assertTrue(sfi.exists());
                assertEquals(ingestInput.sampleFile().getListSizeCount(), sfi.sysFileLines().size());
        }

        public void runHandlerCleanupTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                assertTrue(getProducerPath().toFile().exists());
                HandlerResult hr = new HandlerCleanup().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

                assertFalse(getProducerPath().toFile().exists());
        }

        public void runHandlerTransferTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                //intentional no op
        }

        public void runHandlerInventoryQueueTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                //intentional no op
        }

        public void runHandlerMinterTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                //intentional no op
        }

        public void runAllHandlers(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
                runHandlerDisaggregateTests(ingestInput, ir);
                runHandlerRetrieveTests(ingestInput, ir);
                runHandlerCorroborateTests(ingestInput, ir);
                runHandlerCharacterizeTests(ingestInput, ir);
                runHandlerMinterTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                runHandlerDocumentTests(ingestInput, ir);
                runHandlerDigestTests(ingestInput, ir);
                runHandlerTransferTests(ingestInput, ir);
                runHandlerInventoryQueueTests(ingestInput, ir);
                runHandlerCleanupTests(ingestInput, ir);
        }
}
