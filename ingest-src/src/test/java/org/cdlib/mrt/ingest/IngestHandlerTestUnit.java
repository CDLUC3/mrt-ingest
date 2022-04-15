package org.cdlib.mrt.ingest;

import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.TException;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.cdlib.mrt.ingest.handlers.HandlerCharacterize;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.handlers.HandlerRetrieve;
import org.cdlib.mrt.ingest.handlers.HandlerVerify;
import org.cdlib.mrt.ingest.handlers.HandlerCorroborate;
import org.cdlib.mrt.ingest.handlers.HandlerDigest;
import org.cdlib.mrt.ingest.handlers.HandlerDisaggregate;
import org.cdlib.mrt.ingest.handlers.HandlerDocument;
import org.cdlib.mrt.ingest.handlers.HandlerCleanup;

public class IngestHandlerTestUnit extends IngestHandlerTest {

        public IngestHandlerTestUnit() throws TException {
                super();
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

        public void runAllHandlers(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
                runHandlerDisaggregateTests(ingestInput, ir);
                runHandlerRetrieveTests(ingestInput, ir);
                runHandlerCorroborateTests(ingestInput, ir);
                runHandlerCharacterizeTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                runHandlerDocumentTests(ingestInput, ir);
                runHandlerDigestTests(ingestInput, ir);
                runHandlerCleanupTests(ingestInput, ir);
        }

        @Test
        public void HandlerInitializeTest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
        }

        @Test
        public void HandlerAcceptTest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTestWithDigest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTestWithInvalidDigest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                failHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerDisaggregateTest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDisaggregateTests(ingestInput, ir);
        }

        @Test
        public void HandlerRetrieveTest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                // No retrieval for a simple file... need to retrieve a real file
                runHandlerRetrieveTests(ingestInput, ir);
        }

        @Test
        public void HandlerCorroborate() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                // no corroborate for a single file, need to process a real manifest
                runHandlerCorroborateTests(ingestInput, ir);
        }

        @Test
        public void HandlerCharacterize() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                // Code seems disabled in Merritt
                // [warn] HandlerCharacterize: URL has not been set. Skipping characterization.
                runHandlerCharacterizeTests(ingestInput, ir);
        }

        @Test
        public void HandlerDescribe() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
        }

        @Test
        public void HandlerDocument() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                runHandlerDocumentTests(ingestInput, ir);
        }

        @Test
        public void HandlerDigest() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                runHandlerDigestTests(ingestInput, ir);
        }

        @Test
        public void HandlerCleanup() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                runHandlerCleanupTests(ingestInput, ir);
        }

        @Test
        public void AllHandlersSingleFile() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersZipFile() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.ZipFileAsFile, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersZipFileContainer() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.ZipFileAsContainer, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckm4Blocks() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.FourBlocks, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmBigHunt() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.BigHunt, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmCall911() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.Call911, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmBatchContainers() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.BatchContainers, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.container);
        }

        @Test
        public void AllHandlersCheckmBatchFiles() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.BatchFiles, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.file);
        }

        @Test
        public void AllHandlersCheckmBatchManifests() throws IOException, TException {
                InputFile ingestInput = new InputFile(SampleFile.BatchManifests, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.manifest);
        }
}
