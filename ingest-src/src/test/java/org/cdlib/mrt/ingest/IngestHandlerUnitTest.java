package org.cdlib.mrt.ingest;

import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.TException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class IngestHandlerUnitTest extends IngestHandlerTest {

        public IngestHandlerUnitTest() throws TException {
                super();
        }

        @Test
        public void HandlerInitializeTest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerInitializeTest - Initialize data");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
        }

        @Test
        public void HandlerAcceptTest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerAcceptTest - Test data");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerVerifyTest - Verify data");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileNoDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTestWithDigest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerVerifyTestWithDigest - Verify data with Digest");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerVerifyTestWithInvalidDigest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerVerifyTestWithInvalidDigest - Verify data with bad Digest");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                failHandlerVerifyTests(ingestInput, ir);
        }

        @Test
        public void HandlerDisaggregateTest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerDisaggregateTest - Test disaggregate");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDisaggregateTests(ingestInput, ir);
        }

        @Test
        public void HandlerRetrieveTest() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerRetrieveTest - Test retrieval");
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
                System.out.println("[IngestHandlerUnitTest] HandlerCorroborate - Test corroborate");
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
                System.out.println("[IngestHandlerUnitTest] HandlerCharacterize - Test characterize");
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
                System.out.println("[IngestHandlerUnitTest] HandlerDescribe - Test describe");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
        }

        @Test
        public void HandlerDocument() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] HandlerDocument - Test document");
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
                System.out.println("[IngestHandlerUnitTest] HandlerDigest - Test digest");
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
                System.out.println("[IngestHandlerUnitTest] HandlerCleanup - Test cleanup");
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
                System.out.println("[IngestHandlerUnitTest] AllHandlersSingleFile - Run single file");
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersZipFile() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersZipFile - Run zip file");
                InputFile ingestInput = new InputFile(SampleFile.ZipFileAsFile, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersZipFileContainer() throws TException, IOException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersZipFileContainer - Run zip file containers");
                InputFile ingestInput = new InputFile(SampleFile.ZipFileAsContainer, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckm4Blocks() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckm4Blocks - Run sample data");
                InputFile ingestInput = new InputFile(SampleFile.FourBlocks, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmBigHunt() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckmBigHunt - Run more sample data");
                InputFile ingestInput = new InputFile(SampleFile.BigHunt, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmCall911() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckmCall911 - Run yet more sample data");
                InputFile ingestInput = new InputFile(SampleFile.Call911, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runAllHandlers(ingestInput, ir);
        }

        @Test
        public void AllHandlersCheckmBatchContainers() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckmBatchContainers - Batch manifest of containers");
                InputFile ingestInput = new InputFile(SampleFile.BatchContainers, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.container);
        }

        @Test
        public void AllHandlersCheckmBatchFiles() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckmBatchFiles - Batch manifest of files");
                InputFile ingestInput = new InputFile(SampleFile.BatchFiles, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.file);
        }

        @Test
        public void AllHandlersCheckmBatchManifests() throws IOException, TException {
                System.out.println("[IngestHandlerUnitTest] AllHandlersCheckmBatchManifests - Batch manifest of manifest");
                InputFile ingestInput = new InputFile(SampleFile.BatchManifests, createBatchDir());
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                runQueueHandlerTests(ingestInput, ir, PackageTypeEnum.manifest);
        }
}
