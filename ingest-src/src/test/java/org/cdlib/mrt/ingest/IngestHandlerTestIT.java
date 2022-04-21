package org.cdlib.mrt.ingest;

import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.handlers.HandlerTransfer;
import org.cdlib.mrt.ingest.handlers.HandlerInventoryQueue;
import org.cdlib.mrt.ingest.handlers.HandlerMinter;
import org.cdlib.mrt.utility.TException;
import org.json.JSONException;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class IngestHandlerTestIT extends IngestHandlerTest {
        private int port = 4567;
        public static final String SHOULDER = "99999/aa";

        public IngestHandlerTestIT() throws TException {
                super();
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set");
                }
        }

        @Test
        public void testPortVariable() {
                assertTrue(port > 0);
        }

        @Test
        public void callMockEzid() throws IOException, InterruptedException {
                String url = String.format("http://localhost:%d/shoulder/%s", port, SHOULDER);
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .build();
                HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                String ark = response.body().trim().replace("success: ", "");
                System.out.println(ark);
                String re = "^" + SHOULDER + "[0-9]{7,7}$";
                assertTrue(ark.matches(re));
        }

        // @Test
        public void HandlerTransfer() throws TException, IOException {
                InputFile ingestInput = new InputFile(SampleFile.SingleFileWithDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);
                runHandlerDescribeTests(ingestInput, ir);
                // Requires a storage service
                HandlerResult hr = new HandlerTransfer().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());

        }

        @Test
        public void HandlerMinter() throws TException, IOException, JSONException {
                String url  = ps.getObjectMinterURL().toString();
                url = url.replace("4567", Integer.toString(port));
                ps.setObjectMinterURL(new URL(url));

                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);

                // force minter to set primary id
                ingestInput.getJobState().setPrimaryID(null);

                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);

                ps.setMisc(ingestConfig.getIngestConf().getString("ezid"));
                assertNull(ingestInput.getJobState().getPrimaryID());
                HandlerResult hr = new HandlerMinter().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
                assertTrue(ingestInput.getJobState().getPrimaryID().getValue().matches("^ark:/99999/fk[0-9]{8,8}$"));
        }

        public void runHandlerTransferTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerTransfer().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

        public void runHandlerInventoryQueueTests(InputFile ingestInput, IngestRequest ir) throws TException, IOException {
                HandlerResult hr = new HandlerInventoryQueue().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

        @Test
        public void AllHandlersCheckm4BlocksWithQueue() throws IOException, TException, JSONException {
                InputFile ingestInput = new InputFile(SampleFile.FourBlocks, tempdir);
                InputStream in = ingestInput.sampleFile().getUrl().openStream();
                Files.copy(in, Paths.get(tempdir.resolve(ingestInput.getCopyPath()).toString()),
                                StandardCopyOption.REPLACE_EXISTING);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());

                JobState js = ingestInput.getJobState();
                js.setMisc(ingestConfig.getQueueConf().getString("QueueService"));
                js.setExtra(ingestConfig.getIngestConf().getString("ingestLock"));
                js.setTargetStorage(ps.getTargetStorage());

                js.setObjectState("foo");

                runAllHandlers(ingestInput, ir);
        }


}
