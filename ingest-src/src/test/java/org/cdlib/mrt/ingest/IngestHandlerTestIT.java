package org.cdlib.mrt.ingest;

import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.handlers.HandlerTransfer;
import org.cdlib.mrt.ingest.handlers.HandlerMinter;
import org.cdlib.mrt.utility.TException;
import org.json.JSONException;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

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
                InputFile ingestInput = new InputFile(SampleFile.SingleFileBadDigest, tempdir);
                IngestRequest ir = ingestInput.getIngestRequest(this.im, ingestInput.getJobState());
                ingestInput.moveToIngestDir();

                runHandlerInitializeTests(ingestInput, ir);
                runHandlerAcceptTests(ingestInput, ir);

                ps.setMisc(ingestConfig.getIngestConf().getString("ezid"));
                HandlerResult hr = new HandlerMinter().handle(ps, ir, ingestInput.getJobState());
                assertTrue(hr.getSuccess());
        }

}
