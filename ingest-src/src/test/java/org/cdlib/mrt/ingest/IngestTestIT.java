package org.cdlib.mrt.ingest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

public class IngestTestIT {
    private int port = 4567;
    public static final String SHOULDER = "99999/aa";

    public IngestTestIT() {
        try {
            port = Integer.parseInt(System.getenv("it-server.port"));
        } catch(NumberFormatException e) {
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
        HttpResponse<String> response =
          client.send(request, BodyHandlers.ofString());
        String ark = response.body().trim().replace("success: ", "");
        System.out.println(ark);
        String re = "^" + SHOULDER + "[0-9]{7,7}$";
        assertTrue(ark.matches(re));
    }

    @Test
    public void pretend() {
        BatchState bs = new BatchState();
        System.out.println(bs);
    }

}
