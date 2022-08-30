package org.cdlib.mrt.ingest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import javax.xml.xpath.XPathFactory;
//https://stackoverflow.com/a/22939742/3846548
import org.apache.xpath.jaxp.XPathFactoryImpl;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class ServiceDriverIT {
        private int port = 8080;
        private int mockport = 8096;
        private String cp = "mrtingest";
        private DocumentBuilder db;
        private XPathFactory xpathfactory;

        public ServiceDriverIT() throws ParserConfigurationException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                        mockport = Integer.parseInt(System.getenv("mock-merritt-it.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port = " + port);
                        System.err.println("mock-merritt-it.port = " + mockport);
                }
                db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                xpathfactory = new XPathFactoryImpl();
        }

        public String getContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    if (status > 0) {
                        assertEquals(status, response.getStatusLine().getStatusCode());
                    }

                    if (status > 300) {
                        return "";
                    }
                    String s = new BasicResponseHandler().handleResponse(response).trim();
                    assertFalse(s.isEmpty());
                    return s;
                }
        }

        public JSONObject getJsonContent(String url, int status) throws HttpResponseException, IOException, JSONException {
                String s = getContent(url, status);
                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                return json;
        }

        public List<String> getZipContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    assertEquals(status, response.getStatusLine().getStatusCode());

                    List<String> entries = new ArrayList<>();
                    if (status < 300) {
                            try(ZipInputStream zis = new ZipInputStream(response.getEntity().getContent())){
                                    for(ZipEntry ze = zis.getNextEntry(); ze != null; ze = zis.getNextEntry()) {
                                            entries.add(ze.getName());
                                    }
                            }
                    }

                    return entries;
                }
        }

        public String getJsonString(JSONObject j, String key, String def) throws JSONException {
                return j.has(key) ? j.get(key).toString() :  def;
        }


        public JSONObject getJsonObject(JSONObject j, String key) throws JSONException {
                if (j.has(key) && (j.get(key) instanceof JSONObject)) {
                        return j.getJSONObject(key);
                }
                return new JSONObject();
        }

        public JSONArray getJsonArray(JSONObject j, String key) throws JSONException {
                JSONArray ja = new JSONArray();
                if (j.has(key)) {
                        if (j.get(key) instanceof JSONArray) {
                                return j.getJSONArray(key);
                        }
                        if (j.get(key) instanceof JSONObject) {
                                ja.put(j.get(key));
                        }
                }
                return ja;
        }

        public void clearQueue(String endpoint, String queue) throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/%s/%s", port, cp, endpoint, queue);
                JSONObject json = getJsonContent(url, 200);

                JSONObject j = getJsonObject(json,"que:queueState"); 
                j = getJsonObject(j, "que:queueEntries");
                JSONArray ja = getJsonArray(j, "que:queueEntryState");
                for (int i=0; i < ja.length(); i++) {
                        JSONObject jo = ja.getJSONObject(i);
                        String status = getJsonString(jo, "que:status", "").toLowerCase();
                        if (status.equals("deleted")) {
                                continue;
                        }
                        String id = getJsonString(jo, "que:iD", "");
                        clearQueueEntry(queue, id, status);
                }
        }

        public int countQueue(int sleep, String endpoint, String queue) throws IOException, JSONException, InterruptedException {
                Thread.sleep(sleep);
                String url = String.format("http://localhost:%d/%s/admin/%s/%s", port, cp, endpoint, queue);
                JSONObject json = getJsonContent(url, 200);
                int count = 0;

                JSONObject j = getJsonObject(json,"que:queueState"); 
                j = getJsonObject(j, "que:queueEntries");
                JSONArray ja = getJsonArray(j, "que:queueEntryState");
                for (int i=0; i < ja.length(); i++) {
                        JSONObject jo = ja.getJSONObject(i);
                        String status = getJsonString(jo, "que:status", "").toLowerCase();
                        if (status.equals("deleted")) {
                                continue;
                        }
                        count++;
                }
                System.out.println(String.format("%s/%s: %d", endpoint, queue, count));
                return count;
        }

        public void clearQueueEntry(String queue, String id, String status) throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/deleteq/%s/%s/%s", port, cp, queue, id, status);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);                        
                        HttpResponse response = client.execute(post);
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertEquals(200, response.getStatusLine().getStatusCode());
                }

        }

        @Before
        public void clearQueueDirectory() throws IOException, JSONException {
                clearQueue("queue", "ingest");
                clearQueue("queue-inv", "mrt.inventory.full");
                String url = String.format("http://localhost:%d/ingest-queue", mockport);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpDelete post = new HttpDelete(url);                        
                        HttpResponse response = client.execute(post);
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertEquals(200, response.getStatusLine().getStatusCode());
                }

        }

        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("ing:ingestServiceState"));
                String status = json.getJSONObject("ing:ingestServiceState").getString("ing:submissionState");
                assertEquals("thawed", status);
        }

        public JSONObject ingestFile(String url, File file, boolean batch) throws IOException, JSONException {
                return ingestFile(url, file, "", "", batch);
        }

        public JSONObject ingestFile(String url, File file, String localId, boolean batch) throws IOException, JSONException {
                return ingestFile(url, file, localId, "", batch);
        }

        public JSONObject submitResponse(HttpResponse response, boolean batch) throws IOException, JSONException {
                assertEquals(200, response.getStatusLine().getStatusCode());


                String s = new BasicResponseHandler().handleResponse(response).trim();
                assertFalse(s.isEmpty());

                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                if (batch) {
                        assertTrue(json.has("bat:batchState"));
                        assertEquals("QUEUED", json.getJSONObject("bat:batchState").getString("bat:batchStatus"));

                } else {
                        assertTrue(json.has("job:jobState"));
                        assertEquals("COMPLETED", json.getJSONObject("job:jobState").getString("job:jobStatus"));
                        String jobid =  json.getJSONObject("job:jobState").getString("job:jobID");
                        String primaryid = json.getJSONObject("job:jobState").getString("job:primaryID");
                        //GET http://uc3-mrtdocker01x2-dev.cdlib.org:8080/mrtingest/admin/queue-inv/mrt.inventory.full
                        //POST /deleteq/{queue}/{id}/{fromState}
                }
                return json;

        }

        public JSONObject ingestFile(String url, File file, String localId, String primaryId, boolean batch) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                        builder.addBinaryBody(
                          "file", file, ContentType.DEFAULT_TEXT, file.getName()
                        );
                        builder.addTextBody("profile", "merritt_test_content");
                        if (localId != null) {
                                if (!localId.isEmpty()) {
                                        builder.addTextBody("localIdentifier", localId);
                                }
                        }
                        if (primaryId != null) {
                                if (!primaryId.isEmpty()) {
                                        builder.addTextBody("primaryIdentifier", primaryId);
                                }
                        }
                        builder.addTextBody("submitter", "integration-tests");
                        builder.addTextBody("responseForm", "json");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        return submitResponse(response, batch);
                }

        }

        public JSONObject ingestFromUrl(String url, String contenturl, String filename, String type, boolean batch) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                        URL payload = new URL(contenturl);
                        builder.addBinaryBody(
                          "file", payload.openStream(), ContentType.DEFAULT_TEXT, filename
                        );
                        builder.addTextBody("submitter", "integration-tests");
                        builder.addTextBody("type", type);
                        builder.addTextBody("profile", "merritt_test_content");
                        builder.addTextBody("responseForm", "json");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        return submitResponse(response, batch);
                }

        }

        @Test
        public void FileManifestIngest() throws IOException, JSONException, InterruptedException {
                String filename = "4blocks.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFromUrl(url, contenturl, filename, "manifest", false);
                assertEquals(0, countQueue(1000, "queue", "ingest"));
                assertEquals(1, countQueue(1000, "queue-inv", "mrt.inventory.full"));
        }

        @Test
        public void BatchManifestIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfManifests.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                ingestFromUrl(url, contenturl, filename, "batch-manifest", true);
                assertEquals(3, countQueue(2000, "queue", "ingest"));
                assertEquals(3, countQueue(60000, "queue-inv", "mrt.inventory.full"));
        }

        @Test
        public void BatchFilesIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfFiles.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                ingestFromUrl(url, contenturl, filename, "single-file-batch-manifest", true);
                assertEquals(3, countQueue(2000, "queue", "ingest"));
                assertEquals(3, countQueue(40000, "queue-inv", "mrt.inventory.full"));
        }

        @Test
        public void SimpleFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                assertEquals(0, countQueue(1000, "queue", "ingest"));
                assertEquals(1, countQueue(1000, "queue-inv", "mrt.inventory.full"));
        }

        @Test
        public void QueueFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), true);
                assertEquals(1, countQueue(1000, "queue", "ingest"));
                assertEquals(1, countQueue(20000, "queue-inv", "mrt.inventory.full"));
        }

        @Test
        public void SimpleFileIngestWithLocalid() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), "localid", false);
                /*

http://uc3-mrtdocker01x2-dev.cdlib.org:8080/mrtingest/admin/queue/ingest
{
que:queueState: {
xmlns:que: "http://uc3.cdlib.org/ontology/mrt/ingest/queue",
que:queueEntries: {
que:queueEntryState: {
que:user: "integration-tests",
que:fileType: "file",
que:queueNode: "/ingest",
que:jobID: "jid-04af2cb5-380d-4b48-a7af-95bae851f4f5",
que:batchID: "bid-4bbf8b26-686f-49e6-b1b9-134a823fa25b",
que:profile: "merritt_test_content",
que:date: "Fri Aug 26 23:58:45 UTC 2022",
que:status: "Completed",
que:name: "foo.txt",
que:iD: "mrtQ-030000000000"
}
}
}
}

http://uc3-mrtdocker01x2-dev.cdlib.org:8080/mrtingest/admin/queue-inv/mrt.inventory.full
{"que:queueState":{"xmlns:que":"http://uc3.cdlib.org/ontology/mrt/ingest/queue","que:queueEntries":{"que:queueEntryState":{"que:queueNode":"/mrt.inventory.full","que:manifestURL":"http://mock-merritt-it:4567/manifest/7777/ark%3A%2F99999%2Ffk46558509","que:date":"Fri Aug 26 23:58:45 UTC 2022","que:status":"Pending","que:iD":"mrtQ-000000000000"}}}}
                 */
        }

        @Test
        public void SimpleFileIngestWithArk() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
        }

        @Test
        public void SimpleFileIngestWithArkAndUpdate() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                url = String.format("http://localhost:%d/%s/update-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.txt"), false);
        }

        @Test
        public void SimpleFileIngestWithUpdate() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                String prim = json.getJSONObject("job:jobState").getString("job:primaryID");
                url = String.format("http://localhost:%d/%s/update-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.txt"), "", prim, false);
        }

        @Test
        public void SimpleZipIngest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.zip"), false);
        }

        //POST submit-object - direct
        // submit file
        // submit zip
        // submit manifest
        //POST submit-object/scheme/shoulder/ark - direct
        //POST update-object - direct
        //POST update-object/scheme/shoulder/ark - direct
        //POST request-identifier (from ezid)

        //POST add - queue
        //POST submit - queue
        //POST add/scheme/shoulder/ark -  queue
        //POST update - queue
        //POST update/scheme/shoulder/ark - queue 

        //admin functions

}
