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
                        //System.err.println("it-server.port = " + port);
                        //System.err.println("mock-merritt-it.port = " + mockport);
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

        public int countQueue(int tries, int expected, String endpoint, String queue) throws IOException, JSONException, InterruptedException {
                int count = 0;
                for(int ii = 0; ii < tries && count != expected; ii++) {
                        Thread.sleep(1000);
                        count = 0;
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
                                count++;
                        }        
                }
                //System.out.println(String.format("%s/%s: %d -- %d", endpoint, queue, count, expected));
                assertEquals(expected, count);
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
                /*
                String url = String.format("http://localhost:%d/ingest-queue", mockport);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpDelete post = new HttpDelete(url);                        
                        HttpResponse response = client.execute(post);
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertEquals(200, response.getStatusLine().getStatusCode());
                }
                */
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
                JSONObject json = ingestFromUrl(url, contenturl, filename, "manifest", false);
                
                //System.out.println(json.toString(2));
                
                countQueue(3, 0, "queue", "ingest");
                countQueue(3, 1,"queue-inv", "mrt.inventory.full");
        }

        @Test
        public void BatchManifestIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfManifests.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFromUrl(url, contenturl, filename, "batch-manifest", true);
                
                //System.out.println(json.toString(2));
                
                countQueue(3, 3, "queue", "ingest");
                countQueue(90, 3, "queue-inv", "mrt.inventory.full");
        }

        @Test
        public void BatchFilesIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfFiles.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                ingestFromUrl(url, contenturl, filename, "single-file-batch-manifest", true);
                countQueue(3, 3, "queue", "ingest");
                countQueue(60, 3, "queue-inv", "mrt.inventory.full");
        }

        @Test
        public void SimpleFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                countQueue(3, 0, "queue", "ingest");
                countQueue(3, 1, "queue-inv", "mrt.inventory.full");
        }

        @Test
        public void SimpleFileIngestCheckJob() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                json = getJsonObject(json, "job:jobState");
                String bid = "JOB_ONLY";
                String jid = getJsonString(json, "job:jobID", "");
                String ark = getJsonString(json, "job:primaryID", "");

                countQueue(3, 0, "queue", "ingest");
                countQueue(3, 1, "queue-inv", "mrt.inventory.full");

                url = String.format("http://localhost:%d/%s/admin/jid-erc/%s/%s", port, cp, bid, jid);
                json = getJsonContent(url, 200);
                json = getJsonObject(json, "fil:jobFileState");
                json = getJsonObject(json, "fil:jobFile");
                assertEquals(ark, getJsonString(json, "fil:where-primary", ""));

                url = String.format("http://localhost:%d/%s/admin/jid-file/%s/%s", port, cp, bid, jid);
                json = getJsonContent(url, 200);
                // 8 system files will remain after submission is complete
                assertEquals(8, getFiles(json).size());

                url = String.format("http://localhost:%d/%s/admin/jid-manifest/%s/%s", port, cp, bid, jid);
                json = getJsonContent(url, 200);
                json = getJsonObject(json, "ingmans:manifestsState");
                assertEquals("", getJsonString(json, "ingmans:manifests", "N/A"));
        }

        public List<String> getFiles(JSONObject json) throws JSONException {
                json = getJsonObject(json, "fil:batchFileState");
                json = getJsonObject(json, "fil:jobFile");
                JSONArray jarr = getJsonArray(json, "fil:batchFile");
                ArrayList<String> files = new ArrayList<>();
                for(int i = 0; i < jarr.length(); i++) {
                        String f = getJsonString(jarr.getJSONObject(i), "fil:file", "");
                        files.add(f);
                }
                return files;
        }

        public List<String> getBids() throws JSONException, HttpResponseException, IOException {
                String url = String.format("http://localhost:%d/%s/admin/bids/1", port, cp);
                JSONObject json = getJsonContent(url, 200);
                return getFiles(json);
        }

        public List<String> getJobs(String bid) throws JSONException, HttpResponseException, IOException {
                String url = String.format("http://localhost:%d/%s/admin/bid/%s", port, cp, bid);
                JSONObject json = getJsonContent(url, 200);
                return getFiles(json);
        }

        @Test
        public void QueueFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);
                json = getJsonObject(json, "bat:batchState");
                String bat = getJsonString(json, "bat:batchID", "");
                countQueue(3, 1, "queue", "ingest");
                countQueue(30, 1, "queue-inv", "mrt.inventory.full");

                assertTrue(getBids().contains(bat));
                assertEquals(1, getJobs(bat).size());
        }

        @Test
        public void QueueFileIngestCatchLock() throws IOException, JSONException, InterruptedException {
                //This ark has a time delay in mock-merritt-it to allow the catch of a lock
                String url = String.format("http://localhost:%d/%s/poster/submit/ark/9999/2222", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);
                json = getJsonObject(json, "bat:batchState");
                String bat = getJsonString(json, "bat:batchID", "");

                for(int ii=0; ii<10; ii++) {
                        Thread.sleep(1000);
                        url = String.format("http://localhost:%d/%s/admin/lock/mrt.lock", port, cp);
                        json = getJsonContent(url, 200);
        
                        json = getJsonObject(json, "loc:lockState");
                        json = getJsonObject(json, "loc:lockEntries");
                        JSONArray jarr = getJsonArray(json, "loc:lockEntryState");
                        if (jarr.length() > 0) {
                                ArrayList<String> ids = new ArrayList<>();
                                for(int i = 0; i < jarr.length(); i++) {
                                        ids.add(getJsonString(jarr.getJSONObject(i), "loc:iD", ""));
                                }
                                assertTrue(ids.contains("ark-9999-2222"));        
                        }
                }

                countQueue(3, 1, "queue", "ingest");
                countQueue(30, 1, "queue-inv", "mrt.inventory.full");

                assertTrue(getBids().contains(bat));
                assertEquals(1, getJobs(bat).size());
        }

        @Test
        public void SimpleFileIngestWithLocalid() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), "localid", false);
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
        public void SimpleFileIngestWithUpdate() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                String prim = json.getJSONObject("job:jobState").getString("job:primaryID");

                countQueue(20, 1, "queue-inv", "mrt.inventory.full");

                url = String.format("http://localhost:%d/%s/update-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.txt"), "", prim, false);
        }

        @Test
        public void SimpleZipIngest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.zip"), false);
        }

        public List<String> getQueueNames(String endpoint) throws HttpResponseException, IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/%s", port, cp, endpoint);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "ingq:ingestQueueNameState");
                json = getJsonObject(json, "ingq:ingestQueueName");
                JSONArray jarr = getJsonArray(json, "ingq:ingestQueue");

                ArrayList<String> arr = new ArrayList<>();
                for(int i = 0; i < jarr.length(); i++) {
                        String s = getJsonString(jarr.getJSONObject(i), "ingq:node", "");
                        if (s.isEmpty()) {
                                continue;
                        }
                        arr.add(s);
                }
                return arr;
        }

        public void testQueueValues(String endpoint, String list) throws HttpResponseException, IOException, JSONException {
                List<String> queues = getQueueNames(endpoint);
                String[] vals = list.split(",");
                assertEquals(vals.length, queues.size());
                for(String s: vals) {
                        assertTrue(queues.contains(s));
                }
        }

        @Test
        public void TestQueueNames() throws IOException, JSONException {
                testQueueValues("queues", "ingest");
                testQueueValues("queues-inv", "/mrt.inventory.full");
                testQueueValues("queues-acc", "/accessSmall.1,/accessLarge.1");
        }


        @Test
        public void TestLocks() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/locks", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "ingl:ingestLockNameState");
                json = getJsonObject(json, "ingl:ingestLockName");
                json = getJsonObject(json, "ingl:ingestLock");
                assertEquals("/mrt.lock", getJsonString(json, "ingl:node", ""));

                url = String.format("http://localhost:%d/%s/admin/lock/mrt.lock", port, cp);
                json = getJsonContent(url, 200);
                json = getJsonObject(json, "loc:lockState");
                assertTrue(json.has("loc:lockEntries"));
                assertEquals("", getJsonString(json, "loc:lockEntries", "N/A"));
        }

        @Test
        public void TestProfileNames() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pros:profilesState");
                json = getJsonObject(json, "pros:profiles");
                json = getJsonObject(json, "pros:profileFile");
                assertEquals("merritt_test_content", getJsonString(json, "pros:file", ""));
        }

        @Test
        public void TestAdminProfileNames() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles/admin", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pros:profilesState");
                json = getJsonObject(json, "pros:profiles");
                json = getJsonObject(json, "pros:profileFile");
                assertEquals("admin/docker/collection/merritt_test", getJsonString(json, "pros:file", ""));
        }

        @Test
        public void TestProfileFull() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles-full", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "prosf:profilesFullState");
                json = getJsonObject(json, "prosf:profilesFull");
                json = getJsonObject(json, "prosf:profileState");
                assertEquals("merritt_test_content", getJsonString(json, "prosf:profileID", ""));
        }

        @Test
        public void TestProfileByName() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profile/merritt_test_content", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pro:profileState");
                assertEquals("merritt_test_content", getJsonString(json, "pro:profileID", ""));
        }

        /*
        POST @Path("/requeue/{queue}/{id}/{fromState}")
        POST @Path("/deleteq/{queue}/{id}/{fromState}")
        POST @Path("/{action: hold|release}/{queue}/{id}")
        POST @Path("/release-all/{queue}/{profile}")
        POST @Path("/submission/{request: freeze|thaw}/{collection}")
        POST @Path("/submissions/{request: freeze|thaw}")
        POST @Path("/profile/{type: profile|collection|owner|sla}")
        */
}
