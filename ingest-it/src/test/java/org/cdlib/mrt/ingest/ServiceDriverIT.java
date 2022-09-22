package org.cdlib.mrt.ingest;

import org.junit.Before;
import org.junit.Test;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import javax.xml.xpath.XPathFactory;
//https://stackoverflow.com/a/22939742/3846548
import org.apache.xpath.jaxp.XPathFactoryImpl;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Merritt Ingest Integration Test Driver
 * 
 * This test driver depends on the following stack of services in order to execute.  These services can be launched with docker-compose or with docker-maven-plugin.
 *
 * - tomcat: runs the latest built version of the ingest service.  The ingest-it sub project contains a specialized configuration to workin within the integration test stack.
 * - mock-merritt-it: mock implementation of merritt services (storage, inventory, ezid) as well as a content provider of test data
 *   - https://github.com/CDLUC3/merritt-docker/tree/main/mrt-inttest-services/mock-merritt-it
 * - zookeeper
 * - smtp: for ingest handlers that send mail
 * 
 * This code also re-uses sample test data in https://github.com/CDLUC3/mrt-doc/tree/main/sampleFiles.
 * - An internet connection is required to retrieve these assets.
 * 
 * The mock-merritt-it service can be sent a "/status/stop" command to tell it to return a 404 for test data requests.  The service can be re-enabled with a "/status/start" command.
 * - This is useful to trigger a queue failure that can be re-queued.
 * 
 * Note about Merritt's use of JSON
 * 
 * The Merritt Core libraries handle JSON in a non-standard fashion.
 * 
 * When a JSON Object can contain a JSON array, the following behavior occurs
 * - If more than 2 items exist, they are serialized as a JSONArray
 * - If 1 item exists, it is serialized as a JSONObject
 * - If no items exist, it is serialized as an empty string.
 * 
 * This test code has not been written to trap all serialization variations.
 */
public class ServiceDriverIT {
        private int port = 8080;
        private int mockport = 8096;
        private String cp = "mrtingest";
        private DocumentBuilder db;
        private XPathFactory xpathfactory;
        private String profile = "merritt_test_content";

        /*
         * Initialize the test class
         */
        public ServiceDriverIT() throws ParserConfigurationException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                        mockport = Integer.parseInt(System.getenv("mock-merritt-it.port"));
                } catch (NumberFormatException e) {
                        //use default ports
                }
                db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                xpathfactory = new XPathFactoryImpl();
        }

        /**
         * Make an http request, verify the http response status, return the result as a string
         * @param request HttpGet, HttpPost or HttpDelete request to execute
         * @param status Expected status code value.  If zero, do not validate the return status
         * @return the response from the rquest as a String
         */
        public String getContent(HttpRequestBase request, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
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

        /**
         * Create an HttpGet request for the specified url
         * @param url to retrieve 
         * @param status Expected status code value.  If zero, do not validate the return status
         * @return the response from the rquest as a String
         */
        public String getContent(String url, int status) throws HttpResponseException, IOException {
                return getContent(new HttpGet(url), status);
        }

        /**
         * Make an http request, verify the http response status, return the result as a string
         * @param request HttpGet, HttpPost or HttpDelete request to execute
         * @param status Expected status code value.  If zero, do not validate the return status
         * @return the response from the rquest as a JsonObject
         */
        public JSONObject getJsonContent(HttpRequestBase request, int status) throws HttpResponseException, IOException, JSONException {
                String s = getContent(request, status);
                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                return json;
        }

        /**
         * Create an HttpGet request for the specified url
         * @param url to retrieve 
         * @param status Expected status code value.  If zero, do not validate the return status
         * @return the response from the rquest as a Json object
         */
        public JSONObject getJsonContent(String url, int status) throws HttpResponseException, IOException, JSONException {
                return getJsonContent(new HttpGet(url), status);
        }

        /**
         * Helper method to return a Json string from a Json object
         * @param j json object to parse
         * @param key key to look up in the json object
         * @param def default value if a key is not present
         * @return the String value from the JsonObject or the default value
         */
        public static String getJsonString(JSONObject j, String key, String def) throws JSONException {
                return j.has(key) ? j.get(key).toString() :  def;
        }


       /**
         * Helper method to return a Json object from a Json object
         * @param j json object to parse
         * @param key key to look up in the json object
         * @return the found JSONObject, otherwise an empty JSON object is returned
         */
        public static JSONObject getJsonObject(JSONObject j, String key) throws JSONException {
                if (j.has(key) && (j.get(key) instanceof JSONObject)) {
                        return j.getJSONObject(key);
                }
                return new JSONObject();
        }

       /**
         * Helper method to return a Json array from a Json object
         * @param j json object to parse
         * @param key key to look up in the json object
         * @return the found JSONArray, otherwise an empty JSON array is returned.  A single JSONObject will be placed into an array if necessary.
         */
        public static JSONArray getJsonArray(JSONObject j, String key) throws JSONException {
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

        /**
         * Identify all entries of a queue that are not in a Deleted state.  Call clearQueueEntry to set the queue entry state to deleted.
         * @param endpoint queue, queue-inv, or queue-acc
         * @param queue name of the specific queue. For integration test purposes, only 1 ingest queue will exist
         */
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

        /**
         * Change the status of a specific queue entry to "Deleted".  Note that this does not delete the actual entry from the queue.
         * Queue entries will remain present until the docker stack is deleted and restarted.
         * @param queue name of the queue
         * @param id id of the queue entry
         * @param status current status of the queue entry
         * @throws IOException
         * @throws JSONException
         */
        public void clearQueueEntry(String queue, String id, String status) throws IOException, JSONException {
                if (status.equals("held")) {
                        String url = String.format("http://localhost:%d/%s/admin/release/%s/%s", port, cp, queue, id);
                        try (CloseableHttpClient client = HttpClients.createDefault()) {
                                JSONObject json = getJsonContent(new HttpPost(url), 200);
                                json = getJsonObject(json, "ques:queueEntryState");
                                status = getJsonString(json, "ques:status", "NA").toLowerCase();
                        }        
                }

                String url = String.format("http://localhost:%d/%s/admin/deleteq/%s/%s/%s", port, cp, queue, id, status);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        getJsonContent(new HttpPost(url), 200);
                }

        }

        /**
         * Find an ingest queue entry by batch id and job id
         * @param endpoint should always be "queue"
         * @param queue should always be "ingest"
         * @param bid batch id to locate
         * @param jid job id to locate
         * @return a JSON representation of the queue entry
         */
        public JSONObject findQueueEntry(String endpoint, String queue, String bid, String jid) throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/%s/%s", port, cp, endpoint, queue);
                JSONObject json = getJsonContent(url, 200);

                JSONObject j = getJsonObject(json,"que:queueState"); 
                j = getJsonObject(j, "que:queueEntries");
                JSONArray ja = getJsonArray(j, "que:queueEntryState");
                for (int i=0; i < ja.length(); i++) {
                        JSONObject jo = ja.getJSONObject(i);
                        if (getJsonString(jo, "que:batchID", "").equals(bid)) {
                                if (getJsonString(jo, "que:jobID", "").equals(jid)) {
                                        return jo;
                                }         
                        } 
                }
                return new JSONObject(); 
        }

        /**
         * Count the number of items in a queue that are not in a deleted status
         * @param tries Number of times to repeat the test before quitting - there will be a sleep delay between tries
         * @param expected Expected count to find
         * @param endpoint queue endpoint to query
         * @param queue queue name to query
         * @return the count of items found
          */
        public int countQueue(int tries, int expected, String endpoint, String queue) throws IOException, JSONException, InterruptedException {
                return countQueue(tries, expected, endpoint, queue, "");
        }

        /**
         * 
        * Count the number of items in a queue that have a specific status
         * @param tries Number of times to repeat the test before quitting - there will be a sleep delay between tries
         * @param expected Expected count to find
         * @param endpoint queue endpoint to query
         * @param queue queue name to query
         * @param teststatus Status value to verify.  If blank, count the number of items that are not in a deleted status
         * @return the count of items found
         */
        public int countQueue(int tries, int expected, String endpoint, String queue, String teststatus) throws IOException, JSONException, InterruptedException {
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
                                if (teststatus.isEmpty()) {
                                        if (!status.equals("deleted")) {
                                                count++;
                                        }        
                                } else if (teststatus.equals(status)){
                                        count++;                                        
                                }
                        }        
                }
                assertEquals(expected, count);
                return count;
        }

        /**
         * Reset that status of the test stack before each rest case.
         * - Mark all ingest queue items as deleted
         * - Mark all inventory queue items as deleted
         * - Thaw submission processing (in case it had been frozen)
         * - Thaw submission processing for the test collection (in case it had been frozen)
         * - Tell the mock test service to serve data (in case it had been set to not return data)
         */
        @Before
        public void clearQueueDirectory() throws IOException, JSONException {
                clearQueue("queue", "ingest");
                clearQueue("queue-inv", "mrt.inventory.full");
                String url = String.format("http://localhost:%d/%s/admin/submissions/thaw", port, cp);
                freezeThaw(url, "ing:submissionState", "thawed");
                url = String.format("http://localhost:%d/%s/admin/submission/thaw/%s", port, cp, profile);
                freezeThaw(url, "ing:collectionSubmissionState", "");
                url = String.format("http://localhost:%d/status/start", mockport, cp);
                getJsonContent(new HttpPost(url), 200);
        }

        /**
         * Test the Ingest state endpoint
         */
        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("ing:ingestServiceState"));
                String status = json.getJSONObject("ing:ingestServiceState").getString("ing:submissionState");
                assertEquals("thawed", status);
        }

        /**
         * Formulate a POST request to trigger an ingest submission
         * @param url to the submission endpoint
         * @param file file to be ingested
         * @param batch set to true if the endpoint will queue jobs (sync vs async)
         * @return Json object conveying submission status
         */
        public JSONObject ingestFile(String url, File file, boolean batch) throws IOException, JSONException {
                return ingestFile(url, file, "", "", batch);
        }

        /**
         * Formulate a POST request to trigger an ingest submission
         * @param url to the submission endpoint
         * @param file file to be ingested
         * @param localId if not empty, set a localid for the submission
         * @param batch set to true if the endpoint will queue jobs (sync vs async)
         * @return Json object conveying submission status
         */
        public JSONObject ingestFile(String url, File file, String localId, boolean batch) throws IOException, JSONException {
                return ingestFile(url, file, localId, "", batch);
        }

        /**
         * Formulate a POST request to trigger ingest submission.  Call submitResponse to verify the response status.
         * @param url to the submission endpoint
         * @param file file to be ingested
         * @param localId if not empty, set a localid for the submission
         * @param primaryId if not empty, set a primary id for the submission
         * @param batch set to true if the endpoint will queue jobs (sync vs async)
         * @return Json object conveying submission status
         */
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
                        if (file.getName().endsWith(".checkm")){
                                builder.addTextBody("type", "manifest");
                        }
                        builder.addTextBody("submitter", "integration-tests");
                        builder.addTextBody("responseForm", "json");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        return submitResponse(response, batch);
                }

        }

        /**
         * Verify the response status from an ingest submission.  The response will vary for async vs sync requests.
         * @param response HttpResponse object from the submission request
         * @param batch If true, handle the response as a batch request that will be queued.  If not, handle the response as a single job.
         * @return the json response object
         */
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
                }
                return json;

        }

        /**
         * Formulate a POST request to submit content to ingest by URL
         * @param url to the submission endpoint
         * @param contenturl url to the content to submit
         * @param filename filename to use for the submitted content
         * @param type submission type to assign to the submission
         * @param batch set to true if the endpoint will queue jobs (sync vs async)
         * @return Json object conveying submission status
         */
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

        /**
         * Test the submission of a manifest by URL.
         * 
         * Note: this test downloads data from github.  An internet connection is needed to run the test.
         */
        @Test
        public void FileManifestIngest() throws IOException, JSONException, InterruptedException {
                String filename = "4blocks.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFromUrl(url, contenturl, filename, "manifest", false);
                               
                // due to async processing, no jobs should exist in the ingest queue
                countQueue(3, 0, "queue", "ingest");
                // one object should be reside on the inventory queue 
                countQueue(3, 1,"queue-inv", "mrt.inventory.full");
        }

        /**
         * Test the submission of a manifest file.
         * 
         * Note: this test retrieves test content from the mock-merritt-it container.
         */
        @Test
        public void TestManifest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/mock.checkm"), true);
                            
                // once complete, no jobs should remain in the ingest queue
                // TODO should teh tries be increased?
                countQueue(3, 0, "queue", "ingest");
                // once complete, one object should reside in the inventory queue
                countQueue(30, 1,"queue-inv", "mrt.inventory.full");
        }

        /**
         * Test the submission of a manifest file while forcing a submission failure
         * 
         * Note: this test retrieves test content from the mock-merritt-it container.
         * 
         * The mock-merritt-it service will be instructed to not deliver content.
         * 
         * Initially, the mock-merritt-it service will return a 404 when retrieving content.
         * 
         * The job will fail.
         * 
         * Next, the mock-merritt-it service will be instructed to resume content delivery.
         * 
         * The job will be re-queued and will succeed.
         */
        @Test
        public void TestManifestWithRequeue() throws IOException, JSONException, InterruptedException {
                // tell the mock-merritt-it service to temporarily suspend content delivery
                String url = String.format("http://localhost:%d/status/stop", mockport, cp);
                getJsonContent(new HttpPost(url), 200);

                url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/mock.checkm"), true);
                
                BidJid bidjid = new BidJid(json);
                verifyJid(bidjid);
                json = findQueueEntry("queue", "ingest", bidjid.bid(), bidjid.jid());

                String qid = getJsonString(json, "que:iD", "");

                // expect one failed job on the ingest queue
                countQueue(30, 1, "queue", "ingest", "failed");

                // tell the mock-merritt-it service to resume content delivery
                url = String.format("http://localhost:%d/status/start", mockport, cp);
                getJsonContent(new HttpPost(url), 200);

                // requeue the failed job
                url = String.format("http://localhost:%d/%s/admin/requeue/ingest/%s/failed", port, cp, qid);
                json = getJsonContent(new HttpPost(url), 200);

                // once processing is complete, no jobs shoudl remain on the ingest queue
                countQueue(30, 0, "queue", "ingest");
                // one object should exist in the inventory queue
                countQueue(30, 1, "queue-inv", "mrt.inventory.full");
        }

        @Test
        public void BatchManifestIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfManifests.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFromUrl(url, contenturl, filename, "batch-manifest", true);
                                
                // exepect to see 3 queued jobs 
                countQueue(3, 3, "queue", "ingest");
                // once processing is complete, expect to see 3 objects in the inventory queue
                countQueue(90, 3, "queue-inv", "mrt.inventory.full");
        }

        /**
         * Test the submission of a batch-manifest by URL.
         * 
         * Note: this test downloads data from github.  An internet connection is needed to run the test.
         */
        @Test
        public void BatchFilesIngest() throws IOException, JSONException, InterruptedException {
                String filename = "sampleBatchOfFiles.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                ingestFromUrl(url, contenturl, filename, "single-file-batch-manifest", true);

                // exepect to see 3 queued jobs 
                countQueue(3, 3, "queue", "ingest");
                // once processing is complete, expect to see 3 objects in the inventory queue
                countQueue(60, 3, "queue-inv", "mrt.inventory.full");
        }

        /**
         * Test the submission of a single file.
         */
        @Test
        public void SimpleFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);

                // exepect to see 0 queued jobs 
                countQueue(3, 0, "queue", "ingest");
                // once processing is complete, expect to see 1 objectin the inventory queue
                countQueue(3, 1, "queue-inv", "mrt.inventory.full");
        }

        /**
         * Test the submission of a single file.  Test the endpoints to view job-related data.
        */
        @Test
        public void SimpleFileIngestCheckJob() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                json = getJsonObject(json, "job:jobState");
                String bid = "JOB_ONLY";
                String jid = getJsonString(json, "job:jobID", "");
                String ark = getJsonString(json, "job:primaryID", "");

                // exepect to see 0 queued jobs 
                countQueue(3, 0, "queue", "ingest");
                // once processing is complete, expect to see 1 objectin the inventory queue
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

        /**
         * Extract (from json) the set of files associated with a job
         */
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

        /**
         * Get a set of recently processed batch ids
         */
        public List<String> getBids() throws JSONException, HttpResponseException, IOException {
                String url = String.format("http://localhost:%d/%s/admin/bids/1", port, cp);
                JSONObject json = getJsonContent(url, 200);
                return getFiles(json);
        }

        /**
         * Get the set set of jobs assoicated with a batch
         */
        public List<String> getJobs(String bid) throws JSONException, HttpResponseException, IOException {
                String url = String.format("http://localhost:%d/%s/admin/bid/%s", port, cp, bid);
                JSONObject json = getJsonContent(url, 200);
                return getFiles(json);
        }

        /**
         * Queue the submission of a single file
         */
        @Test
        public void QueueFileIngest() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);
                json = getJsonObject(json, "bat:batchState");
                String bat = getJsonString(json, "bat:batchID", "");

                // expect 1 queued job
                countQueue(3, 1, "queue", "ingest");
                // expect 1 object in the inventory queue
                countQueue(30, 1, "queue-inv", "mrt.inventory.full");

                assertTrue(getBids().contains(bat));
                assertEquals(1, getJobs(bat).size());
        }

        /**
         * Queue the submission of a single file.  Detect the zookeeper lock in place as the submission processes.
         */
        @Test
        public void QueueFileIngestCatchLock() throws IOException, JSONException, InterruptedException {
                //This ark has a time delay in mock-merritt-it to allow the catch of a lock
                String url = String.format("http://localhost:%d/%s/poster/submit/ark/9999/2222", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);
                json = getJsonObject(json, "bat:batchState");
                String bat = getJsonString(json, "bat:batchID", "");

                // look for the presence of a zookeeper lock
                // the lock name should be derived from the submission's primary id (ark)
                boolean found = false;
                for(int ii=0; ii<10 && !found; ii++) {
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
                                found = true;      
                        }
                }
                assertTrue(found);

                // expect 1 queue job
                countQueue(3, 1, "queue", "ingest");
                // expect 1 object in the inventory queue
                countQueue(30, 1, "queue-inv", "mrt.inventory.full");

                assertTrue(getBids().contains(bat));
                assertEquals(1, getJobs(bat).size());
        }

        /**
         * Post a request to freeze/thaw submissions
         * @param url endpont to use for freeze/thaw
         * @param key key to use to confirm the resulting state
         * @param state value to verify for the freeze/thawed state
         * @return response fro the freeze/thaw endpoint
         */
        public JSONObject freezeThaw(String url, String key, String state) throws IOException, JSONException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        JSONObject json = getJsonContent(new HttpPost(url), 200);
                        assertEquals(
                                state, 
                                getJsonString(
                                        getJsonObject(
                                                json, 
                                                "ing:ingestServiceState"
                                        ), 
                                        key, 
                                        ""
                                )
                        );
                        return json;
                }

        }

        /**
         * Helper class to extract a bid and jid from a submission response.
         */
        class BidJid {
                private String bid = "";
                private String jid = "";

                BidJid(JSONObject json) throws JSONException {
                        JSONObject jsonbs = getJsonObject(
                                json,
                                "bat:batchState"
                        );
        
                        bid = getJsonString(
                                jsonbs,
                                "bat:batchID",
                                ""
                        );
        
                        JSONObject jsonjs = getJsonObject(
                                getJsonObject(
                                        jsonbs,
                                        "bat:jobStates"
                                ),
                                "bat:jobState"
                        );
        
                        jid = getJsonString(
                                jsonjs,
                                "bat:jobID",
                                ""
                        );
        
                } 

                public String bid() {
                        return this.bid;
                }

                public String jid() {
                        return this.jid;
                }

                public void setJid(String s) {
                        this.jid = s;
                }
        }

        /**
         * If a bid / jid cannot be extracted from a submission response, perform a lookup to locate the jid.
         * TODO: ask Mark why this is not always available.
         */
        public void verifyJid(BidJid bidjid) throws HttpResponseException, IOException, JSONException {
                if (bidjid.jid().isEmpty()) {
                        String url = String.format("http://localhost:%d/%s/admin/bid/%s", port, cp, bidjid.bid());
                        JSONObject json = getJsonContent(url, 200);
                        json = getJsonObject(json, "fil:batchFileState");
                        json = getJsonObject(json, "fil:jobFile");
                        json = getJsonObject(json, "fil:batchFile");
                        String jid = getJsonString(json, "fil:file", "");
                        bidjid.setJid(jid);
                }

        }

        /**
         * Queue a file ingest while submissions are frozen.  Thaw submsissions and resume processing.
         */
        @Test
        public void QueueFileIngestPauseSubmissions() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/admin/submissions/freeze", port, cp);
                JSONObject json = freezeThaw(url, "ing:submissionState", "frozen");

                url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);

                BidJid bidjid = new BidJid(json);
                verifyJid(bidjid);

                boolean found = false;
                for(int ii=0; ii<20 && !found; ii++) {
                        Thread.sleep(3000);

                        json = findQueueEntry("queue", "ingest", bidjid.bid(), bidjid.jid());

                        found = getJsonString(json, "que:status", "").equals("Pending");
                }
                assertTrue(found);

                url = String.format("http://localhost:%d/%s/admin/submissions/thaw", port, cp);
                json = freezeThaw(url, "ing:submissionState", "thawed");

                found = false;
                for(int ii=0; ii<20 && !found; ii++) {
                        Thread.sleep(3000);

                        json = findQueueEntry("queue", "ingest", bidjid.bid(), bidjid.jid());

                        found = getJsonString(json, "que:status", "").equals("Completed");
                }
                assertTrue(found);
        }

        /**
         * Que a file ingest while a specific collection is frozen.  Thaw the collection and resume processing.
         */
        @Test
        public void QueueFileIngestPauseCollection() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/admin/submission/freeze/%s", port, cp, profile);
                JSONObject json = freezeThaw(url, "ing:collectionSubmissionState", profile);

                url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                json = ingestFile(url, new File("src/test/resources/data/foo.txt"), true);

                BidJid bidjid = new BidJid(json);
                verifyJid(bidjid);

                Thread.sleep(3000);

                json = findQueueEntry("queue", "ingest", bidjid.bid(), bidjid.jid());
                assertEquals("Held", getJsonString(json, "que:status", ""));

                url = String.format("http://localhost:%d/%s/admin/submission/thaw/%s", port, cp, profile);
                json = freezeThaw(url, "ing:collectionSubmissionState", "");

                Thread.sleep(3000);

                json = findQueueEntry("queue", "ingest", bidjid.bid(), bidjid.jid());
                assertEquals("Held", getJsonString(json, "que:status", ""));

                String qid = getJsonString(json, "que:iD", "");
                assertNotEquals("", qid);

                // expect 1 job to be in a held state
                assertEquals(
                        1, 
                        countQueue(3, 1, "queue", "ingest", "held")
                );

                // release all held jobs for the profile
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        url = String.format("http://localhost:%d/%s/admin/release-all/ingest/%s", port, cp, profile);
                        json = getJsonContent(new HttpPost(url), 200);
                }

                // expect to see 1 completed job
                assertEquals(
                        1, 
                        countQueue(30, 1, "queue", "ingest", "completed")
                );

        }

        /**
         * Submit a single file with a local id.
         */
        @Test
        public void SimpleFileIngestWithLocalid() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), "localid", false);
        }

        /**
         * Submit a single file with a primary id specified.
         */
        @Test
        public void SimpleFileIngestWithArk() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
        }

        /**
         * Submit a single file with a primary id specified.  Perform an update on that object.
         */
        @Test
        public void SimpleFileIngestWithArkAndUpdate() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                url = String.format("http://localhost:%d/%s/update-object/ark/1111/2222", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.txt"), false);
        }

        /**
         * Submit a single file.  Locate the primary id and perform an update on that object.
         */
        @Test
        public void SimpleFileIngestWithUpdate() throws IOException, JSONException, InterruptedException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                JSONObject json = ingestFile(url, new File("src/test/resources/data/foo.txt"), false);
                String prim = json.getJSONObject("job:jobState").getString("job:primaryID");

                // expect to see 1 object present in the inventory queue
                countQueue(20, 1, "queue-inv", "mrt.inventory.full");

                url = String.format("http://localhost:%d/%s/update-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.txt"), "", prim, false);
        }

        /**
         * Submit a zip file to be ingested.
         */
        @Test
        public void SimpleZipIngest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/submit-object", port, cp);
                ingestFile(url, new File("src/test/resources/data/test.zip"), false);
        }

        /**
         * Obtain the queue names associted with a queue endpoint
         * @param endpoint queue, queue-inv, queue-acc
         * @return List of queue names
         */
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

        /**
         * Compare a list of queue names to a comma separated string
         * @param endpoint endpoint to query for queue names
         * @param list list of queue names to expect in the list
         */
        public void testQueueValues(String endpoint, String list) throws HttpResponseException, IOException, JSONException {
                List<String> queues = getQueueNames(endpoint);
                String[] vals = list.split(",");
                assertEquals(vals.length, queues.size());
                for(String s: vals) {
                        assertTrue(queues.contains(s));
                }
        }

        /**
         * Test that each queue endpoint returns the expected set of queue names
         */
        @Test
        public void TestQueueNames() throws IOException, JSONException {
                testQueueValues("queues", "ingest");
                testQueueValues("queues-inv", "/mrt.inventory.full");
                testQueueValues("queues-acc", "/accessSmall.1,/accessLarge.1");
        }


        /**
         * Test the lock endpoints.  Assume no locks are present.
         */
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

        /**
         * Test the admin/profiles endpoint returns the expected profile name
         */
        @Test
        public void TestProfileNames() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pros:profilesState");
                json = getJsonObject(json, "pros:profiles");
                JSONArray arr = getJsonArray(json, "pros:profileFile");
                ArrayList<String> names = new ArrayList<>();
                for(int i = 0; i < arr.length(); i++) {
                        names.add(getJsonString(arr.getJSONObject(i), "pros:file", ""));
                }
                assertTrue(names.contains("merritt_test_content"));
        }

        /**
         * Test the admin/profiles endpoint returns the expected admin profile name
         */
        @Test
        public void TestAdminProfileNames() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles/admin", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pros:profilesState");
                json = getJsonObject(json, "pros:profiles");
                json = getJsonObject(json, "pros:profileFile");
                assertEquals("admin/docker/collection/merritt_test", getJsonString(json, "pros:file", ""));
        }

        /**
         * Test the admin/profiles-full endpoint returns the expected profile
         */
        @Test
        public void TestProfileFull() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profiles-full", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "prosf:profilesFullState");
                json = getJsonObject(json, "prosf:profilesFull");
                JSONArray arr = getJsonArray(json, "prosf:profileState");
                ArrayList<String> names = new ArrayList<>();
                for(int i = 0; i < arr.length(); i++) {
                        names.add(getJsonString(arr.getJSONObject(i), "prosf:profileID", ""));
                }
                assertTrue(names.contains("merritt_test_content"));
        }

        /**
         * Test the lookup of a specific profile by name
         */
        @Test
        public void TestProfileByName() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profile/merritt_test_content", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pro:profileState");
                assertEquals("merritt_test_content", getJsonString(json, "pro:profileID", ""));
        }

        /**
         * Test endpoint that inserts a set of form parameters into the TEMPLATE-PROFILE file
         */
        @Test 
        public void TestProfileSubmit() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/admin/profile/profile", port, cp);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                        builder.addTextBody("name", "name");
                        builder.addTextBody("description", "description");
                        builder.addTextBody("collection", "ark:/1111/2222");
                        builder.addTextBody("ark", "ark:/1111/2222");
                        builder.addTextBody("owner", "ark:/1111/2222");
                        builder.addTextBody("storagenode", "7777");
                        builder.addTextBody("modificationdate", "2021-08-09T11:28:22-0700");
                        builder.addTextBody("creationdate", "2021-08-09T11:28:22-0700");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        JSONObject json =  new JSONObject(s);
                        json = getJsonObject(json, "ing:genericState");
                        String profileText = getJsonString(json, "ing:string", "");
                        //System.out.println(profileText.replaceAll("&#10;", "\n"));
                        assertNotEquals("", profileText);
                }

        }

}
