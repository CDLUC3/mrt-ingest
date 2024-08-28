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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.cdlib.mrt.zk.Access;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.BatchState;
import org.cdlib.mrt.zk.IngestState;
import org.cdlib.mrt.zk.Job;
import org.cdlib.mrt.zk.MerrittLocks;
import org.cdlib.mrt.zk.MerrittStateError;
import org.cdlib.mrt.zk.MerrittZKNodeInvalid;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.cdlib.mrt.zk.QueueItem.ZkPaths;

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
        private int zkport = 8084;
        private String cp = "mrtingest";
        private DocumentBuilder db;
        private XPathFactory xpathfactory;
        private String profile = "merritt_test_content";
        private ZooKeeper zk;

        public static final int SLEEP_SUBMIT = 15000;
        public static final int SLEEP_RETRY = 3000;
        public static final int SLEEP_CLEANUP = 500;
        /*
         * Initialize the test class
         */
        public ServiceDriverIT() throws ParserConfigurationException, IOException, KeeperException, InterruptedException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                        mockport = Integer.parseInt(System.getenv("mock-merritt-it.port"));
                        zkport = Integer.parseInt(System.getenv("mrt-zk.port"));
                } catch (NumberFormatException e) {
                        //use default ports
                }
                db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                xpathfactory = new XPathFactoryImpl();
                zk = new ZooKeeper(String.format("localhost:%s", zkport), 100, null);
		try {
                	clearQueue();
		} catch (Exception e) {
                	clearQueue();
		}
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
         * @throws KeeperException 
         * @throws InterruptedException 
         */
        public void clearQueue() throws IOException, JSONException, InterruptedException, KeeperException {
                QueueItemHelper.deleteAll(zk, ZkPaths.Batch.path);
                QueueItemHelper.deleteAll(zk, ZkPaths.BatchUuids.path);
                QueueItemHelper.deleteAll(zk, ZkPaths.Job.path);
                QueueItemHelper.deleteAll(zk, ZkPaths.Locks.path);
                Job.initNodes(zk);
                Access.initNodes(zk);
                MerrittLocks.initLocks(zk);
        }


        /**
         * Reset that status of the test stack before each rest case.
         * - Mark all ingest queue items as deleted
         * - Mark all inventory queue items as deleted
         * - Thaw submission processing (in case it had been frozen)
         * - Thaw submission processing for the test collection (in case it had been frozen)
         * - Tell the mock test service to serve data (in case it had been set to not return data)
         * @throws KeeperException 
         * @throws InterruptedException 
         */
        @Before
        public void clearQueueDirectory() throws InterruptedException, KeeperException  {
                MerrittLocks.unlockIngestQueue(zk);
                MerrittLocks.unlockCollection(zk, profile);
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
         * @return batch id
         */
        public String ingestFile(String url, File file) throws IOException, JSONException {
                return ingestFile(url, file, "", "");
        }

        /**
         * Formulate a POST request to trigger an ingest submission
         * @param url to the submission endpoint
         * @param file file to be ingested
         * @param localId if not empty, set a localid for the submission
         * @return batch id
         */
        public String ingestFile(String url, File file, String localId) throws IOException, JSONException {
                return ingestFile(url, file, localId, "");
        }

        /**
         * Formulate a POST request to trigger ingest submission.  Call submitResponse to verify the response status.
         * @param url to the submission endpoint
         * @param file file to be ingested
         * @param localId if not empty, set a localid for the submission
         * @param primaryId if not empty, set a primary id for the submission
         * @return batch id
         */
        public String ingestFile(String url, File file, String localId, String primaryId) throws IOException, JSONException {
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
                        if (file.getName().endsWith("object.checkm")){
                                builder.addTextBody("type", "manifest");
                        }
                        if (file.getName().endsWith("batch.checkm")){
                                builder.addTextBody("type", "batchManifestContainer");
                        }
                        builder.addTextBody("submitter", "integration-tests");
                        builder.addTextBody("responseForm", "json");
                        HttpEntity multipart = builder.build();
                        post.setEntity(multipart);
                        
                        HttpResponse response = client.execute(post);
                        return submitResponse(response);
                }

        }

        /**
         * Verify the response status from an ingest submission.  The response will vary for async vs sync requests.
         * @param response HttpResponse object from the submission request
         * @param batch If true, handle the response as a batch request that will be queued.  If not, handle the response as a single job.
         * @return submitted batch id
         */
        public String submitResponse(HttpResponse response) throws IOException, JSONException {
                assertEquals(200, response.getStatusLine().getStatusCode());


                String s = new BasicResponseHandler().handleResponse(response).trim();
                assertFalse(s.isEmpty());

                JSONObject json =  new JSONObject(s);
                assertNotNull(json);
                assertTrue(json.has("bat:batchState"));
                assertEquals("QUEUED", json.getJSONObject("bat:batchState").getString("bat:batchStatus"));
                JSONObject j = json.getJSONObject("bat:batchState");
                String bid = "";
                if (j.has("bat:batchID")) {
                        bid = j.getString("bat:batchID");
                }
                assertFalse(bid.isEmpty());
                return bid;

        }

        /**
         * Formulate a POST request to submit content to ingest by URL
         * @param url to the submission endpoint
         * @param contenturl url to the content to submit
         * @param filename filename to use for the submitted content
         * @param type submission type to assign to the submission
         * @param batch set to true if the endpoint will queue jobs (sync vs async)
         * @return batch id of the submitted batch
         */
        public String ingestFromUrl(String url, String contenturl, String filename, String type) throws IOException, JSONException {
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
                        return submitResponse(response);
                }

        }

        /**
         * Test the submission of a manifest by URL.
         * 
         * Note: this test downloads data from github.  An internet connection is needed to run the test.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         * @throws MerrittStateError 
         */
        @Test
        public void FileManifestIngest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid, MerrittStateError {
                String filename = "4blocks.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFromUrl(url, contenturl, filename, "manifest");
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
        }

        public void cleanup(Batch batch) {
                try {
                        if (batch.status() == BatchState.Processing) {
                                batch.setStatus(zk, batch.status().success());
                        }
                        if (batch.status() == BatchState.Reporting) {
                                batch.setStatus(zk, batch.status().success());        
                        }
                        //Thread.sleep(SLEEP_CLEANUP);
                        batch.delete(zk);
                } catch(Exception e) {
                        System.out.println(e);
                }
        }

        public Batch getZkBatch(String bid) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
                Batch batch = Batch.findByUuid(zk, bid);
                batch.load(zk);
                return batch;
        }

        public void assertJobCounts(Batch batch, int tries, int total, int completed) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {

                Thread.sleep(SLEEP_SUBMIT);
                assertEquals(total, batch.getProcessingJobs(zk).size() + batch.getCompletedJobs(zk).size());
                for(int i=0; i <= tries; i++) {
                        if (batch.getCompletedJobs(zk).size() == completed) {
                                break;
                        }
                        for (Job j: batch.getProcessingJobs(zk)) {
                                j.load(zk);
				// System.out.println(i + " ---- " + j.status() + " ---- " + j.id());
                                if (j.status() == org.cdlib.mrt.zk.JobState.Recording) {
                                        try {
                                                j.setStatus(zk, j.status().success());
                                        } catch (MerrittStateError e) {
                                                e.printStackTrace();
                                        }
                                }
                        }
                        Thread.sleep(SLEEP_RETRY);
                }
                assertEquals(completed, batch.getCompletedJobs(zk).size());
        }

        public void assertFailedJobCounts(Batch batch, int tries, int total, int failed) throws MerrittZKNodeInvalid, KeeperException, InterruptedException {

                Thread.sleep(SLEEP_SUBMIT);
                assertEquals(total, batch.getProcessingJobs(zk).size() + batch.getFailedJobs(zk).size());
                for(int i=0; i <= tries; i++) {
                        if (batch.getFailedJobs(zk).size() == failed) {
                                break;
                        }
                        Thread.sleep(SLEEP_RETRY);
                }
                assertEquals(failed, batch.getFailedJobs(zk).size());
        }

        /**
         * Test the submission of a manifest file.
         * 
         * Note: this test retrieves test content from the mock-merritt-it container.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         * @throws MerrittStateError 
         */
        @Test
        public void TestManifest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid, MerrittStateError {
		System.out.println("[ServiceDriverIT] TestManifest - Test manifest.");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/mock_object.checkm"));
                            
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
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
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void TestManifestWithRequeue() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid, MerrittStateError {
		System.out.println("[ServiceDriverIT] TestManifestWithRequeue - Test manifest while forcing failure.");
                // tell the mock-merritt-it service to temporarily suspend content delivery
                String url = String.format("http://localhost:%d/status/stop", mockport, cp);
                getJsonContent(new HttpPost(url), 200);
                Thread.sleep(2000);

                url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/mock_object.checkm"));
                Batch batch = getZkBatch(bid);
                assertFailedJobCounts(batch, 15, 1, 1);

                // tell the mock-merritt-it service to resume content delivery
                url = String.format("http://localhost:%d/status/start", mockport, cp);
                getJsonContent(new HttpPost(url), 200);
                Thread.sleep(2000);
                
                for(Job j: batch.getFailedJobs(zk)) {
                        j.load(zk);
                        j.setStatus(zk, org.cdlib.mrt.zk.JobState.Estimating);
                }
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
        }

        @Test
        public void BatchManifestIngest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] BatchManifestIngest - Submit batch manifest.");
                String filename = "sampleBatchOfManifests.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFromUrl(url, contenturl, filename, "batch-manifest");
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 30, 3, 3);
                cleanup(batch);
        }

        /**
         * Test the submission of a batch-manifest by URL.
         * 
         * Note: this test downloads data from github.  An internet connection is needed to run the test.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void BatchFilesIngest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] BatchFilesIngest - Submit batch manifest by URL.");
                String filename = "sampleBatchOfFiles.checkm";
                String contenturl = "https://raw.githubusercontent.com/CDLUC3/mrt-doc/main/sampleFiles/" + filename;
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFromUrl(url, contenturl, filename, "single-file-batch-manifest");
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 30, 3, 3);
                cleanup(batch);
        }

        /**
         * Test the submission of a single file.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void SimpleFileIngest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngest - Submit single file.");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 30, 1, 1);
                cleanup(batch);
        }

        /**
         * Test the submission of a single file.  Test the endpoints to view job-related data.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
        */
        @Test
        public void SimpleFileIngestCheckJob() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngestCheckJob - Submit single file. Test Job endpoints");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 30, 1, 1);
                Job job = getJob(batch);
                String jid = job.jid();
                String ark = job.primaryId();

                url = String.format("http://localhost:%d/%s/admin/jid-erc/%s/%s", port, cp, bid, jid);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "fil:jobFileState");
                json = getJsonObject(json, "fil:jobFile");
                //fyi, the ark is currently written as objectID on the data object, but it should be written to the primaryID
                assertEquals(ark, getJsonString(json, "fil:where-primary", ""));

                url = String.format("http://localhost:%d/%s/admin/jid-file/%s/%s", port, cp, bid, jid);
                json = getJsonContent(url, 200);
                // 7 system files will remain after submission is complete
                assertEquals(7, getFiles(json).size());

                url = String.format("http://localhost:%d/%s/admin/jid-manifest/%s/%s", port, cp, bid, jid);
                json = getJsonContent(url, 200);
                json = getJsonObject(json, "ingmans:manifestsState");
                assertEquals("", getJsonString(json, "ingmans:manifests", "N/A"));
                cleanup(batch);
        }


        public Job getJob(Batch batch) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
                List<Job> jobs = batch.getCompletedJobs(zk);
                assertEquals(1, jobs.size());
                Job job = jobs.get(0);
                job.load(zk);
                return job;
        }

        public Job getProcessingJob(Batch batch) throws KeeperException, InterruptedException, MerrittZKNodeInvalid {
                List<Job> jobs = batch.getProcessingJobs(zk);
                assertEquals(1, jobs.size());
                Job job = jobs.get(0);
                job.load(zk);
                return job;
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
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void QueueFileIngest() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] QueueFileIngest - Submit single file.");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));

                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                assertTrue(getBids().contains(bid));
                assertEquals(1, getJobs(bid).size());
                cleanup(batch);
        }

        /**
         * Queue the submission of a single file.  Detect the zookeeper lock in place as the submission processes.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void QueueFileIngestCatchLock() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] QueueFileIngestCatchLock - Submit and dectect lock.");
                //This ark has a time delay in mock-merritt-it to allow the catch of a lock
                String url = String.format("http://localhost:%d/%s/poster/submit/ark/9999/2222", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));

                // look for the presence of a zookeeper lock
                // the lock name should be derived from the submission's primary id (ark)
                boolean found = false;
                for(int ii=0; ii<20 && !found; ii++) {
                        if (MerrittLocks.checkLockObjectStorage(zk, "ark:/9999/2222")) {
                                found = true;
                                break;
                        }
                        Thread.sleep(5000);
                }
                assertTrue(found);

                Batch batch = getZkBatch(bid);
                // expect 1 queue job
                assertJobCounts(batch, 15, 1, 1);

                assertTrue(getBids().contains(bid));
                assertEquals(1, getJobs(bid).size());
                cleanup(batch);
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
         * Queue a file ingest while submissions are frozen.  Thaw submsissions and resume processing.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void QueueFileIngestPauseSubmissions() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] QueueFileIngestPauseSubmissions - Submit while Ingest frozen, then thaw");
                MerrittLocks.lockIngestQueue(zk);
                assertTrue(MerrittLocks.checkLockIngestQueue(zk));

                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertEquals(0, batch.getProcessingJobs(zk).size() + batch.getCompletedJobs(zk).size());

                MerrittLocks.unlockIngestQueue(zk);
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
        }

        /**
         * Que a file ingest while a specific collection is frozen.  Thaw the collection and resume processing.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void QueueFileIngestPauseCollection() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid, MerrittStateError {
		System.out.println("[ServiceDriverIT] QueueFileIngestPauseCollection - Submit to a frozen collection, then thaw");
                MerrittLocks.lockCollection(zk, profile);

                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));

                Thread.sleep(SLEEP_SUBMIT);

                Batch batch = getZkBatch(bid);
                Job job = getProcessingJob(batch);
                assertEquals(org.cdlib.mrt.zk.JobState.Held, job.status());

                MerrittLocks.unlockCollection(zk, profile);

                Thread.sleep(2000);

                job.setStatus(zk, org.cdlib.mrt.zk.JobState.Pending);
                job.unlock(zk);

                Thread.sleep(2000);

                assertJobCounts(batch, 30, 1, 1);
                cleanup(batch);
        }

        /**
         * Submit a single file with a local id.
         * @throws MerrittZKNodeInvalid 
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        @Test
        public void SimpleFileIngestWithLocalid() throws IOException, JSONException, KeeperException, InterruptedException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithLocalid - single localID (form parm)");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"), "localid");
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
                assertEquals("localid", job.localId());
                cleanup(batch);
        }

        /**
         * Submit a single file with duplicate local ids.
         * @throws MerrittZKNodeInvalid 
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        @Test
        public void SimpleFileIngestWithLocalids() throws IOException, JSONException, KeeperException, InterruptedException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithLocalids - multiple localIDs with duplicates (form parm)");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"), "localid;localid;localid;localidtwo;localidtwo");
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
                assertEquals("localid;localidtwo", job.localId());
                cleanup(batch);
        }

        /**
         * Submit container with ERC data (mrt-erc.txt) Form parameter
         * @throws MerrittZKNodeInvalid 
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        @Test
        public void SimpleContainerLocalIDWithForm() throws IOException, JSONException, KeeperException, InterruptedException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleContainerLocalIDWithForm - Form parameter localIDs and ERC file (mrt-erc.txt)");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/test_erc.zip"), "localidfour; localidfive");
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
		// Form parameter localIDs 4/5 are appended with ERC file localIDs 1/2/3
                assertEquals("localidfour;localidfive;localid;localidtwo;localidthree", job.localId());
                assertEquals("ark:/99999/dummy", job.primaryId());
                cleanup(batch);
        }

        /**
         * Submit Batch manifest with ERC data along with ERC data file (mrt-erc.txt)
         * @throws MerrittZKNodeInvalid 
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        @Test
        public void SimpleBatchManifestLocalIDWithERCFile() throws IOException, JSONException, KeeperException, InterruptedException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngestLocalIDWithERCForm - Batch manifest with localIDs and ERC file (mrt-erc.txt)");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/localid_batch.checkm"), "");
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
		// Manifest localIDs 4/5/6 are appended with ERC file localIDs 1/2/3
                assertEquals("localidfour;localidfive;localidsix;localid;localidtwo;localidthree", job.localId());
                assertEquals("ark:/99999/dummy", job.primaryId());
                cleanup(batch);
        }

        /**
         * Submit Batch manifest and Form parameter localID
         * @throws MerrittZKNodeInvalid 
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        @Test
        public void SimpleBatchManifestLocalIDWithForm() throws IOException, JSONException, KeeperException, InterruptedException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleBatchManifestLocalIDWithForm - Batch manifest with localIDs and Form parameters");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/localid2_batch.checkm"), "localidten");
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
		// Form localID 10 is overwritten with Manifest localIDs 7/8/9
		// LocalID Form parameter can not be applied to Batch manifest
                assertEquals("localidseven;localideight;localidnine", job.localId());
                cleanup(batch);
        }

        /**
         * Submit a single file with a primary id specified.
         * @throws InterruptedException 
         * @throws KeeperException 
         * @throws MerrittZKNodeInvalid 
         */
        @Test
        public void SimpleFileIngestWithArk() throws IOException, JSONException, MerrittZKNodeInvalid, KeeperException, InterruptedException {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithArk - Submit with primaryID (form parm)");
                String url = String.format("http://localhost:%d/%s/poster/submit/ark/1111/2222", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
                assertEquals("ark/1111/2222", job.primaryId());
                cleanup(batch);
        }

        /**
         * Submit a single file with a primary id specified.  Perform an update on that object.
         * @throws InterruptedException 
         * @throws KeeperException 
         * @throws MerrittZKNodeInvalid 
         */
        @Test
        public void SimpleFileIngestWithArkAndUpdate() throws IOException, JSONException, MerrittZKNodeInvalid, KeeperException, InterruptedException {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithArk - Update with primaryID (form parm)");
                String url = String.format("http://localhost:%d/%s/poster/submit/ark/1111/2222", port, cp);
                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
                url = String.format("http://localhost:%d/%s/poster/update/ark/1111/2222", port, cp);
                bid = ingestFile(url, new File("src/test/resources/data/test.txt"));
                batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
                assertEquals("ark/1111/2222", job.primaryId());
                cleanup(batch);
        }
        
        /**
         * Submit a single file.  Locate the primary id and perform an update on that object.
         * @throws MerrittZKNodeInvalid 
         * @throws KeeperException 
         */
        @Test
        public void SimpleFileIngestWithUpdate() throws IOException, JSONException, InterruptedException, KeeperException, MerrittZKNodeInvalid {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithArk - Update");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                String bid = ingestFile(url, new File("src/test/resources/data/foo.txt"));
                Thread.sleep(SLEEP_SUBMIT);
                Batch batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                Job job = getJob(batch);
                String prim = job.primaryId();
                cleanup(batch);

                url = String.format("http://localhost:%d/%s/poster/update", port, cp);
                bid = ingestFile(url, new File("src/test/resources/data/test.txt"), "", prim);
                Thread.sleep(SLEEP_SUBMIT);
                batch = getZkBatch(bid);
                assertJobCounts(batch, 15, 1, 1);
                cleanup(batch);
        }

        /**
         * Submit a zip file to be ingested.
         */
        @Test
        public void SimpleZipIngest() throws IOException, JSONException {
		System.out.println("[ServiceDriverIT] SimpleFileIngestWithArk - Submit with Zip payload");
                String url = String.format("http://localhost:%d/%s/poster/submit", port, cp);

                ingestFile(url, new File("src/test/resources/data/test.zip"));
        }

        /**
         * Test the admin/profiles endpoint returns the expected profile name
         */
        @Test
        public void TestProfileNames() throws IOException, JSONException {
                System.out.println("[ServiceDriverIT] TestProfileNames - Test the profiles endpoint.");
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
                System.out.println("[ServiceDriverIT] TestAdminProfileNames - Test the admin/profiles endpoint.");
                String url = String.format("http://localhost:%d/%s/admin/profiles/admin", port, cp);
                JSONObject json = getJsonContent(url, 200);
                json = getJsonObject(json, "pros:profilesState");
                json = getJsonObject(json, "pros:profiles");
                json = getJsonObject(json, "pros:profileFile");
                assertEquals("admin/docker/collection/merritt_test", getJsonString(json, "pros:file", ""));
        }

        /**
         * Test the admin/profiles-full endpoint 
         */
        @Test
        public void TestProfileFull() throws IOException, JSONException {
                System.out.println("[ServiceDriverIT] TestProfileFull - Test the profiles full state.");
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
                System.out.println("[ServiceDriverIT] TestProfileByName - Test the profiles by name.");
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
