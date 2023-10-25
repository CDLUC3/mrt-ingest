/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
package org.cdlib.mrt.ingest.handlers;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.NoSuchElementException;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.LocalIDUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.queue.DistributedLock;
import org.cdlib.mrt.queue.DistributedLock.Ignorer;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.HttpGet;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.json.JSONObject;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


/**
 * Call storage service with add version request
 * @author mreyes
 */
public class HandlerTransfer extends Handler<JobState>
{

    private static final String NAME = "HandlerTransfer";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private Integer defaultStorage = null;
    private StoreNode storeNode = null;			// Worker
    private StoreNode originalStoreNode = null;		// Load balancer
    private ZooKeeper zooKeeper;
    private String zooConnectString = null;
    private String zooLockNode = null;
    private DistributedLock distributedLock;
    private String hostKey = "canonicalHostname";
    private String hostDomain = "cdlib.org";
    private String hostDockerDomain = "store";
    private String hostIntegrationTestDomain = "(it-server|mock-merritt-it)";
    private String hostIgnoreDomain = "(localhost|N/A)";
    private URL storeURL = null;
    private File tempFile = null;


    /**
     * Adds a version of requested object to storage service
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

        HttpResponse clientResponse = null;
	String action = "/add/";

	zooConnectString = jobState.grabMisc();
	zooLockNode = jobState.grabExtra();
	boolean lock = getLock(jobState.getPrimaryID().getValue(), jobState.getJobID().getValue());
        HashMap<String,Object> msgMap = new HashMap<>();        // Non string logging

	try {
	    originalStoreNode = profileState.getTargetStorage();
            if (DEBUG) System.out.println("[info] " + MESSAGE + " Original Storage endpoint: " + originalStoreNode.getStorageLink().toString());
	    storeURL = getStoreHost(originalStoreNode.getStorageLink());
	    if (storeURL == null) {
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + "Unable to request a Storage worker");
	       throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "Unable to request a Storage worker");
	    }
	    storeNode = new StoreNode(storeURL, originalStoreNode.getNodeID());

	    // build REST url 
	    if (jobState.grabUpdateFlag()) action = "/update/";
	    String url = storeNode.getStorageLink().toString() + action + storeNode.getNodeID() + 
			"/" + URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8");

            HttpClient httpClient = HTTPUtil.getHttpClient(url, StorageUtil.STORAGE_CONNECT_TIMEOUT);
            HttpPost httppost = new HttpPost(url);
	    List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
	    params.add(new BasicNameValuePair("t", "xml"));

            File manifestFile = new File(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
	    if (manifestFile.length() < (1024L * 1024L)) {		// < 1 MB
	       // Push manifest to Storage as a form parm
               String manifest = getManifest(manifestFile);
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + " manifest: " + manifest);
  	       params.add(new BasicNameValuePair("manifest", manifest));
	    } else {
	       // Storage will Pull manifest via an exposed URL
               String manifestURL = getManifestURL(ingestRequest, manifestFile);
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + " manifestURL: " + manifestURL);
  	       params.add(new BasicNameValuePair("url", manifestURL));
	    }

            if (jobState.grabUpdateFlag()) {
            	File deleteFile = new File(ingestRequest.getQueuePath(), "system/mrt-delete.txt");
		if (deleteFile.exists()) {
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + " delete file found: " + deleteFile.getName());
  	       	    params.add(new BasicNameValuePair("delete", processDeleteFile(deleteFile)));
		}
	    }

	    // Update the LocalID db
	    try {
		if (jobState.getLocalID() == null) {
		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "No Local ID present - null");
		    throw new NoSuchElementException("No Local ID present - null");
		}
		if (jobState.getLocalID().getValue().contains("(:unas)")) {
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + "No Local ID present - (:unas)");
		} else {
                   if (DEBUG) System.out.println("[debug] " + MESSAGE + "Updating LocalID db pid: " + jobState.getPrimaryID().getValue() + " lid: " + jobState.getLocalID().getValue());
		   LocalIDUtil.addLocalID(profileState, jobState.getPrimaryID().getValue(), jobState.getLocalID().getValue());
		}
	    } catch (NoSuchElementException nse) {
		// Do nothing - No local ID specified
	    } catch (Exception e) {
                System.err.println("[error] " + MESSAGE + "failed to update LocalID db: " + e.getMessage());
		throw e;
	    }

            long startTime = DateUtil.getEpochUTCDate();

	    // make service request
	    try {
                httppost.setHeader("Content-Type", MediaType.APPLICATION_FORM_URLENCODED);
		httppost.setHeader("JID", jobState.getJobID().getValue());
		httppost.setHeader("hostname", InetAddress.getLocalHost().getHostName());
		httppost.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8.name()));

                clientResponse = httpClient.execute(httppost);
	    } catch (Exception e) {
		e.printStackTrace();
		throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url); 
	    }
            int responseCode = clientResponse.getStatusLine().getStatusCode();
            String responseMessage = clientResponse.getStatusLine().getReasonPhrase();
            String responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");
	    if (DEBUG) System.out.println("[debug] " + MESSAGE + " response code " + responseCode);

            // Log POST
            long endTime = DateUtil.getEpochUTCDate();
            ThreadContext.put("Method", "StoragePost");
            ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
            ThreadContext.put("JobID", jobState.getJobID().getValue());
            ThreadContext.put("URL", url.toString());
            ThreadContext.put("ResponsePhrase", responseMessage);
            ThreadContext.put("ResponseBody", responseBody);
            msgMap.put("DurationMs", endTime - startTime);
            msgMap.put("ResponseCode", responseCode);
            LogManager.getLogger().info(msgMap);

	    if (responseCode != 200) {
                try {
		    if (responseCode != 400) {
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(responseMessage);
		    } else {
			// mrt-delete.txt processing error
                        throw new TException.REQUEST_INVALID(responseMessage);
		    }
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
		    e.printStackTrace();
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url);
                }
	    }

	    jobState.setCompletionDate(new DateState(DateUtil.getCurrentDate()));
	    jobState.setVersionID(getVersionID(responseBody));

	    return new HandlerResult(true, "SUCCESS: transfer", responseCode);
	} catch (TException te) {
	    te.printStackTrace();
	    LogManager.getLogger().error(te);
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            LogManager.getLogger().error(e);
            String msg = "[error] " + MESSAGE + "processing transfer: " + e.getMessage();

            return new HandlerResult(false, msg);
	} finally {
            ThreadContext.clearMap();
            msgMap.clear();
            msgMap = null;
	    clientResponse = null;
	    System.out.println("[debug] " + MESSAGE + " Releasing Zookeeper lock: " + this.zooKeeper.toString());
	    releaseLock();
	}
    }
   

    /**
     * Make sure that delete file is property formatted
     *
     * @param deleteFile delete file
     * @return String delete file as string 
     */
    private String processDeleteFile(File deleteFile) {
	try {
	    FileInputStream fstream = new FileInputStream(deleteFile);
	    DataInputStream in = new DataInputStream(fstream);
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String strLine = null;
	    String strFile = "";
	    while ((strLine = br.readLine()) != null)   {
	    	// Line By Line
		if (! StringUtil.isEmpty(strLine)) {
		    if (! strLine.startsWith("producer/")) {
		        strLine = "producer/" + strLine; 
		    }
		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "delete entry: " + strLine);
		    strFile += strLine + "\n";
		}
	}

	    return strFile;
	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
    }


    /**
     * Create a string that will define the value for a manifest based storage service call
     *
     * @param manifestFile manifest created by handler "Manifest"
     * @return String manifest as a string representation
     */
    private String getManifest(File manifestFile) {
	try {
	    return FileUtil.file2String(manifestFile);
	} catch (TException te) {
	    te.printStackTrace();
	    return null;
	}
    }


    /**
     * Create a URL to be used by Storage to retrieve manifest
     *
     * @param ingestRequst ingestRequest
     * @param manifestFile manifest created by handler "Manifest"
     * @return String manifest URL as a string representation
     */
    private String getManifestURL(IngestRequest ingestRequest, File manifestFile) {
	try {
            // requires symlink from webapps/ingestqueue to home ingest queue directory
            URL link = new URL(ingestRequest.getLink());
            String port = "";
            String path = link.getPath();
            if (link.getPort() != -1) port = ":" + link.getPort();
            String baseURL = link.getProtocol() + "://" + link.getHost() + port + path +
                 "/ingestqueue/" + ingestRequest.getJob().grabBatchID().getValue() + "/" + ingestRequest.getQueuePath().getName();

	    baseURL += "/system/" + manifestFile.getName();

	    return baseURL;
	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
    }

    /**

    /**
     * extract version ID from storage service response
     * xml response form: <ver:versionID>versionID</ver:versionID>
     *
     * @param response storage service response in XML format
     * @return Integer version ID
     */
    
    private Integer getVersionID(String response)
	throws Exception
	{

	Integer versionID = Integer.valueOf("0");	// default is current version

	try {
	    DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true); 
	    domFactory.setExpandEntityReferences(true);

	    DocumentBuilder builder = domFactory.newDocumentBuilder();
	    builder.setErrorHandler(new SimpleErrorHandler());
	    Document document = builder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
	    XPath xpath = XPathFactory.newInstance().newXPath();
	    //XPathExpression expr = xpath.compile("//*[local-name()='versionID']");
	    XPathExpression expr = xpath.compile("//*[local-name()='identifier']");

	    String xpathS = (String) expr.evaluate(document);
	    if (StringUtil.isNotEmpty(xpathS)) {
		if (DEBUG) System.out.println("[debug] version ID: " + xpathS);
		versionID = Integer.valueOf(xpathS);
	    } else {
		if (DEBUG) System.out.println("[warn] Can not determine object version ID. Default: 0");
	    }
	    return versionID;

	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "getting version ID: " + e.getMessage();
            throw new Exception(msg);
	}
    }

    public String getName() {
	return NAME;
    }

    /**
     * Lock on primary identifier.  Will loop unitil lock obtained.
     *
     * @param String primary ID of object (ark)
     * @param String jobID
     * @return Boolean result of obtaining lock
     */
    private boolean getLock(String primaryID, String payload) {
    try {

       // Zookeeper treats slashes as nodes
       String lockID = primaryID.replace(":", "").replace("/", "-");

       zooKeeper = new ZooKeeper(zooConnectString, DistributedLock.sessionTimeout, new Ignorer());
       distributedLock = new DistributedLock(zooKeeper, zooLockNode, lockID, null);
       boolean locked = false;

	while (! locked) {
	    try {
               System.out.println("[info] " + MESSAGE + " Attempting to gain lock");
	       locked = distributedLock.submit(payload);
	    } catch (Exception e) {
              if (DEBUG) System.err.println("[debug] " + MESSAGE + " Exception in gaining lock: " + lockID);
	    }
	    if (locked) break;
            System.out.println("[info] " + MESSAGE + " UNABLE to Gain lock for ID: " + lockID + " Waiting 15 seconds before retry");
	    Thread.currentThread().sleep(15 * 1000);	// Wait 15 seconds before attempting to gain lock for ID
	}
        if (DEBUG) System.out.println("[debug] " + MESSAGE + " Gained lock for ID: " + lockID + " -- " + payload);
	    
	} catch (Exception e) {
	    e.printStackTrace();
	    return false;
	}
    return true;
    }


    /**
     * Release lock
     *
     * @param none needed inputs are global
     * @return void
     */
    private void releaseLock() {
    	try {

		this.distributedLock.cleanup();
		this.distributedLock = null;
		this.zooKeeper = null;
	
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * Request storage worker
     *
     * @param String Hostname API
     * @return hostname
     */
    private URL getStoreHost(URL storeHostURL) {
	String newHostURL = null;
    	try {
           tempFile = File.createTempFile("hostname", "txt");

           // Library should retry 3 times, but lets use belt and suspenders
           for (int i=0; i <= 2; i++) {
               try {
                   HttpGet.getFile(new URL(storeHostURL.toString() + "/hostname"), tempFile, 60000, null);
                   break;
               } catch (Exception ste) {
                   System.err.println("[ERROR] Getting a storage node on attempt: " + i);
                   ste.printStackTrace();
               }
               if (i==2) throw new Exception("[ERROR] Failure to retrieve Storage worker after number of attempts: " + i);
           }

	   String stringResponse = FileUtil.file2String(tempFile);
	   System.out.println("Response for storage worker request: " + stringResponse);

           JSONObject jsonResponse = JSONUtil.string2json(stringResponse);
	   String hostname = null;
	   if (jsonResponse == null) {
	      // Response not in JSON format
	      hostname = stringResponse;
	   } else {
	      hostname = jsonResponse.getString(hostKey);
	   }

	   if ( hostname.matches(hostIgnoreDomain)) {
              if (DEBUG) System.out.println("[info] " + MESSAGE + " Storage endpoint should not change: " + hostname);
	      newHostURL = storeHostURL.toString();
	   } else if ( ! (hostname.contains(hostDomain) || hostname.equalsIgnoreCase(hostDockerDomain) || hostname.matches(hostIntegrationTestDomain))) {
              if (DEBUG) System.out.println("[warning] " + MESSAGE + " Storage endpoint does not contain correct domain: " + hostname);
              // String msg = "[error] " + MESSAGE + " Storage endpoint does not contain correct domain: " + hostname;
              // throw new Exception(msg);
	   } else {
	      newHostURL = storeHostURL.getProtocol() + "://" + hostname + ":" + storeHostURL.getPort() + storeHostURL.getPath();
              if (DEBUG) System.out.println("[info] " + MESSAGE + " Storage worker endpoint: " + newHostURL);
	   }
	   return new URL(newHostURL);
	} catch (Exception e) {
	    e.printStackTrace();
            System.err.println(e.getMessage());
	    return null;
	} finally {
	    tempFile.delete();
	    tempFile = null;
	}
    }

    // XML parser error handler
    public class SimpleErrorHandler implements ErrorHandler {
        public void warning(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }
    
        public void error(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }
    
        public void fatalError(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }
    }

}

