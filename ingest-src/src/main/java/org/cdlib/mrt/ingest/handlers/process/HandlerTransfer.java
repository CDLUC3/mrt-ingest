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
package org.cdlib.mrt.ingest.handlers.process;

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

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.LocalIDUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.HttpGet;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.cdlib.mrt.zk.MerrittLocks;

import org.json.JSONException;
import org.json.JSONObject;

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

        HashMap<String,Object> msgMap = new HashMap<>();        // Non string logging
        HttpResponse clientResponse = null;
	String action = "add/";

        try {
	    originalStoreNode = profileState.getTargetStorage();

	    // build REST url 
	    if (jobState.grabUpdateFlag()) {
		action = "update/";
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + "Object update requested.  Overriding default 'add'");
	    }
		
	    String mode = action + originalStoreNode.getNodeID() + "/" + URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"); 

            File manifestFile = new File(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
            String manifestURL = getManifestURL(ingestRequest, manifestFile);
	    if (DEBUG) System.out.println("[debug] " + MESSAGE + " manifestURL: " + manifestURL);

	    String delete = "";
            if (jobState.grabUpdateFlag()) {
            	File deleteFile = new File(ingestRequest.getQueuePath(), "system/mrt-delete.txt");
		if (deleteFile.exists()) {
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + " delete file found: " + deleteFile.getName());
  	       	    delete = processDeleteFile(deleteFile);
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

            // Populate Storage data 
            try {
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + "Storage ZK Manifest URL: " + manifestURL);
               jobState.setStoreManifestURL(manifestURL);
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + "Storage ZK Mode: " + mode);
               jobState.setStoreMode(mode);
	       if (StringUtil.isNotEmpty(delete)) {
                   if (DEBUG) System.out.println("[debug] " + MESSAGE + "Storage ZK Delete detected: " + delete); 
                   jobState.setStoreDelete(delete);
	       }
            } catch (Exception e) {
               System.err.println(MESSAGE + "[WARN] error setting ZK Store JobState: " + e.getMessage());
            }

            // Log POST
            long endTime = DateUtil.getEpochUTCDate();
            ThreadContext.put("Method", "StoragePost");
            ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
            ThreadContext.put("JobID", jobState.getJobID().getValue());
            ThreadContext.put("URL", manifestURL);
            LogManager.getLogger().info(msgMap);

	    // jobState.setCompletionDate(new DateState(DateUtil.getCurrentDate()));
	    // jobState.setVersionID(getVersionID(responseBody));

	    return new HandlerResult(true, "SUCCESS: Transfer");
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
     * JSON response - ver:identifier
     *
     * @param response storage service response in JSON format
     * @return Integer version ID
     */
    
    private Integer getVersionID(String response)
	throws Exception
	{

	Integer versionID = Integer.valueOf("0");	// default is current version

	try {
            JSONObject jsonResponse = JSONUtil.string2json(response);
	    if (jsonResponse != null) {
		versionID = jsonResponse.getJSONObject("ver:versionState").getInt("ver:identifier");
		String objectID = jsonResponse.getJSONObject("ver:versionState").getString("ver:objectID");
		if (DEBUG) System.out.println("[debug] Object ID: " + objectID);
		if (DEBUG) System.out.println("[debug] Version ID: " + versionID.toString());
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

}
