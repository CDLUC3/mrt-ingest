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


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import java.util.NoSuchElementException;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.LocalIDUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

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
    private StoreNode storeNode = null;

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

  	ClientResponse clientResponse = null;
	String action = "/add/";

	try {

	    storeNode = profileState.getTargetStorage();

	    // build REST url 
	    if (jobState.grabUpdateFlag()) action = "/update/";
	    String url = storeNode.getStorageLink().toString() + action + storeNode.getNodeID() + 
			"/" + URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8");
	    Client client = Client.create();	// reuse?  creation is expensive

            /* fix client timeout problem */
            client.setConnectTimeout(new Integer(StorageUtil.STORAGE_CONNECT_TIMEOUT));
            client.setReadTimeout(new Integer(StorageUtil.STORAGE_READ_TIMEOUT));

	    WebResource webResource = client.resource(url);
	    Form formData = new Form();
  	    formData.add("t", "xml");

            File manifestFile = new File(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
	    if (manifestFile.length() < (1024L * 1024L)) {		// < 1 MB
	       // Push manifest to Storage as a form parm
               String manifest = getManifest(manifestFile);
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + " manifest: " + manifest);
  	       formData.add("manifest", manifest);
	    } else {
	       // Storage will Pull manifest via an exposed URL
               String manifestURL = getManifestURL(ingestRequest, manifestFile);
	       if (DEBUG) System.out.println("[debug] " + MESSAGE + " manifestURL: " + manifestURL);
  	       formData.add("url", manifestURL);
	    }

            if (jobState.grabUpdateFlag()) {
            	File deleteFile = new File(ingestRequest.getQueuePath(), "system/mrt-delete.txt");
		if (deleteFile.exists()) {
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + " delete file found: " + deleteFile.getName());
  	    	    formData.add("delete", processDeleteFile(deleteFile));
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

	    // make service request
	    try {
  	        clientResponse = webResource.type(MediaType.APPLICATION_FORM_URLENCODED).
			header("JID", jobState.getJobID().getValue()).
			header("hostname", InetAddress.getLocalHost().getHostName() ).
			post(ClientResponse.class, formData);
	    } catch (Exception e) {
		e.printStackTrace();
		throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url); 
	    }
	    if (DEBUG) System.out.println("[debug] " + MESSAGE + " response code " + clientResponse.getStatus());

	    if (clientResponse.getStatus() != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
		    if (clientResponse.getStatus() != 400) {
                        TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE tExceptionResponse = 
			    clientResponse.getEntity(TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE.class);
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(tExceptionResponse.getError());
		    } else {
			// mrt-delete.txt processing error
                        TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                        throw new TException.REQUEST_INVALID(tExceptionResponse.getError());
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
	    jobState.setVersionID(getVersionID(clientResponse.getEntity(String.class)));

	    return new HandlerResult(true, "SUCCESS: transfer", clientResponse.getStatus());
	} catch (TException te) {
	    te.printStackTrace();
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing transfer: " + e.getMessage();

            return new HandlerResult(false, msg);
	} finally {
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

