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
package org.cdlib.mrt.ingest.utility;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.ByteArrayInputStream;

import java.net.URL;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;

import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * LocalID service wrapper methods
 * @author mreyes
 */
public class LocalIDUtil
{

    private static final String NAME = "LocalIDUtil";
    private static final String MESSAGE = NAME + ": ";
    private LoggerInf logger = null;
    private static final boolean DEBUG = true;
    public static final int LOCALID_TIMEOUT = (5 * 60 * 1000);

    public static String fetchPrimaryID(ProfileState profileState, String localID)
        throws TException
    {

        HttpResponse clientResponse = null;
        try {
	    // First lets fetch from our localID DB on Local ID service
	    String primaryID = null;
            URL localIDURL = profileState.getLocalIDURL();

            // build REST url
            String url = localIDURL.toString() + "/primary/" +
			URLEncoder.encode(profileState.getOwner(), "utf-8") + "/" +
			URLEncoder.encode(localID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] PrimaryID fetch URL: " + url);
            HttpClient httpClient = HTTPUtil.getHttpClient(url, LOCALID_TIMEOUT);
            HttpGet httpget = new HttpGet(url);

            // make service request
            try {
                clientResponse = httpClient.execute(httpget);
            } catch (Exception e) {
                e.printStackTrace();
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": localid service: " + url);
            }
            int responseCode = clientResponse.getStatusLine().getStatusCode();
            String responseMessage = clientResponse.getStatusLine().getReasonPhrase();
            String responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");

	    if (responseCode != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    throw new TException.REQUEST_INVALID(responseMessage);
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": local ID service: " + url);
                }
	    } 

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            domFactory.setExpandEntityReferences(true);

            DocumentBuilder builder = domFactory.newDocumentBuilder();
            builder.setErrorHandler(null);
            Document document = builder.parse(new ByteArrayInputStream(responseBody.getBytes("UTF-8")));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression expr = xpath.compile("//*[local-name()='primaryIdentifier']");

            String xpathS = (String) expr.evaluate(document);
            if (StringUtil.isNotEmpty(xpathS)) {
                if (DEBUG) System.out.println("[debug] primary ID: " + xpathS);
                primaryID = xpathS;
            } else {
                if (DEBUG) System.out.println("[debug] Can not determine primary ID from localID DB");
		primaryID = null;
            }

            return primaryID;

        } catch (TException te) {
	    throw te;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to map localID. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }

    public static String fetchLocalID(ProfileState profileState, String primaryID)
        throws TException
    {

        HttpResponse clientResponse = null;
        try {

	    //Vector localIDs = null;
	    String localIDs = null;
            URL localIDURL = profileState.getLocalIDURL();

            // build REST url
            String url = localIDURL.toString() + "/local/" + 
			URLEncoder.encode(primaryID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] LocalID fetch URL from localID db: " + url);

            HttpClient httpClient = HTTPUtil.getHttpClient(url, LOCALID_TIMEOUT);
            HttpGet httpget = new HttpGet(url);

            // make service request
            try {
                clientResponse = httpClient.execute(httpget);
            } catch (Exception e) {
                e.printStackTrace();
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": localid service: " + url);
            }

            int responseCode = clientResponse.getStatusLine().getStatusCode();
            String responseMessage = clientResponse.getStatusLine().getReasonPhrase();
            String responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");

	    if (responseCode != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    throw new TException.REQUEST_INVALID(responseMessage);
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": localID service: " + url);
                }
	    } 

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            domFactory.setExpandEntityReferences(true);

            DocumentBuilder builder = domFactory.newDocumentBuilder();
            builder.setErrorHandler(null);
            Document document = builder.parse(new ByteArrayInputStream(responseBody.getBytes("UTF-8")));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression expr = xpath.compile("//*[local-name()='localID']");

 	    NodeList nl = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    	    int i = 0;
    	    for (i = 0; i < nl.getLength(); i++) {
		Node n = nl.item(i);
		String localid = n.getTextContent();
		if (StringUtil.isNotEmpty(localid)) {
                    if (DEBUG) System.out.println("[debug] LocalID found (localID db): " + localid);
		    if (localIDs == null) localIDs = localid;
		    else localIDs += "; " + localid;
		}
    	    }

	    if (StringUtil.isEmpty(localIDs)) if (DEBUG) System.out.println("[debug] Can not determine local ID");
            return localIDs;

        } catch (TException te) {
	    throw te;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to map primaryID to localID. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }

    // Add a local identifier to localID database
    public static void addLocalID(ProfileState profileState, String primaryID, String localID)
        throws TException
    {

        HttpResponse clientResponse = null;
        try {

            URL localIDURL = profileState.getLocalIDURL();

            // build REST url
            String url = localIDURL.toString() + "/primary/" + 
		URLEncoder.encode(primaryID, "utf-8") + "/" + 
		URLEncoder.encode(profileState.getOwner(), "utf-8") + "/" + URLEncoder.encode(localID, "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] LocalID add URL for localID db: " + url);

            HttpClient httpClient = HTTPUtil.getHttpClient(url, LOCALID_TIMEOUT);
            HttpPost httppost = new HttpPost(url);
            httppost.setHeader("Content-Type", MediaType.APPLICATION_FORM_URLENCODED);

	    int retryCount = 0;
	    int responseCode;
	    String responseMessage = null;
	    String responseBody = null;
	    while (true) {
		try {
            	     // make service request
                     clientResponse = httpClient.execute(httppost);
            	     responseCode = clientResponse.getStatusLine().getStatusCode();
            	     responseMessage = clientResponse.getStatusLine().getReasonPhrase();
                     responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");
   	    	     String response = null;
		     break;
                } catch (Exception ce) {
                     if (retryCount >= 3) throw ce;
                     System.err.println("[error] " + MESSAGE + ": " + ce.getMessage());
                     retryCount++;
                }
            }
 
	    if (responseCode != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    throw new TException.REQUEST_INVALID(responseMessage);
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": localID service: " + url);
                }
	    } else {
                if (DEBUG) System.out.println("[debug] LocalID updated: " + localID);
   	        if (DEBUG) System.out.println(responseBody);
		// all done, fall through to end 
	    }

        } catch (TException te) {
	    throw te;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to add localID. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }

}
