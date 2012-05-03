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


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.net.URL;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams; 
import org.apache.http.params.HttpParams; 
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Simple minter dedicated for ingest
 * @author mreyes
 */
public class MintUtil
{

    private static final String NAME = "MintUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final String EOL = "%0A";
    private LoggerInf logger = null;
    private Properties conf = null;
    private Properties ingestProperties = null;
    private static final boolean DEBUG = true;

    public static Identifier getJobID()
        throws TException
    {
	try {
            return new Identifier("jid-" + UUID.randomUUID().toString(), Identifier.Namespace.Local);
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in minting job ID - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("error in minting job ID");
	}
    }

    public static Identifier getBatchID()
        throws TException
    {
	try {
            return new Identifier("bid-" + UUID.randomUUID().toString(), Identifier.Namespace.Local);
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in minting batch ID - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("error in minting batch ID");
	}
    }

    public static String processObjectID(ProfileState profileState, JobState jobState, IngestRequest ingestRequest, boolean mint)
        throws TException
    {
	return processObjectID(profileState, jobState, ingestRequest, mint, false);
    }

    public static String processObjectID(ProfileState profileState, JobState jobState, IngestRequest ingestRequest, boolean mint, boolean shadow)
        throws TException
    {
	// EZID implemntation.
	try {
	    DefaultHttpClient httpClient = new DefaultHttpClient();
	    if (isDevelopment(profileState)) {
		// ignore self signed certs
		httpClient = wrapClient(httpClient);
	    }

	    // authenticate
	    String misc = null;
	    if ((misc = profileState.getMisc()) == null) {
	        System.err.println("[warning] " + MESSAGE + "EZID credentials not found.");
		throw new TException.GENERAL_EXCEPTION("EZID credentials not found.");
	    }

	    String[] auth = misc.split(":");
	    Credentials credentials = new UsernamePasswordCredentials( auth[0], auth[1] );
	    httpClient.getCredentialsProvider().setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), credentials);

	    String url = profileState.getObjectMinterURL().toString();
	    HttpEntityEnclosingRequestBase httpCommand = null;
	    if ( ! mint) {
	        // Update an ID.  Fails if ID does not exist.
	        if (shadow) 
		    url = url.replaceFirst("/shoulder.*", "/id/") + jobState.getLocalID().getValue();
		else
		    url = url.replaceFirst("/shoulder.*", "/id/") + jobState.getPrimaryID().getValue();
		System.out.println("[info] " + MESSAGE + "updating ID: " + url);
	    }

	    String target = "";
	    String group = "";

	    // Eventually we will not need group for access, but for now assume the first ARK in collection is a valid group
	    Iterator collections = profileState.getCollection().iterator();
  	    while (collections.hasNext()) {
    		String collection = (String) collections.next();
		if (collection.startsWith("ark:/")) {
		    System.out.println("[info] " + MESSAGE + "Found group identifier: " + collection);
		    group = "&group=" + escape(collection);
		    break;
		} else {
		    System.err.println("[warning] " + MESSAGE + "Collection ID is not a valid group identifier: " + collection);
		}
  	    }
	    if (StringUtil.isEmpty(group))
	        System.err.println("[warning] " + MESSAGE + "No group found. Thus no group info in EZID target URL");
	    try {
		// e.g. http://merritt.cdlib.org/m/{objectID}
		// Need to double encode the id, as EZID will HEX percent decode
		target = "_target: " + ingestRequest.getServiceState().getTargetID() + "/m/" +
			URLEncoder.encode(URLEncoder.encode(jobState.getPrimaryID().getValue(), "UTF-8"), "UTF-8");
	        System.out.println("[info] " + MESSAGE + "Target url: " + target);
	    } catch (Exception e) { }

	    // Is context available?
	    String context = "";
	    try {
	        context = "mrt.creator: " + escape(profileState.getContext());
	    } catch (Exception e) { }

	    httpCommand = new HttpPost(url);
	    httpCommand.addHeader("Content-Type", "text/plain");
	    if (! shadow) httpCommand.setEntity(new StringEntity(getMetadata(jobState) + "\n" + context + "\n" + target, "UTF-8"));
	    else httpCommand.setEntity(new StringEntity(target, "UTF-8"));

            String responseBody = null;
	    HttpResponse httpResponse = null;
	    try {
                httpResponse = httpClient.execute(httpCommand);
		responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
	    } catch (HttpHostConnectException hhce) {
	        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("error in connecting to host: " + url);
	    } catch (org.apache.http.client.HttpResponseException hre) {
		System.err.println("[error] " + MESSAGE + "request failed with status code: " + hre.getStatusCode());
		System.err.println("[error] " + MESSAGE + "request failed with status message: " + hre.getMessage());
		responseBody = "failed";
	    }
	    if (responseBody.startsWith("success")) {
                System.out.println("[info] " + MESSAGE + responseBody);
                System.out.println("[info] " + MESSAGE + url);
                System.out.println("[info] " + MESSAGE +  "erc: " + getMetadata(jobState));
                System.out.println("[info] " + MESSAGE + context);
                System.out.println("[info] " + MESSAGE + target);
	    }
	    String expectedResponse = "success:";		// e.g. success: ark:/99999/fk42z13f2
	    System.out.println("[info] POST " + responseBody);

	    String id = new String(responseBody);
	    if ( ! id.startsWith(expectedResponse)) {
	        if (! mint) {
	            System.out.println("[info] " + MESSAGE + "could not update, attempting to create: " + url);
	            httpCommand = new HttpPut(url);
	    	    httpCommand.addHeader("Content-Type", "text/plain");
	            httpCommand.setEntity(new StringEntity(getMetadata(jobState) + "\n" + context + "\n" + target, "UTF-8"));

		    try {
                	httpResponse = httpClient.execute(httpCommand);
                        responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
		    } catch (HttpHostConnectException hhce) {
		       throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("error in connecting to host: " + url);
		    } catch (org.apache.http.client.HttpResponseException hre) {
			System.err.println("[error] " + MESSAGE + "request failed with status code: " + hre.getStatusCode());
			System.err.println("[error] " + MESSAGE + "request failed with message: " + hre.getMessage());
			responseBody = "failed";
		        throw new TException.GENERAL_EXCEPTION("error in creating identifier: " + responseBody);
		    }
	    	    System.out.println("[info] PUT " + responseBody);
	       	    id = new String(responseBody);
	            if ( ! id.startsWith(expectedResponse)) {
		        throw new TException.GENERAL_EXCEPTION("error in creating identifier: " + url);
		    }
		    System.out.println("[info] " + MESSAGE + "created new ID: " + url);
	        } else {
		    throw new TException.GENERAL_EXCEPTION("error in minting identifier: " + responseBody + " --- " + url);
		}
	    }

	    try {
	        return StringUtil.squeeze(id.substring(expectedResponse.length()));
	    } catch (Exception e) {
	        return "(:unas)";
	    }
	    
	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in processing ID - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("error in processing ID");
	}
    }

    private static String getMetadata(JobState jobState)
        throws TException
    {
        try {
	    if (StringUtil.isNotEmpty(jobState.grabERC())) return jobState.grabERC();

	    String md = "";
	    try {
		md += "who: " + escape(jobState.getObjectCreator()) + EOL;
	    } catch (Exception e) {}
	    try {
		md += "what: " + escape(jobState.getObjectTitle()) + EOL;
	    } catch (Exception e) {}
	    try {
		md += "when: " + escape(jobState.getObjectDate()) + EOL;
	    } catch (Exception e) {}
	    try {
		md += "where: " + escape(jobState.getPrimaryID().getValue()) + EOL;
	    } catch (Exception e) {}
	    try {
		md += "where: " + escape(jobState.getLocalID().getValue()) + EOL;
	    } catch (Exception e) {}

            return "erc: " + md;
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("error processing metadata");
        }
    }

    private static String escape(String input)
        throws TException
    {
        try {
		return input.replaceAll("%", "%25").replaceAll("\n", "%0A").replaceAll("\r", "%0D").replaceAll(":", "%3A");
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("escaping ERC metadata");
        }
    }



    private static String getTargetURL(JobState jobState)
        throws TException
    {
        try {
	    String md = "";
	    return jobState.grabTargetStorage().getStorageLink().toString() + "/state/" + jobState.grabTargetStorage().getNodeID() + "/" + 
			    URLEncoder.encode(jobState.getPrimaryID().getValue(), "UTF-8");
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("error accessing target URL");
        }
    }

    private static boolean isDevelopment(ProfileState profileState) {
	return ! profileState.getObjectMinterURL().toString().contains("https://n2t.net/");
    }

    // http://theskeleton.wordpress.com/2010/07/24/
	// avoiding-the-javax-net-ssl-sslpeerunverifiedexception-peer-not-authenticated-with-httpclient/
    public static DefaultHttpClient wrapClient(DefaultHttpClient base) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {
 
                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException { }

                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException { }
 
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory ssf = new SSLSocketFactory(ctx);
            ssf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static String fetchPrimaryID(ProfileState profileState, String localID)
        throws TException
    {

        ClientResponse clientResponse = null;
        try {

	    String primaryID = null;
            StoreNode storeNode = profileState.getTargetStorage();

            // build REST url
            String url = storeNode.getStorageLink().toString() + "/primary/" + storeNode.getNodeID() + "/" + 
			URLEncoder.encode(profileState.getOwner(), "utf-8") + "/" +
			URLEncoder.encode(localID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] LocalID/PrimaryID fetch URL: " + url);
            Client client = Client.create();    // reuse?  creation is expensive
            WebResource webResource = client.resource(url);

            // make service request
            clientResponse = webResource.get(ClientResponse.class);
	    int status = clientResponse.getStatus();
   	    String response = null;
 
	    if (status != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                    throw new TException.REQUEST_INVALID(tExceptionResponse.getError());
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url);
                }
	    } else {
   	        response = clientResponse.getEntity(String.class);
	    }

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            domFactory.setExpandEntityReferences(true);

            DocumentBuilder builder = domFactory.newDocumentBuilder();
            builder.setErrorHandler(null);
            Document document = builder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression expr = xpath.compile("//*[local-name()='primaryIdentifier']");

            String xpathS = (String) expr.evaluate(document);
            if (StringUtil.isNotEmpty(xpathS)) {
                if (DEBUG) System.out.println("[debug] primary ID: " + xpathS);
                primaryID = xpathS;
            } else {
                if (DEBUG) System.out.println("[debug] Can not determine primary ID");
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

}
