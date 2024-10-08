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


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Set;
import java.util.LinkedHashSet;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.HttpResponse;

import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;


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
    protected static final Logger log4j2 = LogManager.getLogger();

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
        HashMap<String,Object> msgMap = new HashMap<>();        // Non string logging

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
		url = url.replaceFirst("/shoulder.*", "/id/") + jobState.getPrimaryID().getValue();
		System.out.println("[info] " + MESSAGE + "updating ID: " + url);
	    }

	    String target = "";
	    String group = "";

	    // Eventually we will not need group for access, but for now assume the first ARK in collection is a valid group
	    Iterator<String> collections = profileState.getCollection().iterator();
  	    while (collections.hasNext()) {
    		String collection = collections.next();
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
		if (ingestRequest.getRetainTargetURL()) {
		   target = "";
	           System.out.println("[info] " + MESSAGE + "Found retain existing Target URL.  Not setting _target for EZID.");
		}
	    } catch (Exception e) { }

	    // Is context available?
	    String context = "";
	    try {
	        context = "mrt.creator: " + escape(profileState.getContext());
	    } catch (Exception e) { }

	    // Is co-owner available?
	    String coowner = "";
	    try {
		if (profileState.getEzidCoowner() != null) {
	            // coowner = "\n" + "_coowners: " + profileState.getEzidCoowner() + "\n";
	            coowner = "_owner: " + profileState.getEzidCoowner();
                    System.out.println("[info] " + MESSAGE + "Found EZID co-owner: " + profileState.getEzidCoowner());
		}
	    } catch (Exception e) { }

	    // Is this an admin object??
	    String adminHeader = "";
	    String aggregateType = profileState.getAggregateType();
	    try {
		if (aggregateType != null) {
		    if (aggregateType.matches("MRT-collection|MRT-owner|MRT-service-level-agreement")) {
                        System.out.println("[info] " + MESSAGE + "Object is admin.  Setting to not harvest and to make reserved.");
			// Do not harvest and flag as a reserved ID
	                adminHeader = "_status: reserved" + "\n" + "_export: no";
		    }
		}
	    } catch (Exception e) {
	        System.err.println("[warning] " + MESSAGE + "Could not determine if object is admin aggregate type: " + aggregateType);
	    }

	    try {
	        httpCommand = new HttpPost(url);
	    } catch (java.lang.IllegalArgumentException iae) {
		throw new TException.INVALID_OR_MISSING_PARM("Target hostname or primary ID not valid: " + url);
	    }
	    httpCommand.addHeader("Content-Type", "text/plain");
	    String stringEntity = null;

	    // Populate with ERC profile and other fields, if supplied
	    stringEntity = getMetadata(jobState) + "\n" + context + "\n" + target + "\n" + coowner + "\n" + adminHeader;
            httpCommand.setEntity(new StringEntity(stringEntity, "UTF-8"));

            System.out.println("[info] POST stringEntity: " + stringEntity);

            String responseBody = null;
	    HttpResponse httpResponse = null;
	    String statusPhrase = null;
	    int statusCode;
            long startTime = DateUtil.getEpochUTCDate();

            int retryCount = 0;
            while (true) {
	    	try {
		     httpResponse = httpClient.execute(httpCommand);
		     responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
		     statusCode = httpResponse.getStatusLine().getStatusCode();
		     statusPhrase = httpResponse.getStatusLine().getReasonPhrase();
		     break;
	    	} catch (HttpHostConnectException hhce) {
		     retryCount++;
                     if (retryCount >= 3) {
	                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("error in connecting to host: " + url);
		     }
		     System.err.println("[error] " + MESSAGE + "error connecting to host. " + hhce.getMessage());
		     System.err.println("Wait 5 seconds and retry attempt: " + retryCount);
		     Thread.sleep(5000);
	    	} catch (Exception e) {
		     retryCount++;
                     if (retryCount >= 3) {
			throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("minting ID from endpoint: " + url);
		     }
		     System.err.println("[error] " + MESSAGE + "request failed with status message: " + e.getMessage());
		     System.err.println("Wait 5 seconds and retry attempt: " + retryCount);
		     Thread.sleep(5000);
		     responseBody = "failed";
	    	}
	    }

	    // Log POST
            long endTime = DateUtil.getEpochUTCDate();
	    ThreadContext.put("Method", "EzidPost");
	    ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
	    ThreadContext.put("JobID", jobState.getJobID().getValue());
	    ThreadContext.put("URL", url);
	    ThreadContext.put("ResponsePhrase", statusPhrase);
	    ThreadContext.put("ResponseBody", responseBody);
            msgMap.put("DurationMs", endTime - startTime);
            msgMap.put("ResponseCode", statusCode);
	    LogManager.getLogger().info(msgMap);

            System.out.println("[info] " + MESSAGE + "response code: " + statusCode);
            System.out.println("[info] " + MESSAGE + "response phrase: " + statusPhrase);
	    if (responseBody.startsWith("success")) {
                System.out.println("[info] " + MESSAGE + "response body: " + responseBody);
	    }
	    String expectedResponse = "success:";		// e.g. success: ark:/99999/fk42z13f2

	    String id = new String(responseBody);
	    if ( ! id.startsWith(expectedResponse)) {
        	startTime = DateUtil.getEpochUTCDate();
	        if (! mint) {
	            System.out.println("[info] " + MESSAGE + "could not update, attempting to create/update: " + url + "?update_if_exists=yes");
                    Thread.sleep(15000);
	            httpCommand = new HttpPut(url + "?update_if_exists=yes");
	    	    httpCommand.addHeader("Content-Type", "text/plain");
		    
		    System.out.println("[info] PUT stringEntity: " + stringEntity);
	            httpCommand.setEntity(new StringEntity(stringEntity, "UTF-8"));

		    try {
                	httpResponse = httpClient.execute(httpCommand);
                        responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
		    } catch (HttpHostConnectException hhce) {
		       throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("error in connecting to host: " + url);
		    } catch (org.apache.http.client.HttpResponseException hre) {
			System.err.println("[error] " + MESSAGE + "request failed with status code: " + hre.getStatusCode());
			System.err.println("[error] " + MESSAGE + "request failed with message: " + hre.getMessage());
			responseBody = "failed";
		        throw new TException.GENERAL_EXCEPTION("error in creating/updating identifier: " + responseBody);
		    }
	    	    System.out.println("[info] PUT " + responseBody);
	       	    id = new String(responseBody);
	            if ( ! id.startsWith(expectedResponse)) {
		        throw new TException.GENERAL_EXCEPTION("error in creating/updating identifier: " + url);
		    }
		    System.out.println("[info] " + MESSAGE + "created/updated ID: " + url);
	        } else {
		    System.err.println("[error] " + MESSAGE + "Encountered incorrect response during mint attempt: " + statusCode + " : " + statusPhrase);
		    retryCount = 1;
		    System.err.println("[error] " + MESSAGE + "Awaiting retry");

                    while (true) {
                        if (retryCount >= 3) {
		            throw new TException.GENERAL_EXCEPTION("error in minting identifier: " + responseBody + " --- " + url);
                        } else {
                            Thread.sleep(15000);
		        }
		        System.err.println("[info] " + MESSAGE + "Attempting retry: " + retryCount);
                        httpResponse = httpClient.execute(httpCommand);
                        responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
            	        id = new String(responseBody);
                        retryCount++;
                        statusCode = httpResponse.getStatusLine().getStatusCode();
                        statusPhrase = httpResponse.getStatusLine().getReasonPhrase();

                        if (statusCode < 500) break;
		        System.err.println("[error] " + MESSAGE + "Encountered incorrect response during mint attempt: " + statusCode + " : " + statusPhrase);
                        System.err.println(MESSAGE + "Wait 15 seconds and retry attempt: " + retryCount);
		    }
		}
		// Log PUT
        	endTime = DateUtil.getEpochUTCDate();
            	ThreadContext.put("Method", "EzidPut");
            	ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
            	ThreadContext.put("JobID", jobState.getJobID().getValue());
            	ThreadContext.put("URL", url);
            	ThreadContext.put("ResponsePhrase", statusPhrase);
            	ThreadContext.put("ResponseBody", responseBody);
            	msgMap.put("DurationMs", endTime - startTime);
            	msgMap.put("ResponseCode", statusCode);
            	LogManager.getLogger().info(msgMap);
	    }

	    try {
	        return StringUtil.squeeze(id.substring(expectedResponse.length()));
	    } catch (Exception e) {
	        return "(:unas)";
	    }
	    
	} catch (TException tex) {
            LogManager.getLogger().error(tex);
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            LogManager.getLogger().error(ex);
            String err = MESSAGE + "error in processing ID - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("error in processing ID");
        } finally {
            ThreadContext.clearMap();
            msgMap.clear();
            msgMap = null;
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

    private static List<String> getDataCiteMetadata(IngestRequest ingestRequest, ProfileState profileState, JobState jobState)
        throws TException
    {
        try {

	    String dataCiteMetadata = null;
	    List<String> md = new ArrayList();

	    if ((dataCiteMetadata = jobState.grabDataCiteMetadata()) != null) {
		md.add("datacite: " + escape(dataCiteMetadata));
	    } else {
	        try {
		    if (jobState.getObjectCreator() == null) 
		        md.add("datacite.creator: " + escape("(:unas)"));
		    else
		        md.add("datacite.creator: " + escape(jobState.getObjectCreator()));
	        } catch (Exception e) {}
	        try {
		    if (jobState.getObjectTitle() == null) 
		        md.add("datacite.title: " + escape("(:unas)"));
		    else
		        md.add("datacite.title: " + escape(jobState.getObjectTitle()));
	        } catch (Exception e) {}
	        try {
		    if (jobState.getObjectDate() == null) 
			md.add("datacite.publicationyear: " + escape(getCurrentYear()));
		    else 
			md.add("datacite.publicationyear: " + escape(jobState.getObjectDate()));
	        } catch (Exception e) {}
	        try {
		    md.add("datacite.publisher: " + escape(profileState.getOwner()));
	        } catch (Exception e) {}
	        try {
		    if (ingestRequest.getDataCiteResourceType() != null) 
		        md.add("datacite.resourcetype: " + escape(ingestRequest.getDataCiteResourceType()));
		    else
		        md.add("datacite.resourcetype: " + escape(createResourceType(jobState).toString()));
	        } catch (TException te) { 
	            throw te;
	        } catch (Exception e) {}
	    }

            return md;
        } catch (TException te) { 
	    throw te; 
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("error processing datacite metadata");
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


    private static ResourceTypeEnum createResourceType(JobState jobState)
        throws TException
    {
	String DCformat = null;
	String resourceType = null;
        try {
	    // map mimetype to DataCite resource type
	    DCformat = jobState.getDCformat();

	    try {
	        if (DCformat.startsWith("application/")) resourceType = "Dataset";
	        if (DCformat.startsWith("audio/"))resourceType = "Sound";
	        if (DCformat.startsWith("example/")) resourceType = "Text";
	        if (DCformat.startsWith("image/")) resourceType = "Image";
	        if (DCformat.startsWith("message/")) resourceType = "Text";
	        if (DCformat.startsWith("model/")) resourceType = "Model";
	        if (DCformat.startsWith("multipart/")) resourceType = "Collection";
	        if (DCformat.startsWith("text/")) resourceType = "Text";
	        if (DCformat.startsWith("video/")) resourceType = "Film";
	    } catch (Exception e) {
		if (DEBUG) System.out.println("[WARN] " + MESSAGE + "No valid DC Format specified");
	    }

	    if (resourceType == null) {
		resourceType = "Dataset";
		if (DEBUG) System.out.println("[INFO] " + MESSAGE + 
		    "Could not determing datacite resource type. Using default: " + resourceType);
	    }
	    return ResourceTypeEnum.setResourceType(resourceType);
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("mapping mimetype to DataCite resource type: " + DCformat);
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
	// return ! profileState.getObjectMinterURL().toString().contains("https://n2t.net/");
	return true;
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


    private static String getCurrentYear() {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

    public static String sanitize(String s) {
        Set<String> set = new LinkedHashSet<String>();

        String rebuild = "";
        boolean first = true;
        for (String p: s.split(";")) {
            p = p.trim();

            if (! set.contains(p)) {
                if (first) {
                    rebuild = p;
                    first = false;
                } else {
                    rebuild += ";" + p;
                }
                set.add(p);
            }
        }
        if (first) rebuild = s;
        System.out.println("[info] " + MESSAGE + "sanitized localid: " + s + " ---> " + rebuild);

        return rebuild;
    }

}
