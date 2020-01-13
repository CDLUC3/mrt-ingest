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
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import java.security.SecureRandom;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;

import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.json.JSONObject;


/**
 * simple json tools and couchDB interface routines
 * @author mreyes
 */
public class JSONUtil
{

    private static final String NAME = "JSONUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String DELIMITER = "\t";
    private LoggerInf logger = null;

    /**
     * create json 
     *
     * @param jsonObject object to update
     * @param key string 
     * @param value json string
     * @return JSON object
     */
    public static JSONObject string2json(String jsonObjectString)
        throws TException
    {
	JSONObject jsonObject = null;
	try {
	    jsonObject = new JSONObject(jsonObjectString);
	} catch (Exception e) { }

	return jsonObject;
    }


    /**
     * read json 
     *
     * @param jsonObject object
     * @return jsonobject as string
     */
    public static String json2string(JSONObject jsonObject)
        throws TException
    {
	
	try {
	    return jsonObject.toString();
	} catch (Exception e) { }

	return null;
    }

    /**
     * post jobstate data. relying on couchDB API 
     * (http://wiki.apache.org/couchdb/HTTP_Document_API)
     *
     * @param jsonObject object
     * @return boolean hosting status
     */
    public static boolean updateJobState(ProfileState profileState, JobState jobState)
        throws TException
    {
	
	URL url = null;
        ClientResponse clientResponse = null;
        WebResource webResource = null;
        FormatType formatType = null;
        Client client = null;

	try {

	    // define db ID
            url = new URL(profileState.getStatusURL().toString() + "/" + jobState.grabBatchID().getValue().toString());
            String credentials = url.getUserInfo();
            String protocol = url.getProtocol();

            // https - trust all certs
            if (protocol.equals("https")) {
                X509TrustManager tm = new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException { }
                    public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException { }
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                };
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " Setting SSL protocol");
                ClientConfig config = new DefaultClientConfig();
                SSLContext ctx = SSLContext.getInstance("TLS");
                ctx.init(null, new TrustManager[]{tm}, new SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());

                config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(
                    new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                }, ctx));

                client = Client.create(config);
            } else {
                client = Client.create();    // reuse?  creation is expensive
            }

            // HTTP Basic authentication (optional)
            if (credentials != null) {
                try {
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + " Setting Basic Authenication parameters");
                    String[] cred = credentials.split(":");
                    client.addFilter(new HTTPBasicAuthFilter(cred[0], java.net.URLDecoder.decode(cred[1])));
                } catch (Exception e) {
		    e.printStackTrace();
                    if (DEBUG) System.out.println("[warn] " + MESSAGE + " Basic Authenication parmeters not valid: " + credentials);
                    TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                    throw new TException.REQUEST_INVALID(e.getMessage());
                }
                // remove credential from URL 
                String urlString = url.toString();
                url = new URL(urlString.replaceFirst(credentials + "@", ""));
            }

	    // create DB
            try {
                webResource = client.resource(url.toString());
                clientResponse = webResource.put(ClientResponse.class);
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " database create: " + clientResponse.toString());
            } catch (Exception ignore) { /* may already exist */ }

	    // define document ID
            url = new URL(profileState.getStatusURL().toString() + "/" + jobState.grabBatchID().getValue() 
		+ "/" + jobState.getJobID().getValue());
            webResource = client.resource(url.toString());

	    // remove old versions
	    int deleteResponse = 200;		// expected response from REST DELETE
	    String jsonObjectString = null;
	    while (deleteResponse == 200) {
                try {
		    // get exsting revision number
		    clientResponse = webResource.get(ClientResponse.class);
		    if (clientResponse.getStatus() != 200) break; 
		    JSONObject jsonObject = string2json((String) clientResponse.getEntity(String.class));
		    if (jsonObject.isNull("_rev")) break;

		    // include revision in DELETE request
                    String etag = (String) jsonObject.get("_rev");
                    clientResponse = webResource.header("If-Match", etag).delete(ClientResponse.class);
		    deleteResponse = clientResponse.getStatus();
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + " document delete: " + clientResponse.toString());
                } catch (com.sun.jersey.api.client.ClientHandlerException che) { 
		    che.printStackTrace();
                    TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                    throw new TException.REQUEST_INVALID(che.getMessage());
                } catch (Exception e) { 
                     e.printStackTrace();
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Deleting previous versions: " + url);
		}
	    }

	    // create document
            try {
                String jobStateString = removeNamespaceJobState(jobState);
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " jobState: " + jobStateString);

                clientResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).put(ClientResponse.class, jobStateString);
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " document create: " + clientResponse.toString());
            } catch (Exception e) {
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Posting service: " + url);
            }

            if (clientResponse.getStatus() != 201) {
                try {
                    // can not create
                    TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE tExceptionResponse = clientResponse.getEntity(TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE.class);
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(tExceptionResponse.getError());
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    e.printStackTrace();
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Posting service: " + url);
                }
            }


            url = new URL(profileState.getStatusURL().toString() + "/" 
		+ jobState.grabBatchID().getValue() + "/_design/status");
            webResource = client.resource(url.toString());
            String designDoc = null;
	    // add views (design doc)
            try {
                designDoc = FileUtil.file2String(profileState.getStatusView());
                clientResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).put(ClientResponse.class, designDoc);

            } catch (Exception ignore) { /* may already exist */ }
            if (clientResponse.getStatus() == 201) {
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " design document: " + designDoc);
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " design document create: " + clientResponse.toString());
            }

	    return true;

	} catch (Exception e) { 
            if (DEBUG) System.out.println("[error] " + MESSAGE + " Error posting data to istatus: " + url.toString());
	    // e.printStackTrace();
	    return false; 
	}
    }

    public static String getName() {
        return JSONUtil.class.toString();
    }

    private static String removeNamespaceJobState(JobState jobState) {

	String jobStateString = "";
        FormatterUtil formatterUtil = new FormatterUtil();
	try {

	    jobStateString = formatterUtil.doStateFormatting(jobState, FormatType.json).replaceAll("job:","").
        	    replaceFirst("\"xmlns:job\":\"http://uc3.cdlib.org/ontology/mrt/ingest/job\",","");
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	   formatterUtil = null;
	}

	return jobStateString;
    }

   public static void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
        String server = "";
        String owner = "";
        MultiPartEmail email = new MultiPartEmail();

        try {
            email.setHostName(ingestRequest.getServiceState().getMailHost());     // production machines are SMTP enabled
            if (profileState.getAdmin() != null) {
                for (Iterator<String> admin = profileState.getAdmin().iterator(); admin.hasNext(); ) {
                    // admin will receive notifications
                    String recipient = admin.next();
                    if (StringUtil.isNotEmpty(recipient)) email.addTo(recipient);
                }
            }
            String ingestServiceName = ingestRequest.getServiceState().getServiceName();
            if (StringUtil.isNotEmpty(ingestServiceName))
                if (ingestServiceName.contains("Development")) server = " [Development]";
                else if (ingestServiceName.contains("Stage")) server = " [Stage]";
            if (StringUtil.isNotEmpty(profileState.getContext())) owner = " [owner: " + profileState.getContext() + "]";
            email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");
            email.setSubject("[Warning] Istatus request failed " + server + owner);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
        } catch (Exception e) { e.printStackTrace(); }

        return;
    }

}
