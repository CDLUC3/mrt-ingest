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
import com.sun.jersey.api.client.config.ClientConfig; 
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

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
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

public class HandlerCallback extends Handler<JobState> {
    
    FormatterUtil formatterUtil = new FormatterUtil();
    URL url = null;
    private boolean notify = true;
    private boolean error = true;
    private static final String NAME = "HandlerCallback";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;


    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, 
                                JobState jobState) throws TException {

        ClientResponse clientResponse = null;
	FormatType formatType = null;
 
        try {

            url = new URL(profileState.getCallbackURL().toString());
	    String credentials = url.getUserInfo();
	    String protocol = url.getProtocol();
            Client client = null;

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
		    client.addFilter(new HTTPBasicAuthFilter(cred[0], cred[1]));
		} catch (Exception e) {
            	    if (DEBUG) System.out.println("[warn] " + MESSAGE + " Basic Authenication parmeters not valid: " + credentials);
		}
		// remove credential from URL 
		String urlString = url.toString();
		url = new URL(urlString.replaceFirst(credentials + "@", ""));
	    }

            WebResource webResource = client.resource(url.toString());

            if (ingestRequest.getNotificationFormat() != null) {
		// POST parm overrides profile parm
		formatType = ingestRequest.getNotificationFormat();
            	if (DEBUG) System.out.println("[info] " + MESSAGE + " Notification Format set as a POST parameter: " + formatType);
            } else if (profileState.getNotificationFormat() != null) {
		formatType = profileState.getNotificationFormat();     
            	if (DEBUG) System.out.println("[info] " + MESSAGE + " Notification Format set as a Profile parameter: " + formatType);
	    } else {
		formatType = FormatType.valueOf("xml");		// default
            	if (DEBUG) System.out.println("[info] " + MESSAGE + " Notification Format not set.  Default: " + formatType);
	    }

            String jobStateString = formatterUtil.doStateFormatting(jobState, formatType);

            // make service request
            try {
                clientResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).put(ClientResponse.class, jobStateString);
            } catch (Exception e) {
		e.printStackTrace();
                error = true;
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Callback service: " + url);
            }
            if (DEBUG) System.out.println("[debug] " + MESSAGE + " response code " + clientResponse.getStatus());

            if (clientResponse.getStatus() != 200) {
                error = true;
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE tExceptionResponse = clientResponse.getEntity(TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE.class);
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(tExceptionResponse.getError());
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
		    e.printStackTrace();
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Callback service: " + url);
                }
            }

            String msg = String.format("SUCCESS: %s completed successfully", getName());

            return new HandlerResult(true, msg, 0);
        } catch (Exception ex) {
            String msg = String.format("WARNING: %s could not make Callback URL service request: %s", getName(), url);
	    ex.printStackTrace();
            return new HandlerResult(true, msg, 0);
        } finally {
            if (error) {
                if (DEBUG) System.out.println("[error] Callback request failed: " + url);
                if (notify && error) notify(jobState, profileState, ingestRequest);
                clientResponse = null;
            }

	    formatterUtil = null;
        }
    }

    protected void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
        String server = "";
        String owner = "";
        MultiPartEmail email = new MultiPartEmail();

        try {
            email.setHostName("localhost");     // production machines are SMTP enabled
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
            email.setSubject("[Warning] Callback request failed " + server + owner);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
        } catch (Exception e) { e.printStackTrace(); }

        return;
    }

    public String getName() {
	return HandlerCallback.class.toString();
    }
}
