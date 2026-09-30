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
package org.cdlib.mrt.ingest.handlers.notify;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.lang.String;
import java.net.URL;
import java.util.HashMap;
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
import javax.mail.internet.InternetAddress;
import java.util.ArrayList;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;

import org.apache.commons.mail.MultiPartEmail;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


public class HandlerCallback extends Handler<JobState> {
    
    FormatterUtil formatterUtil = new FormatterUtil();
    URL requestURL = null;
    private boolean notify = true;
    private boolean error = false;
    private static final String NAME = "HandlerCallback";
    private static final String MESSAGE = NAME + ": ";
    public static final int CALLBACK_TIMEOUT = (5 * 60 * 1000);
    protected static final Logger log4j = LogManager.getLogger();

    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, 
                                JobState jobState) throws TException {

	FormatType formatType = null;
        HashMap<String,Object> msgMap = new HashMap<>();        // Non string logging
        try {
            Thread.sleep(5);

	    try {
	       // Add merritt callback and Job ID to pathname (e.g. mc/<jid>)
               requestURL = new URL(profileState.getCallbackURL().toString() + "/mc/" + jobState.getJobID().getValue());
	    } catch (Exception e) {
                log4j.error("[error] " + MESSAGE + " Callback URL not defined or not valid: " + profileState.getCallbackURL());

		// Callback not supported by profile.  Exit gracefully
		if (profileState.getCallbackURL() == null) return new HandlerResult(true, "Callback not supported", 0);

                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Callback service: " + profileState.getCallbackURL());
	    }
	    String credentials = requestURL.getUserInfo();
	    String protocol = requestURL.getProtocol();
            log4j.debug("[debug] " + MESSAGE + " Callback URL and pathname: " + requestURL.toString());

    	    HttpClient httpClient = HTTPUtil.getHttpClient(requestURL.toString(), CALLBACK_TIMEOUT);
	    String authHeader = null;

	    // HTTP Basic authentication (optional)
	    if (credentials != null) {
		try {
            	    log4j.debug("[debug] " + MESSAGE + " Setting Basic Authenication parameters");
		    String[] cred = credentials.split(":");
		    authHeader = HTTPUtil.getBasicAuthenticationHeader(cred[0], cred[1]);
		} catch (Exception e) {
            	    log4j.warn("[warn] " + MESSAGE + " Basic Authenication parmeters not valid: " + credentials);
		}
		// remove credential from URL 
		String urlString = requestURL.toString();
		requestURL = new URL(urlString.replaceFirst(credentials + "@", ""));
	    }


            if (ingestRequest.getNotificationFormat() != null) {
		// POST parm overrides profile parm
		formatType = ingestRequest.getNotificationFormat();
            	log4j.info("[info] " + MESSAGE + " Notification Format set as a POST parameter: " + formatType);
            } else if (profileState.getNotificationFormat() != null) {
		formatType = profileState.getNotificationFormat();     
            	log4j.info("[info] " + MESSAGE + " Notification Format set as a Profile parameter: " + formatType);
	    } else {
		formatType = FormatType.valueOf("json");		// default
            	log4j.info("[info] " + MESSAGE + " Notification Format not set.  Default: " + formatType);
	    }

            String jobStateString = formatterUtil.doStateFormatting(jobState, formatType);

	    log4j.debug("[CALLBACK] Job String Stage: " + jobStateString);
            HttpPost httppost = new HttpPost(requestURL.toString());
            HttpResponse clientResponse = null;
            long startTime = DateUtil.getEpochUTCDate();

            // make service request
	    int retryCount = 0;
	    while (true) {
                try {
            	    httppost.setHeader("Content-Type", MediaType.APPLICATION_JSON);
            	    if (authHeader != null) httppost.setHeader("Authorization", authHeader);
            	    httppost.setEntity(new StringEntity(jobStateString, "UTF-8"));
            	    clientResponse = httpClient.execute(httppost);
		    break;
                } catch (Exception e) {
                    if (retryCount > 2) {
                        error = true;
			log4j.error("Exception:" + e, e);
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Callback service: " + requestURL);
		    }
                    retryCount++;
                    log4j.error("[error] " + MESSAGE + ": Could not make Callback request: " + e.getMessage());
                }
	    }

            long endTime = DateUtil.getEpochUTCDate();
	    int responseCode = clientResponse.getStatusLine().getStatusCode();
            log4j.debug("[debug] " + MESSAGE + " response code " + responseCode);

	    Family responseFamily = Family.familyOf(responseCode);

            String responseMessage = clientResponse.getStatusLine().getReasonPhrase();
            String responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");

	    if (responseFamily.equals(Response.Status.Family.SUCCESSFUL)) {
    		// 200s
            	log4j.info("[info] " + MESSAGE + " Callback successful: " + responseMessage);
	    } else if (responseFamily.equals(Response.Status.Family.CLIENT_ERROR)) {
    		// 400s
            	log4j.error("[error] " + MESSAGE + " Callback client side error: " + responseMessage);
	    } else if (responseFamily.equals(Response.Status.Family.SERVER_ERROR)) {
    		// 500s
            	log4j.error("[error] " + MESSAGE + " Callback server side error: " + responseMessage);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": Callback service: " + requestURL);
	    } else if (responseFamily.equals(Response.Status.Family.REDIRECTION)) {
    		// 300s
            	log4j.warn("[warn] " + MESSAGE + " Callback redirection encountered: " + responseMessage);
	    } else if (responseFamily.equals(Response.Status.Family.INFORMATIONAL)) {
    		// 100s
            	log4j.warn("[warn] " + MESSAGE + " Callback informational response: " + responseMessage);
	    } else if (responseFamily.equals(Response.Status.Family.OTHER)) {
    		// Other
            	log4j.warn("[warn] " + MESSAGE + " Callback other response: " + responseMessage);
	    }
            if (responseBody != null) log4j.info("[info] " + MESSAGE + " Callback response body: " + responseBody);

            String msg = String.format("SUCCESS: %s completed successfully", getName());

            // Log POST
            ThreadContext.put("Method", "CallbackPost");
            ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
            ThreadContext.put("JobID", jobState.getJobID().getValue());
            ThreadContext.put("URL", requestURL.toString());
            ThreadContext.put("ResponsePhrase", responseMessage);
            ThreadContext.put("ResponseBody", responseBody);
            msgMap.put("ResponseCode", responseCode);
            msgMap.put("DurationMs", endTime - startTime);
            msgMap.put("Retries", retryCount);
            LogManager.getLogger().info(msgMap);

            return new HandlerResult(true, msg, 0);
        } catch (InterruptedException ie) {
            return new HandlerResult(false, "[error]: " + MESSAGE + " Interrupted detected - forcing failure");
        } catch (Exception ex) {
            String msg = String.format("WARNING: %s could not make Callback URL service request: %s", getName(), requestURL);
	    log4j.error("Exception:" + ex, ex);
            LogManager.getLogger().error(ex);
            return new HandlerResult(true, msg, 0);
        } finally {
            ThreadContext.clearMap();
            msgMap.clear();
            msgMap = null;
            if (error) {
                log4j.error("[error] Callback request failed: " + requestURL + " * notifying users * ");
                if (notify && error) notify(jobState, profileState, ingestRequest);
            }

	    formatterUtil = null;
        }
    }

    protected void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
        String server = "";
        String owner = "";
        String contact = profileState.getEmailContact();
        String replyTo = profileState.getEmailReplyTo();
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

	    // email contact
            email.setFrom(contact, "UC3 Merritt Support");

            // email reply to
            if ( replyTo != null ) {
               ArrayList emailReply = new ArrayList();
               emailReply.add(new InternetAddress(replyTo));
               email.setReplyTo(emailReply);
            } else {
               log4j.warn("[warn] " + MESSAGE + "Email replyTo not found.");
            }


            email.setSubject("[Warning] Callback request failed " + server + owner);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
        } catch (Exception e) { 
	    log4j.error("Exception:" + e, e);
	}

        return;
    }

    public String getName() {
	return HandlerCallback.class.toString();
    }
}
