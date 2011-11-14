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

import java.io.IOException;
import java.util.Iterator;

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
import org.cdlib.mrt.ingest.utility.StateUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

public class HandlerCallback extends Handler<JobState> {
    
    StateUtil stateUtil = new StateUtil();
    String url = null;
    protected boolean notify = true;
    protected boolean error = false;

    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, 
                                JobState jobState) throws TException {

        ClientResponse clientResponse = null;
	FormatType formatType = null;
 
        try {

            // build REST url 
            url = profileState.getCallbackURL().toString();
	    try {
	        formatType = FormatType.valueOf(profileState.getCallbackFormat());
	    } catch (Exception e) {
		// default
	        formatType = FormatType.valueOf("xml");
	    }

            Client client = Client.create();    // reuse?  creation is expensive
            WebResource webResource = client.resource(url);

            Form formData = new Form();
            formData.add("jobstate", stateUtil.doStateFormatting(jobState, formatType));

            // make service request
            try {
                clientResponse = webResource.type(MediaType.APPLICATION_FORM_URLENCODED).post(ClientResponse.class, formData);
            } catch (Exception e) {
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

	    stateUtil = null;
        }
    }

    protected void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
        String server = "";
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
            email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");
            email.setSubject("[Warning] Callback request failed " + server);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
        } catch (Exception e) { e.printStackTrace(); }

        return;
    }

    public String getName() {
	return HandlerCallback.class.toString();
    }
}
