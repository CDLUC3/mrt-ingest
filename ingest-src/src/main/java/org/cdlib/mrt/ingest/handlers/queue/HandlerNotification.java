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
package org.cdlib.mrt.ingest.handlers.queue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.ByteArrayDataSource;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


/**
 * notify user of queue submission results
 * @author mreyes
 */
public class HandlerNotification extends Handler<BatchState>
{

    protected static final String NAME = "HandlerNotification";
    private static final String SERVICE = "Ingest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;

    /**
     * notify user(s)
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param batchState
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, BatchState batchState) 
	throws TException 
    {
   MultiPartEmail email = new MultiPartEmail();
        FormatterUtil formatterUtil = new FormatterUtil();
        FormatType formatType = null;
        String batchID = batchState.getBatchID().getValue();

	try {

  	    email.setHostName("localhost");	// production machines are SMTP enabled
	    for (Notification recipient : profileState.getContactsEmail()) {
		try {
  	    	    email.addTo(recipient.getContactEmail());
		} catch (Exception e) { 
		    e.printStackTrace();
		    notify(e.getMessage() + " - " + recipient.getContactEmail(), profileState, ingestRequest); 
		}
	    }
  	    email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");

            String server = null;
            String status = "OK";

            // admin object?
            String aggregate = "";
            try {
               if (StringUtil.isNotEmpty(profileState.getAggregateType()))
                   aggregate = "[" + profileState.getAggregateType() + "] ";
            } catch (NullPointerException npe) {}

            // instance?
            try {
               String ingestServiceName = ingestRequest.getServiceState().getServiceName();
               if (StringUtil.isNotEmpty(ingestServiceName))
                    if (ingestServiceName.contains("Development")) server = "dev";
                    else if (ingestServiceName.contains("Stage")) server = "stg";
            } catch (NullPointerException npe) {}

            // attachment: batch state with user defined formatting
            if (ingestRequest.getNotificationFormat() != null) formatType = ingestRequest.getNotificationFormat();
            else if (profileState.getNotificationFormat() != null) formatType = profileState.getNotificationFormat();	// POST parm overrides profile parm

            try {
                email.attach(new ByteArrayDataSource(formatterUtil.doStateFormatting(batchState, formatType), formatType.getMimeType()),
                    batchID + "." + formatType.getExtension(), "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
            } catch (Exception e) {
e.printStackTrace();
                if (DEBUG) System.out.println("[warn] " + MESSAGE + "Could not determine format type.  Setting to default.");
                // human readable
                email.attach(new ByteArrayDataSource("Completion of Ingest - " + batchState.dump("Notification Report"), "text/plain"),
                     batchID + ".txt", "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
            }

            email.setSubject(FormatterUtil.getSubject(SERVICE, server, status, "Submission Queued", aggregate + batchState.getBatchID().getValue()));
  	    email.setMsg(batchState.dump("", false, false));

	    try {
  	        email.send();
	    } catch (Exception e) {
		e.printStackTrace();
		// do not fail?
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " notification completed", 0);

	} catch (Exception e) {
	    e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + ": in submission notification: " + e.getMessage();
	    System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);

        } finally {
            formatterUtil = null;
            formatType = null;
        }
    }
   
    public String getName() {
	return NAME;
    }

    public void notify(String message, ProfileState profileState, IngestRequest ingestRequest) {
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
            email.setSubject("[Warning] Error in user notification" + server);
            email.setMsg(message);
            email.send();
        } catch (Exception e) {};

        return;
    }

}
