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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.ByteArrayDataSource;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


/**
 * notify user of ingest results
 * @author mreyes
 */
public class HandlerNotification extends Handler<BatchState>
{

    private static final String NAME = "HandlerNotification";
    private static final String SERVICE = "Ingest";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;

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
        JobState jobState = ingestRequest.getJob();
	boolean isBatch = true;
	boolean batchComplete = false;
        FormatterUtil formatterUtil = new FormatterUtil();
	FormatType formatType = null;

	try {
	    // Is this a batch submission?
	    if (jobState.grabBatchID().getValue().equalsIgnoreCase(ProfileUtil.DEFAULT_BATCH_ID)) {
		isBatch = false;
	    }

	    String batchID = batchState.getBatchID().getValue();
            if ( isBatch) {
                if (BatchState.getBatchCompletion(batchID) == BatchState.getBatchState(batchID).getJobStates().size()) {
		    batchComplete = true;
	        }
	    }

  	    email.setHostName("localhost");	// production machines are SMTP enabled
	    for (Notification recipient : profileState.getContactsEmail()) {
		try {
  	    	    email.addTo(recipient.getContactEmail());
		} catch (Exception e) { }
	    }
	    if (profileState.getAdmin() != null) {
		for (Iterator<String> admin = profileState.getAdmin().iterator(); admin.hasNext(); ) {
		    // admin will receive all completion notifications
		    String recipient = admin.next();
		    if (StringUtil.isNotEmpty(recipient)) email.addBcc(recipient);
		}
	    }
  	    email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");
	    String status = "OK";

	    // instance?
	    String server = null;
	    try {
		String ingestServiceName = ingestRequest.getServiceState().getServiceName();
		if (StringUtil.isNotEmpty(ingestServiceName))
		    if (ingestServiceName.contains("Development")) server = "dev";
		    else if (ingestServiceName.contains("Stage")) server = "stg";
	    } catch (NullPointerException npe) {}

	    if ( ! isBatch) {
		jobState = batchState.getJobState(batchState.getJobStates().keySet().iterator().next());	// s/b only one key

  	        email.setSubject(FormatterUtil.getSubject(SERVICE, server, status, "Job Processed", jobState.getJobID().getValue()));
  	        email.setMsg(jobState.dump("", "", "\n", null));
  	        email.send();
	    } else if (batchComplete) {
		// send summary in body and report as attachment
		try {
 	            if (batchState.getBatchStatus() == BatchStatusEnum.FAILED) {
		        status = "Fail";
		    }
		} catch (Exception e) {}

		// admin object?
		String aggregate = "";
		try {
		   if (StringUtil.isNotEmpty(profileState.getAggregateType()))
		       aggregate = "[" + profileState.getAggregateType() + "] ";
		} catch (NullPointerException npe) {}

  	        email.setSubject(FormatterUtil.getSubject(SERVICE, server, status, "Submission Processed", aggregate + batchState.getBatchID().getValue()));

		// Comma delimited
		//email.attach(new ByteArrayDataSource(batchState.dump("", false, true), "text/csv; header=present"),
			 //batchID + ".csv", "Comma delimited Job Report for " +  batchID, EmailAttachment.ATTACHMENT);

		// attachment: batch state with user defined formatting
		formatType = profileState.getNotificationFormat();
		try {
		    email.attach(new ByteArrayDataSource(formatterUtil.doStateFormatting(batchState, profileState.getNotificationFormat()), formatType.getMimeType()),
			batchID + "." + formatType.getExtension(), "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
		} catch (Exception e) {
		    e.printStackTrace();
		    // human readable
		    email.attach(new ByteArrayDataSource("Completion of Ingest - " + batchState.dump("Notification Report"), "text/plain"),
			 batchID + ".txt", "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
		}

		email.setMsg(batchState.dump("", false, false));	// summary only
		try {
  	            email.send();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    } else {
	        System.out.println("[info] " + MESSAGE + "batch is not complete.  No notification necessary");
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " notification completed", 0);

	} catch (Exception e) {
	    e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "in notification: " + e.getMessage();
	    throw new TException.GENERAL_EXCEPTION(msg);
	} finally {
            formatterUtil = null;
	    formatType = null;
	}
    }


    public String getName() {
	return NAME;
    }

}
