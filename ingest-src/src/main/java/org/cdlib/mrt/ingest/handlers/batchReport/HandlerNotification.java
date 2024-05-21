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
package org.cdlib.mrt.ingest.handlers.batchReport;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import javax.mail.internet.InternetAddress;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.ByteArrayDataSource;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.Job;
import org.cdlib.mrt.zk.ZKKey;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.cdlib.mrt.zk.MerrittJsonKey;
import org.json.JSONArray;
import org.json.JSONObject;



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
    public static int sessionTimeout = 40000;
    private ZooKeeper zooKeeper = null;

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
	boolean batchComplete = false;
	boolean verbose = false;
	boolean csv = false;
        FormatterUtil formatterUtil = new FormatterUtil();
	FormatType formatType = null;

	try {
            try {
                if (profileState.getNotificationSuppression().equalsIgnoreCase("full")) {
                    if (DEBUG) System.out.println("[info] " + MESSAGE + "Detected suppression of completion notification: " + profileState.getNotificationSuppression());
                    return new HandlerResult(true, "SUCCESS: " + NAME + " notification suppressed", 0);
                } 
            } catch (Exception e) {}
	    try {
                if (profileState.getNotificationType().equalsIgnoreCase("verbose")) {
                    if (DEBUG) System.out.println("[info] " + MESSAGE + "Detected 'verbose' format type.");
                    verbose = true;
                }
	    } catch (Exception e) {}
	    try {
                if (profileState.getNotificationType().equalsIgnoreCase("additional")) {
                    if (DEBUG) System.out.println("[info] " + MESSAGE + "Detected 'additional' notification (CSV).");
                    csv = true;
                }
	    } catch (Exception e) {}

            zooKeeper = new ZooKeeper(batchState.grabTargetQueue(), sessionTimeout, new Ignorer());

	    String batchID = batchState.getBatchID().getValue();
	    Batch batch = Batch.findByUuid(zooKeeper, batchID);

	    List<Job> jobs = batch.getProcessingJobs(zooKeeper);
	    if (jobs.isEmpty()) {
                if (DEBUG) System.out.println("[info] " + MESSAGE + "Detected BATCH is complete: " + batch.batchUuid());
		batchComplete = true;
	    } else {
               return new HandlerResult(true, "SUCCESS: " + NAME + " Exiting, Batch not complete: " + batchID, 0);
	    }

	    // Completed Jobs
	    jobs = batch.getCompletedJobs(zooKeeper);
	    JSONArray jac = new JSONArray();
	    for (Job jobCompleted: jobs) {
   	        jobCompleted.load(zooKeeper);
   	        jac.put(jobCompleted.data());
	    }
	    System.out.println("JOBS COMPLETED -----------------------");
	    JSONObject jcomplete = new JSONObject();
	    jcomplete.put("completedJobs", jac);
	    System.out.println(jcomplete);

	    // Failed Jobs
	    jobs = batch.getFailedJobs(zooKeeper);
	    JSONArray jaf = new JSONArray();
   	    JSONObject jftemp = new JSONObject();
	    for (Job jobFailed: jobs) {
   	        jobFailed.load(zooKeeper);
   	        jftemp = jobFailed.data();
 		jftemp.put("Status", jobFailed.jsonProperty(zooKeeper, ZKKey.STATUS).getString(MerrittJsonKey.Status.key()));
 		jftemp.put("Message", jobFailed.jsonProperty(zooKeeper, ZKKey.STATUS).getString(MerrittJsonKey.Message.key()));
   	        jaf.put(jftemp);
	    }
	    System.out.println("JOBS FAILED -----------------------");
	    JSONObject jfail = new JSONObject();
	    jfail.put("failedJobs", jaf);
	    System.out.println(jfail);

	    batch.loadProperties(zooKeeper);
  	    email.setHostName(ingestRequest.getServiceState().getMailHost());	// production machines are SMTP enabled
	    if (jobState.grabAltNotification() == null) {
	        for (Notification recipient : profileState.getContactsEmail()) {
		    try {
  	    	        email.addTo(recipient.getContactEmail());
		    } catch (Exception e) { }
	        }
	    } else {
		// use alternate notification, not profile settings
	        try {
  	    	    email.addTo(jobState.grabAltNotification());
		} catch (Exception e) { }
	    }

	    if (profileState.getAdmin() != null) {
		for (Iterator<String> admin = profileState.getAdmin().iterator(); admin.hasNext(); ) {
		    // admin will receive all completion notifications
		    String recipient = admin.next();
		    if (StringUtil.isNotEmpty(recipient)) email.addBcc(recipient);
		}
	    }
	
            // email contact
	    String contact = profileState.getEmailContact();
  	    email.setFrom(contact, "UC3 Merritt Support");

            // email reply to
	    String replyTo = profileState.getEmailReplyTo();
	    if ( replyTo != null ) {
               ArrayList emailReply = new ArrayList();
               emailReply.add(new InternetAddress(replyTo));
               email.setReplyTo(emailReply);
	    } else {
               if (DEBUG) System.err.println("[warning] " + MESSAGE + "Email replyTo not found.");
	    }


	    String status = "OK";

	    // instance?
	    String server = null;
	    try {
		String ingestServiceName = ingestRequest.getServiceState().getServiceName();
		if (StringUtil.isNotEmpty(ingestServiceName))
		    if (ingestServiceName.contains("Development")) server = "dev";
		    else if (ingestServiceName.contains("Stage")) server = "stg";
	    } catch (NullPointerException npe) {}


	    if (batchComplete) {
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


System.out.println("BATCH STATUS ----------> " + batchState.getBatchStatus());

		// subject
		if (! verbose) 
  	            email.setSubject(FormatterUtil.getSubject(SERVICE, server, status, "Submission Processed", aggregate + batchState.getBatchID().getValue()));
		else {
		    String statusDisplay = "";
		    if (server != null) statusDisplay = " [Merritt " + server + "]";
		    try {
  	                if (status.equals("OK")) email.setSubject("Submission Processed" + statusDisplay + ": " + jobState.getLocalID().getValue());
  	                else email.setSubject("Submission Failed" + statusDisplay + ": " + jobState.getLocalID().getValue());
		    } catch (Exception e) {
  	                if (status.equals("OK")) email.setSubject("Submission Processed" + statusDisplay + ": " + jobState.getPrimaryID().getValue());
  	                else email.setSubject("Submission Failed" + statusDisplay + ": " + jobState.getPrimaryID().getValue());
		    }
		}

		if ( ! jaf.isEmpty()) 
		    email.attach(new ByteArrayDataSource(jfail.toString(), "application/json; header=present"), "failed.json", "Error report for " + batchID, EmailAttachment.ATTACHMENT);
		if ( ! jac.isEmpty()) 
		    email.attach(new ByteArrayDataSource(jcomplete.toString(), "application/json; header=present"), "completed.json", "Completion report for " + batchID, EmailAttachment.ATTACHMENT);

/*
		if (csv) {
		    // Comma delimited
		    email.attach(new ByteArrayDataSource(batchState.dump("", false), "text/csv; header=present"),
			 batchID + ".csv", "Comma delimited Job Report for " +  batchID, EmailAttachment.ATTACHMENT);
		}

		if (! status.equals("OK")) {
		    // Create CSV error attachment
		    email.attach(new ByteArrayDataSource(batchState.dump("", false, true), "text/csv; header=present"),
			 "error-" + batchID + ".csv", "Comma delimited Job Error Report for " +  batchID, EmailAttachment.ATTACHMENT);
		}
*/

                // attachment: batch state with user defined formatting
                if (ingestRequest.getNotificationFormat() != null) formatType = ingestRequest.getNotificationFormat();
                else if (profileState.getNotificationFormat() != null) formatType = profileState.getNotificationFormat();     // POST parm overrides profile parm

/*
		try {
		    email.attach(new ByteArrayDataSource(formatterUtil.doStateFormatting(batchState, formatType), formatType.getMimeType()),
			batchID + "." + formatType.getExtension(), "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
		} catch (Exception e) {
	            if (DEBUG) System.out.println("[warn] " + MESSAGE + "Could not determine format type.  Setting to default.");
		    // human readable
		    email.attach(new ByteArrayDataSource("Completion of Ingest - " + batchState.dump("Notification Report"), "text/plain"),
			 batchID + ".txt", "Full report for " +  batchID, EmailAttachment.ATTACHMENT);
		}
*/

		if (! verbose) {
		    email.setMsg(batchDump(batch, ""));
System.out.println(batchDump(batch, ""));
		} else {
  	            email.setMsg(getVerboseMsg(jobState));
		}

		try {
  	            email.send();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    } else {
	        if (DEBUG) System.out.println("[info] " + MESSAGE + "batch is not complete.  No notification necessary");
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

    private String getVerboseMsg(JobState jobState) {

	String id = null;
	String completionDate = "null";
        try {
	    id = jobState.getLocalID().getValue();
	} catch (Exception e) {
	    id = jobState.getPrimaryID().getValue();
	}
        try {
	    completionDate = jobState.getCompletionDate().toString();
	} catch (Exception e) {
	    completionDate = "unknown";
	}

	String msg = 
	    "The dataset \"" + jobState.getObjectTitle() + "\" was successfully uploaded at " + completionDate + ".\n"
	  + "The identifier associated with this dataset is " + id + ". Please retain\n"
	  + "this email for your records.\n\n"
	  + "To access this dataset and associated metadata, use the following URL: \n"
	  + jobState.getPersistentURL() + "\n";
	return msg;
    }

    public String getName() {
	return NAME;
    }

   public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
   }


    // was in BatchState
    public String batchDump(Batch batch, String header)
    {

	JSONObject b = null;
        JSONObject batchJSON = batch.data();
        String batchIDS = (batch != null) ? batch.batchUuid() : "";
        String submissionDateS = (batchJSON != null) ? batchJSON.getString("submissionDate") : "";
        String completionDateS = DateUtil.getCurrentDate().toString();
        String userAgentS = (batchJSON != null) ? batchJSON.getString("submitter") : "";
        String batchStatusS = "COMPLETED";
        String batchStatusMessageS = "Success";
	if (batch.hasFailure()) {
	   // Grab error message from first failure
	   try {
	      Job jf = batch.getFailedJobs(zooKeeper).get(0);
	      batchStatusS = jf.jsonProperty(zooKeeper, ZKKey.STATUS).getString(MerrittJsonKey.Status.key());
	      batchStatusMessageS = jf.jsonProperty(zooKeeper, ZKKey.STATUS).getString(MerrittJsonKey.Message.key());
	   } catch (Exception eee) {}
	}


        String queuePriorityS = "00";	// default

	int completed = 0;
	int failed = 0;
	int pending = 0;

	try {
	   completed = batch.getCompletedJobs(zooKeeper).size();
	   failed = batch.getFailedJobs(zooKeeper).size();
	   pending = batch.getProcessingJobs(zooKeeper).size();
	} catch (Exception zke) {
	   System.err.println("[ERROR] Could not determine status for batch: " + batch.id());
	}

	// gather job status
	String jobStateS = "\n\n\n";
	jobStateS = jobStateS + "\t:Number of pending job(s): " + pending + "\n";
	jobStateS = jobStateS + "\t:Number of completed job(s): " + completed + "\n";
	jobStateS = jobStateS + "\t:Number of failed job(s): " + failed + "\n";
	jobStateS = jobStateS + "\n";
	jobStateS = jobStateS.substring(1, jobStateS.length() - 1 );

        if (StringUtil.isNotEmpty(batchIDS)) header += "\n" + "Submission ID: " + batchIDS + "\n";
        if (StringUtil.isNotEmpty(jobStateS)) header += "Job(s): " + jobStateS + "\n";
        if (StringUtil.isNotEmpty(userAgentS)) header += "User agent: " + userAgentS + "\n";
        if (StringUtil.isNotEmpty(queuePriorityS)) header += "Queue Priority: " + queuePriorityS + "\n";
        if (StringUtil.isNotEmpty(submissionDateS)) header += "Submission date: " + submissionDateS + "\n";
        if (StringUtil.isNotEmpty(completionDateS)) header += "Completion date: " + completionDateS + "\n";
        if (StringUtil.isNotEmpty(batchStatusS)) header += "Status: " + batchStatusS + "\n";
        if (StringUtil.isNotEmpty(batchStatusMessageS)) header += "Status Message: " + batchStatusMessageS + "\n";

        return header; 

    }



}
