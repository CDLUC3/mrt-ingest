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
import java.lang.Boolean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.Job;
import org.json.JSONObject;


/**
 * Submit batch submission data
 * zookeeper is the defined queueing service
 * @author mreyes
 */
public class HandlerSubmit extends Handler<BatchState>
{

    protected static final String NAME = "HandlerSubmit";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;
    public static int sessionTimeout = 40000;

    /**
     * Submit batch manifest jobs to queing service
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, BatchState batchState) 
	throws TException 
    {

	//boolean isHighPriority = false;
	//String priorityBoolean = "0";
	String priority = null;
	File file = null;
        FormatType formatType = null;
	String status = null;
        Properties properties = new Properties();
        JSONObject jproperties = new JSONObject();
        JSONObject jidentifiers = new JSONObject();
	String primaryID = null;
	String localID = null;
        ZooKeeper zooKeeper = null;

	try {
            // open a single connection to zookeeper for all queue posting
            // todo: create an interface
            zooKeeper = new ZooKeeper(batchState.grabTargetQueue(), sessionTimeout, new Ignorer());
	    priority = calculatePriority(batchState.getJobStates().size());		// 00-99 (0=highest)
	    if (profileState.getPriority() != null) {
		priority = profileState.getPriority();
	    	System.out.println("[info] Overwriting calculated queue priority: " + priority);
	    }
	    System.out.println("[info] queue priority: " + priority);
	    // isHighPriority = (Integer.parseInt(priority) <= Integer.parseInt(profileState.grabPriorityThreshold()));
	    // if (isHighPriority) priorityBoolean = "1";
	    // System.out.println("[info] Priority Job status: " + isHighPriority);

	    // common across all jobs in batch
	    jproperties.put("batchID", batchState.getBatchID().getValue());
	    jproperties.put("profile", ingestRequest.getProfile().getValue());
	    jproperties.put("type", ingestRequest.getPackageType().getValue());
	    if (StringUtil.isNotEmpty(ingestRequest.getJob().grabUserAgent()))
	        jproperties.put("submitter", ingestRequest.getJob().grabUserAgent());
	    jproperties.put("queuePriority", priority);
	    // optional input parameters
	    if (StringUtil.isNotEmpty(ingestRequest.getResponseForm()))
	    	jproperties.put("responseForm", ingestRequest.getResponseForm());
	    if (StringUtil.isNotEmpty(ingestRequest.getJob().getObjectCreator()))
	    	jproperties.put("creator", ingestRequest.getJob().getObjectCreator());
	    if (StringUtil.isNotEmpty(ingestRequest.getJob().getObjectTitle()))
	    	jproperties.put("title", ingestRequest.getJob().getObjectTitle());
	    if (StringUtil.isNotEmpty(ingestRequest.getJob().getObjectDate()))
	    	jproperties.put("date", ingestRequest.getJob().getObjectDate());

	    if (ingestRequest.getJob().getPrimaryID() != null) {
	        // jproperties.put("objectID", ingestRequest.getJob().getPrimaryID().getValue());
	        primaryID =  ingestRequest.getJob().getPrimaryID().getValue();
	    }
	    if (ingestRequest.getJob().getLocalID() != null) {
	        // jproperties.put("localID", ingestRequest.getJob().getLocalID().getValue());
	        localID = ingestRequest.getJob().getLocalID().getValue();
	    }
	    jidentifiers = Job.createJobIdentifiers(primaryID, localID);

	    if (ingestRequest.getJob().grabAltNotification() != null)
	        jproperties.put("notification", ingestRequest.getJob().grabAltNotification());

            // attachment: batch state with user defined formatting
            if (ingestRequest.getNotificationFormat() != null) formatType = ingestRequest.getNotificationFormat();
            else if (profileState.getNotificationFormat() != null) formatType = profileState.getNotificationFormat();     // POST parm overrides profile parm
	    try {
	        jproperties.put("notificationFormat", formatType.toString());
	    } catch (Exception e) { }
	    if (ingestRequest.getDataCiteResourceType() != null)
	        jproperties.put("DataCiteResourceType", ingestRequest.getDataCiteResourceType());

	    // process Dublin Core (optional)
	    if (ingestRequest.getDCcontributor() != null)
	        jproperties.put("DCcontributor", ingestRequest.getDCcontributor());
	    if (ingestRequest.getDCcoverage() != null)
	        jproperties.put("DCcoverage", ingestRequest.getDCcoverage());
	    if (ingestRequest.getDCcreator() != null)
	        jproperties.put("DCcreator", ingestRequest.getDCcreator());
	    if (ingestRequest.getDCdate() != null)
	        jproperties.put("DCdate", ingestRequest.getDCdate());
	    if (ingestRequest.getDCdescription() != null)
	        jproperties.put("DCdescription", ingestRequest.getDCdescription());
	    if (ingestRequest.getDCformat() != null)
	        jproperties.put("DCformat", ingestRequest.getDCformat());
	    if (ingestRequest.getDCidentifier() != null)
	        jproperties.put("DCidentifier", ingestRequest.getDCidentifier());
	    if (ingestRequest.getDClanguage() != null)
	        jproperties.put("DClanguage", ingestRequest.getDClanguage());
	    if (ingestRequest.getDCpublisher() != null)
	        jproperties.put("DCpublisher", ingestRequest.getDCpublisher());
	    if (ingestRequest.getDCrelation() != null)
	        jproperties.put("DCrelation", ingestRequest.getDCrelation());
	    if (ingestRequest.getDCrights() != null)
	        jproperties.put("DCrights", ingestRequest.getDCrights());
	    if (ingestRequest.getDCsource() != null)
	        jproperties.put("DCsource", ingestRequest.getDCsource());
	    if (ingestRequest.getDCsubject() != null)
	        jproperties.put("DCsubject", ingestRequest.getDCsubject());
	    if (ingestRequest.getDCtitle() != null)
	        jproperties.put("DCtitle", ingestRequest.getDCtitle());
	    if (ingestRequest.getDCtype() != null)
	        jproperties.put("DCtype", ingestRequest.getDCtype());

	    // for all jobs in batch
	    Map<String, JobState> jobStates = (HashMap) batchState.getJobStates();
	    Iterator iterator = jobStates.keySet().iterator();
	    while(iterator.hasNext()) {
	        JobState jobState = (JobState) jobStates.get(iterator.next());

		jobState.setBatchID(batchState.getBatchID());
		jproperties.put("jobID", jobState.getJobID().getValue());	// overwrite if exists
		jproperties.put("filename", jobState.getPackageName());
		try {
		    jproperties.put("digestType", jobState.getHashAlgorithm());
		} catch (Exception e) { properties.remove("digestType"); }
		try {
		    jproperties.put("digestValue", jobState.getHashValue());
		} catch (Exception e) { properties.remove("digestValue"); }
	        try {
		    jproperties.put("objectID", jobState.getPrimaryID().getValue());
		} catch (Exception e) { 
		    if (ingestRequest.getJob().getPrimaryID() == null)  jproperties.remove("objectID"); 
		}
		try {
		    jproperties.put("localID", jobState.getLocalID().getValue());
		} catch (Exception e) { 
		    if (ingestRequest.getJob().getLocalID() == null) jproperties.remove("localID");
		}

		String pid = "";
		String lid = "";
		if (jobState.getPrimaryID() != null)
                    pid = jobState.getPrimaryID().getValue();
		if (jobState.getLocalID() != null)
                    lid = jobState.getLocalID().getValue();
                jidentifiers = Job.createJobIdentifiers(pid, lid);

		try {
		    jproperties.put("title", jobState.getObjectTitle());
		} catch (Exception e) { if (StringUtil.isEmpty(ingestRequest.getJob().getObjectTitle())) jproperties.remove("title"); }
		try {
		    jproperties.put("creator", jobState.getObjectCreator());
		} catch (Exception e) { if (StringUtil.isEmpty(ingestRequest.getJob().getObjectCreator())) jproperties.remove("creator"); }
		try {
		    jproperties.put("date", jobState.getObjectDate());
		} catch (Exception e) { if (StringUtil.isEmpty(ingestRequest.getJob().getObjectDate())) jproperties.remove("date"); }
		try {
		    jproperties.put("note", jobState.getNote());
		} catch (Exception e) { if (StringUtil.isEmpty(ingestRequest.getJob().getNote())) jproperties.remove("note"); }
		try {
		    if (jobState.getObjectType() != null) jproperties.put("type", jobState.getObjectType());
		} catch (Exception e) { }
		try {
		    if (ingestRequest.getRetainTargetURL()) {
			System.out.println("[info] " + MESSAGE + "Setting retainTargetURL to true");
			jproperties.put("retainTargetURL", "true");
		    }
		} catch (Exception e) { }
		try {
	    	    jproperties.put("update", new Boolean (jobState.grabUpdateFlag()));
		} catch (Exception e) {
		    // default
	    	    jproperties.put("update", new Boolean(false));
		}

		int retryCount = 0;
	        Job job = null;
		while (true) {
		    try {
			// Create Job 
			System.out.println("[info] queue submission: " + jproperties.toString() 
				+ "  --- Priority: " + priority 
				+ " --- Identifiers: " + jidentifiers.toString());
			job = Job.createJob(zooKeeper, ingestRequest.getBatch().id(), Integer.parseInt(priority), jproperties, jidentifiers);


			break;
		    } catch (Exception e) {
			e.printStackTrace();

			// Batch failure
			Batch batch = new Batch(job.bid());
            		batch.setStatus(zooKeeper, org.cdlib.mrt.zk.BatchState.Failed, "Failed");

			return new HandlerResult(false, "FAIL: " + NAME + " Submission failed: " + e.getMessage(), 0);
		    }
		}

		jobState.setJobStatus(JobStatusEnum.PENDING);
		job.unlock(zooKeeper);
	    }

	    // global
	    System.out.println("[info] QueueHandlerSubmit: Ready to process requests.");

	    return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	} catch (Exception e) {
	    e.printStackTrace();
            String msg = "[error] " + MESSAGE + "submitting batch: " + batchState.getBatchID().getValue() + " : " + e.getMessage();
	    System.err.println(msg);
            return new HandlerResult(false, msg, 10);
	} finally {
	    try {
		zooKeeper.close();
	    } catch (Exception e) {
	    }
	}
    }
   
    public String getName() {
	return NAME;
    }

    // very simple priority queue algorithm  (05 - 99)
    private String calculatePriority(int batchSize) {
	
	Double a = new Double((Math.log(new Integer(batchSize).doubleValue()) * 10.0d) + 5);
	if (a > 99.0d) a = 99.0d;
	if (a < 0.0d) a = 0.0d;
	String priority = String.format("%02d", a.intValue());
	System.out.println("[info] Calculated queue priority: " + priority);
	return priority;
    }

    private String getWorkerID() {

	String workerID = "0";
	
	try {
	    // Set in setenv.sh (e.g. ingest01-stg)
	    String workerEnv = System.getenv("WORKERNAME");
	    workerID = workerEnv.substring("ingest0".length(), "ingest0".length() + 1);
	    System.out.println("[info] Setting Ingest worker: " + workerID);

	} catch (Exception e ) {
	    System.out.println("[info] Can not calculate Ingest worker.  Setting to '0'.");
	}

	return workerID;
    }

   public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
   }


}
