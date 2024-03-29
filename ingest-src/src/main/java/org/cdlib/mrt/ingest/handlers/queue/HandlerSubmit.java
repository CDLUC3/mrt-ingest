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

import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

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

	boolean isHighPriority = false;
	String priorityBoolean = "0";
	File file = null;
        FormatType formatType = null;
	String status = null;
        Properties properties = new Properties();
        JSONObject jproperties = new JSONObject();
        ZooKeeper zooKeeper = null;

	try {
	    BatchState.putBatchReadiness(batchState.getBatchID().getValue(), 0);

            // open a single connection to zookeeper for all queue posting
            // todo: create an interface
            zooKeeper = new ZooKeeper(batchState.grabTargetQueue(), DistributedQueue.sessionTimeout, new Ignorer());
	    String priority = calculatePriority(batchState.getJobStates().size());		// 00-99 (0=highest)
	    if (profileState.getPriority() != null) {
		priority = profileState.getPriority();
	    	System.out.println("[info] Overwriting calculated queue priority: " + priority);
	    }
	    System.out.println("[info] queue priority: " + priority);
	    isHighPriority = (Integer.parseInt(priority) <= Integer.parseInt(profileState.grabPriorityThreshold()));
	    if (isHighPriority) priorityBoolean = "1";
	    System.out.println("[info] Priority Job status: " + isHighPriority);
            DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, batchState.grabTargetQueueNode(), priority + priorityBoolean + getWorkerID(), null);	// default priority

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
	    if (ingestRequest.getJob().getPrimaryID() != null)
	        jproperties.put("objectID", ingestRequest.getJob().getPrimaryID().getValue());
	    if (ingestRequest.getJob().getLocalID() != null)
	        jproperties.put("localID", ingestRequest.getJob().getLocalID().getValue());
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

		System.out.println("[info] queue submission: " + jproperties.toString());
		int retryCount = 0;
		while (true) {
		    try {
                        distributedQueue.submit(jproperties.toString().getBytes());
			break;
		    } catch (ConnectionLossException cle) {
			if (retryCount >= 3) throw cle;
	    	        System.err.println("[error] " + MESSAGE + "Lost queue connection, requeuing: " + cle.getMessage());
                	retryCount++;
		    }
		}

		jobState.setJobStatus(JobStatusEnum.PENDING);
	    }

	    // global
	    System.out.println("[info] QueueHandlerSubmit: updating batch state.");
	    BatchState.putBatchState(batchState.getBatchID().getValue(), batchState);
	    System.out.println("[info] QueueHandlerSubmit: Ready to process requests.");
	    BatchState.putBatchCompletion(batchState.getBatchID().getValue(), 0); 	//initialize
	    BatchState.putBatchReadiness(batchState.getBatchID().getValue(), 1);
	    // serialize object to disk
	    //ProfileUtil.writeTo(batchState, ingestRequest.getQueuePath());

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
