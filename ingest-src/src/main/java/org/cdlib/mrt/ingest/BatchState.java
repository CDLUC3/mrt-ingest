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
package org.cdlib.mrt.ingest;

import java.util.Iterator;
import java.io.Serializable;
import java.lang.Cloneable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Batch State information
 * @author mreyes
 */
public class BatchState
        implements BatchStateInf, StateInf, Serializable, Cloneable
{

    private Identifier batchID = null;
    private String batchLabel = null;
    private ProfileState batchProfile = null;
    private String userAgent = null;
    private DateState submissionDate = null;
    private DateState completionDate = null;
    private BatchStatusEnum batchStatus = null;
    private String batchStatusMessage = null;
    private String queueConnectionString = null;
    private String queueNode = null;
    private Map<String, JobState> jobStates = new HashMap<String, JobState>();
    private static Map<String, BatchState> batchStates = new HashMap<String, BatchState>();
    private static Map<String, Integer> batchReadiness = new HashMap<String, Integer>();
    private static Map<String, Integer> batchCompletion = new HashMap<String, Integer>();
    private static Map<String, String> batchQueuePath = new HashMap<String, String>();
    private boolean completion = false;
    private boolean updateFlag = false;

    public BatchState clone() throws CloneNotSupportedException {  
        BatchState copy = (BatchState) super.clone();  
        return copy;  
    }  

    synchronized public static Map getBatchStates () {
      return batchStates;
    } 
    synchronized public static BatchState getBatchState (String id) {
      return batchStates.get(id);
    } 
    synchronized public static void putBatchState (String id, BatchState batchState) {
      batchStates.put(id, batchState);
    }
    synchronized public static void removeBatchState (String id) {
      batchStates.remove(id);
    }

    synchronized public static int getBatchReadiness (String id) {
      try {
          return batchReadiness.get(id);
      } catch (Exception e) {
	  return 0;
      }
    } 
    synchronized public static void putBatchReadiness (String id, int batchReady) {
      batchReadiness.put(id, batchReady);
    }
    synchronized public static void removeBatchReadiness (String id) {
      batchReadiness.remove(id);
    }

    synchronized public static int getBatchCompletion (String id) {
      try {
          return batchCompletion.get(id);
      } catch (Exception e) {
	  return 0;
      }
    } 
    synchronized public static void putBatchCompletion (String id, int batchComplete) {
      batchCompletion.put(id, batchComplete);
    }
    synchronized public static void removeBatchCompletion (String id) {
      batchCompletion.remove(id);
    }

    // use for shutdown
    synchronized public static void putQueuePath (String id, String queuePath) {
      batchQueuePath.put(id, queuePath);
    }
    synchronized public static String getQueuePath (String id) {
      return batchQueuePath.get(id);
    }
    synchronized public static void removeQueuePath (String id) {
      batchQueuePath.remove(id);
    }

    @Override
    public Identifier getBatchID() {
        return batchID;
    }

    /**
     * Set batch identifier
     * @param Identifier batch identifier
     */
    public void setBatchID(Identifier batchID) {
        this.batchID = batchID;
    }

    public String getBatchLabel() {
        return batchLabel;
    }

    /**
     * Set batch label
     * @param String batch label
     */
    public void setBatchLabel(String batchLabel) {
        this.batchLabel = batchLabel;
    }

    /**
     * Get user agent
     * @return String submitting user agent
     */
    public String getUserAgent() {
        return this.userAgent;
    }

    /**
     * Set user agent
     * @param String submitting user agent
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    @Override
    public DateState getSubmissionDate() {
        return submissionDate;
    }

    /**
     * Set submission date-time
     * @param DateState submission date-time
     */
    public void setSubmissionDate(DateState submissionDate) {
        this.submissionDate = submissionDate;
    }

    @Override
    public DateState getCompletionDate() {
        return completionDate;
    }

    /**
     * Set completion date-time
     * @param DateState completion date-time
     */
    public void setCompletionDate(DateState completionDate) {
        this.completionDate = completionDate;
    }

    /**
     * get batch status
     * @return batchStatus
     */
    public BatchStatusEnum getBatchStatus()
    {
        return batchStatus;
    }

    /**
     * Set batch status
     * @param BatchStatus currrent batch status
     */
    public void setBatchStatus(BatchStatusEnum batchStatus) {
        this.batchStatus = batchStatus;
    }

    /**
     * get status message
     * @return batchStatus
     */
    public String getBatchStatusMessage()
    {
        return batchStatusMessage;
    }

    /**
     * Set status message
     * @param String batch status message
     */
    public void setBatchStatusMessage(String batchStatusMessage) {
        this.batchStatusMessage = batchStatusMessage;
    }

    public String grabTargetQueue()
    {
        return this.queueConnectionString;
    }

    /**
     * Set queue target
     * @param String target queue service
     */
    public void setTargetQueue(String queueConnectionString) {
        this.queueConnectionString = queueConnectionString;
    }

    public String grabTargetQueueNode()
    {
        return this.queueNode;
    }

    /**
     * Set queue target node
     * @param String target queue node
     */
    public void setTargetQueueNode(String queueNode) {
        this.queueNode = queueNode;
    }

    public ProfileState grabBatchProfile()
    {
        return batchProfile;
    }

    /**
     * Set batch profile
     * @param ProfileState profile declares the type of digital object
     */
    public void setBatchProfile(ProfileState batchProfile) {
        this.batchProfile = batchProfile;
    }

    /**
     * Add job to batch
     * @param JobState job
     */
    public void addJob(String id, JobState jobState) {
        this.jobStates.put(id, jobState);
    }

    /**
     * Remove job from batch
     * @param JobState job
     */
    public void removeJob(String id) {
        this.jobStates.remove(id);
    }

    /**
     * Set all jobs
     * @param Job States
     */
    public void setJobStates(Map<String, JobState> jobStates) {
        this.jobStates = jobStates;
    }

    /**
     * Get all jobs
     * @return Job States
     */
    public Map<String, JobState> getJobStates() {
        return this.jobStates;
    }

    // Get single job
    public JobState getJobState(String id) {
        return this.jobStates.get(id);
    }

    /**
     * Set update boolean
     * @param boolean set update flag
     */
    public void setUpdateFlag(boolean updateFlag) {
        this.updateFlag = updateFlag;
    }

    /**
     * Get update boolean
     * @return boolean update flag
     */
    public boolean grabUpdateFlag() {
        return updateFlag;
    }

    /**
     * Set completion date-time
     * @param DateState completion date-time
     */
    public void clear() {
        batchID = null;
        batchLabel = null;
        userAgent = null;
        submissionDate = null;
        completionDate = null;
        batchStatus = null;
    }

    public String dump(String header) {
	return dump(header, true, false);
    }

    public String dump(String header, boolean full, boolean jobonly)
    {
        String batchIDS = (batchID != null) ? batchID.toString() : "";
        String batchLabelS = (batchLabel != null) ? batchLabel : "";
        String submissionDateS = (submissionDate != null) ? submissionDate.toString() : "";
        String completionDateS = (completionDate != null) ? completionDate.toString() : "";
        String userAgentS = (userAgent != null) ? userAgent : "";
        String batchStatusS = (batchStatus != null) ? batchStatus.toString() : "";
        String batchStatusMessageS = (batchStatusMessage != null && batchStatus == BatchStatusEnum.FAILED) ? batchStatusMessage.toString() : "";
        String queuePriorityS = "00";	// default

	int completed = 0;
	int failed = 0;
	int pending = 0;

	// gather job status
	String jobStateS = "\n\n";
	if (jobonly) {
	    // Fixed CSV format
	    header = "\"Job identifier\",";
	    header += "\"Primary identifier\",";
	    header += "\"Local identifier\",";
	    header += "\"Version\",";
	    header += "\"Filename\",";
	    header += "\"Object title\",";
	    header += "\"Object creator\",";
	    header += "\"Object date\",";
	    header += "\"Submission date\",";
	    header += "\"Completion date\",";
	    header += "\"Status\",";
	    header += "\"Status message\"\n";
	}
	Iterator iterator = getJobStates().keySet().iterator();
        while(iterator.hasNext()) {
             JobState jobState = (JobState) jobStates.get(iterator.next());
	    if (full) {
	        jobStateS = jobStateS + jobState.dump("", "\t", "\n", null) + "\n";
	    } else {
		if (jobonly) {
	            //header += jobState.dump("", "\t", "\n", "CSV");
	            header += jobState.dump("");
		} else {
		    if (jobState.getJobStatus() == JobStatusEnum.COMPLETED) completed++;
		    if (jobState.getJobStatus() == JobStatusEnum.FAILED) failed++;
		    if (jobState.getJobStatus() == JobStatusEnum.PENDING) pending++;
		}
	    }
	    queuePriorityS = jobState.grabQueuePriority();
	}
	if (! full)  {
	    jobStateS = jobStateS + "\n";
	    jobStateS = jobStateS + "\tNumber of pending job(s): " + pending + "\n";
	    jobStateS = jobStateS + "\tNumber of completed job(s): " + completed + "\n";
	    jobStateS = jobStateS + "\tNumber of failed job(s): " + failed + "\n";
	    jobStateS = jobStateS + "\n";
	}
	jobStateS = jobStateS.substring(1, jobStateS.length() - 1 );

	if (! jobonly) {
            if (StringUtil.isNotEmpty(batchIDS)) header += "\n\n" + "Submission ID: " + batchIDS + "\n";
            if (StringUtil.isNotEmpty(batchLabelS)) header += "Batch label: " + batchLabelS + "\n";
            if (StringUtil.isNotEmpty(jobStateS)) header += "Job(s): " + jobStateS + "\n";
            if (StringUtil.isNotEmpty(userAgentS)) header += "User agent: " + userAgentS + "\n";
            if (StringUtil.isNotEmpty(queuePriorityS)) header += "Queue Priority: " + queuePriorityS + "\n";
            if (StringUtil.isNotEmpty(submissionDateS)) header += "Submission date: " + submissionDateS + "\n";
            if (StringUtil.isNotEmpty(completionDateS)) header += "Completion date: " + completionDateS + "\n";
            if (StringUtil.isNotEmpty(batchStatusS)) header += "Status: " + batchStatusS + "\n";
            if (StringUtil.isNotEmpty(batchStatusMessageS)) header += "Status message: " + batchStatusMessageS + "\n";
	}

        return header; 

    }
}
