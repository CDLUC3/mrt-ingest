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

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;

import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;

/**
 * Jobs State information
 * @author mreyes
 */
public class JobsState
        implements JobsStateInf, StateInf, Serializable
{

    protected String ingestServer = null;
    protected ArrayList<JobState> jobsState = new ArrayList(5); 

    /**
     * Set server
     * @param batchState batch
     */
    public void setIngestServer(String ingestServer) {
        this.ingestServer = ingestServer;
    }

    /**
     * Get server
     * @return ingest server
     */
    public String getIngestServer() {
        return ingestServer;
    }

    /**
     * Add job
     * @param batchState batch
     */
    public void addJob(JobState jobState) {
        jobsState.add(jobState);
    }

    /**
     * Remove job
     * @param JobState job
     */
    public void removeJob(int index) {
        jobsState.remove(index);
    }

    /**
     * Get all jobs
     * @return  submitting user agent
     */
    public ArrayList<JobState> getJobsState() {
        return jobsState;
    }

    public void clear() {
        jobsState = null;
    }

    public String dump(String header)
    {
/*
        String batchIDS = (batchID != null) ? batchID.toString() : "";
        String batchLabelS = (batchLabel != null) ? batchLabel : "";
        String submissionDateS = (submissionDate != null) ? submissionDate.toString() : "";
        String completionDateS = (completionDate != null) ? completionDate.toString() : "";
        String userAgentS = (userAgent != null) ? userAgent : "";
        String batchStatusS = (batchStatus != null) ? batchStatus.toString() : "";

	// gather job status
	String jobStateS = "\n\n";
        Iterator iterator = getJobStates().iterator();
        while(iterator.hasNext()) {
            JobState jobState = (JobState) iterator.next();
	    jobStateS = jobStateS + jobState.dump("", "\t", "\n") + "\n";
	}
	jobStateS = jobStateS.substring(1, jobStateS.length() - 1 );

        if (StringUtil.isNotEmpty(batchIDS)) header += "\n\n" + " - Batch ID: " + batchIDS + "\n";
        if (StringUtil.isNotEmpty(batchLabelS)) header += " - Batch label: " + batchLabelS + "\n";
        if (StringUtil.isNotEmpty(jobStateS)) header += " - Job(s): " + jobStateS + "\n";
        if (StringUtil.isNotEmpty(userAgentS)) header += " - User agent: " + userAgentS + "\n";
        if (StringUtil.isNotEmpty(submissionDateS)) header += " - Submission date: " + submissionDateS + "\n";
        if (StringUtil.isNotEmpty(completionDateS)) header += " - Completion date: " + completionDateS + "\n";
        if (StringUtil.isNotEmpty(batchStatusS)) header += " - Status message: " + batchStatusS + "\n";

        return header; 
*/
	return null;

    }
}
