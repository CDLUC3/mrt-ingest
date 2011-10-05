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

import java.net.URL;
import java.util.Date;
import java.util.Vector;
import java.io.Serializable;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.utility.StateInf;

/**
 * Notification State information
 * @author mreyes
 */
public class NotificationState
        implements NotificationStateInf, StateInf, Serializable
{

    protected Identifier jobID = null;
    protected String jobLabel = null;
    protected JobStatusEnum jobStatus = null;
    protected DateState jobCompletionDate = null;
    protected URL supportServiceURL = null;
    protected Identifier objectID = null;
    protected String objectState = null;
    protected Integer versionID = null;

    @Override
    public Identifier getJobID() {
        return jobID;
    }

    /**
     * Set job identifier
     * @param Identifier job identifier
     */
    public void setJobID(Identifier jobID) {
        this.jobID = jobID;
    }

    @Override
    public String getJobLabel() {
        return jobLabel;
    }

    /**
     * Set job label
     * @param String job label
     */
    public void setJobLabel(String jobLabel) {
        this.jobLabel = jobLabel;
    }

    @Override
    public JobStatusEnum getJobStatus() {
        return jobStatus;
    }

    /**
     * Set job status
     * @param JobStatus job status
     */
    public void setJobStatus(JobStatusEnum jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public DateState getJobCompletionDate() {
        return jobCompletionDate;
    }

    /**
     * Set job completion date
     * @param DateState job completion date
     */
    public void setJobCompletionDate(DateState jobCompletionDate) {
        this.jobCompletionDate = jobCompletionDate;
    }

    @Override
    public URL getSupportServiceURL() {
        return supportServiceURL;
    }

    /**
     * Set support service URL
     * @param URL support service URL
     */
    public void setSupportServiceURL(URL supportServiceURL) {
        this.supportServiceURL = supportServiceURL;
    }

    /**
     * Get object state
     * @return Object state
     */
    public String getObjectState() {
        return objectState;
    }

    /**
     * Set object state
     * @param String Object state
     */
    public void setObjectState(String objectState) {
        this.objectState = objectState;
    }

    @Override
    public Identifier getObjectID() {
        return objectID;
    }

    /**
     * Set object identifier
     * @param Identifier object identifier
     */
    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    @Override
    public Integer getVersionID() {
        return versionID;
    }

    /**
     * Set version identifier
     * @param Integer version identifier
     */
    public void setVersionID(Integer versionID) {
        this.versionID = versionID;
    }

    public String dump(String header)
    {
        String jobIDS = (jobID != null) ? jobID.toString() : "";
        String jobLabelS = (jobLabel != null) ? jobLabel : "";
        String jobStatusS = (jobStatus != null) ? jobStatus.toString() : "";
        String jobCompletionDateS = (jobCompletionDate != null) ? jobCompletionDate.toString() : "";
        String supportServiceURLS = (supportServiceURL != null) ? supportServiceURL.toString() : "";
        String objectIDS = (objectID != null) ? objectID.toString() : "";
        String versionIDS = (versionID != null) ? versionID.toString() : "";
        String objectStateS = (objectState != null) ? objectState : "";

        return header + "\n"
                + "   jobID: " + jobIDS + "\n"
                + "   jobLabel: " + jobLabelS + "\n"
                + "   jobStatus: " + jobStatusS + "\n"
                + "   jobCompletionDate: " + jobCompletionDateS + "\n"
                + "   supportServiceURL: " + supportServiceURLS + "\n"
                + "   objectID: " + objectIDS + "\n"
                + "   versionID: " + versionIDS + "\n"
                + "   objectState: " + objectStateS + "?response-form=xhtml";
    }
}
