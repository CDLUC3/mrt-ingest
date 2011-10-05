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

import java.io.File;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.utility.DigestEnum;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.ingest.utility.ResponseFormEnum;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author mreyes
 */
public class IngestRequest
{

    protected static final String NAME = "IngestRequest";
    protected static final String MESSAGE = NAME + ": ";

    protected Identifier profile = null;
    protected int packageSize;
    protected String note = null;
    protected String link = null;	// used to expose data to storage service via manifest

    protected File queueDir;

    protected JobState jobState;
    protected ResponseFormEnum responseForm;
    protected PackageTypeEnum packageType;
    protected IngestServiceState serviceState;
    protected boolean updateFlag;
    protected boolean synchronousMode;

    // constructors
    public IngestRequest(){ jobState = new JobState(); }
    public IngestRequest(String user, String profile, String packageName, String packageType,
		String packageSize, String algorithm, String value, String primaryID, String objectCreator,
		String objectTitle, String objectDate, String responseForm, String note) {
	try {
	    jobState = new JobState(user, packageName, algorithm, value, primaryID, objectCreator, objectTitle, objectDate, note);

	    this.profile = new Identifier(profile);
	    this.setPackageType(packageType);
	    this.setUpdateFlag(false);
	    this.setSynchronousMode(false);
	    if (packageSize != null) this.packageSize = new Integer(packageSize).intValue();
	    ResponseFormEnum.setResponseForm(responseForm);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    // Get job
    public JobState getJob() { 
	return jobState;
     }

    // Set job
    public void setJob(JobState jobState) { 
	this.jobState = jobState;
    }

    /**
     * Get profile
     * @return Submission package profile identifier
     */
    public Identifier getProfile() {
        return profile;
    }

    /**
     * Set profile
     * @param Identifier Submission package profile identifier
     */
    public void setProfile(String  profile) {
	try {
	   this.profile = new Identifier(profile);
	} catch (Exception e) {
	}
    }
    public void setProfile(Identifier profile) {
        this.profile = profile;
    }

    /**
     * Get package type
     * @return Submission package type
     */
    public PackageTypeEnum getPackageType() {
        return packageType;
    }

    /**
     * Set package type
     * @param String Submission package type
     */
    public void setPackageType(String packageType) {
        this.packageType = PackageTypeEnum.setPackageType(packageType);
    }


    /**
     * Get package size
     * @return Submission package size (in octets)
     */
    public int getPackageSize() {
        return packageSize;
    }

    /**
     * Set package size
     * @param String Submission package size (in octets)
     */
    public void setPackageSize(String size) {
        this.packageSize = new Integer(size).intValue();
    }

    /**
     * Get note
     * @return comments
     */
    public String getNote() {
        return note;
    }

    /**
     * Set note
     * @param String note
     */
    public void setNote(String note) {
        this.note = note;
    }

    /**
     * Get response form
     * @return response form
     */
    public String getResponseForm() {
        return responseForm.getValue();
    }

    /**
     * Set response form
     * @param String response form
     */
    public void setResponseForm(String responseForm) {
        this.responseForm = ResponseFormEnum.setResponseForm(responseForm);
    }

    /**
     * Get link
     * @return link ingest service 
     */
    public String getLink() {
        return link;
    }

    /**
     * Set link
     * @param link ingest service 
     */
    public void setLink(String link) {
        this.link = link;
    }

    /**
     * Get queue path
     * @return submission queuing directory
     */
    public File getQueuePath() {
        return queueDir;
    }

    /**
     * Set queue path
     * @param File submission queuing directory
     */
    public void setQueuePath(File queueDir) {
        this.queueDir = queueDir;
    }

    /**
     * Get service state
     * @return service state
     */
    public IngestServiceState getServiceState() {
        return serviceState;
    }

    /**
     * Set service state
     * @param IngestServiceState service state
     */
    public void setServiceState(IngestServiceState serviceState) {
        this.serviceState = serviceState;
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
    public boolean getUpdateFlag() {
        return updateFlag;
    }

    /**
     * Set synchronous boolean
     * @param boolean process queueing in synchronously 
     */
    public void setSynchronousMode(boolean synchronousMode) {
        this.synchronousMode = synchronousMode;
    }

    /**
     * Get synchronous boolean
     * @return queueing done synchronously 
     */
    public boolean getSynchronousMode() {
        return synchronousMode;
    }

    public String dump(String header)
    {
	String object = null;
        if (jobState.getPrimaryID() == null) {
	    object = "";
	} else {
	    object = jobState.getPrimaryID().toString();
	}
        return header + "INGEST REQUEST:"
                    + " - user=" + jobState.getUserAgent()
                    + " - package=" + jobState.getPackageName()
                    + " - object=" + object
                    ;
    }

}

