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
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.ingest.utility.ResponseFormEnum;
import org.cdlib.mrt.ingest.utility.ResourceTypeEnum;
import org.cdlib.mrt.zk.Batch;

/**
 *
 * @author mreyes
 */
public class IngestRequest
{

    private static final String NAME = "IngestRequest";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;

    private Identifier profile = null;
    private int packageSize;
    private int numDownloadThreads = 0;
    private String note = null;
    private String link = null;	// used to expose data to storage service via manifest

    private File queueDir;
    private String ingestQueuePath;	// Shareable e.g. EFS

    private JobState jobState;
    private Batch batch;
    private ResponseFormEnum responseForm;
    private ResourceTypeEnum resourceType;
    // http://dublincore.org/documents/dces/
    private String DCcontributor = null;
    private String DCcoverage = null;
    private String DCcreator = null;
    private String DCdate = null;
    private String DCdescription = null;
    private String DCformat = null;
    private String DCidentifier = null;
    private String DClanguage = null;
    private String DCpublisher = null;
    private String DCrelation = null;
    private String DCrights = null;
    private String DCsource = null;
    private String DCsubject = null;
    private String DCtitle = null;
    private String DCtype = null;

    private FormatType notificationFormat = null; 
    private PackageTypeEnum packageType;
    private IngestServiceState serviceState;
    private boolean updateFlag;
    private boolean synchronousMode;
    private boolean retainTargetURL;

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

    // Get zk batch
    public Batch getBatch() { 
	return batch;
     }

    // Set zk batch
    public void setBatch(Batch batch) { 
	this.batch = batch;
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
	try {
           return responseForm.getValue();
	} catch (Exception e) {
	   return null;
	}
    }

    /**
     * Set response form
     * @param String response form
     */
    public void setResponseForm(String responseForm) {
        this.responseForm = ResponseFormEnum.setResponseForm(responseForm);
    }

    /**
     * Get resource type (datacite requirement)
     * @return resource type
     */
    public String getDataCiteResourceType() {
	try {
            return resourceType.getValue();
	} catch (NullPointerException npe) { return null; }
    }

    /**
     * Get DC elements
     * @return DC element
     */
    public String getDCcontributor() { return DCcontributor; }
    public String getDCcoverage()    { return DCcoverage; }
    public String getDCcreator()     { return DCcreator; }
    public String getDCdate()        { return DCdate; }
    public String getDCdescription() { return DCdescription; }
    public String getDCformat()      { return DCformat; }
    public String getDCidentifier()  { return DCidentifier; }
    public String getDClanguage()    { return DClanguage; }
    public String getDCpublisher()   { return DCpublisher; }
    public String getDCrelation()    { return DCrelation; }
    public String getDCrights()      { return DCrights; }
    public String getDCsource()      { return DCsource; }
    public String getDCsubject()     { return DCsubject; }
    public String getDCtitle()       { return DCtitle; }
    public String getDCtype()        { return DCtype; }

    /**
     * Set DC elements
     * @param String DC element
     */
    public void setDCcontributor(String DCcontributor) { this.DCcontributor = DCcontributor; }
    public void setDCcoverage(String DCcoverage) { this.DCcoverage = DCcoverage; }
    public void setDCcreator(String DCcreator) { this.DCcreator = DCcreator; }
    public void setDCdate(String DCdate) { this.DCdate = DCdate; }
    public void setDCdescription(String DCdescription) { this.DCdescription = DCdescription; }
    public void setDCformat(String DCformat) { this.DCformat = DCformat; }
    public void setDCidentifier(String DCidentifier) { this.DCidentifier = DCidentifier; }
    public void setDClanguage(String DClanguage) { this.DClanguage = DClanguage; }
    public void setDCpublisher(String DCpublisher) { this.DCpublisher = DCpublisher; }
    public void setDCrelation(String DCrelation) { this.DCrelation = DCrelation; }
    public void setDCrights(String DCrights) { this.DCrights = DCrights; }
    public void setDCsource(String DCsource) { this.DCsource = DCsource; }
    public void setDCsubject(String DCsubject) { this.DCsubject = DCsubject; }
    public void setDCtitle(String DCtitle) { this.DCtitle = DCtitle; }
    public void setDCtype(String DCtype) { this.DCtype = DCtype; }

    /**
     * Set response form
     * @param String response form
     */
    public void setDataCiteResourceType(String resourceType) {
        this.resourceType = ResourceTypeEnum.setResourceType(resourceType);
    }

    /**
     * Get notification form
     * @return notification form
     */
    public FormatType getNotificationFormat() {
        return notificationFormat;
    }

    /**
     * Set notification form
     * @param String notification form
     */
    public void setNotificationFormat(String notificationFormat) {
        try {
            this.notificationFormat = FormatType.valueOf(notificationFormat);
        } catch (Exception e) {
            // default
            if (DEBUG) System.out.println("[warn] IngestRequest: Could not assign format type: " + notificationFormat);
            this.notificationFormat = null;
        }
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
     * Get ingest queue path
     * @return Shared queue pathname
     */
    public String getIngestQueuePath() {
        return ingestQueuePath;
    }

    /**
     * Set queue path
     * @param File submission queuing directory
     */
    public void setQueuePath(File queueDir) {
        this.queueDir = queueDir;
    }

    /**
     * Set ingest queue path
     * @param String Shared queue pathname
     */
    public void setIngestQueuePath(String ingestQueuePath) {
        this.ingestQueuePath = ingestQueuePath;
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
     * Set retain EZID target URL boolean
     * @param boolean EZID retain target URL
     */
    public void setRetainTargetURL(boolean retainTargetURL) {
        this.retainTargetURL = retainTargetURL;
    }

    /**
     * Get retain EZID target URL boolean
     * @return boolean EZID retain target URL
     */
    public boolean getRetainTargetURL() {
        return retainTargetURL;
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

    /**
     * Set download thread size
     * @param int Size of download thread pool
     */
    public void setNumDownloadThreads(int numDownloadThreads) {
        this.numDownloadThreads = numDownloadThreads;
    }

    /**
     * Get download thread size
     * @return int num threads
     */
    public int getNumDownloadThreads() {
        return numDownloadThreads;
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
                    + " - user=" + jobState.grabUserAgent()
                    + " - package=" + jobState.getPackageName()
                    + " - object=" + object
                    ;
    }

}

