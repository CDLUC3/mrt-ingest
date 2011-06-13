/*
Copyright (c) 2005-2010, Regents of the University of California
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

import au.com.bytecode.opencsv.CSVWriter;

import java.util.Date;
import java.io.Serializable;
import java.io.StringWriter;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.DigestEnum;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.StateInf;

/**
 * Job State information
 * @author mreyes
 */
public class JobState
        implements JobStateInf, StateInf, Serializable
{

    protected Identifier jobID = null;
    protected Identifier batchID = null;
    protected Identifier primaryID = null;
    protected Identifier localID = null;
    protected String packageName = null;
    protected String objectCreator = null;
    protected String objectTitle = null;
    protected String objectDate = null;
    protected String hashValue = null;
    protected DigestEnum hashAlgorithm;

    protected String objectState = null;
    protected String objectNote = null;
    protected Integer versionID = null;
    protected String jobLabel = null;
    protected StoreNode storeNode;
    protected ProfileState objectProfile = null;
    protected String userAgent = null;
    protected DateState submissionDate = null;
    protected DateState consumptionDate = null;
    protected DateState completionDate = null;
    protected JobStatusEnum jobStatus = null;
    protected String jobStatusMessage = null;
    protected String objectType;
    protected String misc;
    protected String queuePriority;
    protected boolean shadowARK;
    protected boolean updateFlag;


    // constructors
   public JobState(){}
   public JobState(String user, String packageName, String algorithm, String value, String primaryID,
		String objectCreator, String objectTitle, String objectDate) {
       this.packageName = packageName;
       this.objectCreator = objectCreator;
       this.objectTitle = objectTitle;
       this.objectDate = objectDate;
       this.hashValue = value;
       this.userAgent = user;
       this.setPrimaryID(primaryID);
       this.setHashAlgorithm(algorithm);
       this.setShadowARK(false);
   }

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

    /**
     * Get package name
     * @return Submission package
     */
    public String getPackageName() {
        return packageName;
    }

    /**
     * Set package name
     * @param String Submission package
     */
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }



    @Override
    public StoreNode getTargetStorage()
    {
        return storeNode;
    }

    /**
     * Set storage service state
     * @param StoreNode target storage service and node
     */
    public void setTargetStorage(StoreNode storeNode) {
        this.storeNode = storeNode;
    }

    public ProfileState getObjectProfile()
    {
        return objectProfile;
    }

    /**
     * Set object profile
     * @param ProfileState profile declares the type of digital object
     */
    public void setObjectProfile(ProfileState objectProfile) {
        this.objectProfile = objectProfile;
    }

    public String getUserAgent()
    {
        return userAgent;
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
    public DateState getConsumptionDate() {
        return consumptionDate;
    }

    /**
     * Set consumption date-time
     * @param DateState consumption date-time
     */
    public void setConsumptionDate(DateState consumptionDate) {
        this.consumptionDate = consumptionDate;
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
     * get status
     * @return jobStatus
     */
    public JobStatusEnum getJobStatus()
    {
        return jobStatus;
    }

    /**
     * Set job status
     * @param JobStatus currrent job status
     */
    public void setJobStatus(JobStatusEnum jobStatus) {
        this.jobStatus = jobStatus;
    }

    /**
     * get status message
     * @return jobStatus
     */
    public String getJobStatusMessage()
    {
        return jobStatusMessage;
    }

    /**
     * Set status message
     * @param String job status message
     */
    public void setJobStatusMessage(String jobStatusMessage) {
        this.jobStatusMessage = jobStatusMessage;
    }

    /**
     * Get primary identifier
     * @return Identifier primary identifier
     */
    public Identifier getPrimaryID() {
        return primaryID;
    }

    /**
     * Set primary identifier
     * @param String primary identifier
     */
    public void setPrimaryID(String primaryID) {
        try {
           this.primaryID = new Identifier(primaryID);        // default ARK namespace
        } catch (Exception e) {
	   this.primaryID = null;
        }
    }

    /**
     * Get local identifier
     * @return Identifier local identifier
     */
    public Identifier getLocalID() {
        return localID;
    }

    /**
     * Set local identifier
     * @param String local identifier
     */
    public void setLocalID(String localID) {
        try {
           this.localID = new Identifier(localID, Identifier.Namespace.Local);  // default Local namespace
        } catch (Exception e) {
        }
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

    /**
     * Get version ID
     * @return Integer version identifier
     */
    public Integer getVersionID() {
        return versionID;
    }

    /**
     * Set version ID
     * @param Integer version identifier
     */
    public void setVersionID(Integer versionID) {
        this.versionID = versionID;
    }

    /**
     * Get hash algorithm
     * @return Package message digest algorithm
     */
    public String getHashAlgorithm() {
        return hashAlgorithm.getValue();
    }

    /**
     * Set hash algorithm
     * @param String Package message digest algorithm
     */
    public void setHashAlgorithm(String hashAlgorithm) {
        try {
            this.hashAlgorithm = DigestEnum.setDigest(hashAlgorithm.toUpperCase());
        } catch (Exception e) {
            this.hashAlgorithm = null;      // reported downstream
        }
    }

    /**
     * Get hash value
     * @return Package hexadecimal message digest value
     */
    public String getHashValue() {
        return hashValue;
    }

    /**
     * Set hash value
     * @param String Package hexadecimal message digest value
     */
    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    /**
     * Get content creator
     * @return Object Dublin Kernel "who" element
     */
    public String getObjectCreator() {
        return objectCreator;
    }

   /**
     * Set content creator
     * @param String Object Dublin Kernel "who" element
     */
    public void setObjectCreator(String objectCreator) {
        this.objectCreator = objectCreator;
    }

    /**
     * Get content title
     * @return Object Dublin Kernel "what" element
     */
    public String getObjectTitle() {
        return objectTitle;
    }

    /**
     * Set content title
     * @param String Object Dublin Kernel "what" element
     */
    public void setObjectTitle(String objectTitle) {
        this.objectTitle = objectTitle;
    }

    /**
     * Get content date
     * @return Object Dublin Kernel "when" element
     */
    public String getObjectDate() {
        return objectDate;
    }

    /**
     * Set content date
     * @param String Object Dublin Kernel "when" element
     */
    public void setObjectDate(String objectDate) {
        this.objectDate = objectDate;
    }

    /**
     * Get object expository note
     * @return String note
     */
    public String getObjectNote() {
        return objectNote;
    }

    /**
     * Set object expository note
     * @param String note
     */
    public void setObjectNote(String objectNote) {
        this.objectNote = objectNote;
    }

    /**
     * Set object type
     * @param String type
     */
    public void setObjectType(String type) {
        this.objectType = type;
    }

    /**
     * Get object type
     * @return String type
     */
    public String getObjectType() {
        return objectType;
    }

    /**
     * Set misc data
     * @param String misc
     */
    public void setMisc(String misc) {
        this.misc = misc;
    }

    /**
     * Get misc data
     * @return String job queue priority
     */
    public String getMisc() {
        return misc;
    }

    /**
     * Set job queue priority
     * @param String queue priority
     */
    public void setQueuePriority(String priority) {
        this.queuePriority = priority;
    }

    /**
     * Get job queue priority
     * @return String job queue priority
     */
    public String getQueuePriority() {
        return queuePriority;
    }

    /**
     * Set shadow ark boolean
     * @param boolean set shadow ark
     */
    public void setShadowARK(boolean shadowARK) {
        this.shadowARK = shadowARK;
    }

    /**
     * Get shadow ark boolean
     * @return boolean shadow ark
     */
    public boolean getShadowARK() {
        return shadowARK;
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

    public void clear() {
        jobID = null;
        jobLabel = null;
        storeNode = null;
        objectProfile = null;
        userAgent = null;
        submissionDate = null;
        completionDate = null;
        jobStatus = null;
    }

    public String dump(String header)
    {
        return this.dump(header, "", "", "");
    }

    public String dump(String header, String format)
    {
        return this.dump(header, "", "", format);
    }

    public String dump(String header, String indent, String delimiter, String format)
    {
	// populate entries
        String jobIDS = (jobID != null) ? jobID.toString() : "";
        String jobLabelS = (jobLabel != null) ? jobLabel : "";
        String submissionDateS = (submissionDate != null) ? submissionDate.toString() : "";
        String completionDateS = (completionDate != null) ? completionDate.toString() : "";
        String userAgentS = (userAgent != null) ? userAgent : "";
        String primaryIDS = (primaryID != null) ? primaryID.toString() : "";
        String localIDS = (localID != null) ? localID.toString() : "";
        String objectProfileS = (objectProfile != null) ? objectProfile.dump(jobIDS) : "";
        String versionIDS = (versionID != null) ? versionID.toString() : "";
        String objectStateS = (objectState != null) ? objectState : "";
        String jobStatusS = (jobStatus != null) ? jobStatus.toString() : "";
        String jobStatusMessageS = (jobStatusMessage != null) ? jobStatusMessage.toString() : "";
        String objectTitleS = (objectTitle != null) ? objectTitle : "";
        String objectCreatorS = (objectCreator != null) ? objectCreator : "";
        String objectDateS = (objectDate != null) ? objectDate : "";
        String objectAggregateS = "";
	try {
            objectAggregateS = objectProfile.getAggregateType();
	} catch (NullPointerException npe) {}

	// format (TODO rework)
	if (StringUtil.isEmpty(format)) {
	    header += "\n";
            if (StringUtil.isNotEmpty(jobIDS)) header += indent + " - Job ID: " + jobIDS + delimiter;
            if (StringUtil.isNotEmpty(jobLabelS)) header += indent + " - Job Label: " + jobLabelS + delimiter;
            if (StringUtil.isNotEmpty(primaryIDS)) header += indent + " - Primary ID: " + primaryIDS + delimiter;
            if (StringUtil.isNotEmpty(localIDS)) header += indent + " - Local ID: " + localIDS + delimiter;
            if (StringUtil.isNotEmpty(versionIDS)) header += indent + " - Version: " + versionIDS + delimiter;
            if (StringUtil.isNotEmpty(packageName)) header += indent + " - Filename: " + packageName + delimiter;
            if (StringUtil.isNotEmpty(objectTitleS)) header += indent + " - Object title: " + objectTitleS + delimiter;
            if (StringUtil.isNotEmpty(objectCreatorS)) header += indent + " - Object creator: " + objectCreatorS + delimiter;
            if (StringUtil.isNotEmpty(objectDateS)) header += indent + " - Object date: " + objectDateS + delimiter;
            //if (StringUtil.isNotEmpty(objectProfileS)) header += indent + " - object profile: " + objectProfileS + delimiter;
	    // show URL if a) demo mode b) system object (for debugging)
            if ((StringUtil.isNotEmpty(objectStateS) && ProfileUtil.isDemoMode(objectProfile)) || 
		    (StringUtil.isNotEmpty(objectStateS) && ! ProfileUtil.isDemoMode(objectProfile) && StringUtil.isNotEmpty(objectAggregateS)))
		    header += indent + " - Object state: " + objectStateS + "?t=xhtml" + delimiter;
            if (StringUtil.isNotEmpty(submissionDateS)) header += indent + " - Submission date: " + submissionDateS + delimiter;
            if (StringUtil.isNotEmpty(completionDateS)) header += indent + " - Completion date: " + completionDateS + delimiter;
            if (StringUtil.isNotEmpty(jobStatusS)) header += indent + " - Status: " + jobStatusS + delimiter;
            if (StringUtil.isNotEmpty(jobStatusMessageS)) header += indent + " - Status message: " + jobStatusMessageS + delimiter;
	} else if (format.equalsIgnoreCase("CSV")) {
	    String[] fields = new String[12];
            StringWriter stringWriter = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(stringWriter);

            if (StringUtil.isNotEmpty(jobIDS)) fields[0] = jobIDS;
            if (StringUtil.isNotEmpty(primaryIDS)) fields[1] = primaryIDS;
            if (StringUtil.isNotEmpty(localIDS)) fields[2] = localIDS;
            if (StringUtil.isNotEmpty(versionIDS)) fields[3] = versionIDS;
            if (StringUtil.isNotEmpty(packageName)) fields[4] = packageName;
            if (StringUtil.isNotEmpty(objectTitleS)) fields[5] = objectTitleS;
            if (StringUtil.isNotEmpty(objectCreatorS)) fields[6] = objectCreatorS;
            if (StringUtil.isNotEmpty(objectDateS)) fields[7] = objectDateS;
            if (StringUtil.isNotEmpty(submissionDateS)) fields[8] = submissionDateS;
            if (StringUtil.isNotEmpty(completionDateS)) fields[9] = completionDateS;
            if (StringUtil.isNotEmpty(jobStatusS)) fields[10] = jobStatusS;
            if (StringUtil.isNotEmpty(jobStatusMessageS)) fields[11] = jobStatusMessageS;
            csvWriter.writeNext(fields);
	    fields = null;
	    try {
	        csvWriter.close();
	    } catch (Exception e) {}

	    header = stringWriter.toString();
	} else {
	    // unsupported
	    header = null;
	}

	return header;
    }
}
