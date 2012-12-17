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

import au.com.bytecode.opencsv.CSVWriter;

import java.util.Date;
import java.io.Serializable;
import java.io.StringWriter;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.DigestEnum;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.FormatterUtil;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.StateInf;

/**
 * Job State information
 * @author mreyes
 */
public class JobState
        implements JobStateInf, StateInf, Serializable
{

	private Identifier jobID = null;
	private Identifier batchID = null;
	private Identifier primaryID = null;
	private Identifier localID = null;
	private String packageName = null;
	private String objectCreator = null;
	private String objectTitle = null;
	private String objectDate = null;

	// Optional Dublin Core (http://dublincore.org/documents/dces)
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

	private String DataCiteMetadata = null;

	private String hashValue = null;
	private DigestEnum hashAlgorithm;
	private String objectState = null;
	private String objectNote = null;
	private Integer versionID = null;
	private String jobLabel = null;
	private StoreNode storeNode;
	private ProfileState objectProfile = null;
	private String userAgent = null;
	private DateState submissionDate = null;
	private DateState consumptionDate = null;
	private DateState completionDate = null;
	private JobStatusEnum jobStatus = null;
	private String jobStatusMessage = null;
	private String objectType = null;
	private String misc = null;
	private String note = null;
	private String queuePriority = null;
	private String metacatStatus = null;		// only if dataONE handler active
	private String ercData = null;			// EZID pass-thru
	private String altNotification = null;		// EZID pass-thru notificaton
	private String persistentURL = null;		// EZID binding persistent URL
	private boolean shadowARK;
	private boolean updateFlag;
	private boolean cleanup = true;
	private VersionMap versionMap = null;		// storage manifest 


	// constructors
	public JobState(){}
	public JobState(String user, String packageName, String algorithm, String value, String primaryID,
			String objectCreator, String objectTitle, String objectDate, String note) {
		this.packageName = packageName;
		this.objectCreator = objectCreator;
		this.objectTitle = objectTitle;
		this.objectDate = objectDate;
		this.note = note;
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
		public Identifier grabBatchID() {
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
		public StoreNode grabTargetStorage()	// non-displayable
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

	public ProfileState grabObjectProfile()	// non-displayable
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

	public String grabUserAgent()
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
	public String grabObjectState() {	// non-displayable
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
     * @return String misc
     */
    public String grabMisc() {
        return misc;
    }

    /**
     * Set DataCite metadata
     * @param String DataCite metadata
     */
    public void setDataCiteMetadata(String dataCiteMetadata) {
        this.DataCiteMetadata = dataCiteMetadata;
    }

    /**
     * Get DataCite Metadata
     * @return String DataCite Metadata
     */
    public String grabDataCiteMetadata() {
        return DataCiteMetadata;
    }

    /**
     * Set note, expository note regarding the onject's creation
     * @param String note
     */
    public void setNote(String note) {
        this.note = note;
    }

    /**
     * Get note 
     * @return String note
     */
    public String getNote() {
        return note;
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
    public String grabQueuePriority() {		// non-displayable
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
    public boolean grabShadowARK() {		// non-displayable
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
    public boolean grabUpdateFlag() {		// non-displayable
        return updateFlag;
    }

    /**
     * Set erc data. EZID passthru
     * @param String set ERC data
     */
    public void setERC(String ercData) {
        this.ercData = ercData;
    }

    /**
     * Get erc data
     * @return String erc data
     */
    public String grabERC() {		// non-displayable
        return ercData;
    }

    /**
     * Set EZID passthru notification.
     * @param String set altNotification
     */
    public void setAltNotification(String altNotification) {
        this.altNotification = altNotification;
    }

    /**
     * Get EZID passthru notification.
     * @return String alternateNotification
     */
    public String grabAltNotification() {		// non-displayable
        return altNotification;
    }

    /**
     * Set cleanup boolean
     * @param boolean set cleanup
     */
    public void setCleanupFlag(boolean cleanup) {
        this.cleanup = cleanup;
    }

    /**
     * Get cleanup boolean
     * @return boolean cleanup flag
     */
    public boolean grabCleanupFlag() {		// non-displayable
        return cleanup;
    }

    /**
     * Set version map (storage manifest)
     * @param versionmap 
     */
    public void setVersionMap(VersionMap versionMap) {
        this.versionMap = versionMap;
    }

    /**
     * Get version map (storage manifest)
     * @return versionmap
     */
    public VersionMap grabVersionMap() {          // non-displayable
        return versionMap;
    }


    /**
     * Set Metacat status
     * @param Metacat status
     */
    public void setMetacatStatus(String metacatStatus) {
        this.metacatStatus = metacatStatus;
    }

    /**
     * Get Metacat status
     * @return String Metacat status
     */
    public String getMetacatStatus() {
        return metacatStatus;
    }

    /**
     * Set persistent URL
     * @param URL
     */
    public void setPersistentURL(String persistentURL) {
        this.persistentURL = persistentURL;
    }

    /**
     * Get persistent URL
     * @return String URL
     */
    public String getPersistentURL() {
        return persistentURL;
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
        return this.dump(header, "", "", null);
    }

    public String dump(String header, FormatType format)
    {
        return this.dump(header, "", "", format);
    }

    public String dump(String header, String indent, String delimiter, FormatType format)
    {
        FormatterUtil formatterUtil = null;
	try {
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
            String noteS = (note != null) ? note : "";
            String metacatStatusS = (metacatStatus != null) ? metacatStatus : "";
            String persistentURLS = (persistentURL != null) ? persistentURL : "";
            String objectAggregateS = "";
	    try {
                objectAggregateS = objectProfile.getAggregateType();
	    } catch (NullPointerException npe) {}
    
	    if (format == null) {		// human readable format
	        header += "\n";
                if (StringUtil.isNotEmpty(jobIDS)) header += indent + "Job ID: " + jobIDS + delimiter;
                if (StringUtil.isNotEmpty(jobLabelS)) header += indent + "Job Label: " + jobLabelS + delimiter;
                if (StringUtil.isNotEmpty(primaryIDS)) header += indent + "Primary ID: " + primaryIDS + delimiter;
                if (StringUtil.isNotEmpty(localIDS)) header += indent + "Local ID: " + localIDS + delimiter;
                if (StringUtil.isNotEmpty(versionIDS)) header += indent + "Version: " + versionIDS + delimiter;
                if (StringUtil.isNotEmpty(packageName)) header += indent + "Filename: " + packageName + delimiter;
                if (StringUtil.isNotEmpty(objectTitleS)) header += indent + "Object title: " + objectTitleS + delimiter;
                if (StringUtil.isNotEmpty(objectCreatorS)) header += indent + "Object creator: " + objectCreatorS + delimiter;
                if (StringUtil.isNotEmpty(objectDateS)) header += indent + "Object date: " + objectDateS + delimiter;

		// Dublin Core (optional)
                if (StringUtil.isNotEmpty(DCcontributor)) header += indent + "DC contributor: " + DCcontributor + delimiter;
                if (StringUtil.isNotEmpty(DCcoverage)) header += indent + "DC coverage: " + DCcoverage + delimiter;
                if (StringUtil.isNotEmpty(DCcreator)) header += indent + "DC creator: " + DCcreator + delimiter;
                if (StringUtil.isNotEmpty(DCdate)) header += indent + "DC date: " + DCdate + delimiter;
                if (StringUtil.isNotEmpty(DCdescription)) header += indent + "DC description: " + DCdescription + delimiter;
                if (StringUtil.isNotEmpty(DCformat)) header += indent + "DC format: " + DCformat + delimiter;
                if (StringUtil.isNotEmpty(DCidentifier)) header += indent + "DC identifier: " + DCidentifier + delimiter;
                if (StringUtil.isNotEmpty(DClanguage)) header += indent + "DC language: " + DClanguage + delimiter;
                if (StringUtil.isNotEmpty(DCpublisher)) header += indent + "DC publisher: " + DCpublisher + delimiter;
                if (StringUtil.isNotEmpty(DCrelation)) header += indent + "DC relation: " + DCrelation + delimiter;
                if (StringUtil.isNotEmpty(DCrights)) header += indent + "DC rights: " + DCrights + delimiter;
                if (StringUtil.isNotEmpty(DCsubject)) header += indent + "DC subject: " + DCsubject + delimiter;
                if (StringUtil.isNotEmpty(DCtitle)) header += indent + "DC title: " + DCtitle + delimiter;
                if (StringUtil.isNotEmpty(DCtype)) header += indent + "DC type: " + DCtype + delimiter;

                //if (StringUtil.isNotEmpty(objectProfileS)) header += indent + "object profile: " + objectProfileS + delimiter;
	        // show URL if a) demo mode b) system object (for debugging)
                if ((StringUtil.isNotEmpty(objectStateS) && ProfileUtil.isDemoMode(objectProfile)) || 
		        (StringUtil.isNotEmpty(objectStateS) && ! ProfileUtil.isDemoMode(objectProfile) && StringUtil.isNotEmpty(objectAggregateS)))
		        header += indent + "Object state: " + objectStateS + "?t=xhtml" + delimiter;
                if (StringUtil.isNotEmpty(noteS)) header += indent + "Note: " + noteS + delimiter;
                if (StringUtil.isNotEmpty(submissionDateS)) header += indent + "Submission date: " + submissionDateS + delimiter;
                if (StringUtil.isNotEmpty(completionDateS)) header += indent + "Completion date: " + completionDateS + delimiter;
                if (StringUtil.isNotEmpty(metacatStatusS)) header += indent + "Metacat Registration Status: " + metacatStatusS + delimiter;
                if (StringUtil.isNotEmpty(persistentURLS)) header += indent + "Persistent URL: " + persistentURLS + delimiter;
                if (StringUtil.isNotEmpty(jobStatusS)) header += indent + "Status: " + jobStatusS + delimiter;
                if (StringUtil.isNotEmpty(jobStatusMessageS)) header += indent + "Status message: " + jobStatusMessageS + delimiter;

	    //} else if (format.equalsIgnoreCase("CSV")) {
	        //String[] fields = new String[12];
                //StringWriter stringWriter = new StringWriter();
                //CSVWriter csvWriter = new CSVWriter(stringWriter);
    
                //if (StringUtil.isNotEmpty(jobIDS)) fields[0] = jobIDS;
                //if (StringUtil.isNotEmpty(primaryIDS)) fields[1] = primaryIDS;
                //if (StringUtil.isNotEmpty(localIDS)) fields[2] = localIDS;
                //if (StringUtil.isNotEmpty(versionIDS)) fields[3] = versionIDS;
                //if (StringUtil.isNotEmpty(packageName)) fields[4] = packageName;
                //if (StringUtil.isNotEmpty(objectTitleS)) fields[5] = objectTitleS;
                //if (StringUtil.isNotEmpty(objectCreatorS)) fields[6] = objectCreatorS;
                //if (StringUtil.isNotEmpty(objectDateS)) fields[7] = objectDateS;
                //if (StringUtil.isNotEmpty(submissionDateS)) fields[8] = submissionDateS;
                //if (StringUtil.isNotEmpty(completionDateS)) fields[9] = completionDateS;
                //if (StringUtil.isNotEmpty(jobStatusS)) fields[10] = jobStatusS;
                //if (StringUtil.isNotEmpty(jobStatusMessageS)) fields[11] = jobStatusMessageS;
                //csvWriter.writeNext(fields);
	        //fields = null;
	        //try {
	            //csvWriter.close();
	        //} catch (Exception e) {}
    
	        //header = stringWriter.toString();
	    } else {	// state formatter
		try {
                    formatterUtil = new FormatterUtil();
	            return formatterUtil.doStateFormatting(this, format);
		} catch (Exception e) { return null; }
	    }

	    return header;
	} catch (Exception e) {} 
	finally 
        {
	    formatterUtil = null;
        }
	return null;
    }
}
