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
package org.cdlib.mrt.ingest.handlers.estimate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * initialize ingest process
 * @author mreyes
 */
public class HandlerInitialize extends Handler<JobState>
{

    private static final String NAME = "HandlerInitialize";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final int BUFFERSIZE = 4096;
    private static final String FS = System.getProperty("file.separator");
    private LoggerInf logger = null;
    private Properties conf = null;



    /**
     * initialize ingest process
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {

	    File targetDir = new File(ingestRequest.getQueuePath(), "system");
	    if (! targetDir.exists()) targetDir.mkdirs();

	    // grab existing data if this is an update
	    if (jobState.grabUpdateFlag()) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Request for UPDATE");
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Request for ADD");
	    }

	    // metadata file in ANVL format
	    File ingestFile = new File(targetDir, "mrt-ingest.txt");
	    if ( ! createMetadata(profileState, ingestRequest, jobState, ingestFile)) {
	        throw new TException.GENERAL_EXCEPTION("[error] " 
		    + MESSAGE + ": unable to build metadata file: " + ingestFile.getAbsolutePath());
	    }

	    // ownership
	    String owner = profileState.getOwner();
	    if (StringUtil.isNotEmpty(owner)) {
		File ownerFile = null;
	        try { 
		    ownerFile = new File(targetDir, "mrt-owner.txt");
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating owner file: " + ownerFile.getAbsolutePath());
		    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(ownerFile));
		    bufferedWriter.write(owner);
		    bufferedWriter.close();
		} catch (IOException e) {
	            throw new TException.GENERAL_EXCEPTION("[error] " 
		        + MESSAGE + ": unable to build owner file: " + ownerFile.getAbsolutePath());
	        }
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "no owner found; no owner file created.");
	    }

	    // membership
	    if (profileState.getCollection().size() > 0) {
		File membershipFile = null;
	        try { 
		    membershipFile = new File(targetDir, "mrt-membership.txt");
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating membership file: " + membershipFile.getAbsolutePath());
		    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(membershipFile));

		    Iterator iterator = profileState.getCollection().iterator();
		    while(iterator.hasNext()) {
		        bufferedWriter.write((String) iterator.next() + "\n");
		    }
		    bufferedWriter.close();
		} catch (IOException e) {
	            throw new TException.GENERAL_EXCEPTION("[error] " 
		        + MESSAGE + ": unable to build membership file: " + membershipFile.getAbsolutePath());
	        }
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "no collection members; no membership file created.");
	    }

	    // resource map referencing metadata
	    // merritt object model
	    File momFile = new File(targetDir, "mrt-mom.txt");
	    if ( ! createMerrittObjectModel(jobState, profileState, ingestRequest, momFile)) {
	        throw new TException.GENERAL_EXCEPTION("[error] " 
		    + MESSAGE + ": unable to build merritt object model file: " + momFile.getAbsolutePath());
	    }
	    return new HandlerResult(true, "SUCCESS: " + NAME + " has created metadata");
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "failed to create metadata: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }
    }

    /**
     * write metadata to MOM file
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState contains job state info
     * @param ingestFile target file (usually "mrt-ingest.txt")
     * @return successful in writing metadata
     */
    private boolean createMerrittObjectModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File momFile)
	throws TException 
    {
	if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating merritt object model: " + momFile.getAbsolutePath());
	Map<String, Object> momProperties = new LinkedHashMap();	// maintains insertion order
	
	try {
	    momProperties.put("primaryIdentifier", jobState.getPrimaryID().getValue());
	} catch (Exception e) {
	    // momProperties.put("primaryIdentifier", "(:unas)");
	}
	momProperties.put("type", profileState.getObjectType());
	momProperties.put("role", profileState.getObjectRole());
	try {
	    momProperties.put("aggregate", profileState.getAggregateType());
	} catch (Exception e) { }
	try {
	    momProperties.put("localIdentifier", jobState.getLocalID().getValue());
	} catch (Exception e) {
	    // momProperties.put("localIdentifier", "(:unas)");
	}

	return MetadataUtil.writeMetadataANVL(momFile, momProperties, false);
    }


    /**
     * write metadata to target file
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState contains job state info
     * @param ingestFile target file (usually "mrt-ingest.txt")
     * @return successful in writing metadata
     */
    private boolean createMetadata(ProfileState profileState, IngestRequest ingestRequest,
		JobState jobState, File ingestFile)
	throws TException 
    {
	if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating metadata: " + ingestFile.getAbsolutePath());
	Map<String, Object> ingestProperties = new LinkedHashMap();	// maintains insertion order
	
	ingestProperties.put("ingest", ingestRequest.getServiceState().getServiceName());
	ingestProperties.put("submissionDate", jobState.getSubmissionDate().toString());
	ingestProperties.put("batch", jobState.grabBatchID().getValue());
	ingestProperties.put("job", jobState.getJobID().getValue());
	if (StringUtil.isNotEmpty(jobState.grabUserAgent())) {
	    ingestProperties.put("userAgent", jobState.grabUserAgent());
	}
	ingestProperties.put("file", jobState.getPackageName());
	ingestProperties.put("type", ingestRequest.getPackageType().getValue());
	ingestProperties.put("profile", profileState.getProfileID().getValue());

	Iterator collection = profileState.getCollection().iterator();
	String collectionString = "";
	while (collection.hasNext()) {
	    if (StringUtil.isEmpty(collectionString)) {
		collectionString = (String) collection.next();
	    } else {
	        collectionString = collectionString + "; " + (String) collection.next();
	    }
	}
	ingestProperties.put("collection", collectionString);

	ingestProperties.put("storageService", profileState.getTargetStorage().getStorageLink().toString());
	ingestProperties.put("storageNode", new Integer(profileState.getTargetStorage().getNodeID()).toString());

	Iterator contactsEmail = profileState.getContactsEmail().iterator();
	String notificationString = "";
	while (contactsEmail.hasNext()) {
	    if (StringUtil.isEmpty(notificationString)) {
		notificationString = ((Notification) contactsEmail.next()).getContactEmail();
	    } else {
	        notificationString = notificationString + ";" + ((Notification) contactsEmail.next()).getContactEmail();
	    }
	}
	contactsEmail = null;
	ingestProperties.put("notification", notificationString);


	//optional
	try {
	    ingestProperties.put("suppliedIdentifier", jobState.getPrimaryID().getValue());
	} catch (Exception e) {
	    ingestProperties.put("suppliedIdentifier", "(:unas)");	// mint object ID downstream
	}
	try {
            ingestProperties.put("digestType", jobState.getHashAlgorithm());
            ingestProperties.put("digestValue", jobState.getHashValue());
	} catch (Exception e) {
            ingestProperties.put("digestType", "(:unas)");
            ingestProperties.put("digestValue", "(:unas)");
	}

	// dc metadata
	if (StringUtil.isNotEmpty(jobState.getObjectCreator())) {
	    ingestProperties.put("creator", jobState.getObjectCreator());
	} else {
	    ingestProperties.put("creator", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getObjectTitle())) {
	    ingestProperties.put("title", jobState.getObjectTitle());
	} else {
	    ingestProperties.put("title", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getObjectDate())) {
	    ingestProperties.put("date", jobState.getObjectDate());
	} else {
	    ingestProperties.put("date", "(:unas)");
	}

	// more optional
	try {
	    ingestProperties.put("localIdentifier", jobState.getLocalID().getValue());
	} catch (Exception e) {
	    ingestProperties.put("localIdentifier", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getNote())) {
	    ingestProperties.put("note", jobState.getNote());
	} else {
	    ingestProperties.put("note", "(:unas)");
	}

	return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, false);
    }


    public String getName() {
	return NAME;
    }

}
