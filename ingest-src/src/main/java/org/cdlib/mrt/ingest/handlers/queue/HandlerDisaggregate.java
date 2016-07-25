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
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowBatch;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.MessageDigest;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * process batch submission data
 * @author mreyes
 */
public class HandlerDisaggregate extends Handler<BatchState>
{

    protected static final String NAME = "HandlerDisaggregate";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected static final long MAX_MANIFEST_LENGTH = 5000000;
    protected LoggerInf logger = null;
    protected Properties conf = null;

    /**
     * Unpack batch manifest if necessary, create job ID(s)
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, BatchState batchState) 
	throws TException 
    {

	File file = null;
	String status = null;
	PackageTypeEnum packageType = ingestRequest.getPackageType();
	try {
	    boolean result;
	    File queueDir = new File(ingestRequest.getQueuePath().getAbsolutePath());
	    for (String fileS : queueDir.list()) {
	        file = new File(queueDir, fileS);
	    	if (packageType == PackageTypeEnum.batchManifestFile || packageType == PackageTypeEnum.batchManifestContainer
		    || packageType == PackageTypeEnum.batchManifest) {
			System.out.println("[info] " + MESSAGE + "batchManifest specified, unpacking: " + fileS);

			// unpack
			if (! unpack(file, queueDir, batchState)) {
	    		    System.out.println("[error] " + MESSAGE + "processing batchManifest: " + file.getAbsolutePath());
	    		    throw new TException.INVALID_OR_MISSING_PARM("[error] " 
				+ MESSAGE + "processing batchManifest: " + file.getAbsolutePath());
			}
			status = "valid";
	    	} else if (packageType == PackageTypeEnum.file || packageType == PackageTypeEnum.container || packageType == PackageTypeEnum.manifest) {
			System.out.println("[info] " + MESSAGE + "job parm specified, no unpacking needed: " + fileS);
			System.out.println("[info] " + MESSAGE + "batchID: " + batchState.getBatchID().getValue());
			JobState jobState = createJob(file, queueDir);
			jobState.setUpdateFlag(batchState.grabUpdateFlag());

			if (fileS.equals(ingestRequest.getJob().getPackageName())) 
			    jobState.setPackageName(ingestRequest.getJob().getPackageName());
			else
			    // address multiple file submission
			    jobState.setPackageName(fileS);
			try {
                            jobState.setHashAlgorithm(ingestRequest.getJob().getHashAlgorithm());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setHashValue(ingestRequest.getJob().getHashValue());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setPrimaryID(ingestRequest.getJob().getPrimaryID().getValue());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setLocalID(ingestRequest.getJob().getLocalID().getValue());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setObjectTitle(ingestRequest.getJob().getObjectTitle());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setObjectCreator(ingestRequest.getJob().getObjectCreator());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setObjectDate(ingestRequest.getJob().getObjectDate());
			} catch (Exception e) { /* optional */ }
			try {
                            jobState.setNote(ingestRequest.getJob().getNote());
			} catch (Exception e) { /* optional */ }

			batchState.addJob(jobState.getJobID().getValue(), jobState);
			status = "valid";
		} else {
			status = "not-valid";
			String msg = "[error] " + MESSAGE + "specified package type not recognized (file/container/manifest/batchManifest): " + fileS;
			System.err.println(msg);
	    		throw new Exception(msg);
		}
            }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	} catch (TException te) {
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail(), 10);
	} catch (Exception e) {
            String msg = "[error] " + MESSAGE + "processing file: " + file.getAbsolutePath() + " : " + e.getMessage();
	    System.err.println(msg);
            return new HandlerResult(false, msg, 10);
	} finally {
	}
    }
   
    /**
     * unpack batchManifest
     *
     * @param manifest batch manifest (manifest of manifests).  supports local files only 
     * @param target queueDir target directory
     * @param batchState serialized object for later reference
     * @return boolean operation status
     */
    private boolean unpack(File manifestFile, File queueDir, BatchState batchState)
	throws TException
    {
	try {
            Manifest manifest = Manifest.getManifest(new TFileLogger("Jersey", 10, 10), ManifestRowAbs.ManifestType.batch);
	    ManifestRowBatch manifestRow = null;

            if (manifestFile.length() > MAX_MANIFEST_LENGTH) {
                if (DEBUG) System.out.println("[error]  Manifest exceeds size limit: " + manifestFile.getAbsolutePath()
                        + " -- size: " + manifestFile.length());
                throw new TException.REQUEST_INVALID(MESSAGE + "Manifest exceeds size limit. (Max: 5MB)");
            }

            Enumeration en = manifest.getRows(manifestFile);
            while (en.hasMoreElements()) {
                manifestRow = (ManifestRowBatch) en.nextElement();
                FileComponent fileComponent = manifestRow.getFileComponent();
	        String fileName = fileComponent.getIdentifier();
	        if (StringUtil.isEmpty(fileName)) fileName = fileComponent.getURL().getFile().replace("/", "");
                System.out.println("[info] " + MESSAGE + "Queuing is active, batchID: " + batchState.getBatchID().getValue() + " manifest entry: " + fileName);
		JobState jobState = createJob(fileComponent.getURL(), fileName, queueDir);
		jobState.setUpdateFlag(batchState.grabUpdateFlag());

		// particulars are housed in object manifest file
		jobState.setPackageName(fileName);
		try {
		    jobState.setHashAlgorithm(fileComponent.getMessageDigest().getAlgorithm().getJavaAlgorithm());
		} catch (Exception e) {}
		try {
		    jobState.setHashValue(fileComponent.getMessageDigest().getValue());
		} catch (Exception e) {}
		jobState.setPrimaryID(fileComponent.getPrimaryID());
		jobState.setLocalID(fileComponent.getLocalID());
	    	jobState.setObjectType(getObjectType(manifestRow.getProfile()));
		try {
		    jobState.setObjectTitle(fileComponent.getTitle());
		} catch (Exception e) {}
		try {
		    jobState.setObjectCreator(fileComponent.getCreator());
		} catch (Exception e) {}
		try {
		    jobState.setObjectDate(fileComponent.getDate().toString());
		} catch (Exception e) {}
		batchState.addJob(jobState.getJobID().getValue(), jobState);
            }
            System.out.println("[info] " + MESSAGE + "Queuing is complete, batchID: " + batchState.getBatchID().getValue());
	    // manifestFile.delete();	// keep for debugging

    	    return true;
	} catch (TException te) {
	    throw te;
	} catch (Exception e) {
    	    String msg = "[error] " + MESSAGE + "unpacking manifest file: " + manifestFile.getAbsolutePath();
	    System.err.println(msg);
	    throw new TException.GENERAL_EXCEPTION(msg);
	} finally {
	}
    }

    /**
     * create new job
     *
     * @param fileURL file url reference
     * @param queueDir target directory
     * @return jobState
     */
    private JobState createJob(URL fileURL, String fileName, File queueDir)
	throws TException
    {
	try {
	    File tempFile = new File(queueDir, fileName);
	    tempFile.createNewFile();

	    // retry 3 times
            FileUtil.url2File(null, fileURL.toString(), tempFile, 3);

    	    return createJob(tempFile, queueDir);
	} catch (Exception e) {
    	    String msg = "[error] " + MESSAGE + "Could not retrieve url: " + fileURL.toString();
    	    System.out.println(msg);
	    throw new TException.REQUESTED_ITEM_NOT_FOUND(msg);
	} finally {
            try {
            } catch (Exception e) { }
	}
    }

    /**
     * create new job
     *
     * @param fileComponent file component name
     * @param queueDir target directory
     * @return jobState
     */
    private JobState createJob(File fileComponent, File queueDir)
	throws TException
    {
	try {
	    if ( !fileComponent.exists()) {
	   	System.out.println("[error] " + MESSAGE + "batch manifest object does not exist: " + fileComponent.getAbsolutePath());
	    	throw new Exception("[error] " + MESSAGE + "batch manifest object does not exist: " + fileComponent.getAbsolutePath());
	    }
	    JobState jobState = new JobState();
	    jobState.setJobID(MintUtil.getJobID());

	    // seed data into target area
	    File targetDir = new File(queueDir, jobState.getJobID().getValue());
	    if ( ! targetDir.exists()) {
		targetDir.mkdirs();
	    }
	    //FileUtil.copyFile(fileComponent.getName(), fileComponent.getParentFile(), targetDir);
	    File objectManifest = new File(targetDir, fileComponent.getName());
	    new File(fileComponent.getParentFile(), fileComponent.getName()).renameTo(objectManifest);
	    if ( ! objectManifest.exists()) {
	   	System.out.println("[error] " + MESSAGE + "failure to copy batch component into target area: " + targetDir.getAbsolutePath());
	    	throw new Exception("[error] " + MESSAGE + "failure to copy batch component into target area: " + targetDir.getAbsolutePath());
	    } else {
	   	System.out.println("[info] " + MESSAGE + "created new JOB: " + jobState.getJobID().getValue() +
			 " - manifest entry: " + objectManifest.getAbsolutePath()); 
	    }

    	    return jobState;
	} catch (TException te) {
	    throw te;
	} catch (Exception e) {
    	    String msg = "[error] " + MESSAGE + "creating job for component: " + fileComponent.getAbsolutePath();
    	    System.out.println(msg);
	    throw new TException.GENERAL_EXCEPTION(msg);
	} finally {
	}
    }

    /**
     * get job type
     *
     * @param profile profile type
     * @return String
     */
    private String getObjectType(String profile)
	throws Exception
    {
	try {
            if (profile.contains("mrt-batch-manifest")) {
	        // we have an object-manifest, but can not create an enum w/hyphen
                return "manifest";
            } else if (profile.contains("mrt-container-batch-manifest")) {
	        // we have a container manifest
                return "container";
            } else if (profile.contains("mrt-single-file-batch-manifest")) {
	        // we have a file manifest
                return "file";
            } else {
		throw new Exception("profile type not valid: " + profile);
            }
	} catch (Exception e) {
    	    e.printStackTrace();
	    throw new Exception(e.getMessage());
	} finally {
	}
    }

    public String getName() {
	return NAME;
    }

}
