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
package org.cdlib.mrt.ingest.handlers;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.ManifestRowIngest;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.DigestUtil;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.utility.URLEncoder;


/**
 * retrieve data if type is a manifest
 * @author mreyes
 */
public class HandlerRetrieve extends Handler<JobState>
{

    protected static final Logger log4j2 = LogManager.getLogger();
    private static final String NAME = "HandlerRetrieve";
    private static final String MESSAGE = NAME + ": ";
    private int thread_pool_size = 4;	// Default
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;

    /**
     * retrieve data
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {
        boolean failure = false;
        String failureURL = null;
        boolean integrity = true;
        String integrityURL = null;

	File manifestFile = null;
	String status = "";
	ExecutorService executorService = null;

	try {

	    boolean result;
	    PackageTypeEnum packageType = ingestRequest.getPackageType();
	    File targetDir = new File(ingestRequest.getQueuePath(), "producer");
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");

	    if (packageType == PackageTypeEnum.container) {
		System.out.println("[info] " + MESSAGE + "container parm specified, no retrieval necessary.");
	        return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	    } else if (packageType == PackageTypeEnum.file) {
		System.out.println("[info] " + MESSAGE + "file parm specified, no retrieval necessary.");
	        return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	    } else if (packageType == PackageTypeEnum.manifest) {
		String alg = null;
		String val = null;
		try { 
		    alg = jobState.getHashAlgorithm();
		    val = jobState.getHashValue();
		} catch (Exception e) {}
		if (alg == null || val == null) {
		    System.out.println("[info] " + MESSAGE + "no manifest digest data provided. bypassing check");
		} else {
		    System.out.println("[info] " + MESSAGE + "validating manifest checksum: " + jobState.getPackageName());
		    if (validateManifestChecksum(new File(targetDir, jobState.getPackageName()), jobState.getHashAlgorithm(), jobState.getHashValue())) {
		        status = "valid";
        	        if (DEBUG) System.out.println("[info] " + MESSAGE + "manifest fixity check successful: " + jobState.getPackageName());
		    } else {
		        status = "not-valid";
		        throw new TException.FIXITY_CHECK_FAILS("[error] " + MESSAGE + "manifest fixity check fails: " + packageType);
		    }
		    System.out.println("[info] " + MESSAGE + "manifest parm specified, processing manifest(s)");
		}
	    } else {
		System.out.println("[error] " + MESSAGE + "package type not recognized: " + packageType);
		status = "not-valid";
		throw new TException.INVALID_OR_MISSING_PARM("[error] " + MESSAGE + "specified package type not recognized (file/container/manifest): " + packageType);
	    }

	    if ( status.equals("valid") || status.equals("")) {
	        LoggerInf logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);	// stdout logger
                Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.ingest);
	        // we have a manifest request, process all manifest files
	        for (String fileS : targetDir.list()) {
	            manifestFile = new File(targetDir, fileS);

		    if (manifestFile.exists()) {
                        System.out.println("[HandlerRetrieve] Processing manifest file: " + manifestFile.getName());
		    } else if (executorService != null) {
                        System.out.println("[HandlerRetrieve] WARNING: Manifest file does not exist: " + manifestFile.getName());
			continue;
		    } else {
			// manifest does not exist and no other manifests processed...FAIL
		    }
			
                    Enumeration<ManifestRowInf> enumRow = manifest.getRows(new FileInputStream(manifestFile));
                    ManifestRowIngest rowIn = null;
                    FileComponent fileComponent = null;

		    // Dryrun process of manifest 
                    System.out.println("[info] " + MESSAGE + "validating manifest integrity: " + jobState.getPackageName());
                    if (validateManifestIntegrity(manifestFile, logger)) {
                        status = "valid";
                        if (DEBUG) System.out.println("[info] " + MESSAGE + "manifest integrity check successful: " + jobState.getPackageName());
                    } else {
                        status = "not-valid";
                        throw new TException.FIXITY_CHECK_FAILS("[error] " + MESSAGE + "manifest integrity check fails: " + packageType);
                    }

		    if (ingestRequest.getNumDownloadThreads() != 0) {
			thread_pool_size = ingestRequest.getNumDownloadThreads();
                        System.out.println("[HandlerRetrieve] INFO: Setting download pool size to: " + thread_pool_size);
		    }
			
		    executorService = Executors.newFixedThreadPool(thread_pool_size);

    		    List<Future<String>> tasks = new ArrayList<Future<String>>();

		    // process all rows in each manifest file
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
                        if (DEBUG) {
                            System.out.println(fileComponent.dump("handlerRetrieve"));
                        }

			// launch download
                        Future<String> future = executorService.submit(new RetrieveData(fileComponent.getURL(), targetDir, fileComponent.getIdentifier(), jobState));
			tasks.add(future);
                    }

		    // Not blocked w/ callables
      		    executorService.shutdown();

		    while (! executorService.isTerminated()) {
		        System.out.println("awaiting completion of retrievals.... Thread: " + Thread.currentThread().getName());
		        Thread.currentThread().sleep(15000);		// 15 seconds
		    }

		    // check for errors
		    try {
      			for (Future<String> future : tasks) {
        		    String s = future.get();	// blocked, but should be complete
			    if (s != null) {
				failure = true;
				failureURL = s;
				break;
			    }
      			}

		    } catch (Exception e) { 
		        System.err.println("Error in checking download status");
			e.printStackTrace(System.err);
			failure = true;
		    }


		    tasks.clear();
		    // did any retrieval fail?
		    if (failure) {
			if (failureURL.contains("://")) {
		            throw new TException.REQUESTED_ITEM_NOT_FOUND("[error] " + MESSAGE + 
                                "Manifest error (URL retrieval error: " + failureURL + ")");
			} else {
		            throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + 
                                "Manifest error (Duplicate filename: " + failureURL + ")");
			}
		    }

		    // validate checksums
                    enumRow = manifest.getRows(new FileInputStream(manifestFile));
		    DigestUtil digestUtil = new DigestUtil();
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
			if (fileComponent.getMessageDigest() == null) {
		            if (DEBUG) System.out.println("[info] No checksum provided: " + fileComponent.getIdentifier());
			    continue;
			}
			if (fileComponent.getIdentifier() == null) {
			    fileComponent.setIdentifier(fileComponent.getURL().getPath());
		            if (DEBUG) System.out.println("[info] No filename provided.  Using URL name: " + fileComponent.getIdentifier());
			}

			try {
			    digestUtil.doFileFixity(new File(targetDir, fileComponent.getIdentifier()), fileComponent);
		            if (DEBUG) System.out.println("[info] No checksum problems: " + fileComponent.getIdentifier());
			} catch (TException te) {
			    throw te;
			}
		    }
		    digestUtil = null;

		    // save manifest
		    System.out.println("[INFO] saving submitter's manifest file: " + manifestFile.getAbsolutePath());
		    manifestFile.renameTo(new File(systemTargetDir, "mrt-submission-manifest.txt"));
	        }
	    }

            // metadata file in ANVL format
            File ingestFile = new File(systemTargetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, status)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to append metadata file: " + ingestFile.getAbsolutePath());
            }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	} catch (TRuntimeException trex) {
	    trex.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + trex.getDetail());
	} catch (TException te) {
	    te.printStackTrace(System.err);
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing manifest: " + manifestFile.getAbsolutePath() + " : " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    try {
	    	executorService.shutdown();
	    } catch (Exception e) {
	    }
	    //resetFailure();
	}
    }
   
    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param status manifest validity
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String status)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("manifestValidity", status);

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
    }


    /**
     * validate manifest file checksum
     *
     * @param manifestFile manifest file
     * @param digest manifest digest type
     * @param value manifest digest value
     * @return success in validating manifest
     */
    private boolean validateManifestChecksum(File manifestFile, String digest, String value)
        throws TException
    {
	MessageDigestValue messageDigestValue = new MessageDigestValue(manifestFile, digest, new TFileLogger("HandlerRetrieve", 10, 10));
	return value.equals(messageDigestValue.getChecksum());
    }


    /**
     * validate manifest integrity
     *
     * @param manifestFile manifest file
     * @return success in validating manifest
     */
    private boolean validateManifestIntegrity(File manifestFile, LoggerInf logger)
    {
        Manifest manifest = null;
        Enumeration<ManifestRowInf> eRow = null;
        ManifestRowIngest rIn = null;
        FileComponent fComponent = null;

	try {
            manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.ingest);
            eRow = manifest.getRows(new FileInputStream(manifestFile));

            // process all rows in each manifest file
            while (eRow.hasMoreElements()) {
                rIn = (ManifestRowIngest) eRow.nextElement();
                fComponent = rIn.getFileComponent();
                if (DEBUG) {
                    System.out.println("Pre-processing manifest entry: " + rIn.getLine());
                }
    
            }
	    return true;
	} catch (Exception e) {
	    System.err.println("[ERROR] Pre-processing manifest not valid: " + manifestFile.getAbsolutePath());
	    return false;
	} finally {
            manifest = null;
            eRow = null;
            rIn = null;
            fComponent = null;
	} 
    }


    public String getName() {
	return NAME;
    }

}

class RetrieveData implements Callable<String>
{

    private URL url = null;
    private File targetDir = null;
    private String fileName = null;
    private JobState jobState = null;

    // constructor
    public RetrieveData(URL url, File targetDir, String fileName, JobState jobState) {
	this.url = url;
	this.targetDir = targetDir;
	this.fileName = fileName;
	this.jobState = jobState;
    }

    public String call()
	throws Exception
    {
	try {
	    String status = "complete";;
	    long bytes = 0;
	    int retries = 0;
	    if (fileName == null) {
		fileName = url.getFile();
		if (fileName.startsWith("/")) fileName = fileName.substring(1);
	    }
            long startTime = DateUtil.getEpochUTCDate();
            System.out.println("Retrieving remote data: " + url.toString() + " ---- " + fileName);
            File f = new File(targetDir, fileName);
	    new File(f.getParent()).mkdirs();
	    if (! f.exists()) {
	        if (! f.createNewFile()) {
		    throw new Exception("Error creating target file: " + f.getAbsolutePath());
	        }
	    } else {
	        System.out.println("[error] file already exists: " + f.getAbsolutePath());
		throw new IOException("Error file already exists: " + f.getAbsolutePath());
	    }
            for (int i=0; i < 2; i++) {
	        try {
                    FileUtil.url2File(null, url, f, 2);
		    bytes = f.length();
		    break;
		} catch (Exception ste) {
		    System.out.println("[error] error on attempt: " + i);
		    f.delete();
		    status = "fail";
		}
		retries = i;
		if (i==1) throw new Exception();
	    }
            long endTime = DateUtil.getEpochUTCDate();
            ThreadContext.put("BatchID", jobState.grabBatchID().getValue());
            ThreadContext.put("JobID", jobState.getJobID().getValue());
            ThreadContext.put("URL", url.toString());
            ThreadContext.put("DurationMs", String.valueOf(endTime - startTime));
            ThreadContext.put("Retries", String.valueOf(retries));
            ThreadContext.put("Status", status);
            ThreadContext.put("Bytes", String.valueOf(bytes));
            // ThreadContext.put("ResponseCode", String.valueOf(statusCode));
            // ThreadContext.put("ResponsePhrase", statusPhrase);
            // ThreadContext.put("ResponseBody", responseBody);
            LogManager.getLogger().info("RETRIEVEGet");

	    return null;

	} catch (IOException ioe) {
	    ioe.printStackTrace();
	    System.out.println("[error] file already exists " + url.getFile());
	    return new String(url.getFile());
	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println("[error] In retrieval of URL: " + url.toString());
	    return new String(url.toString());
        } finally {
            ThreadContext.clearMap();
        }

    }
}
