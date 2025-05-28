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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.lang.Long;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.http.HttpEntity;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.ingest.utility.DigestUtil;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.ManifestRowIngest;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.zk.ZKKey;



/**
 * Estimate resource needs
 * @author mreyes
 */
public class HandlerEstimate extends Handler<JobState>
{

    private static final String NAME = "HandlerEstimate";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String FS = System.getProperty("file.separator");
    private LoggerInf logger = null;
    private Properties conf = null;
    private int thread_pool_size = 4;   // Default
    protected static final Logger log4j2 = LogManager.getLogger();
    private String zooConnectString = null;


    /**
     * Estimate resource needs
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
	    long submissionSize = 0L;
            PackageTypeEnum packageType = ingestRequest.getPackageType();
            File targetDir = new File(ingestRequest.getQueuePath(), "producer");
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");

            if (packageType == PackageTypeEnum.container || packageType == PackageTypeEnum.file) {
                submissionSize = calulateDirSize(targetDir);
                System.out.println("[info] " + MESSAGE + "Container or File submission.  Size: " + submissionSize);
                jobState.setSubmissionSize(submissionSize);
                return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
            } else if (packageType == PackageTypeEnum.manifest) {
                LoggerInf logger = LoggerAbs.getTFileLogger("testFormatter", 10, 10);   // stdout logger
                Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.ingest);
                // we have a manifest request, process all manifest files
                for (String fileS : targetDir.list()) {
                    manifestFile = new File(targetDir, fileS);

                    if (manifestFile.exists()) {
                        System.out.println("[HandlerEstimate] Processing manifest file: " + manifestFile.getName());
                    } else if (executorService != null) {
                        System.out.println("[HandlerEstimate] WARNING: Manifest file does not exist: " + manifestFile.getName());
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
                        System.out.println("[HandlerEstimate] INFO: Setting processing pool size to: " + thread_pool_size);
                    }

                    // Check for duplicate manifest entries
        	    ArrayList<String> objectNames = new ArrayList<>();        // Non string logging
                    enumRow = manifest.getRows(new FileInputStream(manifestFile));
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
                        String objectName = fileComponent.getIdentifier();
                        if (objectNames.contains(objectName)) {
                            System.err.println("[error] Duplicate manifest entry detected: " + objectName);
			    ThreadContext.put("Duplicate manifest entry detected: ", objectName + " - " + jobState.getPackageName() + " - " + jobState.grabObjectProfile().getCollectionName());
            		    return new HandlerResult(false, "Error: " + NAME + " Duplicate manifest entry detected: " + objectName, 0);
                        } else {
			    objectNames.add(objectName);
			}

                    }

                    executorService = Executors.newFixedThreadPool(thread_pool_size);
                    List<Future<String>> tasks = new ArrayList<Future<String>>();

                    // process all rows in each manifest file
                    enumRow = manifest.getRows(new FileInputStream(manifestFile));
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
                        if (DEBUG) {
                            System.out.println(fileComponent.dump("handlerEstimate"));
                        }

                        // launch download
                        Future<String> future = executorService.submit(new CalculateSize(fileComponent.getURL(), fileComponent.getIdentifier(), jobState));
                        tasks.add(future);
                    }

                    // Not blocked w/ callables
                    executorService.shutdown();

                    while (! executorService.isTerminated()) {
                        System.out.println("awaiting completion of size calculation.... Thread: " + Thread.currentThread().getName());
                        Thread.currentThread().sleep(15000);            // 15 seconds
                    }


                    // Sum manifest size
                    try {
                        for (Future<String> future : tasks) {
                            String s = future.get();    // blocked, but should be complete

			    // If URL, then it is a retrieval error
                            if (s.contains("://")) {
            			return new HandlerResult(false, "ERROR: " + NAME + "Manifest error (URL retrieval error: " + s, 0);
                            }

			    // Convert string to long
                            if (s != null) {
				submissionSize += Long.parseLong(s);
                            }
                        }
			// Differeniiate from a zero length file
			if (submissionSize == 0) submissionSize = -1;
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                        System.err.println("Error in calculating Manifest size");
                    }

                    tasks.clear();

                    if (DEBUG) System.out.println("[info] " + MESSAGE + "manifest size calculation successful: " + jobState.getPackageName() + ": " + submissionSize);
		    jobState.setSubmissionSize(submissionSize);

                }

            } else {
                System.out.println("[error] " + MESSAGE + "package type not recognized: " + packageType);
                status = "not-valid";
                throw new TException.INVALID_OR_MISSING_PARM("[error] " + MESSAGE + "specified package type not recognized (file/container/manifest): " + packageType);
            }

            return new HandlerResult(true, "SUCCESS: " + NAME + " Resource estimation complete: " + ingestRequest.getJob().toString(), 0);
        } catch (Exception e) {
            String msg = "[error] " + MESSAGE + "Estimating resources: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            try {
                executorService.shutdown();
            } catch (Exception e) {
            }
        }

    }



    // Calculate size of data in producer directory
    protected long calulateDirSize(File dir) {
        long length = 0;
        for (File file : dir.listFiles()) {
	    if (file.getName().endsWith("checkm")) continue;
            if (file.isFile())
                length += file.length();
            else
                length += calulateDirSize(file);
        }
        return length;
    }

    public boolean isComponent(String file) {
	if (file.equals("producer")) return false;
	if (file.equals("system")) return false;
	return true;
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
	    e.printStackTrace();
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

class CalculateSize implements Callable<String>
{

    private static final boolean DEBUG = true;
    private URL url = null;
    private String fileName = null;
    private JobState jobState = null;

    // constructor
    public CalculateSize(URL url, String fileName, JobState jobState) {
        this.url = url;
        this.fileName = fileName;
        this.jobState = jobState;
    }

    public String call()
        throws Exception
    {
        HashMap<String,Object> msgMap = new HashMap<>();        // Non string logging
        try {
            long bytes = 0;
            int retries = 0;

            System.out.println("Retrieving remote data size: " + url.toString() + " ---- " + fileName);
            for (int i=0; i < 2; i++) {
                try {
		    HttpURLConnection uc = (HttpURLConnection) url.openConnection();
            	    bytes = uc.getContentLengthLong();

		    if (uc.getResponseCode() == 404) {
                        if (DEBUG) System.out.println("Estimate [error]: " + " URL not retrievable: " + url.toString());
            		return url.toString();
		    }

		    if (bytes == -1 ) {
			ThreadContext.put("Content Length not provided: ", url.toString() + " - " + jobState.grabObjectProfile().getCollectionName());
		        bytes = 0;
		    } else {
                        if (DEBUG) System.out.println("[info] Found size: " + url.toString() + " - Bytes: " + bytes);
		    }
                    break;
                } catch (Exception ste) {
                    System.out.println("[error] retrieving bytes size: " + url.toString() + " retry attempt: " + i);
                }
                retries = i;
                if (i==1) break;        // log error
            }

            return String.valueOf(bytes);

        } catch (Exception e) {
            e.printStackTrace();
            LogManager.getLogger().error(e);
            System.out.println("[error] In retrieval of URL: " + url.toString());
            return "0";
        } finally {
            ThreadContext.clearMap();
        }

    }
}
