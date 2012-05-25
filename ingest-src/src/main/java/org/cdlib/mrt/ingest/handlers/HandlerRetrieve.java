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

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.vocabulary.*;

import org.cdlib.mrt.ingest.handlers.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.lang.Runnable;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileComponentContentInf;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.ManifestRowIngest;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.DigestUtil;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.ResourceMapUtil;
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

    private static final String NAME = "HandlerRetrieve";
    private static final String MESSAGE = NAME + ": ";
    private static final int THREAD_POOL_SIZE = 4;
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
		    System.out.println("[info] " + MESSAGE + "validating manifest: " + jobState.getPackageName());
		    if (validateManifest(new File(targetDir, jobState.getPackageName()), jobState.getHashAlgorithm(), jobState.getHashValue())) {
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

                    Enumeration<ManifestRowInf> enumRow = manifest.getRows(new FileInputStream(manifestFile));
                    ManifestRowIngest rowIn = null;
                    FileComponent fileComponent = null;

		    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    		    List<Future<String>> tasks = new ArrayList<Future<String>>();

		    // process all rows in each manifest file
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
                        if (DEBUG) {
                            System.out.println(fileComponent.dump("handlerRetrieve"));
                        }

			// launch download
                        Future<String> future = executorService.submit(new RetrieveData(fileComponent.getURL(), targetDir, fileComponent.getIdentifier()));
			tasks.add(future);
                    }

		    // check for errors
		    try {
      			for (Future<String> future : tasks) {
        		    String s = future.get();
			    if (s != null) {
				failure = true;
				failureURL = s;
				break;
			    }
      			}

      			executorService.shutdown();
		    } catch (Exception e) { 
			failure = true;
		    }



		    while (! executorService.isTerminated()) {
		        System.out.println("awaiting completion of retrievals....");
		        Thread.currentThread().sleep(5000);		// 5 seconds
		    }

		    tasks.clear();
		    // did any retrieval fail?
		    if (failure) {
		        throw new TException.REQUESTED_ITEM_NOT_FOUND("[error] " + MESSAGE + 
                                "Manifest error (URL retrieval error or manifest has duplicate filename entries): " + failureURL);
		    }

		    // validate checksums
                    enumRow = manifest.getRows(new FileInputStream(manifestFile));
		    DigestUtil digestUtil = new DigestUtil();
                    while (enumRow.hasMoreElements()) {
                        rowIn = (ManifestRowIngest) enumRow.nextElement();
                        fileComponent = rowIn.getFileComponent();
			if (fileComponent.getMessageDigest() == null) {
		            if (DEBUG) System.out.println("[info] No checksum provided: " + fileComponent.getIdentifier());
			    break;
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

	    // If updating and we do not have a container
            File existingProducerDir = new File(ingestRequest.getQueuePath() +
                        System.getProperty("file.separator") + ".producer" + System.getProperty("file.separator"));
            if (jobState.grabUpdateFlag() && existingProducerDir.exists()) {
                System.out.println("[debug] " + MESSAGE + "Found existing producer data, processing.");
                FileUtil.updateDirectory(existingProducerDir, new File(ingestRequest.getQueuePath(), "producer"));
                FileUtil.deleteDir(existingProducerDir);
            }


            // metadata file in ANVL format
            File ingestFile = new File(systemTargetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, status)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to append metadata file: " + ingestFile.getAbsolutePath());
            }

            // update resource map
            File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");
            if ( ! updateResourceMap(profileState, ingestRequest, mapFile, targetDir, manifestFile)) {
                System.err.println("[warn] " + MESSAGE + "Failure to update resource map.");
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
     * write aggregates references to resource map
     *
     * @param profileState profile state
     * @param ingestRequest ingest request
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @param sourceDir source directory 
     * @param manifestFile ignore this
     * @return successful in updating resource map
     */
    private boolean updateResourceMap(ProfileState profileState, IngestRequest ingestRequest, File mapFile, File sourceDir, File manifestFile)
        throws TException {
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath() + " - " + sourceDir.getAbsolutePath());

            Model model = updateModel(profileState, ingestRequest, mapFile, sourceDir, manifestFile);
            if (DEBUG) ResourceMapUtil.dumpModel(model);
            ResourceMapUtil.writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
        }
    }


    public Model updateModel(ProfileState profileState, IngestRequest ingestRequest, File mapFile, File sourceDir, File manifestFile)
        throws Exception
    {
        try {

            // read in existing model
            InputStream inputStream = FileManager.get().open(mapFile.getAbsolutePath());
            if (inputStream == null) {
                String msg = "[error] " + MESSAGE + "failed to update resource map: " + mapFile.getAbsolutePath();
                throw new TException.GENERAL_EXCEPTION(msg);
            }
            Model model = ModelFactory.createDefaultModel();
            model.read(inputStream, null, "TURTLE");

            String versionID = "0";             // current
            String objectIDS = null;
            String ore = "http://www.openarchives.org/ore/terms#";

            try {
                objectIDS = ingestRequest.getJob().getPrimaryID().getValue();
            } catch (Exception e) {
                objectIDS = "OID_UNKNOWN";          // replace when known
            }
            String objectURI = profileState.getTargetStorage().getStorageLink().toString() + "/content/" +
                        profileState.getTargetStorage().getNodeID() + "/" +
                        URLEncoder.encode(objectIDS, "utf-8");

            String resourceMapURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-object-map.ttl", "utf-8");

            // add each component file
            Vector<File> files = new Vector();


            FileUtil.getDirectoryFiles(sourceDir, files);
            for (File file : files) {
                if (file.isDirectory()) continue;
                if (file.getName().equals(manifestFile.getName())) continue;	// ignore manifest file
                // Turtle will not handle whitespace in URL, must encode
                String component = objectURI + "/" + versionID + URLEncoder.encode(file.getPath().substring(file.getPath().indexOf("/producer")), "utf-8");
                model.add(ResourceFactory.createResource(objectURI),
                    ResourceFactory.createProperty(ore + "aggregates"),
                    ResourceFactory.createResource(component));
            }

            return model;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update model: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }


    /**
     * validate manifest file
     *
     * @param manifestFile manifest file
     * @param digest manifest digest type
     * @param value manifest digest value
     * @return success in validating manifest
     */
    private boolean validateManifest(File manifestFile, String digest, String value)
        throws TException
    {
	MessageDigestValue messageDigestValue = new MessageDigestValue(manifestFile, digest, new TFileLogger("HandlerRetrieve", 10, 10));
	return value.equals(messageDigestValue.getChecksum());
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

    // constructor
    public RetrieveData(URL url, File targetDir, String fileName) {
	this.url = url;
	this.targetDir = targetDir;
	this.fileName = fileName;
    }

    public String call()
	throws Exception
    {
	try {
	    if (fileName == null) {
		fileName = url.getFile();
		if (fileName.startsWith("/")) fileName = fileName.substring(1);
	    }
            System.out.println("Retrieving remote data: " + url.toString() + " ---- " + fileName);
            File f = new File(targetDir, fileName);
	    new File(f.getParent()).mkdirs();
	    if (! f.exists()) {
	        if (! f.createNewFile()) {
		    throw new Exception("Error creating target file: " + f.getAbsolutePath());
	        }
	    } else {
	        System.out.println("[warn] file already exists: " + f.getAbsolutePath());
		return null;
	    }
            for (int i=0; i < 2; i++) {
	        try {
                    FileUtil.url2File(null, url, f, 2);
		    break;
		} catch (Exception ste) {
		    System.out.println("[error] error on attempt: " + i);
		    f.delete();
		}
		if (i==1) throw new Exception();
	    }

	    return null;

	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println("[error] Retrieving URL : " + url.toString());
	    return new String(url.toString());
	}
    }
}
