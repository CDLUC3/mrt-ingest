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
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowObject;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;



/**
 * validate manifest
 * @author mreyes
 */
public class HandlerCorroborate extends Handler<JobState>
{

    private static final String NAME = "HandlerCorroborate";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private Integer defaultStorage = null;
    private String manifestName = "mrt-manifest.txt";

    /**
     * validate manifest
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState 
     * @return HandlerResult result in manifest validation processing
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	File manifest = new File(ingestRequest.getQueuePath(), "producer" + System.getProperty("file.separator") + manifestName);
	String validateStatus = "valid";
	String integrityStatus = null;

        try {
            PackageTypeEnum packageType = ingestRequest.getPackageType();
            if (packageType != PackageTypeEnum.container) {
                System.out.println("[info] " + MESSAGE + "specified package type is not a container.  No processing necesary.");
                return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	    } else {
	 	// Let's check for a manifest within container
		if (! manifest.exists()) {
                    System.out.println("[info] " + MESSAGE + "no manifest exists in container.  No processing necesary.");
                    return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
		} else {
                    System.out.println("[info] " + MESSAGE + "Corroborating manifest found in container.");
		} 
	    }

	    // We have a manifest
	    boolean valid = true;
	    try {
	        if (! (valid = checkManifest(manifest, ingestRequest.getQueuePath()))) {
		    valid = false;
                    integrityStatus = "failed";
                    System.err.println("[error] " + MESSAGE + "error in corroborating manifest: " + manifest.getAbsolutePath());
	        } else {
                    integrityStatus = "verified";
	        } 
	    } catch (TException.INVALID_OR_MISSING_PARM te) {
		valid = false;
		validateStatus = "not-valid";	// manifest is not-valid
	    }

            // metadata file in ANVL format
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File ingestFile = new File(systemTargetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, validateStatus, integrityStatus)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to append metadata file: " + ingestFile.getAbsolutePath());
            }

	    if (valid) {
                return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	    } else {
                return new HandlerResult(false, "ERROR: " + NAME + "error in corroborating manifest: " + manifest.getAbsolutePath(), 0);
	    }

	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "ERROR: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "validating manifest: " + e.getMessage();
            return new HandlerResult(false, msg);
	}
    }
   
    public String getName() {
	return NAME;
    }

    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param validateStatus manifest validity
     * @param integrityStatus manifest integrity
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String validateStatus, String integrityStatus)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("manifestValidity", validateStatus);
	if (validateStatus.equals("valid")) {
            ingestProperties.put("manifestIntegrity", integrityStatus);
	}

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
    }

    /**
     * corroborate manifest
     *
     * @param manifestFile user supplied manifest file
     * @param queuePath pathname to container data
     * @return successful in appending metadata
     */
    private boolean checkManifest(File manifestFile, File queuePath)
        throws TException
    {
	boolean check = true;
	final int INITIAL_SIZE = 25;

	try {
	    logger = new TFileLogger("HandlerCorroborate", 10, 10);

            Vector<File> files = new Vector<File>(INITIAL_SIZE);
            Map<String, FileComponent> fcManifest = new HashMap<String, FileComponent>(INITIAL_SIZE);	// user supplied manifest
            Map<String, FileComponent> fcDirectory = new HashMap<String, FileComponent>(INITIAL_SIZE);	// data directory
            FileUtilAlt.getDirectoryFiles(new File(queuePath, "producer"), files);
            FileComponent fc = null;

	    // read data directory
            for (File file : files) {
                FileContent fileContent = FileContent.getFileContent(file, logger);
                fc =  fileContent.getFileComponent();
                String fileName = file.getCanonicalPath();
                String sourceName = new File(queuePath, "producer").getCanonicalPath();
                fileName = fileName.substring(sourceName.length() + 1);
                String urlName = FileUtil.getURLEncodeFilePath(fileName);
                fileName = fileName.replace('\\', '/');
                fc.setIdentifier(fileName);
    
                if (! fileName.equals(manifestName)) fcDirectory.put(fc.getIdentifier(), fc);	// ignore manifest itself
	    }
    
	    // read manifest
            Manifest manIn = null;
	    try {
                manIn = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.object);
	    } catch (Exception e) {
		throw new TException.INVALID_OR_MISSING_PARM("manifest is not-valid");
	    }
            ManifestRowObject manRowIn = null;
            Enumeration en = manIn.getRows(manifestFile);
            while (en.hasMoreElements()) {
                manRowIn = (ManifestRowObject)en.nextElement();
                fc =  manRowIn.getFileComponent();
                fcManifest.put(fc.getIdentifier(), fc);

		// cross-reference supplied manifest with directory data
		FileComponent fcCheck = fcDirectory.get(fc.getIdentifier());
		if (fcCheck == null) {
                    System.err.println("[error] " + MESSAGE + "manifest file entry does not exist in data dir: " + fc.getIdentifier());
		    return false;
		} else {
		    if (fc.getSize() != fcCheck.getSize()) {
                        System.err.println("[error] " + MESSAGE + "manifest file entry filesize does not match data: " + fc.getIdentifier() + "    " +
				fc.getSize() + " -- " + fcCheck.getSize());
		        return false;
		    }
	            if (! fc.getMessageDigest().getValue().equals(fcCheck.getMessageDigest().getValue())) {
                        System.err.println("[error] " + MESSAGE + "manifest file entry digest does not match data: " + fc.getIdentifier() + "    " +
				fc.getMessageDigest().getValue() + " -- " + fcCheck.getMessageDigest().getValue());
		        return false;
		    }
		}
            }

	    // cross-reference data directory data with supplied manifest
	    Iterator it = fcDirectory.keySet().iterator();
	    while(it.hasNext()) {
    		fc = (FileComponent) fcDirectory.get((String) it.next());

		FileComponent fcCheck = fcManifest.get(fc.getIdentifier());
		if (fcCheck == null) {
                    System.err.println("[error] " + MESSAGE + "data file does not exist in manifest: " + fc.getIdentifier());
		    return false;
		} else {
		    if (fc.getSize() != fcCheck.getSize()) {
                        System.err.println("[error] " + MESSAGE + "data file filesize does not match manifest: " + fc.getIdentifier() + "    " +
				fc.getSize() + " -- " + fcCheck.getSize());
		        return false;
		    }
	            if (! fc.getMessageDigest().getValue().equals(fcCheck.getMessageDigest().getValue())) {
                        System.err.println("[error] " + MESSAGE + "data file digest does not match manifest: " + fc.getIdentifier() + "    " +
				fc.getMessageDigest().getValue() + " -- " + fcCheck.getMessageDigest().getValue());
		        return false;
		    }
		}
	    }


	} catch (TException te) {
		te.printStackTrace();
	        throw te;
	} catch (Exception e) {
	        e.printStackTrace();
	        throw new TException.GENERAL_EXCEPTION(e.getMessage());
	}

        return check;
    }
}
