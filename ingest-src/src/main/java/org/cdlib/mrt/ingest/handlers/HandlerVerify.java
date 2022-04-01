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
import java.util.LinkedHashMap;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * verify submission package
 * @author mreyes
 */
public class HandlerVerify extends Handler<JobState>
{

    private static final String NAME = "HandlerVerify";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;

    /**
     * compute/verify checksum
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException
    {

	File submissionPackage = null;
	String digest = null;
	String value = null;

	try {
	    // Check for necessary inputs
	    if (StringUtil.isEmpty(jobState.getPackageName())) {
		throw new TException.INVALID_OR_MISSING_PARM("[error]" + MESSAGE + "package name not supplied");
	    }

	    submissionPackage = new File(ingestRequest.getQueuePath() + "/producer/", new File(jobState.getPackageName()).getName());
	    try {
	        digest = jobState.getHashAlgorithm();
	    } catch (Exception e) {
	        digest = null;
	    }
	    value = jobState.getHashValue();

	    if (StringUtil.isEmpty(digest)) {
		 if (StringUtil.isEmpty(value)) {
		    // no verifying necessary
	    	    return new HandlerResult(true, "[info]: " + MESSAGE + "HandlerVerify not needed.  No checksum specified");
		}
	    } else {
		 if (StringUtil.isEmpty(value)) {
		    // digest specified without value
	    	    return new HandlerResult(true, "[info]: " + 
			MESSAGE + "Checksum verification not needed.  Digest type specified without matching algorithm");
		 }

	    }

	    LoggerInf logger = LoggerAbs.getTFileLogger("debug", 10, 10);
	    MessageDigestValue message = new MessageDigestValue(submissionPackage, digest, logger);

	    String calculatedChecksum = message.getChecksum();

	    if (! calculatedChecksum.equals(value)) {
		throw new TException.FIXITY_CHECK_FAILS("[error] submission package checksum mismatch: " + submissionPackage.getName());
	    }

            // metadata file in ANVL format
	    File targetDir = new File(ingestRequest.getQueuePath(), "system");
            File ingestFile = new File(targetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, digest, value, "valid")) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to build metadata file: " + ingestFile.getAbsolutePath());
            }

	    return new HandlerResult(true, "[info]: " + MESSAGE + " submission checksum matches. handler completed successfully");
	} catch (TException te) {
	    te.printStackTrace(System.err);
	    return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
	    e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing checksum: " + e.getMessage();
	    return new HandlerResult(false, msg);
	} finally {
            // cleanup?
	}
    }
   
    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param digest package digest algorithm
     * @param value package digest value
     * @param status package integrity
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String digest, String value, String status)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        LinkedHashMap<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("packageIntegrity", status);

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
    }

    public String getName() {
	return NAME;
    }

}
