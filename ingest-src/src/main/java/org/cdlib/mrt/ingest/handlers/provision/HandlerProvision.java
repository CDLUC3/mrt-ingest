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
package org.cdlib.mrt.ingest.handlers.provision;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.ThreadContext;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * provision resources
 * @author mreyes
 */
public class HandlerProvision extends Handler<JobState>
{

    private static final String NAME = "HandlerProvision";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private static long SLEEP = (5L * 60L * 1000L);

    /**
     * provision resources for request
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

	    Integer zfsThreshold = new Integer(ingestRequest.getIngestZfsThreshold());
	    if (zfsThreshold == null) {
	        if (DEBUG) System.out.println("[WARN] " + MESSAGE + "ZFS Threshold not defined.  Provisioning not supported.");
	        return new HandlerResult(true, "SUCCESS: " + NAME + " provisioning not supported", 0);
	    }
	    String ingestQueuePath = ingestRequest.getIngestQueuePath();
	    File queueFile = new File(ingestQueuePath);
	    Long capacitySpace = queueFile.getTotalSpace();
	    if (DEBUG) System.out.println("[INFO] " + MESSAGE + "Found estimate size of: " + jobState.grabSubmissionSize());

	    Long freeSpace = queueFile.getFreeSpace() - jobState.grabSubmissionSize();
            Double usage = 100D - ((freeSpace.doubleValue() / capacitySpace.doubleValue()) * 100D);
	    if (usage.intValue() > zfsThreshold ) {
                ThreadContext.put("Provisioning threshold exceeded ", jobState.grabBatchID().getValue());

		while (usage.intValue() > zfsThreshold) {
	            if (DEBUG) System.out.println("[WARN] " + MESSAGE + "ZFS usage exceeds Threshold.  Looping until resolved.");
	            if (DEBUG) System.out.println("[WARN] " + MESSAGE + "Threshold: " + zfsThreshold + " --- Usage: " + usage.intValue());

                    Thread.sleep(SLEEP);
                    queueFile = new File(ingestQueuePath);
	            capacitySpace = queueFile.getTotalSpace();
	            freeSpace = queueFile.getFreeSpace() - jobState.grabSubmissionSize();
                    usage = 100D - ((freeSpace.doubleValue() / capacitySpace.doubleValue()) * 100D);
		}
	    } else { 
	        if (DEBUG) System.out.println("[INFO] " + MESSAGE + "ZFS usage within threshold boundary.");
	        if (DEBUG) System.out.println("[INFO] " + MESSAGE + "Threshold: " + zfsThreshold + " --- Usage: " + usage.intValue());
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " provisioning complete", 0);
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "provisioning failure: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
            ThreadContext.clearMap();
	    // cleanup?
	}
    }
   
    public String getName() {
	return NAME;
    }

}
