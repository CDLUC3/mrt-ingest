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

import org.cdlib.mrt.dataone.map.FixEML;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Alter producer created mrt-eml.xml file (remove when Excel plugin is fixed)
 * @author mreyes
 */
public class HandlerDataupHack extends Handler<JobState>
{

    private static final String NAME = "HandlerDataupHack";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;

    /**
     * Fix EML file if needed
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

	    File stageDir = new File(ingestRequest.getQueuePath(), "producer");
	    File emlFile = new File(stageDir, "mrt-eml.xml");
	    if (! emlFile.exists()) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "No EML data to process. returning... ");
	        return new HandlerResult(true, "SUCCESS: " + NAME + " No EML data found", 0);
	    }

	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Found EML file: " + emlFile.getName());
	    String emlString = FileUtil.file2String(emlFile);

	    String newEmlString = FixEML.fix(emlString);
	    FileUtil.string2File(emlFile, newEmlString);

	    return new HandlerResult(true, "SUCCESS: " + NAME + "Fixed EML data ", 0);
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "Could not fix EML data file: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    // cleanup?
	}
    }
   
    public String getName() {
	return NAME;
    }

}
