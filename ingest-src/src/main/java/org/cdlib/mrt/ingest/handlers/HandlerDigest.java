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
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.core.ManifestBuild;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * create manifest based on current queue path
 * @author mreyes
 */
public class HandlerDigest extends Handler<JobState>
{

    private static final String NAME = "HandlerDigest";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private Integer defaultStorage = null;

    /**
     * create manifest for all queue data
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState 
     * @return HandlerResult result in creating manifest
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {

	    File manifest = new File(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
	    manifest.createNewFile();

	    // requires symlink from webapps/ingestqueue to home ingest queue directory
	    URL link = new URL(ingestRequest.getLink());	
	    String baseURL = link.getProtocol() + "://" + link.getHost() + ":" + link.getPort() + 
		 "/ingestqueue/" + ingestRequest.getJob().grabBatchID().getValue() + "/" + ingestRequest.getQueuePath().getName();

	    if (DEBUG) System.out.println("[debug] " + MESSAGE + " baseURL: " + baseURL);

	    // build manifest
	    ManifestBuild.getPostManifest(baseURL, ingestRequest.getQueuePath(), manifest);

	    FileUtil.removeLineFromFile(manifest.getAbsolutePath(), "mrt-manifest.txt", "END");

	    return new HandlerResult(true, "SUCCESS: " + NAME + " created manifest");
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "creating manifest: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }

    }
   
    public String getName() {
	return NAME;
    }

}
