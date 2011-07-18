/*
Copyright (c) 2005-2010, Regents of the University of California
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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

/**
 * move package to staging area "producer" directory
 * @author mreyes
 */
public class HandlerAccept extends Handler<JobState>
{

    protected static final String NAME = "HandlerAccept";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");
    protected LoggerInf logger = null;
    protected Properties conf = null;

    /**
     * copy package to staging area
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

	    boolean result;
	    File sourceDir = ingestRequest.getQueuePath();
	    File targetDir = new File(ingestRequest.getQueuePath(), "producer");
	    for (String fileS : ingestRequest.getQueuePath().list()) {
	    	if ( ! isComponent(fileS)) continue;

	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "copying file to stage dir: " + fileS);
		File sourceFile = new File(sourceDir, fileS);
		File targetFile = new File(targetDir, fileS);
		if (sourceFile.isHidden()) continue;		// process after disaggregate
		FileUtil.copyFile(sourceFile.getName(), sourceDir, targetDir);

		sourceFile.delete();
		if (! targetFile.exists()) {
	            if (DEBUG) System.out.println("[error] " + MESSAGE + "unable to copying file to stage dir: " + fileS);
	    	    // return new HandlerResult(false, "[error]: " + MESSAGE + "unable to copying file to stage dir: " + fileS);
		    throw new TException.REQUESTED_ITEM_NOT_FOUND("[error] " 
			+ MESSAGE + ": unable to copying file to stage dir: " + fileS);
		}
	    }

	    // process update deletions
	    if (jobState.getUpdateFlag()) {
                // process deletions
                File sourceDelete = new File(ingestRequest.getQueuePath() + FS + "producer" + FS + "mrt-delete.txt");
                if (sourceDelete.exists()) {
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + " Found deletion file, moving into system dir");
                    File targetDelete = new File(ingestRequest.getQueuePath() + FS + "system" + FS + "mrt-delete.txt");
                    sourceDelete.renameTo(targetDelete);
                }
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " has copied data to staging area", 0);
	} catch (TException te) {
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
	    String msg = "[error] " + MESSAGE + "copying file to staging directory: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    // cleanup?
	}
    }
   
    public boolean isComponent(String file) {
	if (file.equals("producer")) return false;
	if (file.equals("system")) return false;
	return true;
    }

    public String getName() {
	return NAME;
    }

}
