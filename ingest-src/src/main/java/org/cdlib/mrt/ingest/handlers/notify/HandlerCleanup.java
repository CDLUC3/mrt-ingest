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
package org.cdlib.mrt.ingest.handlers.notify;

import java.io.File;
import java.util.Properties;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.ingest.utility.ZookeeperUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
 
import org.cdlib.mrt.zk.MerrittLocks;

/**
 * remove staging directory
 * @author mreyes
 */
public class HandlerCleanup extends Handler<JobState>
{

    private static final String NAME = "HandlerCleanup";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private ZooKeeper zooKeeper = null;
    private String zooConnectString = null;
    private LoggerInf logger = null;
    private Properties conf = null;
    private boolean unitTest = false;
    private String recycleBinName = "RecycleBin";
    private boolean deletePayload = false;

    /**
     * remove staging area
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

        zooConnectString = jobState.grabMisc();
	if (zooConnectString == null) unitTest = true;
	try {

            if ( ! unitTest) zooKeeper = new ZooKeeper(zooConnectString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
	    File stageDir = new File(ingestRequest.getQueuePath(), "producer");

	    if (! deletePayload) {
	        File recycleBin = new File(ingestRequest.getQueuePath().getParentFile().getParentFile(), recycleBinName);
	        if (! recycleBin.exists()) {
	           if (DEBUG) System.out.println("[debug] " + MESSAGE + "Creating recycle bin directory: " + recycleBin.getAbsolutePath());
	           try {
	              recycleBin.mkdir();
	           } catch (Exception e) {}
	        }


	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "moving staging directory: " + stageDir.getAbsolutePath());
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "target directory: " + recycleBin.getAbsolutePath() + "/" + jobState.getJobID().getValue());
	        stageDir.renameTo(new File(recycleBin.getAbsolutePath(), jobState.getJobID().getValue()));
	        return new HandlerResult(true, "SUCCESS: " + NAME + " moving of staging directory", 0);
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "removing staging directory: " + stageDir.getAbsolutePath());

	        boolean deleteDir = FileUtil.deleteDir(stageDir);
	        if (! deleteDir) {
		    // NFS open files are renamed (See section D2. of http://nfs.sourceforge.net)
                    if (DEBUG) System.out.println("[error] " + MESSAGE + "Failure in removing: " 
		        + stageDir.getAbsolutePath() + "   Continuing.");
	        }
	        return new HandlerResult(true, "SUCCESS: " + NAME + " deletion of staging directory", 0);
	    }
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "removing staging directory: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    // Initiated with Storage Handler.  Keep lock until object completes
	    try {
                if ( ! unitTest) {
            	    System.out.println("[debug] " + MESSAGE + " Releasing Zookeeper Storage lock: " + this.zooKeeper.toString());
		    releaseLock(zooKeeper, jobState.getPrimaryID().getValue());
		    zooKeeper.close();
		}
	    } catch (Exception e) {}
	}
    }
   
    public String getName() {
	return NAME;
    }

    /**
     * Release lock
     *
     * @param none needed inputs are global
     * @return void
     */
    private void releaseLock(ZooKeeper zooKeeper, String primaryID) {

        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(zooConnectString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            } catch  (Exception e ) {
               e.printStackTrace(System.err);
            }
        }

        try {
            MerrittLocks.unlockObjectStorage(zooKeeper, primaryID);
        } catch (KeeperException ke) {
            try {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(zooConnectString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               MerrittLocks.unlockObjectStorage(zooKeeper, primaryID);
            } catch (Exception ee) {}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
            } catch (Exception ze) {}
        }

    }

   public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
   }


}
