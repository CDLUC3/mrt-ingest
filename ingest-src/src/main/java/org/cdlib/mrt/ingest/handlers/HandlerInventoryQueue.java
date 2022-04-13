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

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.DistributedQueue.Ignorer;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.ZooCodeUtil;

public class HandlerInventoryQueue extends Handler<JobState> {
    private ZooKeeper zooKeeper;
    private DistributedQueue distributedQueue;
    private Properties prop = null;
    
    public void submit (byte[] bytes) throws KeeperException, InterruptedException {
        int retryCount = 0;
        while (true) {
            try {
                distributedQueue.submit(bytes);
                return;
            } catch (KeeperException.ConnectionLossException ex) {
                if (retryCount >= 3) throw ex;
                retryCount++;
            } catch (KeeperException.SessionExpiredException ex) {
                if (retryCount >= 3) throw ex;
                retryCount++;
            } catch (InterruptedException ex) {
                if (retryCount >= 3) throw ex;
                retryCount++;
            }
        }
    }

    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, 
                                JobState jobState) throws TException {
        try {
	    // MySQL (full schema)
            zooKeeper = 
                new ZooKeeper(jobState.grabMisc(), DistributedQueue.sessionTimeout, new Ignorer());
            distributedQueue = 
                new DistributedQueue(zooKeeper, jobState.grabExtra(), null);
	    prop = getInventoryProps(profileState, jobState);
	    System.out.println("HandlerInventoryQueue submitting properties [MySQL]: " + prop.toString());
            submit(ZooCodeUtil.encodeItem(prop));
            String msg = String.format("SUCCESS: %s completed successfully", getName());

	
            return new HandlerResult(true, msg, 0);
        } catch (IOException ex) {
            String msg = 
                String.format("WARNING: %s could not connect to Zookeeper", getName());
            return new HandlerResult(true, msg, 0);
        } catch (KeeperException ex) {
            String msg = 
                String.format("WARNING: %s %s.", getName(), ex);
            return new HandlerResult(true, msg, 0);
        } catch (InterruptedException ex) {
            String msg = 
                String.format("WARNING: %s %s.", getName(), ex);
            return new HandlerResult(true, msg, 0);
        //} catch (Exception ex) {
            //String msg = 
                //String.format("WARNING: %s %s.", getName(), ex);
            //return new HandlerResult(true, msg, 0);
        } finally {
	    prop = null;
            try { zooKeeper.close(); }
            catch (Exception ex) {}
        }
    }

    private Properties getInventoryProps(ProfileState profileState, JobState jobState)
    {
        Properties prop = new Properties();

        String manifestURL = jobState.grabObjectState().replace("/state/", "/manifest/");
        prop.setProperty("manifestURL", manifestURL);

        return prop;
    }

    public String getName() {
	return HandlerInventoryQueue.class.toString();
    }
}
