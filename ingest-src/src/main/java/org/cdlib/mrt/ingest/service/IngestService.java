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
*********************************************************************/
package org.cdlib.mrt.ingest.service;


import java.util.Map;

import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.BatchFileState;
import org.cdlib.mrt.ingest.IdentifierState;
import org.cdlib.mrt.ingest.IngestConfig;
import org.cdlib.mrt.ingest.JobFileState;
import org.cdlib.mrt.ingest.JobsState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.ProfilesState;
import org.cdlib.mrt.ingest.ProfilesFullState;
import org.cdlib.mrt.ingest.LockState;
import org.cdlib.mrt.ingest.QueueState;
import org.cdlib.mrt.ingest.QueueEntryState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.IngestLockNameState;
import org.cdlib.mrt.ingest.IngestQueueNameState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.GenericState;
import org.cdlib.mrt.ingest.ManifestsState;
import org.cdlib.mrt.utility.TException;

import org.json.JSONObject;

/**
 * IngestService
 * @author mreyes
 */
public class IngestService
        extends IngestServiceAbs
        implements IngestServiceInf
{
    protected static final String NAME = "IngestService";
    protected static final String MESSAGE = NAME + ": ";


    protected IngestService(
	    IngestConfig ingestConfig)
        throws TException
    {
        super(ingestConfig);
    }

    @Override
    public JobState submit (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    JobState jobState = ingestManager.submit(ingestRequest);
	    return jobState;
	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public IdentifierState requestIdentifier(IngestRequest ingestRequest)
        throws TException
    {
	try {
	    IdentifierState identifierState = ingestManager.requestIdentifier(ingestRequest);
	    return identifierState;

	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public BatchState submitPost (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    BatchState batchState = batchManager.submit(ingestRequest);
	    return batchState;
	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public BatchState submitBatch (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    BatchState batchState = queueManager.submit(ingestRequest);
	    return batchState;
	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public BatchState updatePost (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    BatchState batchState = ingestManager.updateStatus(ingestRequest.getJob().grabBatchID().getValue(), ingestRequest.getJob().getJobID().getValue(), "RESOLVED");
	    return batchState;
	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public IngestServiceState getServiceState()
        throws TException
    {
        return ingestManager.getServiceState();
    }

    @Override
    public IngestServiceState postSubmissionAction(String action, String collection)
        throws TException
    {
        return queueManager.postSubmissionAction(action, collection);
    }

    @Override
    public QueueEntryState postRequeue(String queue, String id, String fromState)
        throws TException
    {
        return queueManager.postRequeue(queue, id, fromState);
    }

    @Override
    public QueueEntryState postHoldRelease(String action, String queue, String id)
        throws TException
    {
        return queueManager.postHoldRelease(action, queue, id);
    }

    @Override
    public QueueEntryState postDeleteq(String queue, String id, String fromState)
        throws TException
    {
        return queueManager.postDeleteq(queue, id, fromState);
    }

    @Override
    public QueueState postCleanupq(String queue)
        throws TException
    {
        return queueManager.postCleanupq(queue);
    }

    @Override
    public QueueState postReleaseAll(String queue, String profile)
        throws TException
    {
        return queueManager.postReleaseAll(queue, profile);
    }

    @Override
    public GenericState postProfileAction(String type, String environment, String notification, 
	Map<String, String> profileParms)
        throws TException
    {
        return adminManager.postProfileAction(type, environment, notification, profileParms);
    }

    @Override
    public JobsState getStatus(String type)
        throws TException
    {
        return ingestManager.getStatus(type);
    }

    @Override
    public ProfileState getProfileState(String profile)
        throws TException
    {
        return adminManager.getProfileState(profile);
    }

    @Override
    public ProfilesState getProfilesState(String profilePath, boolean recurse)
        throws TException
    {
        return adminManager.getProfilesState(profilePath, recurse);
    }

    @Override
    public ProfilesFullState getProfilesFullState()
        throws TException
    {
        return adminManager.getProfilesFullState();
    }

    @Override
    public BatchFileState getQueueFileState(Integer batchAge)
        throws TException
    {
        return adminManager.getQueueFileState(batchAge);
    }

    @Override
    public BatchFileState getBatchFileState(String batchID)
        throws TException
    {
        return getBatchFileState(batchID, null);
    }

    @Override
    public BatchFileState getBatchFileState(String batchID, Integer batchAge)
        throws TException
    {
        return adminManager.getBatchFileState(batchID, batchAge);
    }

    @Override
    public JobFileState getJobFileState(String batchID, String jobID)
        throws TException
    {
        return adminManager.getJobFileState(batchID, jobID);
    }

    @Override
    public BatchFileState getJobViewState(String batchID, String jobID)
        throws TException
    {
        return adminManager.getJobViewState(batchID, jobID);
    }

    @Override
    public ManifestsState getJobManifestState(String batchID, String jobID)
        throws TException
    {
        return adminManager.getJobManifestState(batchID, jobID);
    }

    @Override
    public IngestLockNameState getIngestLockState()
        throws TException
    {
        return queueManager.getIngestLockState();
    }

    @Override
    public IngestQueueNameState getIngestQueueState()
        throws TException
    {
        return queueManager.getIngestQueueState();
    }

    @Override
    public IngestQueueNameState getAccessQueueState()
        throws TException
    {
        return queueManager.getAccessQueueState();
    }

    @Override
    public IngestQueueNameState getInventoryQueueState()
        throws TException
    {
        return queueManager.getInventoryQueueState();
    }

    @Override
    public QueueState getQueueState(String queue)
        throws TException
    {
        return queueManager.getQueueState(queue);
    }

    @Override
    public QueueState getAccessQueueState(String queue)
        throws TException
    {
        return queueManager.getAccessQueueState(queue);
    }

    @Override
    public QueueState getInventoryQueueState(String queue)
        throws TException
    {
        return queueManager.getInventoryQueueState(queue);
    }

    @Override
    public LockState getIngestLockState(String lock)
        throws TException
    {
        return queueManager.getIngestLockState(lock);
    }

    @Override
    public String getIngestServiceProp()
    {
        return ingestManager.getIngestServiceProp();
    }

    @Override
    public JSONObject getIngestServiceConf()
    {
        return ingestManager.getIngestServiceConf();
    }

    @Override
    public JSONObject getQueueServiceConf()
    {
        return queueManager.getQueueServiceConf();
    }
}
