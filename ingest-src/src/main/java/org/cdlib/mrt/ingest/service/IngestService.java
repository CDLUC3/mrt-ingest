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
import org.cdlib.mrt.ingest.IdentifierState;
import org.cdlib.mrt.ingest.IngestConfig;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.ProfilesState;
import org.cdlib.mrt.ingest.JobFileState;
import org.cdlib.mrt.ingest.BatchFileState;
import org.cdlib.mrt.ingest.ManifestsState;
import org.cdlib.mrt.ingest.LockState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.IngestLockNameState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.GenericState;
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
    public BatchState submitPost (IngestRequest ingestRequest, String state)
        throws TException
    {
	try {
	    BatchState batchState = batchManager.submit(ingestRequest, state);
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
    public JobState submitProcess (IngestRequest ingestRequest, String state)
        throws TException
    {
	try {
	    JobState jobState = processManager.submit(ingestRequest, state);
	    return jobState;
	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

/*
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
*/

    @Override
    public IdentifierState requestIdentifier(IngestRequest ingestRequest)
        throws TException
    {
	try {
	    IdentifierState identifierState = processManager.requestIdentifier(ingestRequest);
	    return identifierState;

	} catch (TException te) {
	    te.printStackTrace();
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

/*
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
*/

    @Override
    public IngestServiceState getServiceState()
        throws TException
    {
        return processManager.getServiceState();
    }

    @Override
    public String getIngestServiceProp()
    {
        return processManager.getIngestServiceProp();
    }

    @Override
    public JSONObject getIngestServiceConf()
    {
        return processManager.getIngestServiceConf();
    }

    @Override
    public JSONObject getQueueServiceConf()
    {
        return queueManager.getQueueServiceConf();
    }

    @Override
    public JobFileState getJobFileState(String batchID, String jobID)
        throws TException
    {
        return adminManager.getJobFileState(batchID, jobID);
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



}
