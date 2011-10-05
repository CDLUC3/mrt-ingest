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


import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.JobsState;
import org.cdlib.mrt.ingest.QueueState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

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
            LoggerInf logger,
            Properties confProp)
        throws TException
    {
        super(logger, confProp);
    }

    @Override
    public JobState submit (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    JobState jobState = ingestManager.submit(ingestRequest);
	    return jobState;
	} catch (TException te) {
	    throw te;
	} catch (Exception e) {
	    throw new TException.GENERAL_EXCEPTION(NAME + ": " + e.getMessage());
	}
    }

    @Override
    public BatchState submitPost (IngestRequest ingestRequest)
        throws TException
    {
	try {
	    BatchState batchState = queueManager.submit(ingestRequest);
	    return batchState;
	} catch (TException te) {
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
	    BatchState batchState = ingestManager.updateStatus(ingestRequest.getJob().getBatchID().getValue(), ingestRequest.getJob().getJobID().getValue(), "RESOLVED");
	    return batchState;
	} catch (TException te) {
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
    public JobsState getStatus(String type)
        throws TException
    {
        return ingestManager.getStatus(type);
    }

    @Override
    public QueueState getQueueState()
        throws TException
    {
        return queueManager.getQueueState();
    }

    @Override
    public String getIngestServiceProp()
    {
        return ingestManager.getIngestServiceProp();
    }

    @Override
    public Properties getIngestServiceProps()
    {
        return ingestManager.getIngestServiceProps();
    }

    @Override
    public Properties getQueueServiceProps()
    {
        return queueManager.getQueueServiceProps();
    }
}

