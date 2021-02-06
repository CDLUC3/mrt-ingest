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
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.IdentifierState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.JobsState;
import org.cdlib.mrt.ingest.QueueState;
import org.cdlib.mrt.ingest.ProfilesState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.json.JSONObject;

/**
 * This interface defines the functional API for a Curational Ingest Service
 * @author mreyes
 */
public interface IngestServiceInf
{
    /**
     * Add an object to this storage service
     * @param ingestRequest request
     * @return JobState job state information 
     * @throws TException Exception condition during storage service procssing
     */
    public JobState submit (IngestRequest ingestRequest)
    throws TException;

    /**
     * Add an object to this queue service
     * @param ingestRequest request
     * @return BatchState batch state information 
     * @throws TException Exception condition during storage service procssing
     */
    public BatchState submitPost (IngestRequest ingestRequest)
    throws TException;

    /**
     * Update a object status
     * @param ingestRequest request
     * @return BatchState batch state information 
     * @throws TException Exception condition during storage service procssing
     */
    public BatchState updatePost (IngestRequest ingestRequest)
    throws TException;

    /**
     * Request identifier
     * @param ingestRequest request
     * @return IdentifierState information 
     * @throws TException Exception condition during storage service procssing
     */
    public IdentifierState requestIdentifier (IngestRequest ingestRequest)
    throws TException;

    /**
    /**
     * Get state information about this Storage Service
     * @return IngestServiceState service state information
     * @throws TException Exception condition during storage service procssing
     */
    public IngestServiceState getServiceState()
        throws TException;

    /**
     * Get jobs state information about processed jobs
     * @param type status type
     * @return batchesState batches state information
     * @throws TException Exception condition during storage service procssing
     */
    public JobsState getStatus(String type)
        throws TException;

    /**
     * Get queue state information 
     * @return QueueState state information
     * @throws TException Exception condition during queue service processing
     */
    public QueueState getQueueState()
        throws TException;

    /**
     * Get profiles state information 
     * @return ProfilesState state information
     * @throws TException Exception condition during queue service processing
     */
    public ProfilesState getProfilesState()
        throws TException;

    /**
     * Get ingest home
     * @return IngestService string
     * @throws TException Exception condition during storage service procssing
     */
    public String getIngestServiceProp()
        throws TException;

    /**
     * Get all ingest properties
     * @return IngestService properties
     * @throws TException Exception condition during storage service procssing
     */
    public JSONObject getIngestServiceConf()
        throws TException;

    /**
     * Get all queue properties
     * @return IngestService properties
     * @throws TException Exception condition during storage service procssing
     */
    public JSONObject getQueueServiceConf()
        throws TException;

    /**
     * get logger used for this Ingest Service
     * @return LoggerInf file logger for service
     */
    public LoggerInf getLogger();

}

