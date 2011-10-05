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
package org.cdlib.mrt.ingest;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.StateInf;

/**
 * Job State information
 * @author mreyes
 */
public interface JobStateInf
        extends StateInf
{
    /**
     * Get job identifier
     * @return Identifier job identifier
     */
    public Identifier getJobID();

    /**
     * Get batch identifier
     * @return Identifier batch identifier
     */
    public Identifier getBatchID();

    /**
     * Get target storage service
     * @return StoreNode storage service endpoint
     */
    public StoreNode getTargetStorage();

    /**
     * Get object profile
     * @return ProfileState object profile
     */
    public ProfileState getObjectProfile();

    /**
     * Get user agent
     * @return String submitting user agent
     */
    public String getUserAgent();

    /**
     * Get job submission date-time 
     * @return DateState submission time
     */
    public DateState getSubmissionDate();

    /**
     * Get job consumption date-time 
     * @return DateState submission time
     */
    public DateState getConsumptionDate();

    /**
     * Get job completion date-time 
     * @return DateState completion time
     */
    public DateState getCompletionDate();

    /**
     * Get job status
     * @return JobStatus status
     */
    public JobStatusEnum getJobStatus();

    /**
     * Get job status message
     * @return String status message
     */
    public String getJobStatusMessage();

    /**
     * Get object state
     * @return Object state
     */
    public String getObjectState();

    /**
     * Get object version ID
     * @return Integer version identifier
     */
    public Integer getVersionID();

    /**
     * Get primary ID
     * @return Identifier primary identifier
     */
    public Identifier getPrimaryID();

    /**
     * Get local ID
     * @return Identifier local identifier
     */
    public Identifier getLocalID();

    /**
     * Get object creator
     * @return String creator
     */
    public String getObjectCreator();

    /**
     * Get object title
     * @return String title
     */
    public String getObjectTitle();

    /**
     * Get object date
     * @return String date
     */
    public String getObjectDate();

    /**
     * Get object expository note
     * @return String note
     */
    public String getObjectNote();

    /**
     * Get job queue priority (00-99, 00=highest)
     * @return String priority
     */
    public String getQueuePriority();
}
