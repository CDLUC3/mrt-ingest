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

import java.net.URL;
import java.util.Vector;
import org.cdlib.mrt.core.DateState;

/**
 *
 * @author mreyes
 */
public interface IngestServiceStateInf
{

    /**
     * Get Ingest Service Name
     * @return String ingest Service Name
     */
    public String getServiceName();

    /**
     * Get Ingest Service identifier
     * @return String ingest Service identifier
     */
    public String getServiceID();

    /**
     * Get Ingest Target identifier
     * @return String ingest Target identifier
     */
    public String getTargetID();

    /**
     * Get Service Version
     * @return String service Version
     */
    public String getServiceVersion();

    /**
     * Get customer support contact information
     * @return String contact information
     */
    public String getServiceCustomerSupport();

    /**
     * Get known storage service instances
     * @return Vector<StorageURL> storage instances
     */
    public Vector<StorageURL> getStorageInstances();

    /**
     * Get creation date-time for this instance
     * Corresponds to the date-time of the ingest directory file
     * @return DateState creation date-time for this ingest instance
     */
    public DateState getCreationDateTime();

    /**
     * Get modification date-time for this instance
     * Corresponds to the date-time of the ingest directory file
     * @return DateState modification date-time for this ingest instance
     */
    public DateState getModificationDateTime();

    /**
     * Get service start time
     * Corresponds to the date-time when JVM is started
     * @return DateState service start time
     */
    public DateState getServiceStartTime();

    /**
     * Last Ingest date-time to service
     * Not technically feasible
     * @return DateState Ingest date-time
     */
    public DateState getLastIngestDateTime();

    /**
     * Get service access URL
     * @return URL access URL
     */
    public URL getAccessServiceURL();

    /**
     * Get service support URL
     * @return URL support URL
     */
    public URL getSupportServiceURL();

    /**
     * Get submission state
     * @return String submission state frozen|thawed
     */
    public String getSubmissionState();

    /**
     * Get collection submission state
     * @return String collection submission state - frozen only
     */
    public String getCollectionSubmissionState();

    /**
     * Commands supported by service
     * @return String of supported commands
     */
    public String getCommands();

    /**
     * Dump of Service State dump information
     * @param header dump header
     * @return String service State dump
     */
    public String dump(String header);

    /**
     * Get smtp host name
     * @return smtp host name
     */
	public String getMailHost();

}

