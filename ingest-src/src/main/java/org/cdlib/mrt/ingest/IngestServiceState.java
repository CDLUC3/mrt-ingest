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
import java.util.List;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.StorageURL;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author mreyes
 */
public class IngestServiceState
        implements IngestServiceStateInf, StateInf
{
    private static final String NAME = "IngestServiceState";
    private static final String MESSAGE = NAME + ": ";

    private String serviceName = null;
    private String serviceID = null;
    private String targetID = null;
    private String serviceVersion = null;
    private String serviceCustomerSupport = null;
    private Vector<StorageURL> storageInstances = new Vector<StorageURL>(10);
    private String queueInstance = null;
    private DateState creationDateTime = null;
    private DateState modificationDateTime = null;
    private DateState lastIngestDateTime = null;
    private URL accessServiceURL;
    private URL supportServiceURL;
    private String commands = null;
    private String mailHost = null;

    public IngestServiceState()
            throws TException
    {
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Set Storage Service Name
     * @param String storage Service Name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String getServiceID() {
        return serviceID;
    }

    /**
     * Set Storage Service identifier
     * @param String Storage Service identifier
     */
    public void setServiceID(String serviceID) {
        this.serviceID = serviceID;
    }

    @Override
    public String getTargetID() {
        return targetID;
    }

    /**
     * Set Target identifier (used for permalink w/ EZID)
     * @param String Target identifier
     */
    public void setTargetID(String targetID) {
        this.targetID = targetID;
    }

    @Override
    public String getServiceVersion() {
        return serviceVersion;
    }

    /**
     * Set Storage Service version
     * @param String storage Service version
     */
    public void setServiceVersion(String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    @Override
    public String getServiceCustomerSupport() {
        return serviceCustomerSupport;
    }

    /**
     * Set contact information
     * @param String contact information
     */
    public void setServiceCustomerSupport(String serviceCustomerSupport) {
        this.serviceCustomerSupport = serviceCustomerSupport;
    }

    @Override
    public Vector<StorageURL> getStorageInstances() {
        return storageInstances;
    }

    /**
     * Add a storage instance to list
     * @param URL storage instance to be added
     */
    public void addStorageInstance(URL storageInstance)
    {
        if (storageInstance == null) return;
	StorageURL urlWrapper = new StorageURL();
	urlWrapper.setURL(storageInstance);
        storageInstances.add(urlWrapper);
    }

    public String getQueueInstance() {
        return this.queueInstance;
    }

    /**
     * Add a queue instance
     * @param String queue instance connection string
     */
    public void addQueueInstance(String queueConnectionString)
    {
        this.queueInstance = queueConnectionString;
    }

    @Override
    public DateState getCreationDateTime() {
        return creationDateTime;
    }

    /**
     * Set creation date-time for this instance
     * Corresponds to the date-time of the instance directory file
     * @param DateState creation date-time for this instance
     */
    public void setCreationDateTime(DateState creationDateTime) {
        this.creationDateTime = creationDateTime;
    }

    @Override
    public DateState getModificationDateTime() {
        return modificationDateTime;
    }

    /**
     * Set modification date
     * @param DateState modification date-time for this instance
     */
    public void setModificationDateTime(DateState modificationDateTime) {
        this.modificationDateTime = modificationDateTime;
    }

    @Override
    public DateState getLastIngestDateTime() {
        return lastIngestDateTime;
    }

    /**
     * Set last ingest date-time for this ingest
     * @param DateState last ingest date-time for this instance
     */
    public void setLastIngestDateTime(DateState lastIngestDateTime) {
        this.lastIngestDateTime = lastIngestDateTime;
    }

    /**
     * Get access service URL
     * @return URL access endpoint
     */
    public URL getAccessServiceURL() {
        return accessServiceURL;
    }

    /**
     * Set access service URL
     * @param URL access service URL
     */
    public void setAccessServiceURL(URL accessServiceURL) {
        this.accessServiceURL = accessServiceURL;
    }

    /**
     * Get support service URL
     * @return URL support endpoint
     */
    public URL getSupportServiceURL() {
        return supportServiceURL;
    }

    /**
     * Set support service URL
     * @param URL support service URL
     */
    public void setSupportServiceURL(URL supportServiceURL) {
        this.supportServiceURL = supportServiceURL;
    }

    @Override
    public String getCommands() {
        return commands;
    }

    /**
     * Set available command string
     * @param String available commands
     */
    public void setCommands(String commands) {
        this.commands = commands;
    }

    @Override
    public String getMailHost() {
        return mailHost;
    }

    /**
     * Set Storage Service identifier
     * @param String Storage Service identifier
     */
    public void setMailHost(String mailHost) {
        this.mailHost = mailHost;
    }

    /**
     * Compare two DateStates and return most recent
     * @param DateState first date
     * @param DateState second date
     * @return DateState most recent date
     */
    protected DateState maxTime(DateState a, DateState b)
    {
        if ((a == null) && (b == null)) return null;
        if ((a == null) && (b != null)) return b;
        if ((a != null) && (b == null)) return a;
        long al = a.getTimeLong();
        long bl = b.getTimeLong();
        if (al > bl) return a;
        return b;
    }

    public String dump(String header)
    {
        return header + ":"
                    + " - serviceName=" + getServiceName()
                    + " - serviceID=" + getServiceID()
                    + " - serviceVersion=" + getServiceVersion()
                    + " - serviceCustomerSupport=" + getServiceCustomerSupport()
                    + " - creationDateTime=" + getCreationDateTime()
                    + " - modificationDateTime=" + getModificationDateTime()
                    + " - lastIngestDateTime=" + getLastIngestDateTime()
                    + " - accessServiceURL=" + getAccessServiceURL().toString()
                    + " - supportServiceURL=" + getSupportServiceURL().toString()
                    ;
    }
}

