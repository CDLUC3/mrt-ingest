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

import java.util.Date;
import java.util.Iterator;
import java.util.Vector;
import java.io.Serializable;
import java.net.URL;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Queue entry information
 * @author mreyes
 */
public class QueueEntryState
        implements QueueEntryStateInf, StateInf, Serializable
{

    private String id = null;
    private String status = null;
    private String date = null;
    private String batchID = null;
    private String jobID = null;
    private String name = null;
    private String user = null;
    private String profile = null;
    private String objectCreator = null;
    private String objectTitle = null;
    private String objectDate = null;
    private String localID = null;
    private String fileType = null;
    private String queueNode = null;

    /**
     * Set entry ID
     * @param String entry ID
     */
    public void setID(String id) {
        this.id = id;
    }

    /**
     * Get entry ID
     * @return String entry ID
     */
    public String getID() {
        return this.id;
    }

    /**
     * Set entry status
     * @param String entry status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Get entry status
     * @return String entry status
     */
    public String getStatus() {
        return this.status;
    }

    /**
     * Set entry date
     * @param String entry date
     */
    public void setDate(String date) {
        this.date = date;
    }

    /**
     * Get entry date
     * @return String entry date
     */
    public String getDate() {
        return this.date;
    }

    /**
     * Set batch ID
     * @param String batch id
     */
    public void setBatchID(String batchID) {
        this.batchID = batchID;
    }

    /**
     * Get batch ID
     * @return String batch ID
     */
    public String getBatchID() {
        return this.batchID;
    }

    /**
     * Set job ID
     * @param String job id
     */
    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    /**
     * Get job ID
     * @return String job ID
     */
    public String getJobID() {
        return this.jobID;
    }

    /**
     * Set filename
     * @param String filename
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get filename
     * @return String filename
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set username
     * @param String user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get username
     * @return String username
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Set profile
     * @param String profile
     */
    public void setProfile(String profile) {
        this.profile = profile;
    }

    /**
     * Get profile
     * @return String profile
     */
    public String getProfile() {
        return this.profile;
    }

    /**
     * Set object creator
     * @param String creator
     */
    public void setObjectCreator(String creator) {
        this.objectCreator = creator;
    }

    /**
     * Get object creator
     * @return String creator
     */
    public String getObjectCreator() {
        return this.objectCreator;
    }

    /**
     * Set object title
     * @param String title
     */
    public void setObjectTitle(String title) {
        this.objectTitle = title;
    }

    /**
     * Get object title
     * @return String title
     */
    public String getObjectTitle() {
        return this.objectTitle;
    }

    /**
     * Set object date
     * @param String date
     */
    public void setObjectDate(String date) {
        this.objectDate = date;
    }

    /**
     * Get object date
     * @return String date
     */
    public String getObjectDate() {
        return this.objectDate;
    }

    /**
     * Set local ID
     * @param String localID
     */
    public void setLocalID(String localID) {
        this.localID = localID;
    }

    /**
     * Get local ID
     * @return String localID
     */
    public String getLocalID() {
        return this.localID;
    }

    /**
     * Set File Type
     * @param String file type
     */
    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    /**
     * Get file type
     * @return String file type
     */
    public String getFileType() {
        return this.fileType;
    }

    /**
     * Set Queue Node
     * @param String queue node
     */
    public void setQueueNode(String queueNode) {
        this.queueNode = queueNode;
    }

    /**
     * Get queue node
     * @return String file type
     */
    public String getQueueNode() {
        return this.queueNode;
    }

    public String toString()
    {

	String delimiter = "  ||  ";
        return "" +
	       "id: " + id + delimiter +
               "status: " + status + delimiter +
               "batchID: " + batchID + delimiter +
               "jobID: " + jobID + delimiter +
               "fileName: " + name + delimiter +
               "objectCreator: " + objectCreator + delimiter +
               "objectTitle: " + objectTitle + delimiter +
               "objectDate: " + objectDate + delimiter +
               "localID: " + localID + delimiter +
               "user: " + user + delimiter +
               "profile: " + profile + delimiter +
               "date: " + date + delimiter;
    }

    public String dump(String header)
    {

        return header  + "\n\n"
                + " - queue entry: " + id + "\n";
    }
}
