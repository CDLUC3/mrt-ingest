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
 * Lock entry information
 * @author mreyes
 */
public class LockEntryState
        implements LockEntryStateInf, StateInf, Serializable
{

    private String id = null;
    private String date = null;
    private String jobID = null;
    private String name = null;

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

    public String toString()
    {

	String delimiter = "  ||  ";
        return "" +
	       "id: " + id + delimiter +
               "jobID: " + jobID + delimiter +
               "date: " + date + delimiter;
    }

    public String dump(String header)
    {

        return header  + "\n\n"
                + " - queue entry: " + id + "\n";
    }
}