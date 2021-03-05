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

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.lang.String;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.JobFileStateInf;
import org.cdlib.mrt.utility.StateInf;

/**
 * Job File State information
 * @author mreyes
 */
public class JobFileState
        implements JobFileStateInf, StateInf, Serializable

{

    private static final String NAME = "JobFileState";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;


    private Map<String, String> jobFile = new LinkedHashMap();


    /**
     * Get job file
     * @return Vector jobs
     */
    public Map<String, String> getJobFile() {
        return jobFile;
    }

    /**
     * Set job file
     * @param String job file
     */
    public void setJobFile(Map<String, String> jobFile) {
	this.jobFile = jobFile;
    }

    /**
     * Add entry into job file
     * @param String key
     * @param String value
     */
    public void addEntry(String key, String value)
    {
        if (key == null || value == null) return;
        jobFile.put(key, value);
    }


    public String dump(String header)
    {
        String outJobFile = "\n\n";
        outJobFile = outJobFile + jobFile.toString();

        return header  + "\n\n"
                + " JobFileState: " + outJobFile + "\n";
    }

}
