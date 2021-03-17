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
import java.util.Vector;

import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.BatchFile;
import org.cdlib.mrt.ingest.BatchManifest;
import org.cdlib.mrt.ingest.BatchFileStateInf;
import org.cdlib.mrt.utility.StateInf;

/**
 * Batch File State information
 * @author mreyes
 */
public class BatchFileState
        implements BatchFileStateInf, StateInf, Serializable

{

    private static final String NAME = "BatchFileState";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;


    private Vector<BatchFile> batchFile = new Vector<BatchFile>();
    private BatchManifest batchManifest = new BatchManifest();


    /**
     * Get batch file
     * @return Vector jobs
     */
    public Vector<BatchFile> getJobFile() {
        return batchFile;
    }

    /**
     * Set batch files
     * @param String batch files
     */
    public void setBatchFile(Vector<BatchFile> bFile) {
	batchFile = bFile;
    }

    /**
     * Add a batch file
     * @param File batch file  to be added
     */
    public void addBatchFile(String bFile, String bFileDate)
    {
        if (bFile == null || bFileDate == null) return;
        BatchFile file = new BatchFile();
        file.setFile(bFile);
        file.setFileDate(bFileDate);
        batchFile.add(file);
    }

    /**
     * Get Batch manifest
     * @return String manifest
     */
    public BatchManifest getBatchManifest() {
        return batchManifest;
    }

    /**
     * Set Batch manifest name
     * @return String manifest
     */
    public void setBatchManifestName(String manifestName) {
        batchManifest.setManifest(manifestName);
    }

    /**
     * Set Batch manifest data
     * @return String manifest
     */
    public void setBatchManifestData(String manifestContent) {
        batchManifest.setContent(manifestContent);
    }

    public String dump(String header)
    {
        // gather profile names
        String outBatchFile = "\n\n";
        Iterator<BatchFile> iterator = batchFile.iterator();
        while(iterator.hasNext()) {
            BatchFile bfile = iterator.next();
            outBatchFile = outBatchFile + bfile.getFile() + "\n";
        }
        outBatchFile = outBatchFile.substring(1, outBatchFile.length() - 1 );

        return header  + "\n\n"
                + " BatchFileState: " + outBatchFile + "\n";
    }

}
