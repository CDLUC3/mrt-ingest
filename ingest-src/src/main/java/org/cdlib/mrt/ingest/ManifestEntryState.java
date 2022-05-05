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

import java.io.Serializable;
import org.cdlib.mrt.utility.StateInf;

/**
 * Manifest entry information
 * @author mreyes
 */
public class ManifestEntryState
        implements ManifestEntryStateInf, StateInf, Serializable
{

    // nfo:fileURL | nfo:hashAlgorithm | nfo:hashValue | nfo:fileSize | nfo:fileLastModified | nfo:fileName | nie:mimeType 
    private String fileName = null;
    private String fileSize = null;
    private String hashAlgorithm = null;
    private String hashValue = null;
    private String mimeType = null;

    /**
     * Set file name
     * @param String name
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Get file name
     * @return String file name
     */
    public String getFileName() {
        return this.fileName;
    }

    /**
     * Set file size
     * @param String file size
     */
    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * Get file size
     * @return String file size
     */
    public String getFileSize() {
        return this.fileSize;
    }

    /**
     * Set hash algorithm
     * @return String hash algorithm
     */
    public void setHashAlgorithm(String hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    /**
     * Get hash algorithm
     * @return String hash algorithm
     */
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }

    /**
     * Set hash value
     * @return String hash value
     */
    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    /**
     * Get hash value
     * @return String hash value
     */
    public String getHashValue() {
        return this.hashValue;
    }

    /**
     * Set mime type
     * @return String mime type
     */
    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    /**
     * Get mime type
     * @return String mime type
     */
    public String getMimeType() {
        return this.mimeType;
    }


    public String dump(String header)
    {

        return header  + "\n\n"
                + " - file: " + fileName + "\n";
    }
}
