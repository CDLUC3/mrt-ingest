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


import java.lang.String;
import org.cdlib.mrt.utility.StateInf;

/**
 * Simple Manifest wrapper needed for formatter
 * @author mreyes
 */
public class BatchManifest
        implements StateInf
{

    protected String batchManifest = null;
    protected String manifestData = null;

    // constructor
    BatchManifest() {
    }


    /**
     * Get Manifest
     * @param String file
     */
    public String getManifest()
    {
        return batchManifest;
    }

    /**
     * Set manifest
     * @param String file
     */
    public void setManifest(String file) {
        this.batchManifest = file;
    }

    /**
     * Get Manifest data
     * @param String content
     */
    public String getContent()
    {
        return manifestData;
    }

    /**
     * Set manifest data
     * @param String content
     */
    public void setContent(String content) {
        this.manifestData = content;
    }


    public String dump(String header)
    {
        return header
                + " - file=" + getManifest();
    }
}
