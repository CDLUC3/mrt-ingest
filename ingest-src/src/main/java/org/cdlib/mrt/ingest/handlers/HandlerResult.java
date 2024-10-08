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
package org.cdlib.mrt.ingest.handlers;

/**
 * Result of handler processing
 */
public class HandlerResult
{
    private boolean success = false;
    private String description = null;
    private int returnCode;


    // Constructors
    public HandlerResult(boolean success, String description) {
	this(success, description, 0);
    }
    public HandlerResult(boolean success, String description, int returnCode) {
	this.success = success;
	this.description = description;
	this.returnCode = returnCode;
    }

    /**
     * set return code
     * @param int return code
     */
    public void setReturnCode(int returnCode) {
	this.returnCode = returnCode;
    }

    /**
     * get return code
     * @return int return code
     */
    public int getReturnCode() {
	return returnCode;
    }

    /**
     * set description 
    /**
     * set success boolean
     * @param boolean success construct
     */
    public void setSuccess(boolean success) {
	this.success = success;
    }

    /**
     * get success boolean
     * @return boolean success construct
     */
    public boolean getSuccess() {
	return success;
    }

    /**
     * set description 
     * @param String description of result
     */
    public void setDescription(String description) {
	this.description = description;
    }

    /**
     * get description
     * @return String result description
     */
    public String getDescription() {
	return description;
    }

    /**
     * Dump the content of this object 
     * @param header header displayed in log entry
     */
    public String dump(String header)
    {
        return header
                + " - success=" + success
                + " - description=" + description;
    }
}
