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

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;

/**
 * Handler State information
 * @author mreyes
 */
public class HandlerState
        implements HandlerStateInf, StateInf, Serializable
{

    private String handlerName = null;
    private Identifier handlerID = null;
    private String handlerDescription = null;
    private DateState creationDate = null;
    private DateState modificationDate = null;


    @Override
    public String getHandlerName() {
        return handlerName;
    }

    /**
     * Set handler name
     * @param String handler name
     */
    public void setHandlerName(String handlerName) {
        this.handlerName = handlerName;
    }

    @Override
    public Identifier getHandlerID() {
        return handlerID;
    }

    /**
     * Set handler identifier
     * @param Indentifier handler identifier
     */
    public void setHandlerID(Identifier handlerID) {
        this.handlerID = handlerID;
    }

    @Override
    public String getHandlerDescription() {
        return handlerDescription;
    }

    /**
     * Set handler description
     * @param String handler description
     */
    public void setHandlerDescription(String handlerDescription) {
        this.handlerDescription = handlerDescription;
    }

    @Override
    public DateState getCreationDate() {
        return creationDate;
    }

    /**
     * Set creation date
     * @param DateState creation date-time
     */
    public void setCreationDate(DateState creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public DateState getModificationDate() {
        return modificationDate;
    }

    /**
     * Set modification date
     * @param DateState modification date
     */
    public void setModificationDate(DateState modificationDate) {
        this.modificationDate = modificationDate;
    }

    public String dump(String header)
    {
        return header
                + " - handlerName=" + handlerName
                + " - handlerID=" + handlerID
                + " - handlerDescription=" + handlerDescription
                + " - creationDate=" + creationDate
                + " - modificationDate=" + modificationDate;
    }
}
