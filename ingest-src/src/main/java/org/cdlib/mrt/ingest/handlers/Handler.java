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

import java.io.*;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;

/**
 * Handler
 * @author mreyes
 */
public abstract class Handler<T extends StateInf>
{
    private static final String NAME = "Handler";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
        
    private String m_checksum = null;
    private String m_checksumType = null;
    private long m_inputFileSize = 0;
    private long m_extractFileSize = 0;
    private File m_jhoveFile = null;
    private File m_component = null;
    private long m_startMS = 0;
    private LoggerInf logger = null;


    /**
     * processer for handler
     * @param ProfileState profile
     * @param IngestRequest ingest request
     * @param state stateInf based class
     * @return void
     */
    abstract public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, T state) throws TException;

    abstract public String getName();
}
