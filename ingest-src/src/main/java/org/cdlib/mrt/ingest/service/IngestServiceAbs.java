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
*********************************************************************/
package org.cdlib.mrt.ingest.service;

import org.cdlib.mrt.ingest.IngestConfig;
import org.cdlib.mrt.ingest.AdminManager;
import org.cdlib.mrt.ingest.IngestManager;
import org.cdlib.mrt.ingest.QueueManager;
import org.cdlib.mrt.ingest.BatchManager;
import org.cdlib.mrt.ingest.ProcessManager;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Common IngestService processes
 * @author mreyes
 */
public class IngestServiceAbs
{
    protected static final String NAME = "IngestServiceAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected LoggerInf logger = null;
    protected IngestManager ingestManager = null;
    protected BatchManager batchManager = null;
    protected QueueManager queueManager = null;
    protected ProcessManager processManager = null;
    protected AdminManager adminManager = null;
    //protected String ingestFileS = null;        // prop "IngestService"

    
    /**
     * service property
     * @return IngestService property
     * @throws org.cdlib.mrt.utility.MException
     */
    //public String getIngestServiceProp() {
        //return ingestFileS;
    //}

    /**
     * IngestService Factory
     * @param logger debug/status logging
     * @param confProp system properties
     * @return IngestService return requested service
     * @throws org.cdlib.mrt.utility.MException
     */
    public static IngestService getIngestService(
            IngestConfig ingestConfig)
        throws TException
    {
        IngestService ingestService = new IngestService(ingestConfig);
        return ingestService;
    }

    /**
     * Constructor
     * @param logger process log
     * @param confProp configuration properties
     * @throws TException process exception
     */
    public IngestServiceAbs(
            IngestConfig ingestConfig)
        throws TException
    {
        if (ingestConfig == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Required Ingest Configuration data is missing");
        }
        this.logger = ingestConfig.getLogger();
        this.ingestManager = IngestManager.getIngestManager(
		ingestConfig.getLogger(),
		ingestConfig.getStoreConf(),
		ingestConfig.getIngestConf(),
		ingestConfig.getQueueConf());
        this.batchManager = BatchManager.getBatchManager(
		ingestConfig.getLogger(),
		ingestConfig.getQueueConf(),
		ingestConfig.getIngestConf());
        this.queueManager = QueueManager.getQueueManager(
		ingestConfig.getLogger(),
		ingestConfig.getQueueConf(),
		ingestConfig.getIngestConf());
        this.processManager = ProcessManager.getProcessManager(
		ingestConfig.getLogger(),
		ingestConfig.getStoreConf(),
		ingestConfig.getIngestConf(),
		ingestConfig.getQueueConf());
        this.adminManager = AdminManager.getAdminManager(
		ingestConfig.getLogger(),
		ingestConfig.getIngestConf());
    }

    /**
     * Return IngestService level logger
     * @return LoggerInf logger
     */
    public LoggerInf getLogger()
    {
        return logger;
    }
}

