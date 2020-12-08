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

package org.cdlib.mrt.ingest.app;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

import org.cdlib.mrt.ingest.IngestConfig;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.service.IngestServiceAbs;
import org.cdlib.mrt.utility.TException;
/**
 * Initialize handling for ingest service in servlet
 * @author mreyes
 */
public class IngestServiceInit
{
    protected IngestServiceInf ingestService = null;
    protected volatile IngestConfig ingestConfig = null;
    protected ServletConfig servletConfig = null;


    /**
     * Get resolved ingest service
     * @return
     */
    public IngestServiceInf getIngestService() {
        return ingestService;
    }

    /**
     * Factory: IngestServiceInit
     * @param servletConfig servlet configuration object
     * @return IngestServiceInit
     * @throws TException
     */
    public static synchronized IngestServiceInit getIngestServiceInit(
            ServletConfig servletConfig)
            throws TException
    {
        String serviceName = "ingestService";
        ServletContext servletContext = servletConfig.getServletContext();
        IngestServiceInit ingestServiceInit  = (IngestServiceInit)servletContext.getAttribute(serviceName);
        if (ingestServiceInit == null) {
            ingestServiceInit = new IngestServiceInit(servletConfig, serviceName);
            servletContext.setAttribute(serviceName, ingestServiceInit);
        }

        return ingestServiceInit;
    }

    /**
     * Constructor
     * @param servletConfig servlet configuration
     * @param serviceName service name for logging and for persistence
     * @throws TException
     */
    public IngestServiceInit(ServletConfig servletConfig, String serviceName)
            throws TException
    {
        this.servletConfig = servletConfig;
        ingestService = IngestServiceAbs.getIngestService(IngestConfig.useYaml());
    }
}
