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
package org.cdlib.mrt.ingest.app.cmd;


import jargs.gnu.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.service.IngestServiceAbs;
import org.cdlib.mrt.ingest.HandlerState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


/**
 * Command line processor
 * <pre>
 * This process involves:
 * - parsing the command line
 * - saving all parameters to this object
 * - execute the requested command
 * The response is then returned as a display or as a file output
 *
 * After the request type has been extracted the subclass calls back to this class
 * to execute the appropriate process.
 *
 * Each command process:
 * - validates each passed parameter
 * - call the appropriate IngestService method with the command line parameters
 * - the responses are either displayed or saved to an explicitly name output file
 * </pre>
 * @author mreyes
 */
public abstract class CmdBase
{

    /**
     * Items can be passed either by-reference or by-value (e.g. manifest)
     */
    public enum ProcessMode
    {
        by_reference("by-reference"),
        by_value("by-value"),
        unmatched("unmatched");

        protected final String cmd;

        ProcessMode(String cmd) {
            this.cmd = cmd;
        }

        public String getCmdType()
        {
            return this.cmd;
        }

        public static ProcessMode setMode(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            t = t.toLowerCase();
            for (ProcessMode p : ProcessMode.values()) {
                if (p.getCmdType().equals(t)) {
                    return p;
                }
            }
            return unmatched;
        }
    }

    protected static final String NAME = "IngestMain";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.anvl;
    protected static final boolean DEBUG = false;

    protected LoggerInf logger = null;
    protected IngestServiceInf ingestService = null;
    protected Identifier objectID = null;
    protected String manifestPath = null;
    protected String requestType = null;
    protected String outFileS = null;
    protected File outFile = null;
    protected File stateFile = null;
    protected File dispFile = null;
    protected StateInf responseState = null;
    protected FormatterInf formatter = null;
    protected Exception processException = null;
    protected String archiveType = null;
    protected ProcessMode responseMode = null;
    protected ProcessMode requestMode = null;




    /**
     * Save positional elements of command line
     * For this implementation the order is fixed
     * @param parts array of command line elements
     * @throws TException
     */
    abstract protected void setParts(String[] parts)
        throws TException;

    /**
     * Call back into abstract class for handling specific actions based on
     * the command request type - here, the first fixed command line entry
     * @param requestTypeIn process name - is lowercased on match
     * @throws TException
     */
    abstract protected void call(String requestTypeIn)
        throws TException;

    /**
     * Constructor
     * @param mFrame configuration resolver
     * @throws TException
     */
    protected CmdBase(TFrame mFrame)
        throws TException
    {
        this.logger = mFrame.getLogger();
        this.ingestService = IngestServiceAbs.getIngestService(logger, mFrame.getProperties());
    }

    /**
     * process inline arguments
     * @param args main arguments
     */
    public void runIt(String args[])
    {
        PrintStream stream = null;
        try {

            setParms(args);
            //System.out.println(dump("initial"));
            if (false) return;
            call(requestType);
            if (processException != null) {
                TException mex = null;
                if (processException instanceof TException) {
                    mex = (TException) processException;
                } else {
                    mex = new TException.GENERAL_EXCEPTION(processException);
                }
                responseState = mex;
                if (logger != null) {
                    logger.logError(MESSAGE + "Exception:" + mex, 0);
                    logger.logError(MESSAGE + "trace=" + StringUtil.stackTrace(mex), 10);
                }
                logDebug(MESSAGE + "Exception:" + mex);
                logDebug(MESSAGE + "Trace:" + StringUtil.stackTrace(mex));
            }
            if (responseState != null) {
                try {
                    FileOutputStream outStream = new FileOutputStream(stateFile);
                    stream = new PrintStream(outStream);
                    formatter.format(responseState, stream);

                } catch (TException tex) {
                    throw tex;

                } finally {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (Exception ex) { }
                    }
                }

                // move response to output file if provided
                if ((outFile == null) && StringUtil.isNotEmpty(outFileS)){
                    try {
                        dispFile = new File(outFileS);
                        FileInputStream dis = new FileInputStream(stateFile);
                        FileUtil.stream2File(dis, dispFile);

                    } catch (TException tex) {
                        throw tex;
                    } finally {
                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (Exception ex) { }
                        }
                    }
                }
            }

            if (dispFile == null) {
                String outString = FileUtil.file2String(stateFile);
                System.out.println(outString);
            }


        } catch (Exception ex) {
            if (logger != null) {
                logger.logError(MESSAGE + "Exception:" + ex, 0);
                logger.logError(MESSAGE + "trace=" + StringUtil.stackTrace(ex), 10);
            }
            logDebug(MESSAGE + "Exception:" + ex);
            logDebug(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));

        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ex) { }
            }
        }
    }

    /**
     * use GNU CmdLineParser to handle command line content
     * @param args main arguments
     * @throws TException
     */
    protected void setParms(String args[])
        throws TException
    {
        try {
            CmdLineParser parser = new CmdLineParser();
            CmdLineParser.Option manifestO = parser.addStringOption('M', "manifest");
            CmdLineParser.Option archiveTypeO = parser.addStringOption('A', "archive-type");
            CmdLineParser.Option stateFileO = parser.addStringOption('o', "output");
            CmdLineParser.Option outputFormatO = parser.addStringOption('t', "response-form");
            CmdLineParser.Option requestModeO = parser.addStringOption('R', "request-mode");
            CmdLineParser.Option responseModeO = parser.addStringOption('r', "response-mode");
            parser.parse(args);
            manifestPath = (String)parser.getOptionValue(manifestO);
            archiveType = (String)parser.getOptionValue(archiveTypeO);
            String responseModeS = (String)parser.getOptionValue(responseModeO);
            responseMode = ProcessMode.setMode(responseModeS);
            String requestModeS = (String)parser.getOptionValue(requestModeO);
            requestMode = ProcessMode.setMode(requestModeS);
            String[] parts = parser.getRemainingArgs();
            logDebug("parts.length=" + parts.length);
            for (String part: parts) {
                logDebug("part=" + part);
            }

            setParts(parts);

            String outputFormatS = (String)parser.getOptionValue(outputFormatO);
            FormatterInf.Format outputFormat = null;
            if (StringUtil.isEmpty(outputFormatS)) {
                outputFormat = DEFAULT_OUTPUT_FORMAT;
            } else {
                outputFormatS = outputFormatS.toLowerCase();
                outputFormat = FormatterInf.Format.valueOf(outputFormatS);
                if (outputFormat == null) {
                    outputFormat = DEFAULT_OUTPUT_FORMAT;
                }
            }
            formatter = FormatterAbs.getFormatter(outputFormat, logger);
            outFileS = (String)parser.getOptionValue(stateFileO);
            stateFile = FileUtil.getTempFile("test", ".txt");

        } catch (Exception ex) {
            logDebug(MESSAGE + "Exception:" + ex);
            logDebug(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    /**
     * Dump extracted command line data
     * @param header header for constructed string
     * @return
     */
    protected String dump(String header)
    {
        StringBuffer buf = new StringBuffer(1000);
        String out = header + ":"
                + " - objectID=" + objectID
                + " - requestType=" + requestType;
        buf.append(out);
        try {
            if (outFile != null) {
                buf.append(" - outFile=" + outFile.getCanonicalPath());
            }
            if (stateFile != null) {
                buf.append(" - stateFile=" + stateFile.getCanonicalPath());
            }
        } catch (Exception ex) {}
        if (ingestService != null) {
            buf.append(" - logmsgmax=" + ingestService.getLogger().getMessageMaxLevel());
            buf.append(" - logerrmax=" + ingestService.getLogger().getErrorMaxLevel());
        }
        return buf.toString();
    }

    /**
     * Execute submit
     */
    protected void submit()
    {
        logDebug("call submit");
        try {
	    IngestRequest ingestRequest = new IngestRequest();
            testObjectID();
            testManifest();
            File manifestFile = new File(manifestPath);
            if (!manifestFile.exists()) {
                throw new TException.REQUEST_INVALID (
                        "addVersion - manifest not found");
            }
	    ingestRequest.setQueuePath(manifestFile);
            JobState jobState = ingestService.submit(ingestRequest);
            responseState = jobState;

        } catch (Exception ex) {
            setException(ex);
        }
    }

    /**
     * Execute getServiceState
     */
    protected void getServiceState()
    {
        logDebug("call getIngestState");
        try {
            responseState = ingestService.getServiceState();

        } catch (Exception ex) {
            setException(ex);
        }
    }

    protected void setException(Exception ex)
    {
        processException = ex;
        logDebug(MESSAGE + "Exception:" + ex);
        logDebug(MESSAGE + "trace:" + StringUtil.stackTrace(ex));
    }

    protected void testArchiveType()
        throws TException
    {
        if (StringUtil.isEmpty(archiveType)) {
            throw setException("Archive type required but not found");
        }
        archiveType = archiveType.toLowerCase();
        if ("*zip*tar*targz*".indexOf("*" + archiveType + "*") < 0) {
            throw setException("Archive type not supported");
        }
    }

    protected void testObjectID()
        throws TException
    {
        if ((objectID == null) || (objectID.toString() == null)) {
            throw setException("ObjectID required but not found");
        }
    }

    protected void testManifest()
        throws TException
    {
        if (StringUtil.isEmpty(manifestPath)) {
            throw setException("Manifest required but not found");
        }
    }

    protected void testOutputFile()
        throws TException
    {
        if (StringUtil.isEmpty(outFileS)) {
            throw setException("Output file required but not found");
        }
    }

    protected TException setException(String header)
    {
        return new TException.REQUEST_INVALID(
                MESSAGE + header);
    }


    protected void logDebug(String msg)
    {
        if (DEBUG) System.out.println("***>" + msg);
        //logger.logMessage(msg, 0, true);
    }

}
