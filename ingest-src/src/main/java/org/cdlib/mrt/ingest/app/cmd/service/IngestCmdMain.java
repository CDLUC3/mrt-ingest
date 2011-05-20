/*
Copyright (c) 2005-2010, Regents of the University of California
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
package org.cdlib.mrt.ingest.app.cmd.service;

import jargs.gnu.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.UUID;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.service.IngestServiceAbs;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


public class IngestCmdMain
{


    protected static final String NAME = "IngestMain";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.anvl;
    protected static final boolean DEBUG = false;

    protected LoggerInf logger = null;
    protected IngestServiceInf ingestService = null;
    protected Integer ingestID = -1;
    protected Identifier objectID = null;
    protected File queuePath = null;
    protected String requestType = null;
    protected String outFileS = null;
    protected File outFile = null;
    protected File stateFile = null;
    protected File dispFile = null;
    protected StateInf responseState = null;
    protected FormatterInf formatter = null;
    protected Exception processException = null;
    protected String archiveType = null;
    protected Identifier jobID = null;
    protected Identifier batchID = null;
    protected IngestRequest ingestRequest = null;

    protected IngestCmdMain(TFrame mFrame)
        throws TException
    {
        this.logger = mFrame.getLogger();
        this.ingestService = IngestServiceAbs.getIngestService(logger, mFrame.getProperties());
    }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame mFrame = null;
        IngestCmdMain ingestMain = null;
        try
        {
            /*
            	GetOptions('O|object=s' => \$objectID,
			'M=s' => \$manifest,
			'o|output=s' => \$outFile,
			't|response-form=s' => \$formatType);
             */

            String propertyList[] = {
                "resources/TIngestService.properties",
                "resources/TIngestServiceLocal.properties"};

            mFrame = new TFrame(propertyList, NAME);

            ingestMain = new IngestCmdMain(mFrame);
            ingestMain.runIt(args);

        }  catch(Exception e)  {
            if ((ingestMain != null) && (ingestMain.logger != null)) {
                ingestMain.logger.logError(
                    "Main: Encountered exception:" + e, 0);
                ingestMain.logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    public void runIt(String args[])
    {
        PrintStream stream = null;
        try {

            setParms(args);

            call(requestType);
            if (processException != null) {
                TException mex = null;
                if (processException instanceof TException) {
                    mex = (TException) processException;
                } else {
                    mex = new TException.GENERAL_EXCEPTION(processException);
                }
                responseState = mex;
            }
            if (responseState != null) {
                try {
                    FileOutputStream outStream = new FileOutputStream(stateFile);
                    stream = new PrintStream(outStream);
                    formatter.format(responseState, stream);

                } catch (TException tex) {
                    throw tex;
                } catch (Exception e) {
                    throw e;
                } finally {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (Exception ex) { ex.printStackTrace(); }
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
            log(MESSAGE + "Exception:" + ex);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));

        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ex) { }
            }
        }
    }

    public void setParms(String args[])
        throws TException
    {
        try {
            requestType = args[0];
            requestType = requestType.toLowerCase();
            CmdLineParser parser = new CmdLineParser();
            CmdLineParser.Option userIDO = parser.addStringOption("submitter");
            CmdLineParser.Option objectIDO = parser.addStringOption("object");
            CmdLineParser.Option packageO = parser.addStringOption("package");
            CmdLineParser.Option profileIDO = parser.addStringOption("profile");
            CmdLineParser.Option packageTypeO = parser.addStringOption("type");
            CmdLineParser.Option digestTypeO = parser.addStringOption("digestType");
            CmdLineParser.Option digestValueO = parser.addStringOption("digestValue");
            CmdLineParser.Option dcCreatorO = parser.addStringOption("creator");
            CmdLineParser.Option dcTitleO = parser.addStringOption("title");
            CmdLineParser.Option dcDateO = parser.addStringOption("date");

            CmdLineParser.Option stateFileO = parser.addStringOption('o', "output");
            CmdLineParser.Option outputFormatO = parser.addStringOption('t', "response-form");
            CmdLineParser.Option debugO = parser.addStringOption('D', "debug");
            CmdLineParser.Option helpO = parser.addStringOption('h', "help");
            CmdLineParser.Option inputFormatO = parser.addStringOption('T', "request-form");	// differs from type??
            CmdLineParser.Option verboseO = parser.addStringOption('v', "verbose");
            CmdLineParser.Option versionO = parser.addStringOption('V', "version");

            parser.parse(args);

	    // object ID
            String objectIDS = (String) parser.getOptionValue(objectIDO);
            if (StringUtil.isNotEmpty(objectIDS)) {
                objectID = new Identifier(objectIDS);
            }

	    // profile ID
            String profileIDS = (String) parser.getOptionValue(profileIDO);
            if (StringUtil.isEmpty(profileIDS) && requestType.equals("getservicestate")) {
		profileIDS = "default";
            }

	    // user ID
            String userIDS = (String) parser.getOptionValue(userIDO);
            if (StringUtil.isEmpty(userIDS) && requestType.equals("submit")) {
                throw new Exception("user ID not defined");
            }
	
	    // package
            String packageS = (String) parser.getOptionValue(packageO);
            if (StringUtil.isNotEmpty(packageS) && (requestType.startsWith("submit"))) {
                File fromFile = new File(packageS);
		if (! fromFile.exists()) throw new Exception("package file not found: " + fromFile.getAbsolutePath());

	        File toFile = new File(ingestService.getIngestServiceProp() + "/queue");
		if (! toFile.exists()) throw new Exception("home directory file not found: " + toFile.getAbsolutePath());

		batchID = new Identifier(ProfileUtil.DEFAULT_BATCH_ID);
		/* Command line batch not supported
		//if (requestType.equals("submitbatch")) {
		    // batchID = MintUtil.getBatchID();
		} */
		jobID = MintUtil.getJobID();
			
                queuePath = new File(toFile + System.getProperty("file.separator") + batchID.getValue(), jobID.getValue());
                queuePath.mkdirs();
		FileUtil.copyFile(fromFile.getName(), fromFile.getParentFile(), queuePath);
		File newPackage =  new File(queuePath, fromFile.getName());
		if (! newPackage.exists())
			throw new Exception("could not create package: " + newPackage.getAbsolutePath());
            }

	    // package type
            String packageTypeS = (String) parser.getOptionValue(packageTypeO);

	    // response/form
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
	    outFileS = (String) parser.getOptionValue(stateFileO);
	    stateFile = FileUtil.getTempFile("test", ".txt");

	    // checksum
	    String digestType = (String) parser.getOptionValue(digestTypeO);;
	    String digestValue = (String) parser.getOptionValue(digestValueO);;

	    // DC
            String dcCreator = (String) parser.getOptionValue(dcCreatorO);
            String dcTitle = (String) parser.getOptionValue(dcTitleO);
            String dcDate = (String) parser.getOptionValue(dcDateO);

            ingestRequest = new IngestRequest(userIDS, profileIDS, packageS, packageTypeS, null,
			digestType, digestValue, objectIDS, dcCreator, dcTitle, dcDate, outputFormatS);
	    ingestRequest.getJob().setBatchID(batchID);

        } catch (Exception ex) {
	    ex.printStackTrace();
            log(MESSAGE + "Exception:" + ex);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

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

    protected void call(String requestType)
        throws TException
    {
        if (requestType.equals("submit")) {
            submit();
	// Command line batch not supported
        //} else if (requestType.equals("submitbatch")) {
            //submitBatch();
        } else if (requestType.equals("getservicestate")) {
            getServiceState();
        } else {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED(MESSAGE + "requestType not defined");
        }

    }

    protected void setStateFile()
        throws TException
    {
            if (outFileS != null) {
                stateFile = new File(outFileS);
            } else {
                stateFile = FileUtil.getTempFile("test", ".txt");
            }
    }

    protected void submit()
    {
        log("call submit");
        try {
            // testObjectID();
            testQueuePath();

	    ingestRequest.setQueuePath(queuePath);

            // make needed directories
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    ingestRequest.getJob().setJobID(jobID);
            JobState jobState = ingestService.submit(ingestRequest);
            responseState = jobState;

        } catch (Exception ex) {
	    ex.printStackTrace();
            setException(ex);
        }
    }

/*  Command line batch not supported
    protected void submitBatch()
    {
        log("call submit batch");
        try {
            // testObjectID();
            testQueuePath();

	    queuePath = queuePath.getParentFile();
	    ingestRequest.setQueuePath(queuePath);

            // make needed directories
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    ingestRequest.getJob().setJobID(jobID);
            BatchState batchState = ingestService.submitPost(ingestRequest);
            responseState = batchState;

        } catch (Exception ex) {
	    ex.printStackTrace();
            setException(ex);
        }
    }
*/

    protected void getServiceState()
    {
        log("call getServiceState");
        try {
            IngestServiceState ingestState= ingestService.getServiceState();
            responseState = ingestState;
        } catch (Exception ex) {
            setException(ex);
        }
    }

    protected void setException(Exception ex)
    {
        processException = ex;
        log(MESSAGE + "Exception:" + ex);
        log(MESSAGE + "trace:" + StringUtil.stackTrace(ex));
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

    protected void testQueuePath()
        throws TException
    {
        if (! queuePath.exists()) {
            throw setException("Queue directory not found: " + queuePath.getAbsolutePath());
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


    protected void log(String msg)
    {
        if (DEBUG) System.out.println("***>" + msg);
        //logger.logMessage(msg, 0, true);
    }

}
