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
package org.cdlib.mrt.ingest.app.jersey;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.InterruptedException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sun.jersey.spi.CloseableService;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletConfig;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowBatch;
import org.cdlib.mrt.core.ManifestRowIngest;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.MessageDigest;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.IdentifierState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.IngestServiceState;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.app.ValidateCmdParms;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.ResponseFormEnum;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Base Jersey handling for Ingest
 * Keep the Jersey as thin as possible.
 * Jersey provides the servlet layer for ingest RESTful interface
 * @author mreyes
 */
public class JerseyBase
{

    protected static final String NAME = "JerseyBase";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.xml;
    protected static final boolean DEBUG = true;
    protected static final String NL = System.getProperty("line.separator");
    protected static final String FS = System.getProperty("file.separator");

    protected LoggerInf defaultLogger = new TFileLogger("Jersey", 10, 10);
    protected JerseyCleanup jerseyCleanup = new JerseyCleanup();

    /**
     * Format file from input State file
     * @param responseState object to be formatted
     * @param formatType user requested format type
     * @param logger file logger
     * @return formatted data with MimeType
     * @throws TException
     */
    protected TypeFile getStateFile(StateInf responseState, FormatType outputFormat, LoggerInf logger)
            throws TException
    {
        if (responseState == null) return null;
        PrintStream stream = null;
        TypeFile typeFile = new TypeFile();
        try {
            if (outputFormat == FormatType.serial) {
                typeFile.formatType = outputFormat;
                if (responseState instanceof Serializable) {
                    Serializable serial = (Serializable)responseState;
                    typeFile.file = FileUtil.getTempFile("state", ".ser");
                    SerializeUtil.serialize(serial, typeFile.file);
                }
            }

            if (typeFile.file == null) {
                FormatterInf formatter = getFormatter(outputFormat, logger);
                FormatterInf.Format formatterType = formatter.getFormatterType();
                String foundFormatType = formatterType.toString();
                typeFile.formatType = outputFormat.valueOf(foundFormatType);
		String ext = typeFile.formatType.getExtension();
                typeFile.file = FileUtil.getTempFile("state", "." + ext);
                FileOutputStream outStream = new FileOutputStream(typeFile.file);
                stream = new PrintStream(outStream, true, "utf-8");
                formatter.format(responseState, stream);
            }
            return typeFile;

        } catch (TException tex) {
            System.err.println("Stack:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch (Exception ex) {
            System.err.println("Stack:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + " Exception:" + ex);

        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ex) { }
            }
        }

    }

    /**
     * Validate that the user passed format legit
     * @param formatType user passed format
     * @param form type of format: "state", "archive", "file"
     * @return DataType.ResponseForm form of user format
     * @throws TException
     */
    protected FormatType getFormatType(String formatType, String form)
            throws TException
    {
        try {
            if (StringUtil.isEmpty(formatType)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }
            formatType = formatType.toLowerCase();
	    FormatType format = FormatType.valueOf(formatType);
            if (!format.getForm().equals(form)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }

        return format;
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
        }
    }


    /**
     * Add an object to this ingest service
     * @param ingestRequest complete request information
     * @param request http request information
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response submit(IngestRequest ingestRequest, HttpServletRequest request, CloseableService cs, ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
	    ingestRequest.getJob().setJobID(MintUtil.getJobID());
            log("addVersion submit entered:"
                    + " - ingestRequest=" + ingestRequest.dump("submit")
                    + " - request=" + request.toString()
                    );
            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            ingestRequest = getFormData(ingestRequest, request, ingestService.getIngestServiceProp() + "/queue", logger);
	    if (DEBUG) System.out.println("[info] queuepath: " + ingestRequest.getQueuePath().getAbsolutePath());
            jerseyCleanup.addTempFile(ingestRequest.getQueuePath());
            StateInf responseState = submit(ingestRequest, ingestService, logger);

            return getStateResponse(responseState, ingestRequest.getResponseForm(), logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, ingestRequest.getResponseForm(), logger);

        } catch (Exception ex) {
            System.err.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * Request an ID
     * @param ingestRequest complete request information
     * @param request http request information
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response requestIdentifier(IngestRequest ingestRequest, HttpServletRequest request, CloseableService cs, ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
	    ingestRequest.getJob().setJobID(MintUtil.getJobID());
            log("requestIdentifer entered:"
                    + " - ingestRequest=" + ingestRequest.dump("requestIdentifier")
                    + " - request=" + request.toString()
                    );
            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            ingestRequest = getFormData(ingestRequest, request, ingestService.getIngestServiceProp() + "/queue", logger);
	    if (DEBUG) System.out.println("[info] queuepath: " + ingestRequest.getQueuePath().getAbsolutePath());
            jerseyCleanup.addTempFile(ingestRequest.getQueuePath());

            StateInf responseState = requestIdentifier(ingestRequest, ingestService, logger);

            return getStateResponse(responseState, ingestRequest.getResponseForm(), logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, ingestRequest.getResponseForm(), logger);

        } catch (Exception ex) {
            System.err.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
    /**
     * Add an object to this ingest service
     * @param ingestRequest request information
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected StateInf submit(
            IngestRequest ingestRequest,
            IngestServiceInf ingestService,
            LoggerInf logger)
        throws TException
    {
        try {
	    // make needed directories
	    new File(ingestRequest.getQueuePath(), "system").mkdir();
	    new File(ingestRequest.getQueuePath(), "producer").mkdir();

            StateInf responseState = ingestService.submit(ingestRequest);
            return responseState;

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            if (DEBUG) System.err.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
    /**
     * request id from ide service
     * @param ingestRequest request information
     * @param sc ServletConfig used to get system configuration
     * @return version state information for added item
     * @throws TException processing exception
     */
    protected StateInf requestIdentifier(
            IngestRequest ingestRequest,
            IngestServiceInf ingestService,
            LoggerInf logger)
        throws TException
    {
        try {
            StateInf responseState = ingestService.requestIdentifier(ingestRequest);

            return responseState;

        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            if (DEBUG) System.err.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Submit an object to queue service
     * @param ingestRequest complete request information
     * @param request http request information
     * @param sc ServletConfig used to get system configuration
     * @return formatted version state information
     * @throws TException processing exception
     */
    public Response submitPost(IngestRequest ingestRequest, HttpServletRequest request, CloseableService cs, ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {

	    // batch processing
	    ingestRequest.getJob().setBatchID(MintUtil.getBatchID());	

            log("submit to queue request entered:"
                    + " - ingestRequest=" + ingestRequest.dump("submitPost")
                    + " - request=" + request.toString()
                    );
            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            ingestRequest = getFormData(ingestRequest, request, ingestService.getIngestServiceProp() + "/queue", logger);
	    if (DEBUG) System.out.println("[info] queuepath: " + ingestRequest.getQueuePath().getAbsolutePath());
            jerseyCleanup.addTempFile(ingestRequest.getQueuePath());
            BatchState responseState = ingestService.submitPost(ingestRequest);

            int retryCount = 0;
	    Response response = null;
	    while (true) {
		try {
                    response = getStateResponse(responseState, ingestRequest.getResponseForm(), logger, cs, sc);
		    break;
                } catch (Exception e) {
		    // For large POST
                    if (retryCount >= 6) {	// 30 seconds
                        // We can not wait any longer. Let's return BatchState with current state (may be missing some JobStates)
                        if (DEBUG) System.out.println("[error] JerseyBase: Truncating BatchState response:" + responseState.getBatchID().getValue());
                        BatchState batchState = responseState.clone();
                        response = getStateResponse(batchState, ingestRequest.getResponseForm(), logger, cs, sc);
                    } else {
                        if (DEBUG) System.out.println("[error] JerseyBase: Batch State is not yet stable (still adding jobs) " + responseState.getBatchID().getValue() + "  Retrying...");
		    }
                    try {
                        Thread.sleep(1000 * 5);
                    } catch (InterruptedException ie) {}
                    retryCount++;       // Poster has not yet finished creating all jobs in batch
                }
		finally {
		}
	    }

            return response;

        } catch (TException tex) {
            return getExceptionResponse(tex, ingestRequest.getResponseForm(), logger);

        } catch (Exception ex) {
            System.err.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    /**
     * Get Response to a formatted State object
     * @param responseState State object to format
     * @param formatType user specified format type
     * @param logger system logging
     * @return Jersey Response referencing formatted State object (as File)
     * @throws TException process exceptions
     */
    protected Response getStateResponse(
            StateInf responseState,
            String formatType,
            LoggerInf logger,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");
        } catch (TException tex) {
            responseState = tex;
            format = FormatType.xml;
        }

        try {
            typeFile = getStateFile(responseState, format, logger);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (TException tex) {
	    tex.printStackTrace();
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + typeFile.formatType);
        }
        log("getStateResponse:" + typeFile.formatType
                + " - formatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());

        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).build();
    }


    /**
     * Validate and return object identifier
     * @param parm String containing objectID
     * @return object identifier
     * @throws TException invalid object identifier format
     */
    protected Identifier getObjectID(String parm)
        throws TException
    {
        return ValidateCmdParms.validateObjectID(parm);
    }


    /**
     * Return a Jersey Response object after formatting an exception
     * @param exception process exception to format
     * @param formatType format to use on exception (default xml)
     * @param logger system logger
     * @return Jerse Response referencing formatted Exception output
     * @throws TException process exceptions
     */
    protected Response getExceptionResponse(TException exception, String formatType, LoggerInf logger)
        throws TException
    {
        if (DEBUG) System.err.println("TRACE:" + StringUtil.stackTrace(exception));
        int httpStatus = exception.getStatus().getHttpResponse();
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");

        } catch (TException dtex) {
            format = FormatType.xml;
        }
        try {
            typeFile = getStateFile(exception, format, logger);

        } catch (TException dtex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType + "Exception:" + dtex);
        }
        log("getStateResponse:" + formatType
                + " - formatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).status(httpStatus).build();
    }


    /**
     * Extract addversion form data
     * @param ingestRequest complete request package
     * @param httpRequest servlet request
     * @param homeDir ingest home directory
     * @param logger process logger
     * @return ingest request
     * @throws TException process exception
     */
    private IngestRequest getFormData(
            IngestRequest ingestRequest,
            HttpServletRequest request,
            String homeDir,
            LoggerInf logger)
        throws TException
    {
    	FileItem item = null;

        try {

	boolean filename = false;

        // unique queue directory
        File queueDir = null;
	if (ingestRequest.getJob().grabBatchID() != null) {
	    // queue processing
            queueDir = new File(homeDir, ingestRequest.getJob().grabBatchID().getValue()); 
	} else {
	    // direct ingest processing.
	    ingestRequest.getJob().setBatchID(new Identifier(ProfileUtil.DEFAULT_BATCH_ID, Identifier.Namespace.Local));
            queueDir = new File(homeDir, ingestRequest.getJob().grabBatchID().getValue() + FS + 
	    ingestRequest.getJob().getJobID().getValue());
	}
	if ( ! queueDir.exists()) queueDir.mkdirs();

	// Create a factory for disk-based file items
	DiskFileItemFactory factory = new DiskFileItemFactory();
	factory.setSizeThreshold(0);
	factory.setRepository(queueDir);
	ServletFileUpload upload = new ServletFileUpload(factory);
	List<FileItem> items = null;
	try {
	   items = (List<FileItem>) upload.parseRequest(request); 
	} catch (Exception e) {
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + e);
	}

	if (DEBUG) System.err.println("[debug] form items: " + items.toString());

	// Process form fields
	Iterator<FileItem> iter = items.iterator();
	while (iter.hasNext()) {
    	   item = iter.next();

	   String field = null;
	   try {
    	   	if (item.isFormField()) {
		    if (item.getFieldName().equals("submitter")) {
		       field = "submitter";
		       ingestRequest.getJob().setUserAgent(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] submitter: " + ingestRequest.getJob().grabUserAgent());
		    } else if (item.getFieldName().equals("object")){
		       field = "object";
		       ingestRequest.getJob().setPrimaryID(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] object: " + ingestRequest.getJob().getPrimaryID());
		    } else if (item.getFieldName().equals("profile")){
		       field = "profile";
		       ingestRequest.setProfile(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] profile: " + ingestRequest.getProfile());
		    } else if (item.getFieldName().equals("filename")){
		       filename = true;	
		       field = "filename";
		       ingestRequest.getJob().setPackageName(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] package name(filename): " + ingestRequest.getJob().getPackageName());
		    } else if (item.getFieldName().equals("type")){
		       field = "type";
		       // object-manifest and batch-Manifest can not be an enum (hyphens)
		       if (item.getString("utf-8").matches("object-manifest"))
		           ingestRequest.setPackageType("manifest");
		       else if (item.getString("utf-8").contains("single-file-batch-manifest"))
		           ingestRequest.setPackageType("batchManifestFile");
		       else if (item.getString("utf-8").contains("container-batch-manifest"))
		           ingestRequest.setPackageType("batchManifestContainer");
		       else if (item.getString("utf-8").contains("batch-manifest"))
		           ingestRequest.setPackageType("batchManifest");
		       else 
		           ingestRequest.setPackageType(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] file type: " + ingestRequest.getPackageType());
		    } else if (item.getFieldName().equals("size")){
		       field = "size";
		       ingestRequest.setPackageSize(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] file size: " + ingestRequest.getPackageSize());
		    } else if (item.getFieldName().equals("digestType")){
		       field = "digestType";
		       ingestRequest.getJob().setHashAlgorithm(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] algorithm: " + ingestRequest.getJob().getHashAlgorithm());
		    } else if (item.getFieldName().equals("digestValue")){
		       field = "digestValue";
		       ingestRequest.getJob().setHashValue(item.getString("utf-8"));
		       if (DEBUG) System.err.println("[debug] value: " + ingestRequest.getJob().getHashValue());
		    } else if (item.getFieldName().equals("creator")){
		       field = "creator";
		       ingestRequest.getJob().setObjectCreator(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("title")){
		       field = "title";
		       ingestRequest.getJob().setObjectTitle(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("date")){
		       field = "date";
		       ingestRequest.getJob().setObjectDate(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("localIdentifier")){
		       field = "localIdentifier";
		       ingestRequest.getJob().setLocalID(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("primaryIdentifier")){
		       field = "primaryIdentifier";
		       ingestRequest.getJob().setPrimaryID(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("note")){
		       field = "note";
		       ingestRequest.getJob().setNote(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("erc")){
		       field = "erc";
		       ingestRequest.getJob().setERC(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("notification")){
		       field = "notification";
		       ingestRequest.getJob().setAltNotification(item.getString("utf-8"));
		    } else if (item.getFieldName().equals("responseForm")) {
		       field = "responseForm";
        	       String responseForm = processFormatType(ingestRequest.getResponseForm(), item.getString("utf-8"));

            	       ingestRequest.setResponseForm(responseForm);
		       if (DEBUG) System.err.println("[debug] response form: " + ingestRequest.getResponseForm());
		    } else if (item.getFieldName().equals("notificationFormat")) {
		       field = "notificationFormat";
        	       String notificationForm = item.getString("utf-8");

            	       ingestRequest.setNotificationFormat(notificationForm);
		       if (DEBUG) System.err.println("[debug] notificationFormat: " + ingestRequest.getNotificationFormat());
		    } else if (item.getFieldName().equals("DC.contributor")) {
		       field = "DC.contributor";
        	       String DCcontributor = item.getString("utf-8");

            	       ingestRequest.setDCcontributor(DCcontributor);
		       if (DEBUG) System.err.println("[debug] DC.contributor: " + ingestRequest.getDCcontributor());
		    } else if (item.getFieldName().equals("DC.coverage")) {
		       field = "DC.coverage";
        	       String DCcoverage = item.getString("utf-8");

            	       ingestRequest.setDCcoverage(DCcoverage);
		       if (DEBUG) System.err.println("[debug] DC.coverage: " + ingestRequest.getDCcoverage());
		    } else if (item.getFieldName().equals("DC.creator")) {
		       field = "DC.creator";
        	       String DCcreator = item.getString("utf-8");

            	       ingestRequest.setDCcreator(DCcreator);
		       if (DEBUG) System.err.println("[debug] DC.creator: " + ingestRequest.getDCcreator());
		    } else if (item.getFieldName().equals("DC.date")) {
		       field = "DC.date";
        	       String DCdate = item.getString("utf-8");

            	       ingestRequest.setDCdate(DCdate);
		       if (DEBUG) System.err.println("[debug] DC.date: " + ingestRequest.getDCdate());
		    } else if (item.getFieldName().equals("DC.description")) {
		       field = "DC.description";
        	       String DCdescription = item.getString("utf-8");

            	       ingestRequest.setDCdescription(DCdescription);
		       if (DEBUG) System.err.println("[debug] DC.description: " + ingestRequest.getDCdescription());
		    } else if (item.getFieldName().equals("DC.format")) {
		       field = "DC.format";
        	       String DCformat = item.getString("utf-8");

            	       ingestRequest.setDCformat(DCformat);
		       if (DEBUG) System.err.println("[debug] DC.format: " + ingestRequest.getDCformat());
		    } else if (item.getFieldName().equals("DC.identifier")) {
		       field = "DC.identifier";
        	       String DCidentifier = item.getString("utf-8");

            	       ingestRequest.setDCidentifier(DCidentifier);
		       if (DEBUG) System.err.println("[debug] DC.identifier: " + ingestRequest.getDCidentifier());
		    } else if (item.getFieldName().equals("DC.language")) {
		       field = "DC.language";
        	       String DClanguage = item.getString("utf-8");

            	       ingestRequest.setDClanguage(DClanguage);
		       if (DEBUG) System.err.println("[debug] DC.language: " + ingestRequest.getDClanguage());
		    } else if (item.getFieldName().equals("DC.publisher")) {
		       field = "DC.publisher";
        	       String DCpublisher = item.getString("utf-8");

            	       ingestRequest.setDCpublisher(DCpublisher);
		       if (DEBUG) System.err.println("[debug] DC.publisher: " + ingestRequest.getDCpublisher());
		    } else if (item.getFieldName().equals("DC.relation")) {
		       field = "DC.relation";
        	       String DCrelation = item.getString("utf-8");

            	       ingestRequest.setDCrelation(DCrelation);
		       if (DEBUG) System.err.println("[debug] DC.relation: " + ingestRequest.getDCrelation());
		    } else if (item.getFieldName().equals("DC.rights")) {
		       field = "DC.rights";
        	       String DCrights = item.getString("utf-8");

            	       ingestRequest.setDCrights(DCrights);
		       if (DEBUG) System.err.println("[debug] DC.rights: " + ingestRequest.getDCrights());
		    } else if (item.getFieldName().equals("DC.source")) {
		       field = "DC.source";
        	       String DCsource = item.getString("utf-8");

            	       ingestRequest.setDCsource(DCsource);
		       if (DEBUG) System.err.println("[debug] DC.source: " + ingestRequest.getDCsource());
		    } else if (item.getFieldName().equals("DC.subject")) {
		       field = "DC.subject";
        	       String DCsubject = item.getString("utf-8");

            	       ingestRequest.setDCsubject(DCsubject);
		       if (DEBUG) System.err.println("[debug] DC.subject: " + ingestRequest.getDCsubject());
		    } else if (item.getFieldName().equals("DC.title")) {
		       field = "DC.title";
        	       String DCtitle = item.getString("utf-8");

            	       ingestRequest.setDCtitle(DCtitle);
		       if (DEBUG) System.err.println("[debug] DC.title: " + ingestRequest.getDCtitle());
		    } else if (item.getFieldName().equals("DC.type")) {
		       field = "DC.type";
        	       String DCtype = item.getString("utf-8");

            	       ingestRequest.setDCtype(DCtype);
		       if (DEBUG) System.err.println("[debug] DC.type: " + ingestRequest.getDCtype());
		    } else if (item.getFieldName().equals("DataCite.resourceType")) {
		       field = "DataCite.resourceType";
        	       String resourceType = item.getString("utf-8");

            	       ingestRequest.setDataCiteResourceType(resourceType);
		       if (DEBUG) System.err.println("[debug] DataCite.resourceType: " + ingestRequest.getDataCiteResourceType());
		    } else if (item.getFieldName().equals("synchronousMode")) {
		       field = "synchronousMode";
		       if (item.getString("utf-8").matches("true")) {
		           ingestRequest.setSynchronousMode(true);
		           if (DEBUG) System.err.println("[debug] Synchronous mode set");
			}
		    } else {
            	       System.err.println("[warning] Form field not supported: " + item.getFieldName());
		       // throw new TException.INVALID_OR_MISSING_PARM("Form field not supported: " + item.getFieldName());
		    }
	            item.delete();
	   	}
	    } catch (Exception e) {
            	throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Could not process form field: " + field);
	    }
	}

	// Process data
	iter = items.iterator();
	while (iter.hasNext()) {
    	   item = iter.next();
    	   if ( ! item.isFormField()) {
		String fieldName = item.getFieldName();
		String fileName = item.getName();
		String contentType = item.getContentType();
		boolean isInMemory = item.isInMemory();
		long sizeInBytes = item.getSize();
		String file = null;

		if (DEBUG) System.err.println("[debug] content field name: " + fieldName);
		if (DEBUG) System.err.println("[debug] content file name: " + fileName);
		if (DEBUG) System.err.println("[debug] content type: " + contentType);
		if (DEBUG) System.err.println("[debug] content size: " + sizeInBytes);

		if ((file = ingestRequest.getJob().getPackageName()) != null && filename ){
            	   if (DEBUG) System.out.println("[info] filename parameter set [modal]: " + file);
		   fileName = file;
		}
		ingestRequest.getJob().setPackageName(fileName);

            	if (DEBUG) System.out.println("extracting file: " + fileName);
		File uploadedFile = new File(queueDir, fileName);
		if (uploadedFile.exists()) {
	 	   // in case of multiple uploaded files or duplicate entries
		   uploadedFile = new File(queueDir, UUID.randomUUID().toString() + "-" + fileName);
            	   if (DEBUG) System.out.println("[warn] file exists renaming to : " + uploadedFile.getName());
		}

		try {
		   item.write(uploadedFile);
		} catch (FileNotFoundException fnfe) {
            	   throw new TException.REQUEST_INVALID(MESSAGE + "Input file not found. Likely cause is failure to specify input file.");
		} catch (Exception e) {
            	   throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + e);
		}

		// Simplified ingest code
		if (ingestRequest.getPackageType() == null) {
		    if (DEBUG) System.out.println("[info] No file type found.  Let's do our best to determine.");
		    if (fileName.endsWith(".gz") || fileName.endsWith(".tar") || fileName.endsWith(".zip") || fileName.endsWith(".bz")
			    || fileName.endsWith(".tgz") || fileName.endsWith(".bz2")) {
			ingestRequest.setPackageType("container");
		        if (DEBUG) System.out.println("[info] Found container extension.");
		    } else {
		        String manifestType = null;
		        manifestType = determineBatchManifest(new File(queueDir, fileName));
			
			if (manifestType == null) {
		            if (DEBUG) System.out.println("[info] File NOT a batch manifest.");
		            manifestType = determineObjectManifest(new File(queueDir, fileName));
			    if (manifestType == null) {
		        	if (DEBUG) System.out.println("[info] File NOT an object manifest.");
			    }
			}

		        if (manifestType != null) {
		            if (DEBUG) System.out.println("[info] Found checkm manifest.  Type is: " + manifestType);
			    ingestRequest.setPackageType(manifestType);
		    	} else {
			    ingestRequest.setPackageType("file");
		            if (DEBUG) System.out.println("[info] Unrecognized file.  Assume type is file");
			}
		    }
		}

    	   }
	}

	ingestRequest.setQueuePath(queueDir);
	if (DEBUG) System.out.println(ingestRequest.dump("submit"));
	return ingestRequest;

        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
	    ex.printStackTrace();
            throw new TException.REQUEST_INVALID(
                        "Unable to obtain manifest");
        }
	finally {
	}

    }

    /**
     * return manifest type
     * @param postedFile pathname of uploaded file
     */
    protected String determineBatchManifest(File manifestFile)
    {
	String manifestType = null;
        try {
            if (DEBUG) System.out.println("[info] Attempting to determine manifest type: " + manifestFile.getAbsolutePath());

            Manifest manifest = Manifest.getManifest(new TFileLogger("Jersey", -100, -100), ManifestRowAbs.ManifestType.batch);
            Enumeration<ManifestRowInf> en = manifest.getRows(manifestFile);
            ManifestRowBatch manifestRow = null;
	    String profile = null;
            while (en.hasMoreElements()) {
            	manifestRow = (ManifestRowBatch) en.nextElement();
	        profile = manifestRow.getProfile();

		if (profile != null) {
		    if (profile.endsWith("single-file-batch-manifest")) manifestType = "batchManifestFile";
		    else if (profile.endsWith("container-batch-manifest")) manifestType = "batchManifestContainer";
		    else if (profile.endsWith("batch-manifest")) manifestType = "batchManifest";
		}

		break;
	    }

	    return manifestType;

        } catch (Exception ex) {
	    // ex.printStackTrace();
            if (DEBUG) System.out.println("[warn] Could not determine if file is manifest: " + manifestFile.getName());
	    return manifestType;
        }
    }


    /**
     * return manifest type
     * @param postedFile pathname of uploaded file
     */
    protected String determineObjectManifest(File manifestFile)
    {
	String manifestType = null;
        try {
            if (DEBUG) System.out.println("[info] Attempting to determine manifest type: " + manifestFile.getAbsolutePath());

            Manifest manifest = Manifest.getManifest(new TFileLogger("Jersey", 10, 10), ManifestRowAbs.ManifestType.ingest);
            Enumeration<ManifestRowInf> en = manifest.getRows(manifestFile);
            ManifestRowIngest manifestRow = null;
	    String profile = null;
            while (en.hasMoreElements()) {
            	manifestRow = (ManifestRowIngest) en.nextElement();
	        profile = manifestRow.getProfile();

		if (profile != null) {
		    if (profile.endsWith("ingest-manifest")) manifestType = "manifest";
		}

		break;
	    }

	    return manifestType;
        } catch (Exception ex) {
	    // ex.printStackTrace();
            if (DEBUG) System.out.println("[warn] Could not determine if file is manifest: " + manifestFile.getName());
	    return manifestType;
        }
    }

    /**
    /**
     * return integer if valid or exception if not
     * @param header exception header
     * @param parm String value of parm
     * @return parsed int value
     * @throws TException
     */
    protected int getNumber(String header, String parm)
        throws TException
    {
        try {
            return Integer.parseInt(parm);

        } catch (Exception ex) {
            throw new JerseyException.BAD_REQUEST(header + ": Number required, found " + parm);
        }
    }


    /**
     * Get StateInf formatter using Jersey FormatType
     * Involves mapping Jersey FormatType to FormatterInf.Format type
     * @param outputFormat  Jersey formattype
     * @param logger process logger
     * @return Formatter
     * @throws TException process exception
     */
    protected FormatterInf getFormatter(FormatType outputFormat, LoggerInf logger)
        throws TException
    {
        String formatS = null;
        try {
            formatS = outputFormat.toString();
            FormatterInf.Format formatterType = FormatterInf.Format.valueOf(formatS);
            return FormatterAbs.getFormatter(formatterType, logger);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            if (DEBUG) System.err.println("getFormatter: stack:" + StringUtil.stackTrace(ex));
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("State formatter type not supported:" + formatS);
        }
    }

    /**
     * Normalize name of response file
     * @param fileResponseName non-normalized name
     * @param extension applied extension to name
     * @return normalized name
     */
    protected String getResponseFileName(String fileResponseName, String extension)
    {
        if (StringUtil.isEmpty(fileResponseName)) return "";
        if (StringUtil.isEmpty(extension)) extension = "";
        else extension = "." + extension;
        fileResponseName = getResponseName(fileResponseName) + extension;
        log("getResponseFileName=" + fileResponseName);
        return fileResponseName;
    }


    /**
     * Normalize response name
     * @param name name to normalize
     * @return normalized name
     */
    public static String getResponseName(String name)
    {
        if (StringUtil.isEmpty(name)) return "";
        StringBuffer buf = new StringBuffer();
        String test = "\"*+,<=>?^|";
        char c = 0;
        for (int i=0; i<name.length(); i++) {
            c = name.charAt(i);
            if ((c < 0x21) && (c >= 0)) continue;
            int pos = test.indexOf(c);
            if (pos >= 0) continue;
            buf.append(c);
        }
        if (buf.length() == 0) return "";
        name = buf.toString();
        name = name.replace('/', '=');
        name = name.replace('\\', '=');
        name = name.replace(':', '+');
        return name;
    }

    /**
     * return JerseyCleanup
     * @return JerseyCleanup
     */
    protected JerseyCleanup getJerseyCleanup()
    {
        return jerseyCleanup;
    }

    /**
     * If debug flag on then sysout this message
     * @param msg message to sysout
     */
    protected void log(String msg)
    {
        if (DEBUG) System.err.println("[JerseyBase]>" + msg);
        //logger.logMessage(msg, 0, true);
    }

    /**
     * Process http request header
     * @param accept request parm
     * @param formatType query parm
     */
    protected String processFormatType(String acceptParm, String formatType)
    {
	String newFormatType = "xml";	// default

        // form parameter overrides "Accept" header
        if (StringUtil.isEmpty(formatType)) {
            try {

                newFormatType = FormatType.valueOfMimeType(acceptParm).toString();
            } catch (Exception e) {
                System.out.println("[warning] format type not supported: " + acceptParm + " - setting to: " + newFormatType);
            }
        } else {
	    try {
                FormatType format = FormatType.valueOf(formatType.toLowerCase());
                newFormatType = format.toString();
	    } catch (Exception e) {
                System.out.println("[warning] format type not supported: " + formatType + " - setting to: " + newFormatType);
	    }
	}
	return newFormatType;
    }


    /**
     * Container class for file and Jersey FormatType enum
     */
    public class TypeFile
    {
        public FormatType formatType = null;
        public File file = null;
        public String id = null;
    }

}
