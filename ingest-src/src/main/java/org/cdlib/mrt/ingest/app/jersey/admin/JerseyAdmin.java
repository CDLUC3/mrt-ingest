/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

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
package org.cdlib.mrt.ingest.app.jersey.admin;

import java.util.Map;
import java.util.HashMap;

import org.glassfish.jersey.server.CloseableService;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.QueryParam;

import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.app.jersey.JerseyBase;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Thin Jersey layer for Admin servlet
 * @author mreyes
 */
@Path ("/")
public class JerseyAdmin extends JerseyBase
{

    protected static final String NAME = "JerseyAdmin";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT = FormatterInf.Format.xml;
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");

    // Show service status
    @GET
    @Path("/state")
    public Response getServiceState(
            @QueryParam("t") String formatType,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            formatType = processFormatType(request.getHeader("Accept"), formatType);	// xml default
            log("getState entered:" + " - formatType=" + formatType);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getServiceState();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Show service help
    @GET
    @Path("/help")
    public Response getHelp(
            @QueryParam("t") String formatType,		// xml default
	    @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            formatType = processFormatType(request.getHeader("Accept"), formatType);
            log("getHelp entered:" + " - formatType=" + formatType);

            return getServiceState(formatType, request, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    @POST
    @Path("reset")
    public Response callResetState(
            @DefaultValue("-none-") @QueryParam("log4jlevel") String log4jlevel,
            @DefaultValue("json") @QueryParam("t") String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("getResetState entered:"
                    + " - formatType=" + formatType
                    + " - log4jlevel=" + log4jlevel
                    );
            if (!log4jlevel.equals("-none-")) {
                Log4j2Util.setRootLevel(log4jlevel);
            }
            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getServiceState();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            log(tex.toString());
            throw tex;

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            log(ex.toString());
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Collection Pause or Thaw submissions^M 
    @POST
    @Path("/submission/{request: freeze|thaw}/{collection}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)    // Container, component or manifest file
    public Response postSubmissionAction(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("request") String action,
            @PathParam("collection") String collection,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("Collection processing submission request: " + action + " collection: " + collection);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.postSubmissionAction(action, collection);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Pause or Thaw all submissions
    @POST
    @Path("/submissions/{request: freeze|thaw}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)    // Container, component or manifest file
    public Response postSubmissionAction(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("request") String action,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("Global processing submission request: " + action);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.postSubmissionAction(action, null);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Queue File State
    @GET
    @Path("/bids/{batchAge}")
    public Response getBatchFiletate(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchAge") Integer batchAge,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getBatches for files : " + batchAge.toString() + " days old and newer");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getQueueFileState(batchAge);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


   // Batch File State
    @GET
    @Path("/bid/{batchID}")
    public Response getBatchFiletate(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchID") String batchID,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getBatchID: " + batchID);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getBatchFileState(batchID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Batch File State with age
    @GET
    @Path("/bid/{batchID}/{batchAge}")
    public Response getBatchFiletate(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchID") String batchID,
            @PathParam("batchAge") Integer batchAge,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getBatchID: " + batchID + " for jobs " + batchAge.toString() + " days old and newer");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getBatchFileState(batchID, batchAge);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


    // Job File State
    @GET
    @Path("/jid-erc/{batchID}/{jobID}")
    public Response getJobFileState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchID") String batchID,
            @PathParam("jobID") String jobID,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getFileID: " + batchID);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getJobFileState(batchID, jobID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    // Job File View
    @GET
    @Path("/jid-file/{batchID}/{jobID}")
    public Response getJobViewState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchID") String batchID,
            @PathParam("jobID") String jobID,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing get Job File View: " + batchID + ":" + jobID);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getJobViewState(batchID, jobID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    // Job Manifest State
    @GET
    @Path("/jid-manifest/{batchID}/{jobID}")
    public Response getJobManifestState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("batchID") String batchID,
            @PathParam("jobID") String jobID,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getManifest: " + batchID);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getJobManifestState(batchID, jobID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            System.out.println("[TRACE] " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }


}
