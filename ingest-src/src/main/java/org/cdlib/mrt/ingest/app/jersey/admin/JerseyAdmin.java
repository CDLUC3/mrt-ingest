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

import java.io.InputStream;
import java.util.Map;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import com.sun.jersey.multipart.FormDataParam;
import com.sun.jersey.spi.CloseableService;
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
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.utility.ResponseFormEnum;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
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


    // Get queue state 
    // need to also support state/queue/job/..
    @GET
    @Path("/queues")
    public Response getIngestQueueState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getIngestQueueState");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getIngestQueueState();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException.REQUESTED_ITEM_NOT_FOUND renf) {
            return getStateResponse(renf, formatType, logger, cs, sc);
        } catch (TException tex) {
            throw tex;
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    // Get queue state  - default
    @GET
    @Path("/queue")
    public Response getQueueDefaultState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing Default getQueueState");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getQueueState(null);
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



    // Get queue state 
    // need to also support state/queue/job/..
    @GET
    @Path("/queue/{queue}")
    public Response getQueueState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("queue") String queue,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getQueueState");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getQueueState("/" + queue);
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


    // Get profiles state - Names only
    @GET
    @Path("/profiles")
    public Response getProfilesState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getProfilesState");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getProfilesState();
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


    // Get profiles state - Full
    @GET
    @Path("/profiles-full")
    public Response getProfilesFullState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getProfilesFullState");

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getProfilesFullState();
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


    // Profile state
    @GET
    @Path("/profile/{profile}")
    public Response getProfileState(
            @DefaultValue("json") @QueryParam("t") String formatType,
            @PathParam("profile") String profile,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing getProfileState" + profile);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getProfileState(profile);
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

    // Pause or Thaw submissions
    @POST
    @Path("/submissions/{request: freeze|thaw}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)    // Container, component or manifest file
    public Response postSubmissionAction(
            @DefaultValue("json") @QueryParam("t") String formatType,
            // @PathParam("action") String action,
            @PathParam("request") String action,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            log("processing submission request: " + action);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.postSubmissionAction(action);
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

    // Create profile: submission or admin (collection/owner/sla)
    @POST
    @Path("/profile/{type: profile|collection|owner|sla}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)    // Container, component or manifest file
    public Response postProfileAction(
            @DefaultValue("json") @QueryParam("t") String formatType,
	    @PathParam("type") String type,
            @DefaultValue("") @FormDataParam("environment") String environment,
            @DefaultValue("") @FormDataParam("name") String name,
            @DefaultValue("") @FormDataParam("description") String description,
            @DefaultValue("") @FormDataParam("collection") String collection,
            @DefaultValue("") @FormDataParam("ark") String ark,
            @DefaultValue("") @FormDataParam("owner") String owner,
            @DefaultValue("") @FormDataParam("notification") String notification,
            @DefaultValue("") @FormDataParam("context") String context,
            @DefaultValue("") @FormDataParam("storagenode") String storagenode,
            @DefaultValue("") @FormDataParam("creationdate") String creationdate,
            @DefaultValue("") @FormDataParam("modificationdate") String modificationdate,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            
	    log("processing profile request: " + type);
	    Map<String, String> profileParms = new HashMap();
	    // Only add Template data in Map
	    profileParms.put("NAME", name);
	    profileParms.put("DESCRIPTION", description);
	    profileParms.put("COLLECTION", collection);
	    profileParms.put("ARK", ark);
	    profileParms.put("OWNER", owner);
	    profileParms.put("CONTEXT", context);
	    profileParms.put("STORAGENODE", storagenode);
	    profileParms.put("CREATIONDATE", creationdate);
	    profileParms.put("MODIFICATIONDATE", modificationdate);

            // Accept is overridden by responseForm form parm
            String responseForm = "";
            try {
                responseForm = processFormatType(request.getHeader("Accept"), "");
            } catch (Exception e) {}
            if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

            System.out.println("NAME: " + name);
            System.out.println("STORAGENODE: " + storagenode);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.postProfileAction(type, environment, notification, profileParms);
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
