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
package org.cdlib.mrt.ingest.app.jersey.post;

import com.sun.jersey.spi.CloseableService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;                                                                                                                                
import javax.ws.rs.core.Context;                                                                                                                            
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;                                                                                                                           
import javax.ws.rs.GET;                                                                                                                                     
import javax.ws.rs.POST;                                                                                                                                    
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.app.jersey.JerseyBase;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Thin Jersey layer for Post servlet
 * @author mreyes
 */
@Path ("/")
public class JerseyPost extends JerseyBase
{

    protected static final String NAME = "JerseyPost";
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


    // Get job status
    // NOT YET SUPPORTED
    @GET
    @Path("/status")
    public Response getBatchStates(
            @QueryParam("t") String formatType,
            @DefaultValue("FAILED") @QueryParam("f") String type,
            @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        LoggerInf logger = null;
        try {
            formatType = processFormatType(request.getHeader("Accept"), formatType);	// xml default
            log("getBatchStates entered:" + " - formatType=" + formatType);

            IngestServiceInit ingestServiceInit = IngestServiceInit.getIngestServiceInit(sc);
            IngestServiceInf ingestService = ingestServiceInit.getIngestService();
            logger = ingestService.getLogger();
            StateInf responseState = ingestService.getStatus(type);
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


    // Update object 
    // No object ID supplied in URL
    @POST
    @Path("{request: update}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)	// Container, component or manifest file
    public Response update(
	    @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        log("processing update(no ID, multi-part)");

        // Accept is overridden by responseForm form parm
        String responseForm = "";
        try {
            responseForm = processFormatType(request.getHeader("Accept"), "");
        } catch (Exception e) {}
        if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

	IngestRequest ingestRequest = new IngestRequest();
        ingestRequest.setResponseForm(responseForm);
        ingestRequest.getJob().setPrimaryID(null);
        ingestRequest.setUpdateFlag(true);

        return submitPost(ingestRequest, request, cs, sc);
    }


    // Update object 
    // Object ID supplied in URL
    @POST
    @Path("{request: update}/{scheme}/{shoulder}/{objectid}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)	// Container, component or manifest file
    public Response update(
            @PathParam("scheme") String scheme,
            @PathParam("shoulder") String shoulder,
            @PathParam("objectid") String object,
	    @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        log("processing update (ID, multi-part)");

        // Accept is overridden by responseForm form parm
        String responseForm = "";
        try {
            responseForm = processFormatType(request.getHeader("Accept"), "");
        } catch (Exception e) {}
        if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

	IngestRequest ingestRequest = new IngestRequest();
        ingestRequest.setResponseForm(responseForm);
        ingestRequest.getJob().setPrimaryID(scheme + "/" + shoulder + "/" + object);
        ingestRequest.setUpdateFlag(true);

        return submitPost(ingestRequest, request, cs, sc);
    }


    // Submit entire object 
    // No object ID supplied in URL
    @POST
    @Path("{request: submit|add}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)	// Container, component or manifest file
    public Response submit(
	    @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        log("processing submit (no ID, multi-part)");

        // Accept is overridden by responseForm form parm
        String responseForm = "";
        try {
            responseForm = processFormatType(request.getHeader("Accept"), "");
        } catch (Exception e) {}
        if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

	IngestRequest ingestRequest = new IngestRequest();
        ingestRequest.setResponseForm(responseForm);
        ingestRequest.getJob().setPrimaryID(null);

        return submitPost(ingestRequest, request, cs, sc);
    }


    // Submit entire object 
    // Object ID supplied in URL
    @POST
    @Path("{request: submit|add}/{scheme}/{shoulder}/{objectid}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)	// Container, component or manifest file
    public Response submit(
            @PathParam("scheme") String scheme,
            @PathParam("shoulder") String shoulder,
            @PathParam("objectid") String object,
	    @Context HttpServletRequest request,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        log("processing submit (ID, multi-part)");

        // Accept is overridden by responseForm form parm
        String responseForm = "";
        try {
            responseForm = processFormatType(request.getHeader("Accept"), "");
        } catch (Exception e) {}
        if (StringUtil.isNotEmpty(responseForm)) log("Accept header: - formatType=" + responseForm);

	IngestRequest ingestRequest = new IngestRequest();
        ingestRequest.setResponseForm(responseForm);
        ingestRequest.getJob().setPrimaryID(scheme + "/" + shoulder + "/" + object);

        return submitPost(ingestRequest, request, cs, sc);
    }

}
