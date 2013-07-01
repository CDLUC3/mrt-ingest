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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.FormDataMultiPart;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.ByteArrayDataSource;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileComponentContentInf;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.URLEncoder;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


/**
 * Call fixity service with add item request
 * @author mreyes
 */
public class HandlerFixity extends Handler<JobState>
{

    private static final String NAME = "HandlerFixity";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String CONTEXT_DELIM = "<>";
    private LoggerInf logger = null;
    private Properties conf = null;
    private boolean notify = true;	// notify admins if failure
    private boolean error = false;

    /**
     * Adds an item of requested object to ingest service
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

  	ClientResponse clientResponse = null;
	String fixityURL = null;

	try {

	    // build REST url 
	    try {
	        fixityURL = profileState.getFixityURL().toString();
	    } catch (Exception e) {
		throw new TException.REQUEST_ELEMENT_UNSUPPORTED("[error] " + NAME + ": No fixity service url specified.");
	    }
	    Client client = Client.create();	// reuse?  creation is expensive
	    WebResource webResourceAdd = client.resource(fixityURL + "/add");
	    WebResource webResourceUpdate = client.resource(fixityURL + "/update");
            Manifest manifest = Manifest.getManifest(new TFileLogger("Jersey", 10, 10), ManifestRowAbs.ManifestType.add);


	    // use storage manifest to define components
            FileInputStream manifestInputStream =
		 new FileInputStream(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
            Enumeration<ManifestRowInf> enumRow = manifest.getRows(manifestInputStream);
            FileComponentContentInf rowIn = null;
            FileComponent fileComponent = null;
	    String digestType = "sha-256";
	    String context = null;
	    String contextMain = "|objectid=%s|versionid=%s|fileid=%s|";
	    String contextOwner = "|owner=%s|";
	    String contextCollection = "|member=%s|";

	    // submit each component
            while (enumRow.hasMoreElements()) {
                FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
                rowIn = (FileComponentContentInf)enumRow.nextElement();
                fileComponent = rowIn.getFileComponent();
                if (DEBUG) {
                    System.out.println(fileComponent.dump("HandlerFixity"));
                }

		// make service request
            	formDataMultiPart.field("url", createStorageURL(fileComponent, jobState, profileState));
            	formDataMultiPart.field("source", "web");
            	formDataMultiPart.field("size", fileComponent.getSize() + "");
            	formDataMultiPart.field("digest-type", digestType);
            	formDataMultiPart.field("digest-value", fileComponent.getMessageDigest(digestType).getValue());

		// context main
            	context = String.format(contextMain, jobState.getPrimaryID().getValue(), jobState.getVersionID().toString(), fileComponent.getIdentifier());
		// context w/ owner
                context += CONTEXT_DELIM + String.format(contextOwner, profileState.getOwner());
		// update context w/ members
                Vector<String> members = profileState.getCollection();
		for (String member: members) {
		    context += CONTEXT_DELIM + String.format(contextCollection, member);
		}
            	formDataMultiPart.field("context", context);

            	formDataMultiPart.field("note", "");
            	formDataMultiPart.field("responseForm", "xml");		// alignment w/ fixity spec. 
            	formDataMultiPart.field("response-form", "xml");

	        // make initial service request
	        try {
  	            clientResponse = webResourceAdd.type(MediaType.MULTIPART_FORM_DATA).post(ClientResponse.class, formDataMultiPart);
	        } catch (Exception e) {
		    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": fixity service: " + fixityURL + "/add"); 
	        }
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + " ADD response code " + clientResponse.getStatus());

	        if (clientResponse.getStatus() != 200) {
                    try {
			// most likely exception
			// can only call once, as stream is not reset
                        TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE tExceptionResponse = clientResponse.getEntity(TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE.class);
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(tExceptionResponse.getError());
		    } catch (TException te) {
		        throw te;
                    } catch (Exception e) {
		        // let's report something
		        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": fixity service: " + fixityURL + "/add"); 
	            }
		}
            }

	    return new HandlerResult(true, "SUCCESS: fixity request", clientResponse.getStatus());
	} catch (TException te) {
	    error = true;
            te.printStackTrace(System.err);
	    // does not cause ingest failure
            return new HandlerResult(true, te.getDetail());
	} catch (Exception e) {
	    error = true;
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing fixity request: " + e.getMessage();

	    // does not cause ingest failure
            return new HandlerResult(true, msg);
	} finally {
	     if (error && notify) notify(jobState, profileState, ingestRequest);
	    clientResponse = null;
	}
    }
   
    public String getName() {
        return NAME;
    }

    public String createStorageURL(FileComponent fileComponent, JobState jobState, ProfileState profileState) {
	String url = "";
	try {
            url = profileState.getTargetStorage().getStorageLink().toString() + "/content/" + 
		profileState.getTargetStorage().getNodeID() + "/" + 
		URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8") + "/" + jobState.getVersionID().toString() + "/" +
		URLEncoder.encode(fileComponent.getIdentifier(), "utf-8") + 
		 "?fixity=no";
	    return url;

	} catch (Exception e) { return url; }
    }

    public void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
	String server = "";
	String owner = "";
        MultiPartEmail email = new MultiPartEmail();

	try {
            email.setHostName("localhost");     // production machines are SMTP enabled
            if (profileState.getAdmin() != null) {
                for (Iterator<String> admin = profileState.getAdmin().iterator(); admin.hasNext(); ) {
                    // admin will receive notifications
                    String recipient = admin.next();
                    if (StringUtil.isNotEmpty(recipient)) email.addTo(recipient);
                }
            }
            String ingestServiceName = ingestRequest.getServiceState().getServiceName();
            if (StringUtil.isNotEmpty(ingestServiceName))
                if (ingestServiceName.contains("Development")) server = " [Development]";
                else if (ingestServiceName.contains("Stage")) server = " [Stage]";
            if (StringUtil.isNotEmpty(profileState.getContext())) owner = " [owner: " + profileState.getContext() + "]";
            email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");
            email.setSubject("[Warning] Fixity service not available" + server + owner);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
	} catch (Exception e) {};

        return;
    }
}

