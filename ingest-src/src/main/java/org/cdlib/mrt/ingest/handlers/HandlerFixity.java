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
package org.cdlib.mrt.ingest.handlers;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.FormDataMultiPart;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

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

    protected static final String NAME = "HandlerFixity";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;

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

  	ClientResponse response = null;
	String fixityURL = null;

	try {

	    // build REST url 
	    try {
	        fixityURL = profileState.getFixityURL().toString() + "/add";
	    } catch (Exception e) {
		throw new TException.REQUEST_ELEMENT_UNSUPPORTED("[error] " + NAME + ": No fixity service url specified.");
	    }
	    Client client = Client.create();	// reuse?  creation is expensive
	    WebResource webResource = client.resource(fixityURL);
            Manifest manifest = Manifest.getManifest(new TFileLogger("Jersey", 10, 10), ManifestRowAbs.ManifestType.add);


	    // use storage manifest to define components
            FileInputStream manifestInputStream =
		 new FileInputStream(ingestRequest.getQueuePath().getAbsolutePath() + "/system/mrt-manifest.txt");
            Enumeration<ManifestRowInf> enumRow = manifest.getRows(manifestInputStream);
            FileComponentContentInf rowIn = null;
            FileComponent fileComponent = null;
	    String digestType = "sha-256";
	    String context = "|objectid=%s|versionid=%s|fileid=%s|";

	    // submit each component
            while (enumRow.hasMoreElements()) {
                FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
                rowIn = (FileComponentContentInf)enumRow.nextElement();
                fileComponent = rowIn.getFileComponent();
                if (DEBUG) {
                    System.out.println(fileComponent.dump("HandlerFixity"));
                }

		// make service request
            	formDataMultiPart.field("url", fileComponent.getURL().toString());
            	formDataMultiPart.field("source", "web");
            	formDataMultiPart.field("size", fileComponent.getSize() + "");
            	formDataMultiPart.field("digest-type", digestType);
            	formDataMultiPart.field("digest-value", fileComponent.getMessageDigest(digestType).getValue());
            	formDataMultiPart.field("context", String.format(context, jobState.getPrimaryID().getValue(), jobState.getVersionID().toString(),
			fileComponent.getIdentifier()));
            	formDataMultiPart.field("note", "");
            	formDataMultiPart.field("responseForm", "xml");

	        // make service request
	        try {
  	            response = webResource.type(MediaType.MULTIPART_FORM_DATA).post(ClientResponse.class, formDataMultiPart);
	        } catch (Exception e) {
		    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": fixity service: " + fixityURL); 
	        }
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + " response code " + response.getStatus());

	        if (response.getStatus() != 200) {
		    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": fixity service: " + fixityURL); 
	        }
            }
	    return new HandlerResult(true, "SUCCESS: fixity request", response.getStatus());
	} catch (TException te) {
	    te.printStackTrace();
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing fixity request: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    response = null;
	}
    }
   
    public String getName() {
        return NAME;
    }
}

