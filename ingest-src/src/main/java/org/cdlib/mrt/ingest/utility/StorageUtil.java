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
package org.cdlib.mrt.ingest.utility;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.Integer;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.net.URL;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.ManifestXML;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Storage wrapper methods
 * @author mreyes
 */
public class StorageUtil
{

    private static final String NAME = "StorageUtil";
    private static final String MESSAGE = NAME + ": ";
    private LoggerInf logger = null;
    private static final boolean DEBUG = true;
    private static StoreNode storeNode = null;


    public static String getStorageManifest(ProfileState profileState, String objectID)
        throws TException
    {

        try {
	    File manifestFile = getStorageFile(profileState, objectID, null);
	    if (manifestFile == null) return null;
	    else return FileUtil.file2String(manifestFile);

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in accessing Storage Manifest - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("Error in accessing Storage Manifest");
	}
    }


    public static File getStorageFile(ProfileState profileState, String objectID, String filename)
        throws TException
    {
       ClientResponse clientResponse = null;
       String storageURL = null;

        try {
	    String type = "/content/";
            storeNode = profileState.getTargetStorage();

	    // assume we want a manifest
	    if (StringUtil.isEmpty(filename)) {
		type = "/manifest/";
		filename = "";
	    } else {
		// version and filename
		filename = "/0/" + URLEncoder.encode(filename, "UTF-8");
	    }

            // build REST url 
            storageURL = storeNode.getStorageLink().toString() + type + storeNode.getNodeID() + "/" + URLEncoder.encode(objectID, "UTF-8") + filename;
            Client client = Client.create();    // reuse?  creation is expensive
            WebResource webResource = client.resource(storageURL);

            if (DEBUG) System.out.println("[debug] " + MESSAGE + " storage URL: " + storageURL);

            // make service request
            try {
                clientResponse = webResource.type(MediaType.APPLICATION_FORM_URLENCODED).get(ClientResponse.class);
            } catch (com.sun.jersey.api.client.ClientHandlerException che) {
                che.printStackTrace();
                String msg = "[error] " + MESSAGE + "Could not connect to Storage service: " 
	 	    + profileState.getTargetStorage().getStorageLink().toString() + " --- " + che.getMessage();
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
            } catch (Exception e) {
		e.printStackTrace();
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + storageURL);
            }

            if (clientResponse.getStatus() == 404) return null;
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
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + storageURL);
                }
            }

            if (DEBUG) System.out.println("[debug] " + MESSAGE + " storage URL response: " + clientResponse.toString());
	    // return clientResponse.getEntity(String.class);
	    return clientResponse.getEntity(File.class);

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in accessing Storage file - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("Error in accessing Storage file");
	}
    }


    public static VersionMap getVersionMap(String objectID, String storageManifest)
        throws TException
    {
       InputStream inputstream = null;
       Identifier identifier = null;

        try {
            inputstream = new ByteArrayInputStream(storageManifest.getBytes());
            identifier = new Identifier(objectID);

            return ManifestXML.getVersionMap(identifier, null, inputstream);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create version map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
                identifier = null;
                inputstream = null;
            } catch (Exception e) {}
        }
    }


    public static List<File> getCurrentFiles(VersionMap versionMap, int version)
        throws TException
    {
       List<File> files = null;
       List<FileComponent> fileComponents = null;

        try {
            fileComponents = versionMap.getVersionComponents(version);

            for (FileComponent fileComponent: fileComponents)
                files.add(fileComponent.getComponentFile());

            return files;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create version map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }


    public static String fetchPrimaryID(ProfileState profileState, String localID)
        throws TException
    {

        ClientResponse clientResponse = null;
        try {

	    String primaryID = null;
            StoreNode storeNode = profileState.getTargetStorage();

            // build REST url
            String url = storeNode.getStorageLink().toString() + "/primary/" + storeNode.getNodeID() + "/" + 
			URLEncoder.encode(profileState.getOwner(), "utf-8") + "/" +
			URLEncoder.encode(localID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] PrimaryID fetch URL: " + url);
            Client client = Client.create();    // reuse?  creation is expensive

	    /* fix client timeout problem */
	    client.setReadTimeout(new Integer(30000));
	    client.setConnectTimeout(new Integer(30000));

            WebResource webResource = client.resource(url);

            // make service request
            clientResponse = webResource.get(ClientResponse.class);
	    int status = clientResponse.getStatus();
   	    String response = null;
 
	    if (status != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                    throw new TException.REQUEST_INVALID(tExceptionResponse.getError());
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url);
                }
	    } else {
   	        response = clientResponse.getEntity(String.class);
	    }

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            domFactory.setExpandEntityReferences(true);

            DocumentBuilder builder = domFactory.newDocumentBuilder();
            builder.setErrorHandler(null);
            Document document = builder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression expr = xpath.compile("//*[local-name()='primaryIdentifier']");

            String xpathS = (String) expr.evaluate(document);
            if (StringUtil.isNotEmpty(xpathS)) {
                if (DEBUG) System.out.println("[debug] primary ID: " + xpathS);
                primaryID = xpathS;
            } else {
                if (DEBUG) System.out.println("[debug] Can not determine primary ID");
		primaryID = null;
            }
            return primaryID;

        } catch (TException te) {
	    throw te;
        } catch (com.sun.jersey.api.client.ClientHandlerException che) {
            che.printStackTrace();
            String msg = "[error] " + MESSAGE + "Could not connect to Storage service: " 
	        + profileState.getTargetStorage().getStorageLink().toString() + " --- " + che.getMessage();
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to map localID. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }

    public static String fetchLocalID(ProfileState profileState, String primaryID)
        throws TException
    {

        ClientResponse clientResponse = null;
        try {

	    //Vector localIDs = null;
	    String localIDs = null;
            StoreNode storeNode = profileState.getTargetStorage();

            // build REST url
            String url = storeNode.getStorageLink().toString() + "/local/" + storeNode.getNodeID() + "/" + 
			URLEncoder.encode(primaryID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] LocalID fetch URL from storage db: " + url);
            Client client = Client.create();    // reuse?  creation is expensive

            /* fix client timeout problem */
            client.setConnectTimeout(new Integer(30000));
            client.setReadTimeout(new Integer(30000));

            WebResource webResource = client.resource(url);

            // make service request
            clientResponse = webResource.get(ClientResponse.class);
	    int status = clientResponse.getStatus();
   	    String response = null;
 
	    if (status != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    TExceptionResponse.REQUEST_INVALID tExceptionResponse = clientResponse.getEntity(TExceptionResponse.REQUEST_INVALID.class);
                    throw new TException.REQUEST_INVALID(tExceptionResponse.getError());
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + url);
                }
	    } else {
   	        response = clientResponse.getEntity(String.class);
	    }

            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setNamespaceAware(true);
            domFactory.setExpandEntityReferences(true);

            DocumentBuilder builder = domFactory.newDocumentBuilder();
            builder.setErrorHandler(null);
            Document document = builder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
            XPath xpath = XPathFactory.newInstance().newXPath();
            XPathExpression expr = xpath.compile("//*[local-name()='localID']");

 	    NodeList nl = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    	    int i = 0;
    	    for (i = 0; i < nl.getLength(); i++) {
		Node n = nl.item(i);
		String localid = n.getTextContent();
		if (StringUtil.isNotEmpty(localid)) {
                    if (DEBUG) System.out.println("[debug] LocalID found (storage db): " + localid);
		    if (localIDs == null) localIDs = localid;
		    else localIDs += "; " + localid;
		}
    	    }

	    if (StringUtil.isEmpty(localIDs)) if (DEBUG) System.out.println("[debug] Can not determine local ID");
            return localIDs;

        } catch (TException te) {
	    throw te;
        } catch (com.sun.jersey.api.client.ClientHandlerException che) {
            che.printStackTrace();
            String msg = "[error] " + MESSAGE + "Could not connect to Storage service: " 
	        + profileState.getTargetStorage().getStorageLink().toString() + " --- " + che.getMessage();
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to map primaryID to localID. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }
}
