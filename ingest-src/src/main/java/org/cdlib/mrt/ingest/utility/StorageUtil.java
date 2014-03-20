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

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;

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
    public static final int STORAGE_READ_TIMEOUT = 10800000; 		// 3 hours
    public static final int STORAGE_CONNECT_TIMEOUT = 10800000;	


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
	    // First lets fetch from our localID DB on Storage
	    String primaryID = null;
            StoreNode storeNode = profileState.getTargetStorage();

            // build REST url
            String url = storeNode.getStorageLink().toString() + "/primary/" + storeNode.getNodeID() + "/" + 
			URLEncoder.encode(profileState.getOwner(), "utf-8") + "/" +
			URLEncoder.encode(localID,  "utf-8") + "?t=xml";
            if (DEBUG) System.out.println("[debug] PrimaryID fetch URL: " + url);
            Client client = Client.create();    // reuse?  creation is expensive

	    /* fix client timeout problem */
	    client.setConnectTimeout(new Integer(STORAGE_CONNECT_TIMEOUT));
	    client.setReadTimeout(new Integer(STORAGE_READ_TIMEOUT));

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
                if (DEBUG) System.out.println("[debug] Can not determine primary ID from storage DB");
		primaryID = null;
            }

	    // Next lets try EZID if local looks like a DOI
	    // note: EZID service is SSL so don't use Jersey client
	    if (primaryID == null && localID.toLowerCase().startsWith("doi:")) {
                // build REST url
                url = profileState.getObjectMinterURL().toString();
                url = url.replaceFirst("/shoulder.*", "/id/") + localID.toLowerCase();

                System.out.println("[info] " + MESSAGE + "Searching for shadowedBy ID: " + url);

		DefaultHttpClient httpClient = new DefaultHttpClient();
                httpClient = wrapClient(httpClient);

		// authenticate
		String misc = null;
		if ((misc = profileState.getMisc()) == null) {
                    System.err.println("[warning] " + MESSAGE + "EZID credentials not found.");
                    throw new TException.GENERAL_EXCEPTION("EZID credentials not found.");
		}


		String[] auth = misc.split(":");
		Credentials credentials = new UsernamePasswordCredentials( auth[0], auth[1] );
		httpClient.getCredentialsProvider().setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), credentials);
		HttpGet httpGet = null;

		try {
		    httpGet = new HttpGet(url);
		} catch (java.lang.IllegalArgumentException iae) {
		    throw new TException.INVALID_OR_MISSING_PARM("Target hostname or primary ID not valid: " + url);
		}
		httpGet.addHeader("Accept", "text/plain");


		String responseBody = null;
		HttpResponse httpResponse = null;

		try {
		    httpResponse = httpClient.execute(httpGet);
		    responseBody = StringUtil.streamToString(httpResponse.getEntity().getContent(), "UTF-8");
		} catch (HttpHostConnectException hhce) {
		    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("error in connecting to host: " + url);
		} catch (org.apache.http.client.HttpResponseException hre) {
		    System.err.println("[error] " + MESSAGE + "request failed with status code: " + hre.getStatusCode());
		    System.err.println("[error] " + MESSAGE + "request failed with status message: " + hre.getMessage());
		    responseBody = "failed";
		}

		status = httpResponse.getStatusLine().getStatusCode();

                if (status == 400) {
		    // DOI not registered with EZID
		    if (DEBUG) System.out.println("[warn] " + MESSAGE + " DOI local ID is NOT registered with EZID: " + localID);
		    primaryID = null;
		} else if (status == 200) {
		    // Found DOI in EZID
		    String coowners = "";
		    String owner = "";
		    // extract ARK listed as "_shadowedBy"
		    String[] responseSplit = responseBody.split("\n");
		    for (String element : responseSplit) {
			if (element.startsWith("_shadowedby:")) {
			    primaryID = element.substring(element.indexOf(":") + 1).trim();
			    if (DEBUG) System.out.println("[debug] " + MESSAGE + " Found DOI in EZID, shadow ark is: " + primaryID);
			}
			if (element.startsWith("_coowners:")) {
			    coowners = element.substring(element.indexOf(":") + 1).trim();
			}
			if (element.startsWith("_owner:")) {
			    owner = element.substring(element.indexOf(":") + 1).trim();
			}
		    }
		    // Are we permitted to update this DOI?
		    if ((! coowners.contains("merritt")) && (! owner.contains("merritt"))) {
                        throw new TException.REQUEST_INVALID("[error] " + NAME + ": Merritt not authorized to update DOI: " + url);
		    }
                } else {
		    // error
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": EZID service: " + url);
                }
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
            client.setConnectTimeout(new Integer(STORAGE_CONNECT_TIMEOUT));
            client.setReadTimeout(new Integer(STORAGE_READ_TIMEOUT));

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

    // http://theskeleton.wordpress.com/2010/07/24/
        // avoiding-the-javax-net-ssl-sslpeerunverifiedexception-peer-not-authenticated-with-httpclient/
    public static DefaultHttpClient wrapClient(DefaultHttpClient base) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {

                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException { }

                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException { }

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory ssf = new SSLSocketFactory(ctx);
            ssf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
