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

import java.nio.charset.Charset;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.vocabulary.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpression;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.ResourceMapUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


/**
 * characterize object components
 * @author mreyes
 */
public class HandlerCharacterize extends Handler<JobState>
{

    private static final String NAME = "HandlerCharacterize";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;

    /**
     * characterize object components
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState job state
     * @return HandlerResult result in creating manifest
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {
	    URL url = profileState.getCharacterizationURL();
	    try {
	        if (StringUtil.isEmpty(url.toString())) {
	            System.err.println("[warn] " + MESSAGE + "URL has not been set.  Skipping characterization.");
	    	    return new HandlerResult(true, "SUCCESS: " + NAME + " Skipping characterized");
		} else {
		    if (DEBUG) System.out.println("[debug] " + MESSAGE + " found Char. URL: " + url.toString());
		}
	    } catch (java.lang.NullPointerException npe) {
	        System.err.println("[warn] " + MESSAGE + "URL has not been set.  Skipping characterization.");
	        return new HandlerResult(true, "SUCCESS: " + NAME + " Skipping characterized");
	    }

            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File metadataFile = new File(systemTargetDir, "mrt-jhove2.xml");
            File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");

	    // iterate through all components
	    String characterizeString = ""; 
	    Vector files = new Vector();
	    FileUtil.getDirectoryFiles(new File(ingestRequest.getQueuePath(), "/producer"), files);
	    for (Object object : files.toArray()) {
		File file = (File) object;
		if (file.isDirectory()) continue;
		String fileName = file.getName();
		if (fileName.startsWith("mrt-")) continue;
		if (DEBUG) System.out.println("[debug] " + MESSAGE + " processing file: " + fileName);

		String response = characterize(url, fileName);

		if (StringUtil.isEmpty(response)) {
                    throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": unable to characterize file: " + fileName);
		}
		characterizeString += response;
	    }

            // metadata file in ANVL format
            if ( ! createMetadata(metadataFile, characterizeString)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to create metadata file: " + metadataFile.getAbsolutePath());
            }

	    // update resource map
	    if (! updateResourceMap(jobState, profileState, ingestRequest, mapFile, metadataFile)) {
	        System.out.println("[warn] " + MESSAGE + "Failure to update resource map.");
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " object components characterized");
	} catch (TException te) {
            return new HandlerResult(true, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            String msg = "[error] " + MESSAGE + "error in characterization: " + e.getMessage();
            return new HandlerResult(true, msg);
        } finally {
            // cleanup?
        }
    }
   
    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param response component data
     * @return successful in appending metadata
     */
    private boolean createMetadata(File characterizationFile, String data)
        throws TException
    {
	FileWriter out = null;
	try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating metadata: " + characterizationFile.getAbsolutePath());
	    out = new FileWriter(characterizationFile);
	    out.write(data);
	} catch (Exception e) {
	    return false;
	} finally {
	    try {
	        out.close();
	    } catch (Exception e) {}
	}

        return true;
    }


    /**
     * write metadata references to resource map
     *
     * @param profileState profile state
     * @param ingestRequest characteize request
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @param metadataFile metadata
     * @return successful in updating resource map
     */
    private boolean updateResourceMap(JobState jobState, ProfileState profileState, IngestRequest ingestRequest,
		 File mapFile, File metadataFile)
        throws TException {
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath());

            Model model = updateModel(jobState, profileState, ingestRequest, mapFile, metadataFile);
            if (DEBUG) ResourceMapUtil.dumpModel(model);
            ResourceMapUtil.writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
        }
    }


    public Model updateModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest,
	 	File mapFile, File metadataFile)
        throws Exception
    {
        try {

 	    // read in existing model
 	    String string = FileManager.get().readWholeFileAsUTF8(mapFile.getAbsolutePath());

	    InputStream inputStream = new ByteArrayInputStream(string.getBytes("UTF-8"));
	    if (inputStream == null) {
                String msg = "[error] " + MESSAGE + "failed to update resource map: " + mapFile.getAbsolutePath();
                throw new TException.GENERAL_EXCEPTION(msg);
	    }
            Model model = ModelFactory.createDefaultModel();
	    model.read(inputStream, null, "TURTLE");

            String mrt = "http://uc3.cdlib.org/ontology/mom#";
            String msc = "http://uc3.cdlib.org/ontology/schema#";
            String mts = "http://purl.org/NET/mediatypes/";
            String n2t = "http://" + profileState.getObjectMinterURL().getHost() + "/";

            String versionIDS = "0";    // current
            Integer versionID = jobState.getVersionID();
            if (versionID != null) versionID.toString();
            String objectIDS = null;
            try {
                objectIDS = ingestRequest.getJob().getPrimaryID().getValue();
            } catch (Exception e) {
                objectIDS = "ark:/OID/UNKNOWN";

            }
            String objectURI = ingestRequest.getServiceState().getTargetID() + "/d/" +
                        URLEncoder.encode(objectIDS, "utf-8");
            String object = objectIDS;

            String systemJhove2URI = objectURI + "/" + versionIDS + "/" + URLEncoder.encode("system/" + metadataFile.getName(), "utf-8");

            model.add(ResourceFactory.createResource(n2t + object),
                ResourceFactory.createProperty(mrt + "hasMetadata"),
                ResourceFactory.createResource(systemJhove2URI));
            model.add(ResourceFactory.createResource(systemJhove2URI),
                ResourceFactory.createProperty(mrt + "metadataSchema"),
                ResourceFactory.createResource(msc + "JHOVE2"));
            model.add(ResourceFactory.createResource(systemJhove2URI),
                ResourceFactory.createProperty(mrt + "mimeType"),
                ResourceFactory.createResource(mts + "text/xml"));

            return model;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update model: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }

    private String characterize(URL url, String fileName)
        throws TException
    {

        try {

            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpPost method = new HttpPost(url.toString());

	    MultipartEntity entity = new MultipartEntity();
	    entity.addPart("responseForm", new StringBody("xml", Charset.forName("UTF-8")));
	    entity.addPart("fileName", new StringBody(fileName, Charset.forName("UTF-8")));
	    method.setEntity(entity);

	    HttpResponse httpResponse = httpClient.execute(method);

	    HttpEntity httpEntity = httpResponse.getEntity();
	    String response = StringUtil.streamToString(httpEntity.getContent(), "UTF-8");
	    int status = httpResponse.getStatusLine().getStatusCode();

	    if (status >= 300) {
		System.out.println("[error] " + MESSAGE + "failed to characterize. " + response);
		throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": characterization service: " + url);
	    }


            //DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            //domFactory.setNamespaceAware(true);
            //domFactory.setExpandEntityReferences(true);

            //DocumentBuilder builder = domFactory.newDocumentBuilder();
            //builder.setErrorHandler(new SimpleErrorHandler());
            //Document document = builder.parse(new ByteArrayInputStream(response.getBytes("UTF-8")));
            //XPath xpath = XPathFactory.newInstance().newXPath();
            //XPathExpression expr = xpath.compile("//*[local-name()='primaryIdentifier']");

            //String xpathS = (String) expr.evaluate(document);

	    return response;

        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to characterize. " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }

    public String getName() {
	return NAME;
    }

    // XML parser error handler
    public class SimpleErrorHandler implements ErrorHandler {
        public void warning(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }

        public void error(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }

        public void fatalError(SAXParseException e) throws SAXException {
            System.out.println(e.getMessage());
        }
    }

}
