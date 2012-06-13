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

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.*;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.file.FileDataBodyPart;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.core.MediaType;

import java.security.SecureRandom;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

// mime-type
import net.sf.jmimemagic.Magic;
import net.sf.jmimemagic.MagicMatch;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.MultiPartEmail;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.dataone.content.CreateContent;
import org.cdlib.mrt.dataone.create.DataOneHandler;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.ResourceMapUtil;
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * Submit to dataONE member node
 * @author mreyes
 */
public class HandlerDataONE extends Handler<JobState>
{

    private static final String NAME = "HandlerDataONE";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private boolean notify = true;
    private boolean error = false;

    private static final String MEMBERNODE = "Merritt";
    private static final String OUTFORMAT = "RDF/XML";
    private static final String OUTPUTRESOURCENAME = "system/mrt-dataone-map.rdf";

    private String resourceManifestName = "producer/mrt-dataone-manifest.txt";
    int versionID = 0;	// current version

    private static final String FS = System.getProperty("file.separator");
    private static final String NL = System.getProperty("line.separator");


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
	URL dataoneURL = null;
        logger = new TFileLogger("HandlerDataONE", 10, 10);
        File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
        File metadataFile = new File(systemTargetDir, "mrt-ingest.txt");
        File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");
        jobState.setMetacatStatus("success");	//default

	try {
	    versionID = jobState.getVersionID();
	} catch (Exception e) { versionID = 0; }
	boolean dataONE = (versionID != 0);


	try {
	    // build REST url 
	    try {
	        dataoneURL = profileState.getDataoneURL();
	    } catch (Exception e) {
		throw new TException.REQUEST_ELEMENT_UNSUPPORTED("[error] " + NAME + ": No dataONE service url specified.");
	    }

	    // d1 resource manifest processing
	    File resourceManifest = new File(ingestRequest.getQueuePath() + FS + resourceManifestName);
	    if ( ! resourceManifest.exists()) {
	        resourceManifestName = resourceManifestName.replace("producer", "system");
	        resourceManifest = new File(ingestRequest.getQueuePath() + FS + resourceManifestName);
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " creating D1 resource manifest " + resourceManifest.getName());
		FileUtil.string2File(resourceManifest, 
		    createResourceManifest(new File(ingestRequest.getQueuePath() + FS + "producer")));
	    }

            DataOneHandler handler = DataOneHandler.getDataOneHandler(ingestRequest.getQueuePath(), resourceManifestName, MEMBERNODE, 
		profileState.getOwner(), jobState.getPrimaryID(), versionID, OUTPUTRESOURCENAME, dataoneURL, 
		createStorageURL(jobState, profileState), logger);
	    //if (DEBUG) handler.dumpList();

	    // create resource map
	    if ( ! dataONE)
	        handler.getCreateContentResourceMap(OUTFORMAT, OUTPUTRESOURCENAME);	// just need map for now

	    // update Merritt resource map
	    if ( ! dataONE) {
                if (! updateResourceMap(jobState, profileState, ingestRequest, mapFile, resourceManifest, new File(ingestRequest.getQueuePath() + FS + OUTPUTRESOURCENAME)))
                    throw new TException.GENERAL_EXCEPTION("[error] "
                        + MESSAGE + ": unable to update map file w/ dataONE reference: " + mapFile.getAbsolutePath());
            }

	    // Do not submit to member node
	    if ( ! dataONE)
	    	return new HandlerResult(true, "SUCCESS: dataONE request");
	    
	    // web client setup
            CreateContent createContent = null;
    	    WebResource webResourceCreate = null;
            FormDataMultiPart formDataMultiPart = null;
	    boolean resourceMapSubmitted = false;

            // https - trust all certs
            String credentials = dataoneURL.getUserInfo();
            String protocol = dataoneURL.getProtocol();
            Client client = null;

            if (protocol.equals("https")) {
                X509TrustManager tm = new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException { }
                    public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException { }
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                };
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " Setting SSL protocol");
                ClientConfig config = new DefaultClientConfig();
                SSLContext ctx = SSLContext.getInstance("TLS");
                ctx.init(null, new TrustManager[]{tm}, new SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());

                config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(
                    new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                }, ctx));

                client = Client.create(config);
            } else {
                client = Client.create();    // reuse?  creation is expensive
            }

	    // submit to D1 member node
            for (int i = 0; true; i++) {
                createContent = handler.getCreateContent(i);
	        String outputResourceName = null;

                if (createContent == null) {
		     if ( ! resourceMapSubmitted ) {
		        // submit resource map to D1 member node
	                createContent = handler.getCreateContentResourceMap(OUTFORMAT, OUTPUTRESOURCENAME);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + " RESOURCE MAP id : " + createContent.getComponentPid());
			resourceMapSubmitted = true;
		    } else {
                       if (DEBUG) System.out.println("[debug] " + MESSAGE + " dataONE data requests complete");
		       break;
		    }
		}

	    	String id = createContent.getComponentPid();
		// Whitespace is not supported by D1 
	    	String spacelessID = translateWhiteSpace(createContent.getComponentPid());
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " Submitting to d1 member node, id : " + spacelessID);
	    	webResourceCreate = client.resource(dataoneURL + spacelessID);
                // System.out.println(createContent.dump("-debug-"));

                formDataMultiPart = new FormDataMultiPart();
		File systemMetadataFile = File.createTempFile("d1-sysmetadata", ".xml");
		FileUtil.string2File(systemMetadataFile, createContent.getCreateSystemMetadata());
		formDataMultiPart.getBodyParts().add(new FileDataBodyPart("object", createContent.getCreateFile()));
		formDataMultiPart.getBodyParts().add(new FileDataBodyPart("sysmeta", systemMetadataFile));
		formDataMultiPart.getBodyParts().add(new FormDataBodyPart("pid", id));

		// make service request
		int maxTries = 3;
            	for (int cnt=1; cnt <= maxTries; cnt++) {
	            try {
  	                clientResponse = webResourceCreate.type(MediaType.MULTIPART_FORM_DATA).post(ClientResponse.class, formDataMultiPart);
	            } catch (Exception e) {
		        error = true;
		        jobState.setMetacatStatus("failure");
		        String msg = "[error] " + NAME + ": dateONE service: " + dataoneURL + "/create"; 
		        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
	            }

	            if (clientResponse.getStatus() != 200) {
			if (cnt == maxTries) {
                            try {
		                error = true;
		                jobState.setMetacatStatus("failure");
			        String msg = clientResponse.toString();
		                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
                            } catch (Exception e) {
		                error = true;
		                jobState.setMetacatStatus("failure");
			        String msg = "[error] " + NAME + ": dataONE service: " + dataoneURL;
		                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(msg);
	                    }
			} else {
	                    if (DEBUG) {
				System.out.println("[warn] " + MESSAGE + " ADD response code " + clientResponse.getStatus() + " - " + jobState.getJobID().getValue()
					+ " Attempt: " + cnt + " of " + maxTries);
			    }
			    Thread.currentThread().sleep(5 * 1000);	// wait a bit before reattempting
			}
		    } else {
			break;
		    }
	        }

	        if (DEBUG) System.out.println("[debug] " + MESSAGE + " ADD response code " + clientResponse.getStatus());
	        systemMetadataFile.delete();
		
            }

	    if (clientResponse == null) {
		jobState.setMetacatStatus("failure");
		if (dataONE)
	    	    // optional handler, do not stop future processing
		    return new HandlerResult(true, "ERROR: dataONE request", 500);
	    }

	    return new HandlerResult(true, "SUCCESS: dataONE submission request", clientResponse.getStatus());
	} catch (TException te) {
	    error = true;
	    jobState.setMetacatStatus("failure");
            te.printStackTrace(System.err);
	    if (DEBUG) System.out.println("[error] " + MESSAGE + te.getDetail());

	    // optional handler, do not stop future processing
            return new HandlerResult(true, te.getDetail());
	} catch (Exception e) {
	    error = true;
	    jobState.setMetacatStatus("failure");
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing dataONE request: " + e.getMessage();
	    if (DEBUG) System.out.println(msg);

	    // optional handler, do not stop future processing
            return new HandlerResult(true, msg);
	} finally {
	    if (error) {
                if (DEBUG) System.out.println("[error] dataONE processing failed: " + jobState.getMetacatStatus());
	        if (notify && dataONE) notify(jobState, profileState, ingestRequest);
	        jobState.setCleanupFlag(false);
	        clientResponse = null;
	    }
	    try {
                if (dataONE) createMetadata(metadataFile, jobState.getMetacatStatus());
	    } catch (Exception e) {
		try {
                    createMetadata(metadataFile, "failure");
		} catch (Exception ee) {
                     if (DEBUG) System.out.println("[error] dataONE unable to update Metadata with D1 status");
		}
	    }
	}
    }
   
    public String getName() {
        return NAME;
    }

    protected URL createStorageURL(JobState jobState, ProfileState profileState) {
	String url = "";
	try {
            url = profileState.getTargetStorage().getStorageLink().toString() + "/content/" + 
		profileState.getTargetStorage().getNodeID() + "/" + 
		URLEncoder.encode(jobState.getPrimaryID().getValue(), "UTF-8") + "/" + Integer.toString(versionID) + "/"; 
	    return new URL(url);

	} catch (Exception e) { 
	    e.printStackTrace();
            if (DEBUG) System.out.println("[warn] " + MESSAGE + " Could not create storage URL");
	    return null; 
	}
    }


    private String createResourceManifest(File producerDir) {
        Vector <File> fileNames = new Vector(100);
        try {
            StringBuffer buf = new StringBuffer();
            String header =
                  "# Minimal dataONE resource manifest" + NL
		+ "#%dataonem_0.1" + NL
		+ "#%profile | http://uc3.cdlib.org/registry/ingest/manifest/mrt-dataone-manifest" + NL
		+ "#%prefix  | dom: | http://uc3.cdlib.org/ontology/dataonem#" + NL
		+ "#%prefix  | mrt: | http://uc3.cdlib.org/ontology/mom#" + NL
		+ "#%fields  | dom:scienceMetadataFile | dom:scienceMetadataFormat | dom:scienceDataFile | mrt:mimeType" + NL;

            buf.append(header);
	    FileUtil.getDirectoryFiles(producerDir, fileNames);
            String dispAggregates = getAggregates(fileNames);
            buf.append(dispAggregates);
            buf.append("#%EOF");

            return buf.toString();
	} catch (Exception e) { 
	    e.printStackTrace();
            if (DEBUG) System.out.println("[warn] " + MESSAGE + " Could not create dataONE resource manifest"); 
	    return null;
	}
    }

    protected String getAggregates(Vector<File> fileNames)
        throws Exception
    {
        StringBuffer buf = new StringBuffer();

        try {
	    String DELIMITER = " | ";
            for (int i=0; i < fileNames.size(); i++) {
                File file = fileNames.get(i);

		// mrt-erc.txt | ERC | <file> | mime/type
                String line = "DEFAULT" + DELIMITER
                    + "ERC" + DELIMITER
                    + file.getName() + DELIMITER
                    + getMimetype(file) + NL;
                buf.append(line);
            }
            return buf.toString();

        } catch(Exception ex) {
            throw new TException.GENERAL_EXCEPTION("Error creating aggregates");
        }
    }

    /**
     * write metadata references to resource map
     *
     * @param profileState profile state
     * @param ingestRequest ingest request
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @return successful in updating resource map
     */
    private boolean updateResourceMap(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File mapFile,
                File manifestFile, File resourceMapFile)
        throws TException {
        Model model = null;
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath());

            model = updateModel(jobState, profileState, ingestRequest, mapFile, manifestFile, resourceMapFile);
            if (DEBUG) ResourceMapUtil.dumpModel(model);
            ResourceMapUtil.writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
	    model.close();
        }
    }

    public Model updateModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File mapFile,
                File manifestFile, File resourceMapFile)
        throws Exception
    {
        try {

            // read in existing model
            InputStream inputStream = FileManager.get().open(mapFile.getAbsolutePath());
            if (inputStream == null) {
                String msg = "[error] " + MESSAGE + "failed to update resource map: " + mapFile.getAbsolutePath();
                throw new TException.GENERAL_EXCEPTION(msg);
            }
            Model model = ModelFactory.createDefaultModel();
            model.read(inputStream, null, "TURTLE");

            String mrt = "http://uc3.cdlib.org/ontology/mom#";
            String ore = "http://www.openarchives.org/ore/terms#";
            String msc = "http://uc3.cdlib.org/ontology/schema#";
            String mts = "http://purl.org/NET/mediatypes/";

            String versionIDS = "0";    // current
            Integer versionID = jobState.getVersionID();
            if (versionID != null) versionID.toString();
            String objectIDS = null;
            try {
                objectIDS = ingestRequest.getJob().getPrimaryID().getValue();
            } catch (Exception e) {
                objectIDS = "(:unas)";          // will this ever happen?
            }
            String objectURI = profileState.getTargetStorage().getStorageLink().toString() + "/content/" +
                        profileState.getTargetStorage().getNodeID() + "/" +
                        URLEncoder.encode(objectIDS, "utf-8");
	    // Merritt create manifest?
	    String locDir = "producer/";
	    if (! manifestFile.getAbsolutePath().contains(locDir + manifestFile.getName())) locDir = "system/";
            String manifestURI = objectURI + "/" + versionIDS + "/" + URLEncoder.encode(locDir + manifestFile.getName(), "utf-8");
            String resourceMapURI = objectURI + "/" + versionIDS + "/" + URLEncoder.encode("system/" + resourceMapFile.getName(), "utf-8");

            model.add(ResourceFactory.createResource(objectURI),
                ResourceFactory.createProperty(ore + "aggregates"),
                ResourceFactory.createResource(manifestURI));
            model.add(ResourceFactory.createResource(objectURI),
                ResourceFactory.createProperty(ore + "aggregates"),
                ResourceFactory.createResource(resourceMapURI));
            model.add(ResourceFactory.createResource(objectURI),
                ResourceFactory.createProperty(mrt + "hasMetadata"),
                ResourceFactory.createResource(manifestURI));
            model.add(ResourceFactory.createResource(objectURI),
                ResourceFactory.createProperty(mrt + "hasMetadata"),
                ResourceFactory.createResource(resourceMapURI));

            // D1 resource manifest file
            model.add(ResourceFactory.createResource(manifestURI),
                ResourceFactory.createProperty(mrt + "metadataSchema"),
                ResourceFactory.createResource(msc + "DataONE-manifest"));
            model.add(ResourceFactory.createResource(manifestURI),
                ResourceFactory.createProperty(mrt + "mimeType"),
                ResourceFactory.createResource(mts + "text/anvl"));

            // D1 resource map file
            model.add(ResourceFactory.createResource(resourceMapURI),
                ResourceFactory.createProperty(mrt + "metadataSchema"),
                ResourceFactory.createResource(msc + "DataONE-map"));
            model.add(ResourceFactory.createResource(resourceMapURI),
                ResourceFactory.createProperty(mrt + "mimeType"),
                ResourceFactory.createResource(mts + "text/turtle"));

            return model;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update model: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }


    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param scheme identifier scheme
     * @param namespace identifier namespace
     * @param identifier identifier
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String metacatStatus) 
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("metacatRegistration", metacatStatus);

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
    }


    protected String getMimetype(File file) {
	try {
            Magic parser = new Magic() ;
	    MagicMatch match = parser.getMagicMatch(file, true, true);
	    return match.getMimeType();
	} catch (Exception e) {
            if (DEBUG) System.out.println("[warn] " + MESSAGE + " Could not determine mimetype: " + file.getName()); 
	    return "application/octet-stream";
	}
        // return new MimetypesFileTypeMap().getContentType(file);
    }

    protected void notify(JobState jobState, ProfileState profileState, IngestRequest ingestRequest) {
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
            email.setSubject("[Warning] Metacat registration failed " + server + owner);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
	} catch (Exception e) {};

        return;
    }

    private String translateWhiteSpace(String inputString) {
	return inputString.replaceAll(" ", "_");
    }
}

