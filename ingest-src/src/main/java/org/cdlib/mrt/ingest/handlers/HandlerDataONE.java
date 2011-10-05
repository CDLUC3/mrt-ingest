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
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.file.FileDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import javax.activation.MimetypesFileTypeMap;
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
import org.cdlib.mrt.ingest.utility.ProfileUtil;
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

    protected static final String NAME = "HandlerDataONE";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;
    protected boolean notify = false;
    protected boolean error = false;

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
        File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");

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

            DataOneHandler handler = DataOneHandler.getDataOneHandler( ingestRequest.getQueuePath(), resourceManifestName, MEMBERNODE, profileState.getOwner(), 
		jobState.getPrimaryID(), versionID, OUTPUTRESOURCENAME, dataoneURL, createStorageURL(jobState, profileState), logger);
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
	    Client client = Client.create();
    	    WebResource webResourceCreate = null;
            FormDataMultiPart formDataMultiPart = null;
	    boolean resourceMapSubmitted = false;

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

	        //client = Client.create();
	    	String id = URLEncoder.encode(createContent.getComponentPid(), "UTF-8");
                if (DEBUG) System.out.println("[debug] " + MESSAGE + " Submitting to d1 member node, id : " + id);
	    	webResourceCreate = client.resource(dataoneURL + id);
                System.out.println(createContent.dump("-debug-"));

                formDataMultiPart = new FormDataMultiPart();
		File systemMetadataFile = File.createTempFile("d1-sysmetadata", ".xml");
		FileUtil.string2File(systemMetadataFile, createContent.getCreateSystemMetadata());
		formDataMultiPart.getBodyParts().add(new FileDataBodyPart("object", createContent.getCreateFile()));
		formDataMultiPart.getBodyParts().add(new FileDataBodyPart("sysmeta", systemMetadataFile));

		// make service request
	        try {
  	            clientResponse = webResourceCreate.type(MediaType.MULTIPART_FORM_DATA).post(ClientResponse.class, formDataMultiPart);
	        } catch (Exception e) {
		    e.printStackTrace();
		    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": dateONE service: " + dataoneURL + "/create"); 
	        }
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + " ADD response code " + clientResponse.getStatus());

	        if (clientResponse.getStatus() != 200) {
                    try {
                        TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE tExceptionResponse = clientResponse.getEntity(TExceptionResponse.EXTERNAL_SERVICE_UNAVAILABLE.class);
                        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(tExceptionResponse.getError());
		    } catch (TException te) {
		        throw te;
                    } catch (Exception e) {
		        // let's report something
		        throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": dataONE service: " + dataoneURL + createContent.getComponentPid()); 
	            }
		}

		systemMetadataFile.delete();
            }

	    if (clientResponse == null) 
		if (dataONE)
		    return new HandlerResult(false, "ERROR: dataONE request", 500);

	    return new HandlerResult(true, "SUCCESS: dataONE submission request", clientResponse.getStatus());
	} catch (TException te) {
            te.printStackTrace(System.err);

            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing dataONE request: " + e.getMessage();

            return new HandlerResult(false, msg);
	} finally {
	     if (error && notify) notify(jobState, profileState, ingestRequest);
	    clientResponse = null;
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
                String line = "mrt-erc.txt" + DELIMITER
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
            // if (DEBUG) dumpModel(model);
            writeModel(model, mapFile);

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

   public static void writeModel(Model model, File mapFile)
        throws TException
    {
        FileOutputStream fos = null;
        try {
            String [] formats = { "RDF/XML", "RDF/XML-ABBREV", "N-TRIPLE", "TURTLE", "TTL", "N3"};
            String format = formats[4]; // Turtle

            fos = new FileOutputStream(mapFile);
            model.write(fos, format);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to write resource map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
                fos.flush();
            } catch (Exception e) {}
        }
    }

   public static void dumpModel(Model model)
    {
        System.out.println( "[debug] dump resource map - START");

        // list the statements in the graph
        StmtIterator iter = model.listStatements();

        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt      = iter.nextStatement();         // get next statement
            Resource  subject   = stmt.getSubject();   // get the subject
            Property  predicate = stmt.getPredicate(); // get the predicate
            RDFNode   object    = stmt.getObject();    // get the object

            System.out.print(subject.toString());
            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                System.out.print(object.toString());
            } else {
                // object is a literal
                System.out.print(" \"" + object.toString() + "\"");
            }
            System.out.println(" .");
        }
        System.out.println( "[debug] dump resource map - END");
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
            email.setFrom("uc3@ucop.edu", "UC3 Merritt Support");
            email.setSubject("[Warning] dataONE member node not available" + server);
            email.setMsg(jobState.dump("Job notification", "\t", "\n", null));
            email.send();
	} catch (Exception e) {};

        return;
    }
}

