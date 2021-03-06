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
import com.hp.hpl.jena.vocabulary.*;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.ws.rs.core.MediaType;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * initialize ingest process
 * @author mreyes
 */
public class HandlerInitialize extends Handler<JobState>
{

    private static final String NAME = "HandlerInitialize";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final int BUFFERSIZE = 4096;
    private static final String FS = System.getProperty("file.separator");
    private LoggerInf logger = null;
    private Properties conf = null;



    /**
     * initialize ingest process
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {

	    File targetDir = new File(ingestRequest.getQueuePath(), "system");
	    if (! targetDir.exists()) targetDir.mkdirs();

	    // grab existing data if this is an update
	    if (jobState.grabUpdateFlag()) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Request for UPDATE");
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Request for ADD");
	    }

	    // metadata file in ANVL format
	    File ingestFile = new File(targetDir, "mrt-ingest.txt");
	    if ( ! createMetadata(profileState, ingestRequest, jobState, ingestFile)) {
	        throw new TException.GENERAL_EXCEPTION("[error] " 
		    + MESSAGE + ": unable to build metadata file: " + ingestFile.getAbsolutePath());
	    }

	    // ownership
	    String owner = profileState.getOwner();
	    if (StringUtil.isNotEmpty(owner)) {
		File ownerFile = null;
	        try { 
		    ownerFile = new File(targetDir, "mrt-owner.txt");
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating owner file: " + ownerFile.getAbsolutePath());
		    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(ownerFile));
		    bufferedWriter.write(owner);
		    bufferedWriter.close();
		} catch (IOException e) {
	            throw new TException.GENERAL_EXCEPTION("[error] " 
		        + MESSAGE + ": unable to build owner file: " + ownerFile.getAbsolutePath());
	        }
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "no owner found; no owner file created.");
	    }

	    // membership
	    if (profileState.getCollection().size() > 0) {
		File membershipFile = null;
	        try { 
		    membershipFile = new File(targetDir, "mrt-membership.txt");
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating membership file: " + membershipFile.getAbsolutePath());
		    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(membershipFile));

		    Iterator iterator = profileState.getCollection().iterator();
		    while(iterator.hasNext()) {
		        bufferedWriter.write((String) iterator.next() + "\n");
		    }
		    bufferedWriter.close();
		} catch (IOException e) {
	            throw new TException.GENERAL_EXCEPTION("[error] " 
		        + MESSAGE + ": unable to build membership file: " + membershipFile.getAbsolutePath());
	        }
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "no collection members; no membership file created.");
	    }

	    // resource map referencing metadata
	    File resourceMapFile = new File(targetDir, "mrt-object-map.ttl");
	    if ( ! createResourceMap(jobState, profileState, ingestRequest, ingestFile, resourceMapFile)) {
	        throw new TException.GENERAL_EXCEPTION("[error] " 
		    + MESSAGE + ": unable to build resource map file: " + resourceMapFile.getAbsolutePath());
	    }

	    // merritt object model
	    File momFile = new File(targetDir, "mrt-mom.txt");
	    if ( ! createMerrittObjectModel(jobState, profileState, ingestRequest, momFile)) {
	        throw new TException.GENERAL_EXCEPTION("[error] " 
		    + MESSAGE + ": unable to build merritt object model file: " + momFile.getAbsolutePath());
	    }
	    return new HandlerResult(true, "SUCCESS: " + NAME + " has created metadata");
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "failed to create metadata: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }
    }

    /**
     * write metadata to MOM file
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState contains job state info
     * @param ingestFile target file (usually "mrt-ingest.txt")
     * @return successful in writing metadata
     */
    private boolean createMerrittObjectModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File momFile)
	throws TException 
    {
	if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating merritt object model: " + momFile.getAbsolutePath());
	Map<String, Object> momProperties = new LinkedHashMap();	// maintains insertion order
	
	try {
	    momProperties.put("primaryIdentifier", jobState.getPrimaryID().getValue());
	} catch (Exception e) {
	    // momProperties.put("primaryIdentifier", "(:unas)");
	}
	momProperties.put("type", profileState.getObjectType());
	momProperties.put("role", profileState.getObjectRole());
	try {
	    momProperties.put("aggregate", profileState.getAggregateType());
	} catch (Exception e) { }
	try {
	    momProperties.put("localIdentifier", jobState.getLocalID().getValue());
	} catch (Exception e) {
	    // momProperties.put("localIdentifier", "(:unas)");
	}

	return MetadataUtil.writeMetadataANVL(momFile, momProperties, false);
    }


    /**
     * write metadata to target file
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param jobState contains job state info
     * @param ingestFile target file (usually "mrt-ingest.txt")
     * @return successful in writing metadata
     */
    private boolean createMetadata(ProfileState profileState, IngestRequest ingestRequest,
		JobState jobState, File ingestFile)
	throws TException 
    {
	if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating metadata: " + ingestFile.getAbsolutePath());
	Map<String, Object> ingestProperties = new LinkedHashMap();	// maintains insertion order
	
	ingestProperties.put("ingest", ingestRequest.getServiceState().getServiceName());
	ingestProperties.put("submissionDate", jobState.getSubmissionDate().toString());
	ingestProperties.put("batch", jobState.grabBatchID().getValue());
	ingestProperties.put("job", jobState.getJobID().getValue());
	if (StringUtil.isNotEmpty(jobState.grabUserAgent())) {
	    ingestProperties.put("userAgent", jobState.grabUserAgent());
	}
	ingestProperties.put("file", jobState.getPackageName());
	ingestProperties.put("type", ingestRequest.getPackageType().getValue());
	ingestProperties.put("profile", profileState.getProfileID().getValue());

	Iterator collection = profileState.getCollection().iterator();
	String collectionString = "";
	while (collection.hasNext()) {
	    if (StringUtil.isEmpty(collectionString)) {
		collectionString = (String) collection.next();
	    } else {
	        collectionString = collectionString + "; " + (String) collection.next();
	    }
	}
	ingestProperties.put("collection", collectionString);

	ingestProperties.put("storageService", profileState.getTargetStorage().getStorageLink().toString());
	ingestProperties.put("storageNode", new Integer(profileState.getTargetStorage().getNodeID()).toString());

	Iterator contactsEmail = profileState.getContactsEmail().iterator();
	String notificationString = "";
	while (contactsEmail.hasNext()) {
	    if (StringUtil.isEmpty(notificationString)) {
		notificationString = ((Notification) contactsEmail.next()).getContactEmail();
	    } else {
	        notificationString = notificationString + ";" + ((Notification) contactsEmail.next()).getContactEmail();
	    }
	}
	ingestProperties.put("notification", notificationString);


	//optional
	try {
	    ingestProperties.put("suppliedIdentifier", jobState.getPrimaryID().getValue());
	} catch (Exception e) {
	    ingestProperties.put("suppliedIdentifier", "(:unas)");	// mint object ID downstream
	}
	try {
            ingestProperties.put("digestType", jobState.getHashAlgorithm());
            ingestProperties.put("digestValue", jobState.getHashValue());
	} catch (Exception e) {
            ingestProperties.put("digestType", "(:unas)");
            ingestProperties.put("digestValue", "(:unas)");
	}

	// dc metadata
	if (StringUtil.isNotEmpty(jobState.getObjectCreator())) {
	    ingestProperties.put("creator", jobState.getObjectCreator());
	} else {
	    ingestProperties.put("creator", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getObjectTitle())) {
	    ingestProperties.put("title", jobState.getObjectTitle());
	} else {
	    ingestProperties.put("title", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getObjectDate())) {
	    ingestProperties.put("date", jobState.getObjectDate());
	} else {
	    ingestProperties.put("date", "(:unas)");
	}

	// more optional
	try {
	    ingestProperties.put("localIdentifier", jobState.getLocalID().getValue());
	} catch (Exception e) {
	    ingestProperties.put("localIdentifier", "(:unas)");
	}
	if (StringUtil.isNotEmpty(jobState.getNote())) {
	    ingestProperties.put("note", jobState.getNote());
	} else {
	    ingestProperties.put("note", "(:unas)");
	}

	return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, false);
    }


    /**
     * write metadata references to resource map
     *
     * @param profileState profile state
     * @param ingestFile source file (usually "mrt-ingest.txt")
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @return successful in writing resource map
     */
    private boolean createResourceMap(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, 
		File ingestFile, File ingestResourceMap)
	throws TException {
	try {
	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating resource map: " + ingestResourceMap.getAbsolutePath());

	    Model model = createModel(jobState, profileState, ingestRequest, ingestFile);
	    if (DEBUG) dumpModel(model);
	    writeModel(model, ingestResourceMap);

	    return true;
	//} catch (TException te) {
		//throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    String msg = "[error] " + MESSAGE + "failed to create resource map: " + e.getMessage();
	    System.err.println(msg);
	    throw new TException.GENERAL_EXCEPTION(msg);
	} finally {
	}
    }

    public Model createModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File ingestFile)
	throws Exception
    {
        try {
	    String versionID = "0";		// current
	    String objectIDS = null;
	    try {
	        objectIDS = jobState.getPrimaryID().getValue();
	    } catch (Exception e) {
		// localID
		objectIDS = "ark:/OID/UNKNOWN";
	    }
            String objectURI = ingestRequest.getServiceState().getTargetID() + "/d/" +
                        URLEncoder.encode(objectIDS, "utf-8");
            String object = objectIDS;

	    String metadataURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/" + ingestFile.getName(), "utf-8");
	    String membershipURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-membership.txt", "utf-8"); 
	    String momURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-mom.txt", "utf-8"); 
	    String resourceMapURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-object-map.ttl", "utf-8"); 
	    String ownerURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-owner.txt", "utf-8"); 

            String mrt = "http://uc3.cdlib.org/ontology/mom#";
            String ore = "http://www.openarchives.org/ore/terms#";
            String msc = "http://uc3.cdlib.org/ontology/schema#";
            String mts = "http://purl.org/NET/mediatypes/";
            String n2t = profileState.getPURL();

            Model model = ModelFactory.createDefaultModel();
            model.setNsPrefix("mrt", mrt);
            model.setNsPrefix("ore", ore);
            model.setNsPrefix("msc", msc);

	    String localIdentifier = null;
            try {
                localIdentifier = jobState.getLocalID().getValue();
            } catch (Exception e) {
                localIdentifier = "(:unas)";
            }

	    // object
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(ore + "aggregates"), 
		ResourceFactory.createResource(metadataURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(ore + "aggregates"), 
		ResourceFactory.createResource(membershipURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(ore + "aggregates"), 
		ResourceFactory.createResource(momURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(ore + "aggregates"), 
		ResourceFactory.createResource(ownerURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(ore + "aggregates"), 
		ResourceFactory.createResource(resourceMapURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(mrt + "hasMetadata"), 
		ResourceFactory.createResource(metadataURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(mrt + "hasMetadata"), 
		ResourceFactory.createResource(membershipURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(mrt + "hasMetadata"), 
		ResourceFactory.createResource(momURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(mrt + "hasMetadata"), 
		ResourceFactory.createResource(ownerURI));
	    model.add(ResourceFactory.createResource(n2t + object),
		ResourceFactory.createProperty(mrt + "hasMetadata"), 
		ResourceFactory.createResource(resourceMapURI));

	    // metadata
	    model.add(ResourceFactory.createResource(metadataURI),
		ResourceFactory.createProperty(mrt + "metadataSchema"), 
		ResourceFactory.createResource(msc + "MRT-ingest"));
	    model.add(ResourceFactory.createResource(metadataURI),
		ResourceFactory.createProperty(mrt + "mimeType"), 
		ResourceFactory.createResource(mts + "text/x-anvl"));

	    // membership
	    model.add(ResourceFactory.createResource(membershipURI),
		ResourceFactory.createProperty(mrt + "metadataSchema"), 
		ResourceFactory.createResource(msc + "MRT-membership"));
	    model.add(ResourceFactory.createResource(membershipURI),
		ResourceFactory.createProperty(mrt + "mimeType"), 
		ResourceFactory.createResource(mts + "text/plain"));
	
	    // merritt object model
	    model.add(ResourceFactory.createResource(momURI),
		ResourceFactory.createProperty(mrt + "metadataSchema"), 
		ResourceFactory.createResource(msc + "MRT-MOM"));
	    model.add(ResourceFactory.createResource(momURI),
		ResourceFactory.createProperty(mrt + "mimeType"), 
		ResourceFactory.createResource(mts + "text/plain"));
	
	    // ownership
	    model.add(ResourceFactory.createResource(ownerURI),
		ResourceFactory.createProperty(mrt + "metadataSchema"), 
		ResourceFactory.createResource(msc + "MRT-owner"));
	    model.add(ResourceFactory.createResource(ownerURI),
		ResourceFactory.createProperty(mrt + "mimeType"), 
		ResourceFactory.createResource(mts + "text/plain"));
	
	    // resource map
	    model.add(ResourceFactory.createResource(resourceMapURI),
		ResourceFactory.createProperty(mrt + "metadataSchema"), 
		ResourceFactory.createResource(msc + "MRT-ORE"));
	    model.add(ResourceFactory.createResource(resourceMapURI),
		ResourceFactory.createProperty(mrt + "mimeType"), 
		ResourceFactory.createResource(mts + "text/turtle"));
	    model.add(ResourceFactory.createResource(resourceMapURI),
		ResourceFactory.createProperty(ore + "describes"), 
                ResourceFactory.createResource(n2t + object));
	
            return model;
        } catch (Exception e) {
	    e.printStackTrace();
	    String msg = "[error] " + MESSAGE + "failed to create model: " + e.getMessage();
	    throw new TException.GENERAL_EXCEPTION(msg);
        }
	
    }


    public static void writeModel(Model model, File resourceMapFile)
	throws TException
    {
    	FileOutputStream fos = null;
	try {
            String [] formats = { "RDF/XML", "RDF/XML-ABBREV", "N-TRIPLE", "TURTLE", "TTL", "N3"};
            String format = formats[4];	// Turtle

	    fos = new FileOutputStream(resourceMapFile);
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


    public String getName() {
	return NAME;
    }

}
