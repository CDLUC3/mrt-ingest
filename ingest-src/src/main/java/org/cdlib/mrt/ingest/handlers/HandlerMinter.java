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

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.vocabulary.*;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import java.io.ByteArrayInputStream;
import java.io.File;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
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
import org.cdlib.mrt.ingest.utility.TExceptionResponse;
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
 * mint object URL if necessary
 * @author mreyes
 */
public class HandlerMinter extends Handler<JobState>
{

    protected static final String NAME = "HandlerMinter";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;

    /**
     * mint object ID
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState 
     * @return HandlerResult result in creating manifest
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File metadataFile = new File(systemTargetDir, "mrt-ingest.txt");
            File momFile = new File(systemTargetDir, "mrt-mom.txt");
            File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");
	    boolean resetObject = true;	 // recheck this logic now that we have localIDs
	    String returnValue = null;
	    String assignedObjectID = null;
	    String retrievedObjectID = null;
	    boolean mint = true;

	    // Need to read mrt-erc.txt data if available.
	    // This is also done in HandlerDescribe, but needs to also be done here for ID binding.  Cache results? 
	    File producerErcFile = new File(ingestRequest.getQueuePath(), "producer/mrt-erc.txt");
	    if (producerErcFile.exists()) {
                Map<String, String> producerERC = MetadataUtil.readMetadataANVL(producerErcFile);
           	// erc file in ANVL format
           	readERC(jobState, producerERC);
            }

	    if (ProfileUtil.isDemoMode(profileState)) {
	        if (jobState.getPrimaryID() != null) {
		    jobState.setLocalID(jobState.getPrimaryID().getValue());
	    	    jobState.setPrimaryID(null);
		}
	    }

	    Identifier localID = jobState.getLocalID();
	    if (localID != null && jobState.getPrimaryID() == null) retrievedObjectID = MintUtil.fetchPrimaryID(profileState, localID.getValue());

	    if (jobState.getPrimaryID() != null) {
		if (retrievedObjectID != null) {
		    if (! retrievedObjectID.equals(jobState.getPrimaryID().getValue())) {
		        throw new TException.INVALID_OR_MISSING_PARM("[error]" + MESSAGE + "local ID and primary ID mapping is incorrect: " +
			        retrievedObjectID + " - " + jobState.getPrimaryID().getValue());
		    } else {
	                System.out.println("[debug] " + MESSAGE + "Primary ID and Local ID mapping is correct: " + retrievedObjectID + " --- " + localID);
		    }
		}
	    } else {
		if (retrievedObjectID != null) {
	    	    jobState.setPrimaryID(retrievedObjectID);
	            System.out.println("[debug] " + MESSAGE + "Primary ID found from local ID: " + retrievedObjectID + " --- " + localID);
		}
	    }

	    if (jobState.getPrimaryID() != null) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "objectID found, no minting necessary.");
		mint = false;
	    }

	    if (mint) {
	        returnValue = MintUtil.processObjectID(profileState, jobState, mint);
		if (profileState.getIdentifierScheme() ==  Identifier.Namespace.ARK) {
		    assignedObjectID = returnValue;
	            jobState.setPrimaryID(assignedObjectID);
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "objectID minted: " + assignedObjectID);
		} else {
		    // expect DOI and shadow ARK
		    String[] parse = returnValue.split("\\|");
		    if (parse[0].startsWith("doi")) {
	        	 System.out.println("[info] " + MESSAGE + "Setting DOI to local ID: " + parse[0]);
		        jobState.setLocalID(parse[0]);
		    } else {
	                System.err.println("[warn] " + MESSAGE + "Failure to mint a DOI identifier.");
                	throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": failure to mint DOI identifier: " + parse[0]);
		    }
		    if (parse[1].startsWith("ark")) {
	        	System.out.println("[info] " + MESSAGE + "Setting shadow ARK to primary ID: " + parse[1]);
		        assignedObjectID = parse[1];
	                jobState.setPrimaryID(assignedObjectID);
			jobState.setShadowARK(true);
		    } else {
	                System.err.println("[warn] " + MESSAGE + "Failure to mint an ARK shadow identifier.");
                	throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": failure to mint an ARK shadow identifier: " + parse[1]);
		    }
		}

	    }

	    // At this point we have a primary identifer.  Make sure it is an ARK.
	    if (! jobState.getPrimaryID().getValue().startsWith("ark")) {
	        System.err.println("[warn] " + MESSAGE + "Primary ID is not an ARK: " + jobState.getPrimaryID().getValue());
               	throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Primary ID is not an ARK: " + jobState.getPrimaryID().getValue());
	    }

	    // update metadata (ERC, target URL and context)
	    returnValue = MintUtil.processObjectID(profileState, jobState, false);
	    if (! returnValue.startsWith("ark")) {
	        System.err.println("[warn] " + MESSAGE + "Could not update identifier: " + returnValue);
               	throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Could not update identifier: " + returnValue);
	    }
	    // need to update shadow ARK?
	    if (jobState.getShadowARK()) {
	        returnValue = MintUtil.processObjectID(profileState, jobState, false, true);
	        if (returnValue.startsWith("ark")) {
	            System.err.println("[warn] " + MESSAGE + "Could not update identifier: " + returnValue);
               	    throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Could not update identifier: " + returnValue);
	        }
	    }

	    // update resource map
	    if ( ! updateResourceMap(profileState, ingestRequest, mapFile, resetObject)) {
	        System.err.println("[warn] " + MESSAGE + "Failure to update resource map.");
	    }

            // metadata file in ANVL format
            if ( ! createMetadata(metadataFile, profileState.getIdentifierScheme().toString(), 
			profileState.getIdentifierNamespace(), assignedObjectID, retrievedObjectID)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to append metadata file: " + metadataFile.getAbsolutePath());
            }

	    String localValue = "";
	    try {
	        localValue = jobState.getLocalID().getValue();
	    } catch (Exception e) {}

            // mom file in ANVL format
            if ( ! updateMom(momFile, jobState.getPrimaryID().getValue(), localValue)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to update mom file: " + momFile.getAbsolutePath());
            }

	    // update resource map
	    if (! updateResourceMap(profileState, ingestRequest, mapFile, resetObject)) {
	        System.out.println("[warn] " + MESSAGE + "Failure to update resource map.");
	    }

	    if (mint) {
	        return new HandlerResult(true, "SUCCESS: " + NAME + " object ID minted");
	    } else {
	    	return new HandlerResult(true, "SUCCESS: " + NAME + " no object ID minting required");
	    }
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "minting identifier: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
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
    private boolean createMetadata(File ingestFile, String scheme, String namespace, String assignedIdentifier, String retrievedIdentifier)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

	if (StringUtil.isNotEmpty(assignedIdentifier)) 
	    ingestProperties.put("assignedIdentifier", assignedIdentifier);
	else 
	    ingestProperties.put("assignedIdentifier", "(:unas)");

	if (StringUtil.isNotEmpty(retrievedIdentifier)) 
	    ingestProperties.put("retrievedIdentifier", retrievedIdentifier);
	else 
	    ingestProperties.put("retrievedIdentifier", "(:unas)");

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
    }

    /**
     * update results to mom file
     *
     * @param momFile merritt object model file
     * @param scheme identifier scheme
     * @param namespace identifier namespace
     * @param identifier identifier
     * @return successful in appending object file
     */
    private boolean updateMom(File momFile, String primaryIdentifier, String localIdentifier)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating momFile: " + momFile.getAbsolutePath());
        Map<String, Object> momProperties = new LinkedHashMap();   // maintains insertion order

	// read existing MOM data
	momProperties = MetadataUtil.readMomANVL(momFile);

	if (StringUtil.isNotEmpty(primaryIdentifier)) 
	    momProperties.put("primaryIdentifier", primaryIdentifier);
	else 
	    momProperties.put("primaryIdentifier", "(:unas)");

	if (StringUtil.isNotEmpty(localIdentifier)) {
	    if (momProperties.containsValue("localIdentifier")) {
	        if (((String) momProperties.get("localIdentifier")).contains("(:unas)")) {
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + "assigning localID in momFile: " + localIdentifier);
	        } else {
		    if (! StringUtil.squeeze(localIdentifier).equals(StringUtil.squeeze((String) momProperties.get("localIdentifier")))) {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "overriding localID in momFile: " 
			    +  momProperties.get("localIdentifier") + " --- " + localIdentifier);
		    } else {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "local ID has not changed.  No action taken");
		    }
	        }
	    }
	    momProperties.put("localIdentifier", localIdentifier);
	} else {
	    if (momProperties.containsValue("localIdentifier")) {
	        if (((String) momProperties.get("localIdentifier")).contains("(:unas)")) {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "no localID defined, removing momFile entry");
	    	    momProperties.remove("localIdentifier");
	        } else {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "no localID created in minter, preserving existing localID");
	        }
	    }
	}


        return MetadataUtil.writeMetadataANVL(momFile, momProperties, false);
    }

    /**
     * create citation file
     *
     * @param JobState populate metadata fields if necessary
     * @param producerERC producer supplied metadata
     * @return successful in creating erc file
     */
    private void readERC(JobState jobState, Map producerERC)
        throws TException
    {
        String objectCreator = jobState.getObjectCreator();
        String objectTitle = jobState.getObjectTitle();
        String objectDate = jobState.getObjectDate();
        String primaryIdentifier = null;
        String localIdentifier = null;
        try {
            primaryIdentifier = jobState.getPrimaryID().getValue();
        } catch (Exception e) {
            primaryIdentifier = "(:unas)";
        }
        try {
             localIdentifier = jobState.getLocalID().getValue();
        } catch (Exception e) {
            localIdentifier = "(:unas)";
        }

        // update jobState if necessary
        if (producerERC != null) {
            Iterator producerERCitr = producerERC.keySet().iterator();
            while (producerERCitr.hasNext()) {
                String key = (String) producerERCitr.next();
                String value = (String) producerERC.get(key);

		// Only update localID, as all other fields are populated in HandlerDescribe
                // ercProperties.put(key, value);
                final String DELIMITER = " ; ";

                String append = "";
                if (key.matches("who")) {
		    if (objectCreator != null) append = DELIMITER + objectCreator;
		    jobState.setObjectCreator(value + append);
		}
                append = "";
                if (key.matches("what")) {
		    if (objectTitle != null) append = DELIMITER + objectTitle;
		    jobState.setObjectTitle(value + append);
		}
                append = "";
                if (key.matches("when")) {
		    if (objectDate != null) append = DELIMITER + objectDate;
		    jobState.setObjectDate(value + append);
		}
                if (key.matches("where") && ! value.contains("ark:")) jobState.setLocalID(value);
            }
        } else {
            if (DEBUG) System.out.println("No additional ERC metadata found");
        }
    }


    /**
     * write metadata references to resource map
     *
     * @param profileState profile state
     * @param ingestRequest ingest request
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @param resetObject object-url may not have been known, assign now
     * @return successful in updating resource map
     */
    private boolean updateResourceMap(ProfileState profileState, IngestRequest ingestRequest, File mapFile, 
		boolean resetObject)
        throws TException {
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath());

            Model model = updateModel(profileState, ingestRequest, mapFile, resetObject);
            if (DEBUG) dumpModel(model);
            writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
        }
    }


    public Model updateModel(ProfileState profileState, IngestRequest ingestRequest, File mapFile,
		boolean resetObject)
        throws Exception
    {
        try {

 	    // read in existing model
 	    String string = FileManager.get().readWholeFileAsUTF8(mapFile.getAbsolutePath());
	    if (resetObject) {
            	if (DEBUG) System.out.println("[debug] " + MESSAGE + "assigning objectID");
		string = string.replaceAll("OID_UNKNOWN", URLEncoder.encode(ingestRequest.getJob().getPrimaryID().getValue(), "UTF-8"));
	    }

	    InputStream inputStream = new ByteArrayInputStream(string.getBytes("UTF-8"));
	    if (inputStream == null) {
                String msg = "[error] " + MESSAGE + "failed to update resource map: " + mapFile.getAbsolutePath();
                throw new TException.GENERAL_EXCEPTION(msg);
	    }
            Model model = ModelFactory.createDefaultModel();
	    model.read(inputStream, null, "TURTLE");

            String mrt = "http://uc3.cdlib.org/ontology/mom#";

            String versionID = "0";             // current
            String objectIDS = null;
            try {
                objectIDS = ingestRequest.getJob().getPrimaryID().getValue();
            } catch (Exception e) {
                objectIDS = "(:unas)";		// will this ever happen?
            }
            String objectURI = profileState.getTargetStorage().getStorageLink().toString() + "/content/" +
                        profileState.getTargetStorage().getNodeID() + "/" +
                        URLEncoder.encode(objectIDS, "utf-8");


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

    public String getName() {
	return NAME;
    }

}
