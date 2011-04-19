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
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * process Dublin Kernel elements
 * @author mreyes
 */
public class HandlerDescribe extends Handler<JobState>
{

    protected static final String NAME = "HandlerDescribe";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;
    protected Integer defaultStorage = null;

    /**
     * process metadata
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState 
     * @return HandlerResult result in metadata processing
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File producerTargetDir = new File(ingestRequest.getQueuePath(), "producer");
            File systemErcFile = new File(systemTargetDir, "mrt-erc.txt");
            File producerErcFile = new File(producerTargetDir, "mrt-erc.txt");
            File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");

	    Map<String, String> producerERC = null;
	    if (producerErcFile.exists()) {
	        producerERC = MetadataUtil.readMetadataANVL(producerErcFile);
	    }

            // erc file in ANVL format
            if ( ! createERC(jobState, systemErcFile, producerERC)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to create ERC file: " + systemErcFile.getAbsolutePath());
            }

            // update resource map
            if (! updateResourceMap(jobState, profileState, ingestRequest, mapFile, systemErcFile, producerErcFile)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to update map file w/ ERC reference: " + systemErcFile.getAbsolutePath());
            }

	    return new HandlerResult(true, "SUCCESS: " + MESSAGE + "Success in creating ERC data file.", 0);

	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing metadata: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }

    }


    /**
     * create citation file
     *
     * @param JobState populate metadata fields if necessary
     * @param ercFile erc file
     * @param producerERC producer supplied metadata
     * @return successful in creating erc file
     */
    private boolean createERC(JobState jobState, File ercFile, Map producerERC)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating erc: " + ercFile.getAbsolutePath());
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

	ArrayList arrayWhere = new ArrayList();
        Map<String, Object> ercProperties = new LinkedHashMap();   // maintains insertion order; support duplicate keys

        ercProperties.put("erc", "");
        if ( StringUtil.isNotEmpty(objectCreator)) ercProperties.put("who", objectCreator);
	else ercProperties.put("who", "(:unas)");

        if ( StringUtil.isNotEmpty(objectTitle)) ercProperties.put("what", objectTitle);
        else ercProperties.put("what", "(:unas)");
        
        if ( StringUtil.isNotEmpty(objectDate)) ercProperties.put("when", objectDate);
        else ercProperties.put("when", "(:unas)");

        if ( StringUtil.isNotEmpty(primaryIdentifier)) 
	    arrayWhere.add(primaryIdentifier);

        if ( StringUtil.isNotEmpty(localIdentifier)) 
	    arrayWhere.add(localIdentifier);
        else 
	    arrayWhere.add("(:unas)");
        ercProperties.put("where", arrayWhere);

	// update jobState if necessary
	if (producerERC != null) {
	    Iterator producerERCitr = producerERC.keySet().iterator();
	    while (producerERCitr.hasNext()) {
	        String key = (String) producerERCitr.next();
	        String value = (String) producerERC.get(key);

		final String DELIMITER = " ; ";
		String append = "";
		// append
	        if (key.matches("who")) {
		    if (objectCreator != null)  append = DELIMITER + jobState.getObjectCreator();
		     // jobState.setObjectCreator(value + append);
		}
	        if (key.matches("what")) {
		    append = "";
		    if (objectTitle != null) append = DELIMITER + jobState.getObjectTitle(); 
		    // jobState.setObjectTitle(value + append);
		}
	        if (key.matches("when")) {
		    append = "";
		    if (objectDate != null) append = DELIMITER + jobState.getObjectDate();
		    // jobState.setObjectDate(value + append);
		}
	        if (key.matches("where") && ! value.contains("ark:")) {
		    append = "";
		    try {
		        if (localIdentifier != null && ! localIdentifier.equals(jobState.getLocalID().getValue()))
			    append = DELIMITER + jobState.getLocalID().getValue();
		    } catch (Exception e) {}
		    jobState.setLocalID(value + append);
		}
	    }
	} else {
	    if (DEBUG) System.out.println("No additional ERC metadata found");
	}

	// Any qualified data?
	if (producerERC != null) {
	    Iterator producerERCitr = producerERC.keySet().iterator();
	    while (producerERCitr.hasNext()) {
	        String key = (String) producerERCitr.next();
	        String value = (String) producerERC.get(key);

	        if (key.startsWith("who/") || key.startsWith("what/") || key.startsWith("when/") || key.startsWith("where/")) {
		    ercProperties.put(key, value);
		}
	    }
	}

        return MetadataUtil.writeMetadataANVL(ercFile, ercProperties, true);
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
		File systemErcFile, File producerErcFile)
        throws TException {
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath());

            Model model = updateModel(jobState, profileState, ingestRequest, mapFile, systemErcFile, producerErcFile);
            if (DEBUG) dumpModel(model);
            writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
        }
    }

    public Model updateModel(JobState jobState, ProfileState profileState, IngestRequest ingestRequest, File mapFile,
		File systemErcFile, File producerErcFile)
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

            String versionIDS = "0";	// current
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
            String systemErcURI = objectURI + "/" + versionIDS + "/system/" + systemErcFile.getName();

            model.add(ResourceFactory.createStatement(ResourceFactory.createResource(objectURI),
                ResourceFactory.createProperty(mrt + "hasMetadata"),
                ResourceFactory.createResource(systemErcURI)));

	    // system ERC
            model.add(ResourceFactory.createStatement(ResourceFactory.createResource(systemErcURI),
                ResourceFactory.createProperty(mrt + "metadataSchema"),
                ResourceFactory.createPlainLiteral("ERC")));
            model.add(ResourceFactory.createStatement(ResourceFactory.createResource(systemErcURI),
                ResourceFactory.createProperty(mrt + "mimeType"),
                ResourceFactory.createPlainLiteral("text/anvl")));

	    // producer ERC
	    if (producerErcFile.exists()) {
        	if (DEBUG) System.out.println("[debug] " + MESSAGE + "found ERC data: " + producerErcFile.getAbsolutePath());
                String producerErcURI = objectURI + "/" + versionIDS + "/producer/" + producerErcFile.getName();

                model.add(ResourceFactory.createStatement(ResourceFactory.createResource(objectURI),
                    ResourceFactory.createProperty(mrt + "hasMetadata"),
                    ResourceFactory.createResource(producerErcURI)));
                model.add(ResourceFactory.createStatement(ResourceFactory.createResource(producerErcURI),
                    ResourceFactory.createProperty(mrt + "metadataSchema"),
                    ResourceFactory.createPlainLiteral("ERC")));
                model.add(ResourceFactory.createStatement(ResourceFactory.createResource(producerErcURI),
                    ResourceFactory.createProperty(mrt + "mimeType"),
                    ResourceFactory.createPlainLiteral("text/anvl")));
	    }

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
