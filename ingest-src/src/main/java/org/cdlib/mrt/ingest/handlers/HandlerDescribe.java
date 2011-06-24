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
     * create/merge citation file
     *
     * @param JobState populate metadata fields if necessary
     * @param ercFile erc file
     * @param producerERC producer supplied metadata
     * @return successful in creating erc file
     */
    private boolean createERC(JobState jobState, File systemErcFile, Map producerERC)
        throws TException
    {
	final String DELIMITER = "; ";
	String append = "";
	String objectCreator = "";
	String objectTitle = "";
	String objectDate = "";
	String primaryIdentifier = "";
	String localIdentifier = "";

	// read existing ERC if applicable
        Map<String, String> systemERC = new LinkedHashMap();	// maintains insertion order
        if (systemErcFile.exists()) {
            systemERC = MetadataUtil.readMetadataANVL(systemErcFile);
        }

        if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating/updating erc: " + systemErcFile.getAbsolutePath());
	try {
	    objectCreator = jobState.getObjectCreator().replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	} catch (Exception e) {
	    objectCreator = "(:unas)";
	}
	try {
	    objectTitle = jobState.getObjectTitle().replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	} catch (Exception e) {
	    objectTitle = "(:unas)";
	}
	try {
	    objectDate = jobState.getObjectDate().replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	} catch (Exception e) {
	    objectDate = "(:unas)";
	}
	try {
	    primaryIdentifier = jobState.getPrimaryID().getValue().replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	} catch (Exception e) {
	    primaryIdentifier = "(:unas)";
	}
	try {
	     localIdentifier = jobState.getLocalID().getValue().replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	} catch (Exception e) {
	    localIdentifier = "(:unas)";
	}

	ArrayList arrayWhere = new ArrayList();
        Map<String, Object> ercProperties = new LinkedHashMap();   // maintains insertion order

        ercProperties.put("erc", "");
        ercProperties.put("who", objectCreator);
        ercProperties.put("what", objectTitle);
        ercProperties.put("when", objectDate);
        if ( StringUtil.isNotEmpty(primaryIdentifier)) 
	    arrayWhere.add(primaryIdentifier);

        if ( StringUtil.isNotEmpty(localIdentifier)) 
	    arrayWhere.add(localIdentifier);
        else 
	    arrayWhere.add("(:unas)");

	// update jobState/citation file with producer supplied values
	if (producerERC != null) {
	    Iterator producerERCitr = producerERC.keySet().iterator();
	    while (producerERCitr.hasNext()) {
	        String key = (String) producerERCitr.next();
	        String value = (String) producerERC.get(key);

	        if (key.matches("who") && ! value.contains("(:unas)")) {
		    append = "";
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Creator producer data found: " + value);
		    if (! objectCreator.contains("(:unas)")) {
		        if (! value.contains(objectCreator)) {
		            append = DELIMITER + objectCreator;
		            jobState.setObjectCreator(value + append);
		            ercProperties.put(key, value + append);
			    objectCreator = value + append;
			}
		    } else {
			ercProperties.put(key, objectCreator);
		    }
		}
	        if (key.matches("what") && ! value.contains("(:unas)")) {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Title producer data found: " + value);
		    if (! objectTitle.contains("(:unas)")) {
		        if (! value.contains(objectTitle)) {
		            append = DELIMITER + objectTitle; 
		            jobState.setObjectTitle(value + append);
		            ercProperties.put(key, value + append);
			    objectTitle = value + append;
			}
		    } else {
			ercProperties.put(key, objectTitle);
		    }
		}
	        if (key.matches("when") && ! value.contains("(:unas)")) {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Date producer data found: " + value);
		    if (! objectDate.contains("(:unas)")) {
		        if (! value.contains(objectDate)) {
		            append = DELIMITER + objectDate;
		            jobState.setObjectDate(value + append);
		            ercProperties.put(key, value + append);
			    objectDate = value + append;
			}
		    } else {
			ercProperties.put(key, objectDate);
		    }

		}
	        if (key.matches("where") && ! value.contains("ark:") && ! value.contains("(:unas)")) {
		    try {
                        if (localIdentifier != null && ! localIdentifier.contains(value)) {
                            append = DELIMITER + localIdentifier;
                            jobState.setLocalID(value + append);

                            try {
                                int i = arrayWhere.indexOf("(:unas)");
                                if (i >= 0) arrayWhere.remove(i);
                                arrayWhere.add(value + append);
                            } catch (Exception ee) {}
			}
		    } catch (Exception e) {}
		} 
	        if (key.matches("note") || key.matches("how") || key.startsWith("who/") || key.startsWith("what/") || key.startsWith("when/")) {
		    // let other ERC data through 
		    ercProperties.put(key, value);
		}
	    }
	} else {
	    if (DEBUG) System.out.println("No additional producer ERC metadata found");
	}

	// update jobState/citation file with existing system values
	if (systemERC != null) {
	    Iterator systemERCitr = systemERC.keySet().iterator();
	    while (systemERCitr.hasNext()) {
	        String key = ((String) systemERCitr.next()).replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	        String value = ((String) systemERC.get(key)).replaceAll("^\\s+", "").replaceAll("\\s+$", "");

		// append
	        if (key.matches("who") && ! value.contains("(:unas)")) {
		    if (! objectCreator.contains("(:unas)")) {		// any existing producer data?
		        if (value.contains(objectCreator)) {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Creator data (system) already exists: " + objectCreator);
			    ercProperties.put(key, value);
		            jobState.setObjectCreator(value);
		        } else {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Appending additional Creator data (system): " + value);
		            append = DELIMITER + objectCreator; 
		            jobState.setObjectCreator(value + append);
			    ercProperties.put(key, value + append);
			}
		    } else {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Populating Creator with existing data (system): " + value);
			ercProperties.put(key, value);
		        jobState.setObjectCreator(value);
		    }
		}
	        if (key.matches("what")&& ! value.contains("(:unas)")) {
		    if (! objectTitle.contains("(:unas)")) {
		        if (value.contains(objectTitle)) {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Title data (system) already exists: " + objectTitle);
			    ercProperties.put(key, value);
		            jobState.setObjectTitle(value);
		        } else {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Appending additional Title data (system): " + value);
		            append = DELIMITER + objectTitle; 
		            jobState.setObjectTitle(value + append);
			    ercProperties.put(key, value + append);
			}
		    } else {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Populating Title with existing data (system): " + value);
			ercProperties.put(key, value);
		        jobState.setObjectTitle(value);
		    }
		}
	        if (key.matches("when") && ! value.contains("(:unas)")) {
		    if (! objectDate.contains("(:unas)")) {
		        if (value.contains(objectDate)) {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Additional Date data (system) already exists: " + objectDate);
			    ercProperties.put(key, value);
		            jobState.setObjectDate(value);
		        } else {
        		    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Found additional Date data (system): " + value);
		            append = DELIMITER + objectDate; 
		            jobState.setObjectDate(value + append);
			    ercProperties.put(key, value + append);
			}
		    } else {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Populating Date with existing data (system): " + value);
			ercProperties.put(key, value);
		        jobState.setObjectDate(value);
		    }
		}
	        if (key.matches("where") && ! value.contains("ark:") && ! value.contains("(:unas)")) {
		    try {
		        if (localIdentifier != null && ! localIdentifier.contains(value)) {
			    append = DELIMITER + localIdentifier;
		            jobState.setLocalID(value + append);

			    try {
		                int i = arrayWhere.indexOf("(:unas)");
		                if (i >= 0) arrayWhere.remove(i);
		                arrayWhere.add(value + append);
			    } catch (Exception ee) {}
			}
		    } catch (Exception e) {}
		}
	        if (key.matches("note") || key.matches("how") || key.startsWith("who/") || key.startsWith("what/") || key.startsWith("when/")) {
		    // let other ERC data through 
		    ercProperties.put(key, value);
		}
	    }
	} else {
	    if (DEBUG) System.out.println("No additional system ERC metadata found");
	}

        ercProperties.put("where", arrayWhere);
        return MetadataUtil.writeMetadataANVL(systemErcFile, ercProperties, false);
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
