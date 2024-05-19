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
package org.cdlib.mrt.ingest.handlers.process;

import java.io.ByteArrayInputStream;
import java.io.File;

import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.LocalIDUtil;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * mint object URL if necessary
 * @author mreyes
 */
public class HandlerMinter extends Handler<JobState>
{

    private static final String NAME = "HandlerMinter";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;

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

        Map<String, String> producerDC = null;
        Map<String, String> producerDataCite = null;
        Map<String, String> producerERC = null;
        Map<String, String> previousSystemERC = null;

	try {
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File metadataFile = new File(systemTargetDir, "mrt-ingest.txt");
            File momFile = new File(systemTargetDir, "mrt-mom.txt");
	    boolean resetObject = true;	 // recheck this logic now that we have localIDs
	    String returnValue = null;
	    String assignedObjectID = null;
	    String retrievedObjectID = null;
	    String retrievedLocalID = null;
	    boolean mint = true;
	    boolean haveMetadata = false;

	    // Need to read mrt-dc.xml data if available.
	    // This is also done in HandlerDescribe, but needs to also be done here for ID binding.  Cache results?
	    File producerDCFile = new File(ingestRequest.getQueuePath(), "producer/mrt-dc.xml");
	    if (producerDCFile.exists()) {
                producerDC = MetadataUtil.readDublinCoreXML(producerDCFile);
		// overwrite Form or Manifest parameters
           	haveMetadata = updateMetadata(jobState, producerDC, true, false);
            }

	    // Need to read mrt-datacite.xml data if available.
	    // This is also done in HandlerDescribe, but needs to also be done here for ID binding.  Cache results?
	    File producerDataCiteFile = new File(ingestRequest.getQueuePath(), "producer/mrt-datacite.xml");
	    if (producerDataCiteFile.exists()) {
                producerDataCite = MetadataUtil.readDataCiteXML(producerDataCiteFile);
		// overwrite Form or Manifest parameters
           	haveMetadata = updateMetadata(jobState, producerDataCite, true, true);

		// save datacite content for EZID
		jobState.setDataCiteMetadata(FileUtil.file2String(producerDataCiteFile));
            }

	    // Need to read mrt-erc.txt data if available.
	    // This is also done in HandlerDescribe, but needs to also be done here for ID binding.  Cache results? 
	    File producerErcFile = new File(ingestRequest.getQueuePath(), "producer/mrt-erc.txt");
	    if (producerErcFile.exists()) {
                producerERC = MetadataUtil.readMetadataANVL(producerErcFile);
		// overwrite Form or Manifest parameters or DC data
           	haveMetadata = updateMetadata(jobState, producerERC, true);
            }

	    if (ProfileUtil.isDemoMode(profileState)) {
	        if (jobState.getPrimaryID() != null) {
	            System.out.println("[debug] " + MESSAGE + "demo mode detected, resetting primary id.");
		    jobState.setLocalID(jobState.getPrimaryID().getValue());
	    	    jobState.setPrimaryID(null);
		}
	    }

	    Identifier localID = jobState.getLocalID();
	    if (localID != null && jobState.getPrimaryID() == null && ! localID.getValue().contains("(:unas)"))	
		retrievedObjectID = LocalIDUtil.fetchPrimaryID(profileState, localID.getValue());
	    else
		System.out.println("[debug] " + MESSAGE + "No Local ID specified for object");

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
		if (profileState.getIdentifierScheme() ==  Identifier.Namespace.DOI) {
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "Merritt no longer supports DOI minting: " + profileState.getIdentifierScheme());
                    throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Merritt no longer supports DOI minting: " + profileState.getIdentifierScheme());
		} 
		    
	        returnValue = MintUtil.processObjectID(profileState, jobState, ingestRequest, mint);
		if (profileState.getIdentifierScheme() ==  Identifier.Namespace.ARK) {
		    assignedObjectID = returnValue;
	            jobState.setPrimaryID(assignedObjectID);
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "objectID minted: " + assignedObjectID);
		} else {
	            if (DEBUG) System.out.println("[debug] " + MESSAGE + "Unsupported Identifier scheme: " + profileState.getIdentifierScheme());
                    throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Unsupported Identifier scheme: " + profileState.getIdentifierScheme());
		}
	    }

	    // At this point we'll need to populate primary/local ID with previous version
	    if (jobState.grabUpdateFlag()) {
		// populate metadata
		try {
		    System.out.println("[debug] " + MESSAGE + "Update specified, let's update primary/local IDs'");
		    File previousSystemErcFile = StorageUtil.getStorageFile(profileState, jobState.getPrimaryID().getValue(), "system/mrt-erc.txt");
	    	    if (previousSystemErcFile != null && previousSystemErcFile.exists()) {
                	previousSystemERC = MetadataUtil.readMetadataANVL(previousSystemErcFile);
           		// erc file in ANVL format
           		updateMetadata(jobState, previousSystemERC, true, false);	// update IDs only
            	    } else {
		        System.out.println("[info] " + MESSAGE + "No previous version exists'");
		    }
		} catch (Exception e) {
		    System.out.println("[warn] " + MESSAGE + "Error populating metadata w/ previous version");
		}

		// populate local ID
		try {
		    try {
		        retrievedLocalID = LocalIDUtil.fetchLocalID(profileState, jobState.getPrimaryID().getValue());
		    } catch (NullPointerException npe) {
		    }
		    if (retrievedLocalID != null) {
		        System.out.println("[info] " + MESSAGE + "Found previous local ID (storage db): " + retrievedLocalID);
		        System.out.println("[info] " + MESSAGE + "Appending to current local ID: " + jobState.getLocalID());
		        if (jobState.getLocalID() == null) {
			    jobState.setLocalID(retrievedLocalID);
			} else {
	                    for (String lid : retrievedLocalID.split(";")) {
				if (! jobState.getLocalID().getValue().contains(lid.trim())) {
				    // append
			            jobState.setLocalID(jobState.getLocalID() + "; " + lid.trim());
				} else {
		        	    System.out.println("[warn] " + MESSAGE + "Local ID already contains: " + lid.trim());
				}
			    }
			}
		        System.out.println("[info] " + MESSAGE + "Local ID now set to: " + jobState.getLocalID());
		    } else {
		        System.out.println("[warn] " + MESSAGE + "Could not retrieve local ID.");
		    }
		} catch (Exception e) {
		    System.out.println("[warn] " + MESSAGE + "Error populating local ID w/ previous version");
		}
	    }

	    // At this point we have a primary identifer.  Make sure it is an ARK.
	    if (! jobState.getPrimaryID().getValue().startsWith("ark")) {
	        System.err.println("[warn] " + MESSAGE + "Primary ID is not an ARK: " + jobState.getPrimaryID().getValue());
               	throw new TException.GENERAL_EXCEPTION("[error] " + MESSAGE + ": Primary ID is not an ARK: " + jobState.getPrimaryID().getValue());
	    }

	    // update metadata (ERC, target URL and context)
	    returnValue = MintUtil.processObjectID(profileState, jobState, ingestRequest, false);

	    if (! returnValue.startsWith("ark")) {
	        System.out.println("[info] " + MESSAGE + "Non ark returned by EZID: " + returnValue);
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
            producerDC = null;
            producerDataCite = null;
            producerERC = null;
            previousSystemERC = null;
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

    private boolean updateMetadata(JobState jobState, Map producerData, boolean updateIDs)
        throws TException
    {
	// default is to overwrite previous jobState values
        return updateMetadata(jobState, producerData, updateIDs, true);
    }

    /**
     * update job state w/ metadata
     *
     * @param JobState populate metadata fields if necessary
     * @param producerData producer supplied metadata
     * @param noUpdateIDs do not populate any ID fields (not needed for update)
     * @return boolean do we have necessary ERC data (who/what/where).  Needed for update request
     */
    private boolean updateMetadata(JobState jobState, Map producerData, boolean updateIDs, boolean overwrite)
        throws TException
    {
	boolean haveMetadata = false;
        String objectCreator = jobState.getObjectCreator();
        String objectTitle = jobState.getObjectTitle();
        String objectDate = jobState.getObjectDate();
        String objectPrimaryIdentifier = null;
        String objectLocalIdentifier = null;
        try {
            if (updateIDs) objectPrimaryIdentifier = jobState.getPrimaryID().getValue();
        } catch (Exception e) {
            objectPrimaryIdentifier = "(:unas)";
        }
        try {
             if (updateIDs) objectLocalIdentifier = jobState.getLocalID().getValue();
        } catch (Exception e) {
            objectLocalIdentifier = "(:unas)";
        }

        // update jobState if necessary
        if (producerData != null) {
            Iterator producerDataItr = producerData.keySet().iterator();
            while (producerDataItr.hasNext()) {
                String key = (String) producerDataItr.next();
                String value = (String) producerData.get(key);

                final String DELIMITER = "; ";
                if (key.matches("who") || key.matches("dc.creator") || key.matches("datacite.creator")) {
		    if ( ! trimLeft(trimRight(value)).equals("(:unas)")) {
			if (overwrite || objectCreator == null || objectCreator.equals("(:unas)")) {
			    // overwrite existing value
		            jobState.setObjectCreator(trimLeft(trimRight(value)));
			    if (DEBUG) System.out.println("[info] " + NAME + " found creator in metadata file: " + value);
	    	            haveMetadata = true;
			}
		    }
		}
                if (key.matches("what") || key.matches("dc.title") || key.matches("datacite.title")) {
		    if ( ! trimLeft(trimRight(value)).equals("(:unas)")) {
			if (overwrite || objectTitle == null || objectTitle.equals("(:unas)")) {
			    // overwrite existing value
		            jobState.setObjectTitle(trimLeft(trimRight(value)));
			    if (DEBUG) System.out.println("[info] " + NAME + " found title in metadata file: " + value);
	    	            haveMetadata = true;
			}
		    }
		}
                if (key.matches("when") || key.matches("dc.date") || key.matches("datacite.publicationyear")) {
		    if ( ! trimLeft(trimRight(value)).equals("(:unas)")) {
			if (overwrite || objectDate == null || objectDate.equals("(:unas)")) {
			    // overwrite existing value
		            jobState.setObjectDate(trimLeft(trimRight(value)));
			    if (DEBUG) System.out.println("[info] " + NAME + " found date in metadata file: " + value);
	    	            haveMetadata = true;
			}
		    }
		}

		if (key.matches("dc.contributor")) {
		    jobState.setDCcontributor(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.contributor in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.coverage")) {
		    jobState.setDCcoverage(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.coverage in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.creator")) {
		    jobState.setDCcreator(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.creator in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.date")) {
		    jobState.setDCdate(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.date in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.description")) {
		    jobState.setDCdescription(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.description in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.format")) {
		    jobState.setDCformat(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.format in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.identifier")) {
		    jobState.setDCidentifier(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.identifier in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.language")) {
		    jobState.setDClanguage(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.language in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.publisher")) {
		    jobState.setDCpublisher(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.publisher in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.relation")) {
		    jobState.setDCrelation(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.relation in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.rights")) {
		    jobState.setDCrights(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.rights in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.source")) {
		    jobState.setDCsource(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.source in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.subject")) {
		    jobState.setDCsubject(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.subject in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.title")) {
		    jobState.setDCtitle(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.title in metadata file: " + value);
	    	    haveMetadata = true;
		}
		if (key.matches("dc.type")) {
		    jobState.setDCtype(trimLeft(trimRight(value)));
		    if (DEBUG) System.out.println("[info] " + NAME + " found dc.type in metadata file: " + value);
	    	    haveMetadata = true;
		}

                // local ID processing
                if (key.matches("where-local") || (key.matches("dc.identifier") && ! jobState.grabObjectProfile().grabSuppressDublinCoreLocalID()) || (key.matches("where:") && ! value.contains("ark:/"))) {
                    if (! trimLeft(trimRight(value)).contains("(:unas)")) {
			// append existing local ID values
			if (updateIDs) {
			    String currentLocalID = null;
			    try {
			        currentLocalID = jobState.getLocalID().getValue();
			    } catch (NullPointerException npe) {}
			    if (currentLocalID != null) {
                                for (String lid : currentLocalID.split(";")) {
                                    if (! value.contains(lid)) {
                                        // append
                                        value += "; " + lid;
                                    } else {
                                        System.out.println("[warn] " + MESSAGE + "Previous version extracted Local ID already contains: " + lid);
                                    }
                                }
			    } 
			    value = sanitize(value);
                            jobState.setLocalID(trimLeft(trimRight(value)));
                            if (DEBUG) System.out.println("[info]" + MESSAGE + "Found local ID(s) in metadata file: " + value);
			}
		    }
                }
                // primary ID processing
                if (key.matches("where-primary") || (key.matches("where:") && value.contains("ark:/"))) {
                    if (! trimLeft(trimRight(value)).contains("(:unas)")) {
			// overwrite existing primary ID (should never be different)
			if (updateIDs) {
                            jobState.setPrimaryID(trimLeft(trimRight(value)));
                            if (DEBUG) System.out.println("[info]" + NAME + " Found primary ID in metadata file: " + value);
			}
		    }
                }
            }
        } else {
            if (DEBUG) System.out.println("[info]" + NAME + " No additional ERC metadata found");
        }

	return haveMetadata;
    }



    public String trimLeft(String s) {
        return s.replaceAll("^\\s+", "");
    }

    public String trimRight(String s) {
        return s.replaceAll("\\s+$", "");
    }

    public String sanitize(String s) {
	String rebuild = "";
	boolean first = true;
	for (String p: s.split(";")) {
	    p = p.trim();
	    if (! rebuild.contains(p)) {
		if (first) {
		    rebuild = p;
		    first = false;
		} else
		    rebuild += "; " + p;
	    }
	}
	if (first) rebuild = s;
        System.out.println("[info] " + MESSAGE + "sanitized localid: " + s + " ---> " + rebuild);

        return rebuild;
    }

    public String getName() {
	return NAME;
    }

}
