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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.StorageUtil;
import org.cdlib.mrt.utility.FileUtil;
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

    private static final String NAME = "HandlerDescribe";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String FS = System.getProperty("file.separator");
    private LoggerInf logger = null;
    private Properties conf = null;
    private Integer defaultStorage = null;
    private File systemTargetDir = null;

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

	Map<String, String> producerERC = null;
	try {
            systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File producerTargetDir = new File(ingestRequest.getQueuePath(), "producer");
            File systemErcFile = new File(systemTargetDir, "mrt-erc.txt");
            File producerErcFile = new File(producerTargetDir, "mrt-erc.txt");
            File producerDCFile = new File(producerTargetDir, "mrt-dc.xml");
            File producerDataCiteFile = new File(producerTargetDir, "mrt-datacite.xml");
            File producerEMLFile = new File(producerTargetDir, "mrt-eml.xml");
            File producerEmbargoFile = new File(producerTargetDir, "mrt-embargo.txt");
            File systemDCFile = new File(systemTargetDir, "mrt-dc.xml");

            // save deletion file
            if (jobState.grabUpdateFlag()) {
                // process deletions
                File sourceDelete = new File(ingestRequest.getQueuePath() + FS + "producer" + FS + "mrt-delete.txt");
                if (sourceDelete.exists()) {
                    if (DEBUG) System.out.println("[debug] " + MESSAGE + " Found deletion file, moving into system dir");
                    File targetDelete = new File(ingestRequest.getQueuePath() + FS + "system" + FS + "mrt-delete.txt");
                    if (! sourceDelete.renameTo(targetDelete)) {
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + " Could not rename deletion file");
		    } 
                }
            }

	    if (producerErcFile.exists()) {
	        producerERC = MetadataUtil.readMetadataANVL(producerErcFile);
	    }

            // erc file in ANVL format
            if ( ! createERC(jobState, systemErcFile, producerERC)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to create ERC file: " + systemErcFile.getAbsolutePath());
            }

	    // Check for embargo data
	    Map<String, String> producerEmbargo = null;
	    if (producerEmbargoFile.exists()) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Embargo data file found");
	        producerEmbargo = MetadataUtil.readEmbargoANVL(producerEmbargoFile);
                // Sanity check
                if ( producerEmbargo.size() < 1 || ! checkEmbargo(producerEmbargo)) {
                    throw new TException.GENERAL_EXCEPTION("[error] "
                        + MESSAGE + ": Embargo data not valid");
		}
	    } else {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "NO Embargo data file found");
	    }


            // Dublin Core file in XML format (system/mrt-dc.xml)
            if ( ! createDC(jobState, (LinkedHashMap) MetadataUtil.readDublinCoreXML(producerDCFile), systemDCFile)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to create Dublin Core file: " + systemDCFile.getAbsolutePath());
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
	    producerERC = null;
            // cleanup?
        }

    }


    /**
     * Sanity check for embargo data
     *
     * @param producerEmbargo producer supplied embargo
     * @return successful validated data
     */
    private boolean checkEmbargo(Map producerEmbargo)
        throws Exception
    {

	if (producerEmbargo != null) {
	    Iterator producerEmbargoItr = producerEmbargo.keySet().iterator();
	    while (producerEmbargoItr.hasNext()) {
	        String key = (String) producerEmbargoItr.next();
	        String value = (String) producerEmbargo.get(key);

	        if (key.toLowerCase().matches("embargoenddate")) {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "Embargo data found: " + value);

		    // "NONE" is supported
		    if (value.toUpperCase().matches(".*NONE.*")) {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Valid Embargo data found: " + value);
			return true;
		    }

		    // regex for ISO8601
		    if (value.toUpperCase().matches(".*\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(([+-]\\d\\d:\\d\\d)|Z)?.*")) {
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "Valid Embargo data found: " + value);
			return true;
		    }
		    
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "No Valid Embargo data found: " + value);
		    return false;
		} else {
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "No Valid Embargo key found: " + key);
		    return false;
		}
	    }
	}
	return true;
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
/*
Now done in HandlerMinter
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
		// local ID in ERC file?
	        if (key.matches("where") && ! value.contains("ark:") && ! value.contains("(:unas)")) {
		    try {
                        append = "";
			value = trimLeft(trimRight(value));
                        if (localIdentifier == null || localIdentifier.contains("(:unas)")) {
                            jobState.setLocalID(value);
	    		    if (DEBUG) System.out.println(MESSAGE + " Found local ID in mrt-erc.txt: " + value);
                        } else if (! localIdentifier.contains(value)) {
                            append = DELIMITER + localIdentifier;
                            jobState.setLocalID(value + append);
	    		    if (DEBUG) System.out.println(MESSAGE + " Found local ID in mrt-erc.txt: " + value);
			} 

                        try {
                            int i = arrayWhere.indexOf("(:unas)");
                            if (i >= 0) arrayWhere.remove(i);
			    // check if already exists
                            if (arrayWhere.indexOf(value) < 0 ) arrayWhere.add(value + append);
                        } catch (Exception ee) {}
		    } catch (Exception e) {}
		} 
		// primary ID in ERC file?
	        if (key.matches("where") && value.contains("ark:") && ! value.contains("(:unas)")) {
		    try {
			// Only update if empty
                        if (primaryIdentifier == null || primaryIdentifier.contains("(:unas)")) { 
                            jobState.setPrimaryID(value);
	    		    if (DEBUG) System.out.println(MESSAGE + " Found primary ID in mrt-erc.txt: " + value);
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
	// -- obsolete -- this file should not exist with new update() logic
	// -- keep for refernce only
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
                // local ID in ERC file?
                if (key.matches("where") && ! value.contains("ark:") && ! value.contains("(:unas)")) {
                    try {
                        append = "";
                        value = trimLeft(trimRight(value));
                        if (localIdentifier == null || localIdentifier.contains("(:unas)")) {
                            jobState.setLocalID(value);
                            if (DEBUG) System.out.println(MESSAGE + " Found local ID in mrt-erc.txt: " + value);
                        } else if (! localIdentifier.contains(value)) {
                            append = DELIMITER + localIdentifier;
                            jobState.setLocalID(value + append);
                            if (DEBUG) System.out.println(MESSAGE + " Found local ID in mrt-erc.txt: " + value);
                        }

                        try {
                            int i = arrayWhere.indexOf("(:unas)");
                            if (i >= 0) arrayWhere.remove(i);
                            // check if already exists
                            if (arrayWhere.indexOf(value) < 0 ) arrayWhere.add(value + append);
                        } catch (Exception ee) {}
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
*/

        ercProperties.put("where", arrayWhere);
        return MetadataUtil.writeMetadataANVL(systemErcFile, ercProperties, " ", false);
    }


    /**
     * create/merge dublin core file
     *
     * @param JobState populate metadata fields if necessary. (input)
     * @param DCFile dublin core file. (input)
     * @param producerDC producer supplied DC metadata. (output)
     * @return successful in creating DC file
     */
    private boolean createDC(JobState jobState, Map producerDC, File systemDCFile)
        throws TException
    {
	final String DC_DELIMITER = "; ";
	String value = null;

        if (DEBUG) System.out.println("[debug] " + MESSAGE + "creating/updating dublin core: " + systemDCFile.getAbsolutePath());
	try {
	    value = jobState.getDCcontributor();
	    if (value != null) {
		String key = "dc.contributor";
		if (producerDC.containsKey(key)) {
		    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCcoverage();
	    if (value != null) {
		String key = "dc.coverage";
		if (producerDC.containsKey(key)) {
		    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
        	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCcreator();
	    if (value != null) {
		String key = "dc.creator";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCdate();
	    if (value != null) {
		String key = "dc.date";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
		        producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCdescription();
	    if (value != null) {
		String key = "dc.description";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
		        producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCformat();
	    if (value != null) {
		String key = "dc.format";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
		        producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCidentifier();
	    if (value != null) {
		String key = "dc.identifier";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDClanguage();
	    if (value != null) {
		String key = "dc.language";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "found DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCpublisher();
	    if (value != null) {
		String key = "dc.publisher";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCrelation();
	    if (value != null) {
		String key = "dc.relation";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCsource();
	    if (value != null) {
		String key = "dc.source";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCsubject();
	    if (value != null) {
		String key = "dc.subject";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCtitle();
	    if (value != null) {
		String key = "dc.title";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }
	try {
	    value = jobState.getDCtype();
	    if (value != null) {
		String key = "dc.type";
		if (producerDC.containsKey(key)) {
                    if (! ((String) producerDC.get(key)).contains(value)) {
			producerDC.put(key, producerDC.get(key) + DC_DELIMITER + value);
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		    }
		} else {
		    producerDC.put(key, value);
        	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "additional DC metadata " + key + ": " + value);
		}
	    }
	} catch (Exception e) { }

	try {
	    MetadataUtil.writeDublinCoreXML(producerDC, systemDCFile);
	    return true;
	} catch (Exception e) { e.printStackTrace(); return false; }
    }



    public String getName() {
	return NAME;
    }

    public String trimLeft(String s) {
        return s.replaceAll("^\\s+", "");
    }
 
    public String trimRight(String s) {
        return s.replaceAll("\\s+$", "");
    } 

}
