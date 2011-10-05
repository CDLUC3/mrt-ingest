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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.lang.NumberFormatException;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.HandlerState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Profile tool
 * @author mreyes
 */
public class ProfileUtil
{

    protected static final String NAME = "ProfileUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final int MAX_HANDLERS = 20;
    public static final String DEFAULT_BATCH_ID = "JOB_ONLY";
    protected LoggerInf logger = null;
    protected Properties conf = null;

    protected static URL url = null;
    protected static URL storageUrl = null;
    protected static int node;

    // extract strings
    protected static final String matchProfileID = "ProfileID";
    protected static final String matchProfileDescription = "ProfileDescription";
    protected static final String matchIdentifierScheme = "Identifier-scheme";
    protected static final String matchIdentifierNamespace = "Identifier-namespace";
    protected static final String matchNotification = "Notification";
    protected static final String matchHandlerIngest = "Handler.";
    protected static final String matchHandlerQueue = "HandlerQueue.";
    protected static final String matchStorageService = "StorageService";
    protected static final String matchStorageNode = "StorageNode";
    protected static final String matchCreationDate = "CreationDate";
    protected static final String matchModificationDate = "ModificationDate";
    protected static final String matchObjectMinterURL = "ObjectMinterURL";
    protected static final String matchCharacterizationURL = "CharacterizationURL";
    protected static final String matchFixityURL = "FixityURL";
    protected static final String matchDataoneURL = "DataoneURL";
    protected static final String matchCollection = "Collection";
    protected static final String matchType = "Type";
    protected static final String matchRole = "Role";
    protected static final String matchAggregate = "Aggregate";
    protected static final String matchOwner = "Owner";
    protected static final String matchContext = "Context";
    
    public static synchronized ProfileState getProfile(Identifier profileName, String ingestDir)
        throws TException
    {
    	TreeMap<Integer,HandlerState> ingestHandlers = new TreeMap();
    	TreeMap<Integer,HandlerState> queueHandlers = new TreeMap();
	ProfileState profileState = new ProfileState();

	try {
                File profileTxt = new File(ingestDir, profileName.getValue() + ".txt");		// assume a text extension
                if (!profileTxt.exists()) {
                    if (DEBUG) System.out.println("[info] Profile name not found. Attempting w/o extension");
                    profileTxt = new File(ingestDir, profileName.getValue());
                    if (!profileTxt.exists()) {
                        throw new TException.INVALID_OR_MISSING_PARM(
                            MESSAGE + "IngestService: profile not found: " + profileTxt.getAbsolutePath());
		    }
                }
                Properties profileProperties = PropertiesUtil.loadFileProperties(profileTxt);
                // Properties profileProperties = PropertiesUtil.loadProperties(profileTxt.getAbsolutePath());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                profileProperties.store(out, null);
                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

	    // clear old data if necessary
	    // if (notification.getContactEmail() != null) notification.getContactEmail().clear();
	
            Enumeration e = profileProperties.propertyNames();
            while( e.hasMoreElements() ) {
                String key = (String) e.nextElement();
                String value = profileProperties.getProperty(key);

                // if (DEBUG) System.out.println("[debug] " + MESSAGE + key + "*=*" + value + "*");

                if (key.startsWith(matchProfileID)) {
                    if (DEBUG) System.out.println("[debug] profile: " + value);
                    profileState.setProfileID(new Identifier(value));
		} else if (key.startsWith(matchProfileDescription)) {
                    if (DEBUG) System.out.println("[debug] profile description: " + value);
		    profileState.setProfileDescription(value);
		} else if (key.startsWith(matchIdentifierScheme)) {
                    if (DEBUG) System.out.println("[debug] identifier scheme: " + value);
		    profileState.setIdentifierScheme(value);
		} else if (key.startsWith(matchIdentifierNamespace)) {
                    if (DEBUG) System.out.println("[debug] identifier namespace: " + value);
		    profileState.setIdentifierNamespace(value);
		} else if (key.startsWith(matchNotification)) {
                    if (DEBUG) System.out.println("[debug] contact email: " + value);
		    profileState.setContactsEmail(new Notification(value));
		} else if (key.startsWith(matchObjectMinterURL)) {
                    if (DEBUG) System.out.println("[debug] object minter URL: " + value);
		    profileState.setObjectMinterURL(new URL(value));
		} else if (key.startsWith(matchCharacterizationURL)) {
                    if (DEBUG) System.out.println("[debug] characterization URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("CharacterizationService parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setCharacterizationURL(new URL(value));
		} else if (key.startsWith(matchFixityURL)) {
                    if (DEBUG) System.out.println("[debug] fixity URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("FixityService parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setFixityURL(new URL(value));
		} else if (key.startsWith(matchDataoneURL)) {
                    if (DEBUG) System.out.println("[debug] dataONE URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("DataONE parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setDataoneURL(new URL(value));
		} else if (key.startsWith(matchCollection)) {
                    if (DEBUG) System.out.println("[debug] collection: " + value);
		    profileState.setCollection(value);
		} else if (key.startsWith(matchHandlerIngest)) {
                    if (DEBUG) System.out.println("[debug] ingest handler: " + value);

                    String handlerIngestS = key.substring(matchHandlerIngest.length());
                    Integer handlerID = Integer.parseInt(handlerIngestS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    ingestHandlers.put(handlerID, (HandlerState) handler);
		} else if (key.startsWith(matchHandlerQueue)) {
                    if (DEBUG) System.out.println("[debug] queue handler: " + value);

                    String handlerQueueS = key.substring(matchHandlerQueue.length());
                    Integer handlerID = Integer.parseInt(handlerQueueS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    queueHandlers.put(handlerID, (HandlerState) handler);
		} else if (key.startsWith(matchStorageService)) {
                    if (DEBUG) System.out.println("[debug] storage service: " + value);
                    try {
                        storageUrl = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("StorageService parameter in profile is not a valid URL: " + value);
                    }
		} else if (key.startsWith(matchStorageNode)) {
                    if (DEBUG) System.out.println("[debug] storage node: " + value);
		    try {
		        node = new Integer(value).intValue();
		    } catch (java.lang.NumberFormatException nfe) {
                        throw new TException.INVALID_CONFIGURATION("StorageNode parameter in profile is not a valid node ID: " + value);
		    } 
		} else if (key.startsWith(matchCreationDate)) {
                    if (DEBUG) System.out.println("[debug] creation date: " + value);
		    profileState.setCreationDate(new DateState(value));
		} else if (key.startsWith(matchModificationDate)) {
                    if (DEBUG) System.out.println("[debug] modification date: " + value);
		    profileState.setModificationDate(new DateState(value));
		} else if (key.startsWith(matchType)) {
                    if (DEBUG) System.out.println("[debug] object type: " + value);
		    if (! profileState.setObjectType(value))
			throw new TException.INVALID_CONFIGURATION("object type not valid: " + value);
		} else if (key.startsWith(matchRole)) {
                    if (DEBUG) System.out.println("[debug] object role: " + value);
		    if (! profileState.setObjectRole(value))
			throw new TException.INVALID_CONFIGURATION("object role not valid: " + value);
		} else if (key.startsWith(matchAggregate)) {
                    if (DEBUG) System.out.println("[debug] aggregate: " + value);
		    if (! profileState.setAggregateType(value))
			if (StringUtil.isNotEmpty(profileState.getAggregateType()))
			    throw new TException.INVALID_CONFIGURATION("aggregate not valid: " + value);
		} else if (key.startsWith(matchOwner)) {
                    if (DEBUG) System.out.println("[debug] owner: " + value);
		    if (! profileState.setOwner(value))
			throw new TException.INVALID_CONFIGURATION("owner not a valid id: " + value);
		} else if (key.startsWith(matchContext)) {
                    if (DEBUG) System.out.println("[debug] context: " + value);
		    profileState.setContext(value);
	        } else {
                    if (DEBUG) System.out.println("[debug] could not procces profile parameter: " + key);
		}
	     }

	     // notification.setContactEmail(contactsEmail);
	     //profileState.setNotification(notification);
	     profileState.setIngestHandlers(ingestHandlers);
	     profileState.setQueueHandlers(queueHandlers);
	     profileState.setTargetStorage(new StoreNode(storageUrl, node));
 
             return profileState;

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            String err = MESSAGE + "error in creating profile ID - Exception:" + ex;

            System.out.println(err + " : " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(err);
	}
    }

    // write serialize object to disk
    public static synchronized void writeTo(BatchState batchState, File targetDir)
        throws Exception {
        try {
            FileOutputStream fout = new FileOutputStream(new File(targetDir, "batchState.obj"));
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(batchState);
            oos.close();
        } catch (Exception e) {
            throw new Exception("[error] " + MESSAGE + " could not write object to disk: " + targetDir.getAbsolutePath());
        }
   }

    // read serialize object from disk
    public static synchronized BatchState readFrom(BatchState batchState, File targetDir)
        throws Exception {
        try {
            FileInputStream fin = new FileInputStream(new File(targetDir, "batchState.obj"));
            ObjectInputStream ois = new ObjectInputStream(fin);
            batchState = (BatchState) ois.readObject();
            ois.close();
	    return batchState;
        } catch (Exception e) {
            throw new Exception("[error] " + MESSAGE + " could not read object from disk: " + targetDir.getAbsolutePath());
        }
   }

    public static boolean isDemoMode(ProfileState profileState) {
        try {
            return profileState.getProfileID().getValue().startsWith("demo_");
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine \"demo\" mode.");
	    e.printStackTrace();
        }
	return true;	// default
   }

}
