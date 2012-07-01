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

    private static final String NAME = "ProfileUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    private static final int MAX_HANDLERS = 20;
    public static final String DEFAULT_BATCH_ID = "JOB_ONLY";
    private LoggerInf logger = null;
    private Properties conf = null;

    private static URL url = null;
    private static URL storageUrl = null;
    private static int node;

    // extract strings
    private static final String matchProfileID = "ProfileID";
    private static final String matchProfileDescription = "ProfileDescription";
    private static final String matchIdentifierScheme = "Identifier-scheme";
    private static final String matchIdentifierNamespace = "Identifier-namespace";
    private static final String matchNotification = "Notification.";
    private static final String matchHandlerIngest = "Handler.";
    private static final String matchHandlerQueue = "HandlerQueue.";
    private static final String matchStorageService = "StorageService";
    private static final String matchStorageNode = "StorageNode";
    private static final String matchCreationDate = "CreationDate";
    private static final String matchModificationDate = "ModificationDate";
    private static final String matchObjectMinterURL = "ObjectMinterURL";
    private static final String matchCharacterizationURL = "CharacterizationURL";
    private static final String matchFixityURL = "FixityURL";
    private static final String matchDataoneURL = "DataoneURL";
    private static final String matchCoordinatingNodeURL = "CoordinatingNodeURL";
    private static final String matchDataoneNodeID = "DataoneNodeID";
    private static final String matchCallbackURL = "CallbackURL";
    private static final String matchStatusURL = "StatusURL";
    private static final String matchStatusView = "StatusView";
    private static final String matchCollection = "Collection.";
    private static final String matchType = "Type";
    private static final String matchRole = "Role";
    private static final String matchAggregate = "Aggregate";
    private static final String matchOwner = "Owner";
    private static final String matchContext = "Context";
    private static final String matchNotificationFormat = "NotificationFormat";
    
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
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("Mint Service parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setObjectMinterURL(url);
		} else if (key.startsWith(matchCharacterizationURL)) {
                    if (DEBUG) System.out.println("[debug] characterization URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("CharacterizationService parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setCharacterizationURL(url);
		} else if (key.startsWith(matchFixityURL)) {
                    if (DEBUG) System.out.println("[debug] fixity URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("FixityService parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setFixityURL(url);
		} else if (key.startsWith(matchDataoneURL)) {
                    if (DEBUG) System.out.println("[debug] dataONE member node URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("Dataone MN parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setDataoneURL(url);
		} else if (key.startsWith(matchCoordinatingNodeURL)) {
                    if (DEBUG) System.out.println("[debug] dataONE coordinating node URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("Dataone CN parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setCoordinatingNodeURL(url);
		} else if (key.startsWith(matchCallbackURL)) {
                    if (DEBUG) System.out.println("[debug] callback URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("DataONE parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setCallbackURL(url);
		} else if (key.startsWith(matchStatusURL)) {
                    if (DEBUG) System.out.println("[debug] status URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("StatusURL parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setStatusURL(url);
		} else if (key.startsWith(matchStatusView)) {
		    File statusView = null;
                    if (DEBUG) System.out.println("[debug] status View: " + value);
                    try {
                         statusView = new File(ingestDir, value);
                    } catch (Exception ex) {
                        throw new TException.INVALID_CONFIGURATION("StatusView file not found: " + statusView.toString());
                    }
		    profileState.setStatusView(statusView);
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
		} else if (key.startsWith(matchDataoneNodeID)) {
                    if (DEBUG) System.out.println("[debug] dataoneNodeID: " + value);
		    profileState.setDataoneNodeID(value);
		} else if (key.startsWith(matchNotificationFormat)) {
                    if (DEBUG) System.out.println("[debug] notification format: " + value);
		    profileState.setNotificationFormat(value);
	        } else {
                    if (DEBUG) System.out.println("[debug] could not procces profile parameter: " + key);
		}
	     }

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
