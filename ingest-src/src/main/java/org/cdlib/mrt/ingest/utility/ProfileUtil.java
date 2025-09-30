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

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Vector;

import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.HandlerState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.ProfilesState;
import org.cdlib.mrt.ingest.utility.S3Util;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.ingest.StoreNode;
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
    // private static final boolean DEBUG = false;
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
    private static final String matchHandlerIngest = "HandlerIngest.";
    private static final String matchHandlerBatchProcess = "HandlerBatchProcess.";
    private static final String matchHandlerBatchReport = "HandlerBatchReport.";
    private static final String matchHandlerQueue = "HandlerQueue.";
    private static final String matchHandlerInitialize = "HandlerInitialize.";
    private static final String matchHandlerEstimate = "HandlerEstimate.";
    private static final String matchHandlerProvision = "HandlerProvision.";
    private static final String matchHandlerDownload = "HandlerDownload.";
    private static final String matchHandlerProcess = "HandlerProcess.";
    private static final String matchHandlerRecord = "HandlerRecord.";
    private static final String matchHandlerNotify = "HandlerNotify.";
    private static final String matchStorageService = "StorageService";
    private static final String matchStorageNode = "StorageNode";
    private static final String matchCreationDate = "CreationDate";
    private static final String matchModificationDate = "ModificationDate";
    private static final String matchObjectMinterURL = "ObjectMinterURL";
    private static final String matchCallbackURL = "CallbackURL";
    private static final String matchPriority = "Priority";
    // private static final String matchStatusURL = "StatusURL";
    // private static final String matchStatusView = "StatusView";
    private static final String matchCollection = "Collection.";
    private static final String matchType = "Type";
    private static final String matchRole = "Role";
    private static final String matchAggregate = "Aggregate";
    private static final String matchOwner = "Owner";
    private static final String matchContext = "Context";
    private static final String matchEzidCoowner = "EZID_co-owner";
    private static final String matchNotificationFormat = "NotificationFormat";
    private static final String matchNotificationType = "NotificationType";
    private static final String matchNotificationSuppression = "NotificationSuppression";
    private static final String matchSuppressDublinCoreLocalID = "SuppressDublinCoreLocalID";

    // Process active profile (S3)
    public static synchronized ProfileState getProfile(Identifier profileName, String ingestDir, String s3endpoint, String accessKey, String secretKey, String profileNode, String profilePath, boolean delete)
        throws TException
    {
	ProfileState profileState;
	try {

	    InputStream inputStream = null;
            Region region = Region.US_WEST_2;

	    String batchID = ingestDir.substring(ingestDir.indexOf("bid-"));
	    File profileFile = createTempFile(batchID + "_" + profileName.getValue());

	    if (! profileFile.exists()) {

                System.out.println("[info] Cached S3 profile file does not exist: " + profileFile.getAbsolutePath());
                System.out.println("[info] Downloading S3 profile: " + profileName.getValue() + " From S3: " + profileNode + "/" + profilePath );
		String s3Path = profilePath + "/" + profileName.getValue();
		S3Client s3Client = null;

		if (! StringUtil.isEmpty(s3endpoint)) {
                    System.out.println("[info] Detected Minio style S3 environment");
		    s3Client = S3Util.getMinioClient(region, accessKey, secretKey, s3endpoint);
		} else {
                    System.out.println("[info] Detected AWS style S3 environment");
		    s3Client = S3Util.getAWSClient(region);
		}


                try {
                    inputStream = S3Util.getObjectSyncInputStream(s3Client, profileNode, s3Path);
		    copyInputStreamToFile(inputStream, profileFile);
	        } catch (Exception e2) {
		    e2.printStackTrace();
		    throw new Exception(e2.getMessage());
	        } finally {
		}
	    } else {
                System.out.println("[info] Cached S3 profile exists: " + profileNode + " - " + profilePath + " - " + profileName.toString());
	    }

	    profileState = getProfile(profileName, profileFile);
	    if (delete) deleteTempFile(batchID + "_" + profileName.getValue());

	    return profileState;

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            String err = MESSAGE + "error in creating profile ID - Exception:" + ex;

            System.out.println(err + " : " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(err);
	} finally {
	    profileState = null;
	}
    }

    // Process active profile (Local file)
    public static synchronized ProfileState getProfile(Identifier profileName, String ingestDir)
        throws TException
    {
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
		return getProfile(profileName, profileTxt);
	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            String err = MESSAGE + "error in creating profile ID - Exception:" + ex;

            System.out.println(err + " : " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(err);
	}
    }

    // Process any profile
    public static synchronized ProfileState getProfile(Identifier profileName, File profileTxt)
        throws TException
    {
    	TreeMap<Integer,HandlerState> ingestHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> batchProcessHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> batchReportHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> queueHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> initializeHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> estimateHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> provisionHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> downloadHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> processHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> recordHandlers = new TreeMap<Integer,HandlerState>();
    	TreeMap<Integer,HandlerState> notifyHandlers = new TreeMap<Integer,HandlerState>();
	ProfileState profileState = new ProfileState();

	try {
                if (!profileTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "IngestService: profile not found: " + profileTxt.getAbsolutePath());
                }
                Properties profileProperties = PropertiesUtil.loadFileProperties(profileTxt);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                profileProperties.store(out, null);
                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

	
            Enumeration<?> e = (Enumeration<?>) profileProperties.propertyNames();
            while( e.hasMoreElements() ) {
                String key = (String) e.nextElement();
                String value = profileProperties.getProperty(key);

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
		//} else if (key.startsWith(matchCharacterizationURL)) {
                    //if (DEBUG) System.out.println("[debug] characterization URL: " + value);
                    //try {
                        //url = new URL(value);
                    //} catch (MalformedURLException muex) {
                        //throw new TException.INVALID_CONFIGURATION("CharacterizationService parameter in profile is not a valid URL: " + value);
                    //}
		    //profileState.setCharacterizationURL(url);
		//} else if (key.startsWith(matchCoordinatingNodeURL)) {
                    //if (DEBUG) System.out.println("[debug] dataONE coordinating node URL: " + value);
                    //try {
                        //url = new URL(value);
                    //} catch (MalformedURLException muex) {
                        //throw new TException.INVALID_CONFIGURATION("Dataone CN parameter in profile is not a valid URL: " + value);
                    //}
		    //profileState.setCoordinatingNodeURL(url);
		} else if (key.startsWith(matchCallbackURL)) {
                    if (DEBUG) System.out.println("[debug] callback URL: " + value);
                    try {
                        url = new URL(value);
                    } catch (MalformedURLException muex) {
                        throw new TException.INVALID_CONFIGURATION("DataONE parameter in profile is not a valid URL: " + value);
                    }
		    profileState.setCallbackURL(url);
		} else if (key.startsWith(matchPriority)) {
                    if (DEBUG) System.out.println("[debug] Priority: " + value);
		    profileState.setPriority(value);
		//} else if (key.startsWith(matchStatusURL)) {
                    //if (DEBUG) System.out.println("[debug] status URL: " + value);
                    //try {
                        //url = new URL(value);
                    //} catch (MalformedURLException muex) {
                        //throw new TException.INVALID_CONFIGURATION("StatusURL parameter in profile is not a valid URL: " + value);
                    //}
		    //profileState.setStatusURL(url);
		} else if (key.startsWith(matchCollection)) {
                    if (DEBUG) System.out.println("[debug] collection: " + value);
                    if ((! value.startsWith("ark:/")) && isValidProfile(profileName.getValue())) throw new TException.INVALID_CONFIGURATION("Collection ID is not a valid: " + value);
		    profileState.setCollection(value);
		    // For display state only - assumes only on collection per object
		    profileState.setCollectionName(value);
		} else if (key.startsWith(matchHandlerIngest)) {
                    if (DEBUG) System.out.println("[debug] ingest handler: " + value);

                    String handlerIngestS = key.substring(matchHandlerIngest.length());
                    Integer handlerID = Integer.parseInt(handlerIngestS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    ingestHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerQueue)) {
                    if (DEBUG) System.out.println("[debug] queue handler: " + value);

                    String handlerQueueS = key.substring(matchHandlerQueue.length());
                    Integer handlerID = Integer.parseInt(handlerQueueS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    queueHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerBatchProcess)) {
                    if (DEBUG) System.out.println("[debug] batch process handler: " + value);

                    String handlerBatchProcessS = key.substring(matchHandlerBatchProcess.length());
                    Integer handlerID = Integer.parseInt(handlerBatchProcessS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    batchProcessHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerBatchReport)) {
                    if (DEBUG) System.out.println("[debug] batch report handler: " + value);

                    String handlerBatchReportS = key.substring(matchHandlerBatchReport.length());
                    Integer handlerID = Integer.parseInt(handlerBatchReportS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    batchReportHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerInitialize)) {
                    if (DEBUG) System.out.println("[debug] initialize handler: " + value);

                    String handlerInitializeS = key.substring(matchHandlerInitialize.length());
                    Integer handlerID = Integer.parseInt(handlerInitializeS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    initializeHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerEstimate)) {
                    if (DEBUG) System.out.println("[debug] estimate handler: " + value);

                    String handlerEstimateS = key.substring(matchHandlerEstimate.length());
                    Integer handlerID = Integer.parseInt(handlerEstimateS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    estimateHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerProvision)) {
                    if (DEBUG) System.out.println("[debug] provision handler: " + value);

                    String handlerProvisionS = key.substring(matchHandlerProvision.length());
                    Integer handlerID = Integer.parseInt(handlerProvisionS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    provisionHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerDownload)) {
                    if (DEBUG) System.out.println("[debug] download handler: " + value);

                    String handlerDownloadS = key.substring(matchHandlerDownload.length());
                    Integer handlerID = Integer.parseInt(handlerDownloadS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    downloadHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerProcess)) {
                    if (DEBUG) System.out.println("[debug] process handler: " + value);

                    String handlerProcessS = key.substring(matchHandlerProcess.length());
                    Integer handlerID = Integer.parseInt(handlerProcessS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    processHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerRecord)) {
                    if (DEBUG) System.out.println("[debug] record handler: " + value);

                    String handlerRecordS = key.substring(matchHandlerRecord.length());
                    Integer handlerID = Integer.parseInt(handlerRecordS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    recordHandlers.put(handlerID, handler);
		} else if (key.startsWith(matchHandlerNotify)) {
                    if (DEBUG) System.out.println("[debug] notify handler: " + value);

                    String handlerNotifyS = key.substring(matchHandlerNotify.length());
                    Integer handlerID = Integer.parseInt(handlerNotifyS);

		    HandlerState handler = new HandlerState();
		    handler.setHandlerName(value);
		    notifyHandlers.put(handlerID, handler);
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
			if ( ! isValidProfile(profileName.getValue())) {
			   // Must be a Template, set to nonsensical value
		           node = new Integer(0).intValue();
			} else {
                           throw new TException.INVALID_CONFIGURATION("StorageNode parameter in profile is not a valid node ID: " + value);
			}
		    } 
		} else if (key.startsWith(matchCreationDate)) {
                    if (DEBUG) System.out.println("[debug] creation date: " + value);
		    DateState ds = new DateState(value);
		    if ((ds.getDate() == null) && isValidProfile(profileName.getValue())) throw new TException.INVALID_CONFIGURATION("Creation Date parameter in profile is not a valid: " + value);
		    profileState.setCreationDate(ds);
		} else if (key.startsWith(matchModificationDate)) {
                    if (DEBUG) System.out.println("[debug] modification date: " + value);
		    DateState ds = new DateState(value);
		    if ((ds.getDate() == null) && isValidProfile(profileName.getValue())) throw new TException.INVALID_CONFIGURATION("Modification Date  parameter in profile is not a valid: " + value);
		    profileState.setModificationDate(ds);
		} else if (key.startsWith(matchType)) {
                    if (DEBUG) System.out.println("[debug] object type: " + value);
		    if (! profileState.setObjectType(value)) throw new TException.INVALID_CONFIGURATION("Object type not valid: " + value);
		} else if (key.startsWith(matchRole)) {
                    if (DEBUG) System.out.println("[debug] object role: " + value);
		    if (! profileState.setObjectRole(value)) throw new TException.INVALID_CONFIGURATION("Object role not valid: " + value);
		} else if (key.startsWith(matchAggregate)) {
                    if (DEBUG) System.out.println("[debug] aggregate: " + value);
		    if (! profileState.setAggregateType(value))
			if (StringUtil.isNotEmpty(profileState.getAggregateType()) && isValidProfile(profileName.getValue())) throw new TException.INVALID_CONFIGURATION("Aggregate not valid: " + value);
		} else if (key.startsWith(matchOwner)) {
                    if (DEBUG) System.out.println("[debug] owner: " + value);
		    if (! profileState.setOwner(value)) {
                        if ( ! isValidProfile(profileName.getValue())) {
                           // Must be a Template, set to non-sensical value
			   profileState.setOwner("ark:/template/profile");
			} else {
			   throw new TException.INVALID_CONFIGURATION("Owner not a valid id: " + value);
			}
		    }
		} else if (key.startsWith(matchContext)) {
                    if (DEBUG) System.out.println("[debug] context: " + value);
		    profileState.setContext(value);
		} else if (key.startsWith(matchEzidCoowner)) {
                    if (DEBUG) System.out.println("[debug] EZID co-owner found: " + value);
		    profileState.setEzidCoowner(value);
		} else if (key.startsWith(matchNotificationFormat)) {
                    if (DEBUG) System.out.println("[debug] notification format: " + value);
		    profileState.setNotificationFormat(value);
		} else if (key.startsWith(matchNotificationType)) {
                    if (DEBUG) System.out.println("[debug] notification type: " + value);
		    profileState.setNotificationType(value);
		} else if (key.startsWith(matchNotificationSuppression)) {
                    if (DEBUG) System.out.println("[debug] notification suppression: " + value);
		    profileState.setNotificationSuppression(value);
		} else if (key.startsWith(matchSuppressDublinCoreLocalID)) {
                    if (DEBUG) System.out.println("[debug] suppress dc.identifer/local ID processing: " + value);
		    if (value.equalsIgnoreCase("true")) profileState.setSuppressDublinCoreLocalID(true);
	        } else {
                    if (DEBUG) System.out.println("[debug] could not procces profile parameter: " + key);
		}
	     }

	     profileState.setIngestHandlers(ingestHandlers);
	     profileState.setBatchProcessHandlers(batchProcessHandlers);
	     profileState.setBatchReportHandlers(batchReportHandlers);
	     profileState.setQueueHandlers(queueHandlers);
	     profileState.setInitializeHandlers(initializeHandlers);
	     profileState.setEstimateHandlers(estimateHandlers);
	     profileState.setProvisionHandlers(provisionHandlers);
	     profileState.setDownloadHandlers(downloadHandlers);
	     profileState.setProcessHandlers(processHandlers);
	     profileState.setRecordHandlers(recordHandlers);
	     profileState.setNotifyHandlers(notifyHandlers);
	     profileState.setTargetStorage(new StoreNode(storageUrl, node));
 
             return profileState;

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            String err = MESSAGE + "error in creating profile ID - Exception:" + ex;

            System.out.println(err + " : " + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(err);
	} finally {
	    ingestHandlers = null;
	    batchProcessHandlers = null;
	    batchReportHandlers = null;
	    queueHandlers = null;
	    estimateHandlers = null;
	    downloadHandlers = null;
	    processHandlers = null;
	    recordHandlers = null;
	    notifyHandlers = null;
	}
    }


    public static synchronized ProfilesState getProfiles(String profileDir, boolean recurse)
        throws TException
    {
	ProfilesState profilesState = new ProfilesState();
        File profileDirectory = new File(profileDir);

	try {
                File[] files = null;
		String filter = isAdmin(profileDir);
		if (! recurse) {
                   files = profileDirectory.listFiles();
		} else {
		   // admin
		   Vector<File> vfiles = new Vector<File>();
		   if (filter == null) {
		       // e.g. /tdr/ingest/profiles/admin
		       FileUtil.getDirectoryFiles(new File(profileDir), vfiles);
		   } else { 
		       // e.g. /tdr/ingest/profiles/admin/docker/sla
		       FileUtil.getDirectoryFiles(new File(profileDir).getParentFile(), vfiles);
		   }
		   files = (File[]) vfiles.toArray(new File[0]);
		}

                for (File profile: files) {
		   String fname = profile.getName();
		   String cpath = profile.getParentFile().getCanonicalPath();
		   String description = null;
		   DateState modDate = null;
                   if (! recurse) {
                      if (profile.isDirectory()) continue;
		      if (! isValidProfile(fname) && ! isTemplate(fname)) continue; 
		   } else {
		      //filter if necessary
		      if (filter != null ) {
		          if (! cpath.endsWith(filter)) continue;
		      }
		      description = getDescription(new File(cpath + "/" + fname));
		      modDate = getModDate(new File(cpath + "/" + fname));
		   }
		   profilesState.addProfileInstance(profile, recurse, description, modDate);
		}

		return profilesState;

	} catch (Exception ex) {
            String err = MESSAGE + "error getting profiles - Exception:" + ex;

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

    public static boolean isValidProfile(String profileName) {
        try {
	    // Convention that all profiles end with "_content"
            return profileName.endsWith("_content");
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine if profile is valid: " + profileName);
        }
	return true;	// default
   }

    public static boolean isTemplate(String profileName) {
        try {
	    // Convention that all tempaltes start with "TEMPLATE"
            return profileName.startsWith("TEMPLATE");
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine if file is a template: " + profileName);
        }
	return true;	// default
   }

    public static String isAdmin(String profileName) {
        try {
	    // is this profile list to be filtered
	    if (profileName.endsWith("/collection")) return "collection";
	    if (profileName.endsWith("/owner")) return "owner";
	    if (profileName.endsWith("/sla")) return "sla";
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine if profile is an admin to be filtered: " + profileName);
        }
	return null;	// default
   }

    public static String getDescription(File profile) {
        try {
	    ProfileState profileState = new ProfileState();
	    Identifier profileID = new Identifier(profile.getName(), Identifier.Namespace.Local);
	    profileState = getProfile(profileID, profile);

	    return profileState.getProfileDescription();
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine retrieve profile description from profile: " + profile.getName());
        }
	return null;
   }

    public static DateState getModDate(File profile) {
        try {
	    ProfileState profileState = new ProfileState();
	    Identifier profileID = new Identifier(profile.getName(), Identifier.Namespace.Local);
	    profileState = getProfile(profileID, profile);

	    return profileState.getModificationDate();
        } catch (Exception e) {
            System.err.println("[warning] " + MESSAGE + " could not determine retrieve profile mod date from profile: " + profile.getName());
        }
	return null;
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


/*
    public static void copyInputStreamToFile(InputStream input, File file) {  
        try {
	    Path path = Paths.get(file.getAbsoultePath());
	    Files.copy(inputStream, outputPath, StandardCopyOption.REPLACE_EXISTING);
	    OutputStream output = new FileOutputStream(file) {
            input.transferTo(output);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
*/

	private static void copyInputStreamToFile(InputStream inputStream, File file)
            throws Exception {

	FileOutputStream outputStream = null;
        // append = false
        try {
	    outputStream = new FileOutputStream(file, false);
            int read;
            byte[] bytes = new byte[1024];
            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
	    outputStream.close();
        } catch (Exception e) {
	    e.printStackTrace();
	}

    }

	private static File createTempFile(String fileName) throws Exception {
            String dir = System.getProperty("java.io.tmpdir");
            return new File(dir + "/" + fileName);
        }

	private static void deleteTempFile(String fileName) throws Exception {
            String dir = System.getProperty("java.io.tmpdir");
            System.out.println("[info] Deleting cached profile: " + fileName);
            new File(dir + "/" + fileName).delete();
        }

}
