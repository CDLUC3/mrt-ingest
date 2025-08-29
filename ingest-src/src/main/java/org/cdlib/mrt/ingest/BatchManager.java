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
package org.cdlib.mrt.ingest;

import java.lang.Thread;
import java.util.Properties;
import java.util.TreeMap;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.lang.ArrayIndexOutOfBoundsException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.NoSuchElementException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.ZookeeperUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.ZooCodeUtil;
import org.cdlib.mrt.zk.MerrittLocks;

import org.json.JSONObject;
import org.json.JSONException;

/**
 * Basic manager for Queuing Service
 * 
 * @author mreyes
 */
public class BatchManager {

	private static final String NAME = "BatchManager";
	private static final String MESSAGE = NAME + ": ";
	private static final boolean DEBUG = true;
	private LoggerInf logger = null;
	private JSONObject queueConf = null;
	private JSONObject ingestConf = null;
	private String queueConnectionString = null;
	private String ingestQNames = null;
	private String ingestLName = null;
	private String accessSmallNode = "/accessSmall.1"; // hard-coded.  Keep in synv with access code
	private String accessLargeNode = "/accessLarge.1"; // hard-coded.  Keep in synv with access code
	private ArrayList<String> m_admin = new ArrayList<String>(20);
        private String emailContact = null;
        private String emailReplyTo = null;
        private String profileNode = null;
        private String profilePath = null;
        private String s3endpoint = null;
        private String s3accesskey = null;
        private String s3secretkey = null;

	private boolean debugDump = false;
	private String ingestFileS = null; // prop "IngestService"

	public JSONObject getQueueServiceConf() {
		return queueConf;
	}

	protected BatchManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			this.logger = logger;
			this.queueConf = queueConf;
			this.ingestConf = ingestConf;
			init(queueConf, ingestConf);
		} catch (TException tex) {
			throw tex;
		}
	}

	public static BatchManager getBatchManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			BatchManager batchManager = new BatchManager(logger, queueConf, ingestConf);
			return batchManager;

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + "batchManager Exception:" + ex;
			logger.logError(msg, LoggerInf.LogLevel.SEVERE);
			logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Initialize the BatchManager
	 * Using a set of Properties identify all storage references.
	 *
	 * @param configs system properties used to resolve Storage references
	 * @throws TException process exceptions
	 */
	public void init(JSONObject queueConf, JSONObject ingestConf) throws TException {
		ZooKeeper zooKeeper = null;
		try {
			if (queueConf == null) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Queue Config properties not set");
			}
			if (ingestConf == null) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Ingest Config properties not set");
			}

			String key = null;
			String value = null;
			String matchIngest = "ingestServicePath";
			String matchQueueService = "QueueService";
			String matchIngestLName = "ingestLock";
			String matchAdmin = "admin";
                        String matchEmailContact = "mail-contact";
                        String matchEmailReplyTo = "mail-replyto";
                        String matchProfileNode = "s3config_bucket";
                        String matchProfilePath = "s3config_prefix";
                        String matchS3endpoint = "s3endpoint";
                        String matchS3accesskey = "s3accesskey";
                        String matchS3secretkey = "s3secretkey";

			String defaultIDKey = "IDDefault";
			Integer storageID = null;

			this.ingestFileS = ingestConf.getString(matchIngest);

			// QueueService - host1:2181,host2:2181
			this.queueConnectionString = queueConf.getString(matchQueueService);
			// All Ingest Lock Names - "zkLock,..."
			this.ingestLName = ingestConf.getString(matchIngestLName);

			// email list
			value = ingestConf.getString(matchAdmin);
			for (String recipient : value.split(";")) {
				m_admin.add((String) recipient);
			}
			// email contact
			emailContact = ingestConf.getString(matchEmailContact);
                        System.out.println("[info] " + MESSAGE + "Contact email: " + emailContact);

                        // email reply-to
                        emailReplyTo = ingestConf.getString(matchEmailReplyTo);
                        System.out.println("[info] " + MESSAGE + "Repy To email: " + emailReplyTo);

                        // Profile Node
                        profileNode = ingestConf.getString(matchProfileNode);
                        System.out.println("[info] " + MESSAGE + "Profile Node: " + profileNode);

                        // Profile Path
                        profilePath = ingestConf.getString(matchProfilePath);
                        System.out.println("[info] " + MESSAGE + "Profile Path: " + profilePath);


                        // Profile Endpoint
                        s3endpoint = ingestConf.getString(matchS3endpoint);
                        System.out.println("[info] " + MESSAGE + "S3 Endpoint: " + s3endpoint);

                        // Profile Access Key
                        s3accesskey = ingestConf.getString(matchS3accesskey);
                        System.out.println("[info] " + MESSAGE + "S3 Access Key: " + s3accesskey);

                        // Profile Secret Key
                        s3secretkey = ingestConf.getString(matchS3secretkey);
                        System.out.println("[info] " + MESSAGE + "S3 Secret Key: " + s3secretkey);

            	        if (! ZookeeperUtil.validateZK(zooKeeper)) {
               	            try {
                   	        // Refresh ZK connection
                   	        zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               	            } catch  (Exception e ) {
                 	        e.printStackTrace(System.err);
               	            }
            	        }

                        System.out.println("[info] " + MESSAGE + "Initializing Zookeeper Locks");
			MerrittLocks.initLocks(zooKeeper);

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + " Exception:" + ex;
			logger.logError(msg, 3);
			logger.logError(StringUtil.stackTrace(ex), 0);
			throw new TException.GENERAL_EXCEPTION(msg);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }

	}

	public IngestServiceState getServiceState() throws TException {
		try {
			IngestServiceState ingestState = new IngestServiceState();
			URL storageInstance = null;
			ingestState.addQueueInstance(queueConnectionString);

			setIngestStateProperties(ingestState);
			return ingestState;

		} catch (TException me) {
			throw me;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public BatchState submit(IngestRequest ingestRequest, String state) throws Exception {
		ProfileState profileState = null;
		try {

			// add service state properties to ingest request
			ingestRequest.setServiceState(getServiceState());

			// assign preliminary batch info
			BatchState batchState = new BatchState();
			batchState.clear();
			batchState.setTargetQueue(queueConnectionString);
			batchState.setUserAgent(ingestRequest.getJob().grabUserAgent());
			batchState.setSubmissionDate(new DateState(DateUtil.getCurrentDate()));
			batchState.setBatchStatus(BatchStatusEnum.QUEUED);
			batchState.setBatchID(ingestRequest.getJob().grabBatchID());
			batchState.setUpdateFlag(ingestRequest.getUpdateFlag());

			// Remove cached profile after final processing
			boolean deleteProfile = false;
			if (state.equals("Report")) deleteProfile = true;

			// assign profile (S3)
                        String batchDir = ingestFileS + "/queue/" + batchState.getBatchID().getValue();

                        profileState = ProfileUtil.getProfile(ingestRequest.getProfile(), batchDir, 
                                s3endpoint, s3accesskey, s3secretkey, profileNode, profilePath, deleteProfile);

			if (m_admin != null)
				profileState.setAdmin(m_admin);
			if (emailContact != null)
				profileState.setEmailContact(emailContact);
                        if (emailReplyTo != null)
                                profileState.setEmailReplyTo(emailReplyTo);

			if (DEBUG)
				System.out.println("[debug] " + profileState.dump("profileState"));

			batchState.setBatchProfile(profileState);

			// link for ingest to expose manifest data
			ingestRequest.setLink(this.getServiceState().getAccessServiceURL().toString());

			// The work of posting is done asynchronously to prevent UI timeout
			Post post = new Post(batchState, ingestRequest, profileState, state);
			Thread postThread = new Thread(post);
 			postThread.start();
                        // Async for initial posting of data only
                        //if ( ! state.matches("Process")) {
                                try {
                                        postThread.join();
                                        if (DEBUG) System.out.println(NAME + "[debug] Synchronous mode processing: " + state);
                                } catch (InterruptedException ignore) {
                                }
                        //}

			return batchState;

		} catch (TException me) {
			throw me;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			if (profileState != null)
				profileState.getQueueHandlers().clear();
		}
	}

	protected void setIngestStateProperties(IngestServiceState ingestState) throws TException {
           ZooKeeper zooKeeper = null;
	   try {
		String SERVICENAME = "name";
		String SERVICEID = "identifier";
		String SERVICEDESCRIPTION = "description";
		String SERVICESCHEME = "service-scheme";
		String SERVICECUST = "customer-support";
		String NODESCHEME = "node-scheme";
		String ACCESSURI = "access-uri";
		String SUPPORTURI = "support-uri";
		String MAILHOST = "mail-host";

		boolean unitTest = false;
                try {
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
		} catch (Exception e) { 
		   // Unit test catch 
		   unitTest = true;
                }

		// name
		String serviceNameS = ingestConf.getString(SERVICENAME);
		if (serviceNameS != null) {
			ingestState.setServiceName(serviceNameS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICENAME + " parameter is not available");
		}

		// identifier
		String serviceIDS = ingestConf.getString(SERVICEID); 
		if (serviceIDS != null) {
			ingestState.setServiceID(SERVICEID);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICEID + " parameter is not available");
		}

		// service-scheme
		String serviceScehmeS = ingestConf.getString(SERVICESCHEME);
		if (serviceScehmeS != null) {
			ingestState.setServiceVersion(serviceScehmeS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICESCHEME + " parameter is not available");
		}

		// access-uri
		String accessServiceUrlS = ingestConf.getString(ACCESSURI);
		if (accessServiceUrlS != null) {
			try {
				ingestState.setAccessServiceURL(new URL(accessServiceUrlS));
			} catch (MalformedURLException muex) {
				throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + ACCESSURI + " parameter is not a valid URL");
			}
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + ACCESSURI + " parameter is not available");
		}

		// support-uri
		String supportServiceUrlS = ingestConf.getString(SUPPORTURI);
		if (supportServiceUrlS != null) {
			try {
				ingestState.setSupportServiceURL(new URL(supportServiceUrlS));
			} catch (MalformedURLException muex) {
				throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + "Support-uri parameter is not a valid URL");
			}
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + " parameter is not available");
		}

		// mail-host
		String mailHost = ingestConf.getString(MAILHOST);
		if (mailHost == null) {
			mailHost = "localhost"; // default
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + MAILHOST + " parameter is not available");
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + MAILHOST + " using default value: " + mailHost);
		}
		ingestState.setMailHost(mailHost);

                String onHold = null;
                // submission state
                if (! unitTest) {
                   if (MerrittLocks.checkLockIngestQueue(zooKeeper)) {
                      onHold = "frozen";
                   } else {
                      onHold = "thawed";
                   }
		} else {
		   // unit test
                   onHold = "thawed";
		}
                ingestState.setSubmissionState(onHold);

                // Collection submission state
                // String heldCollections = MerrittLocks.getHeldCollections(zooKeeper);
                // ingestState.setCollectionSubmissionState(heldCollections);
            } catch (TException me) {
                    throw me;

            } catch (Exception ex) {
                    System.out.println(StringUtil.stackTrace(ex));
                    logger.logError(MESSAGE + "Exception:" + ex, 0);
                    throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            } finally {
		try {
		   zooKeeper.close();
		} catch(Exception ze) {}
	    }
	}

	static Object createObject(String className) {
		Object object = null;
		try {
			Class classDefinition = Class.forName(className);
			object = classDefinition.newInstance();
		} catch (InstantiationException e) {
			System.out.println(e);
		} catch (IllegalAccessException e) {
			System.out.println(e);
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		}
		return object;
	}

	public String capFirst(String str) {
		return str.substring(0, 1).toUpperCase() + str.substring(1);
	}

	public static class Ignorer implements Watcher {
		public void process(WatchedEvent event) {
		}
	}

// Need to post (post to queueing service asyncrhonously due to UI timeout)
	class Post implements Runnable {

		private static final String NAME = "PostBatch";
		private static final String MESSAGE = NAME + ": ";
		private BatchState batchState = null;
		private IngestRequest ingestRequest = null;
		private ProfileState profileState = null;
		//private TreeMap<Integer, HandlerState> batchHandlers = null;
		private String state = null;
		private boolean isError = false;

		// Constructor
		public Post(BatchState batchState, IngestRequest ingestRequest, ProfileState profileState, String state) {
			try {
				this.batchState = batchState;
				this.ingestRequest = ingestRequest;
				this.profileState = profileState;
				this.state = state;
				//this.batchHandlers = new TreeMap<Integer, HandlerState>(profileState.getBatchHandlers());
				//this.batchHandlers = new TreeMap<Integer, HandlerState>();
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}

		public void run() {
			try {
				// call appropriate handlers
				SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap()); // thread-safe
	                        if (state.matches("Process")) sortedMap = profileState.getBatchProcessHandlers();
                        	if (state.matches("Report")) sortedMap = profileState.getBatchReportHandlers();

				//sortedMap = batchHandlers;
				for (Object key : sortedMap.keySet()) {
					String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
					Handler handler = (Handler) createObject(handlerS);
					if (handler == null) {
						throw new TException.INVALID_CONFIGURATION("[error] Could not find queue handler: " + handlerS);
					}
					StateInf stateClass = batchState;
/*
					if (isError && (!handler.getName().equals("HandlerNotification"))) {
						System.out
								.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
						continue;
					}

*/
					HandlerResult handlerResult = null;
					try {
						handlerResult = handler.handle(profileState, ingestRequest, stateClass);
					} catch (Exception e) {
						e.printStackTrace();
						handlerResult.setSuccess(false);
					}

					// Abort if failure
					if (DEBUG)
						System.out.println("[debug] " + handler.getName() + ": " + handlerResult.getDescription());
					if (handlerResult.getSuccess()) {
						batchState.setBatchStatus(BatchStatusEnum.QUEUED);
					} else {
						batchState.setBatchStatus(BatchStatusEnum.FAILED);
						batchState.setBatchStatusMessage(handlerResult.getDescription());
						isError = true;
					}
				}

				// ready for consumer to start processing
				//BatchState.putBatchReadiness(batchState.getBatchID().getValue(), 1);
				System.out.println(MESSAGE + "Completion of posting data to queue: " + batchState.getBatchID().getValue() + " Status: " + batchState.getBatchStatus().toString());

			} catch (Exception e) {
				System.out.println(MESSAGE + "Exception detected while posting data to queue.");
				e.printStackTrace(System.err);
			} finally {
				//batchHandlers = null;
			}
		}

	}

}
