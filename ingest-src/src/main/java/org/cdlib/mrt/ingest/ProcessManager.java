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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.cdlib.mrt.zk.MerrittLocks;

import org.json.JSONObject;

/**
 * Basic manager for processing states 
 * 
 * @author mreyes
 */
public class ProcessManager {

	private static final String NAME = "ProcessManager";
	private static final String MESSAGE = NAME + ": ";
	private static final boolean DEBUG = true;
	private LoggerInf logger = null;
        private JSONObject storeConf = null;
        private JSONObject ingestConf = null;
        private JSONObject queueConf = null;
        private String queueConnectionString = null;
	private Integer defaultStorage = null;
	private URL ingestLink = null;
	private boolean debugDump = false;
        private static int sessionTimeout = 300000;  //5 minutes
	private Hashtable<Integer, URL> m_store = new Hashtable<Integer, URL>(20);
	private Hashtable<Integer, URL> m_access = new Hashtable<Integer, URL>(20);
	private ArrayList<String> m_admin = new ArrayList<String>(20);
	private String m_localID = null;
	private String m_emailContact = null;
	private String m_emailReplyTo = null;
	private String m_ezid = null;
	private String m_purl = null;          // persistent URL
	private String ingestFileS = null;     // prop "IngestService"
        private String sNumDownloadThreads = null; 
        private int numDownloadThreads = 4;    // default download thread pool
        private Long jvmStartTime = null;

	public String getIngestServiceProp() {
		return this.ingestFileS;
	}

	public JSONObject getIngestServiceConf() {
		return ingestConf;
	}

	public URL getIngestLink() {
		return ingestLink;
	}

	protected ProcessManager(LoggerInf logger, JSONObject storeConf, JSONObject ingestConf, JSONObject queueConf) throws TException {
		try {
			// when service is started
			if (jvmStartTime == null)
				jvmStartTime = new Long(DateUtil.getEpochUTCDate());
			this.logger = logger;
			this.storeConf = storeConf;
			this.ingestConf = ingestConf;
			this.queueConf = queueConf;
			init(storeConf, ingestConf, queueConf);
		} catch (TException tex) {
			throw tex;
		}
	}

	public static ProcessManager getProcessManager(LoggerInf logger, JSONObject storeConf, JSONObject ingestConf, JSONObject queueConf) throws TException {
		try {
			ProcessManager ingestManager = new ProcessManager(logger, storeConf, ingestConf, queueConf);
			return ingestManager;

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + "ProcessManager Exception:" + ex;
			logger.logError(msg, LoggerInf.LogLevel.SEVERE);
			logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Initialize the ProcessManager
	 * Using a set of Properties identify all storage references.
	 *
	 * @param config system properties used to resolve Storage references
	 * @throws TException process exceptions
	 */
	public void init(JSONObject storeConf, JSONObject ingestConf, JSONObject queueConf) throws TException {
		try {
                        if (storeConf == null) {
                                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Store Config properties not set");
                        }
                        if (ingestConf == null) {
                                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Ingest Config properties not set");
                        }
                        if (queueConf == null) {
                                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Queue Config properties not set");
                        }

			String key = null;
			String value = null;
			String matchStorage = "store.";
			String matchAccess = "access.";
			String matchLocalID = "localID";
                        String matchQueueService = "QueueService";
			String matchEmailContact = "mail-contact";
			String matchEmailReplyTo = "mail-replyto";
			String matchAdmin = "admin";
			String matchEZID = "ezid";
			String matchPURL = "purl";
			String matchNumDownloadThreads = "NumDownloadThreads";
			String defaultIDKey = "IDDefault";
			Integer id = null;

                        // QueueService - host1:2181,host2:2181
                        this.queueConnectionString = queueConf.getString(matchQueueService);

			// Iterate through store vars for multiple access/store
			Iterator<String> keys = storeConf.keys();
			while(keys.hasNext()) {
				key = keys.next();
				value = storeConf.getString(key);

				// store.1 .. store.n
				if (key.startsWith(matchStorage)) {
					String storageS = key.substring(matchStorage.length());
					id = Integer.parseInt(storageS);
					URL urlValue = null;

					try {
						urlValue = new URL(value);
					} catch (MalformedURLException muex) {
						throw new TException.INVALID_CONFIGURATION("store.n parameter is not a valid URL: " + value);
					}

					m_store.put(id, urlValue);
				}

				// Needed still?
				if (key.equals(defaultIDKey) && StringUtil.isNotEmpty(value)) {
					try {
						this.defaultStorage = Integer.parseInt(value);
					} catch (Exception ex) {
					}
				}

				// access.1 .. access.n
				if (key.startsWith(matchAccess)) {
					String accessS = key.substring(matchAccess.length());
					id = Integer.parseInt(accessS);
					URL urlValue = null;

					try {
						urlValue = new URL(value);
					} catch (MalformedURLException muex) {
						throw new TException.INVALID_CONFIGURATION("access.n parameter is not a valid URL: " + value);
					}

					m_access.put(id, urlValue);
				}

			}

			// localID
			m_localID = storeConf.getString(matchLocalID);

			// admin 
			value = ingestConf.getString(matchAdmin);
			for (String recipient : value.split(";")) {
				m_admin.add((String) recipient);
			}

			// email contact
			m_emailContact = ingestConf.getString(matchEmailContact);
                	System.out.println("[info] " + MESSAGE + "Contact email: " + m_emailContact);

			// email reply-to
			m_emailReplyTo = ingestConf.getString(matchEmailReplyTo);
                	System.out.println("[info] " + MESSAGE + "Repy To email: " + m_emailReplyTo);

			// ingestServicePath
			this.ingestFileS = ingestConf.getString("ingestServicePath");

			// ezid 
			m_ezid = ingestConf.getString(matchEZID);

			// Download thread pool size
        		try {
            		   sNumDownloadThreads = ingestConf.getString("NumDownloadThreads");
            		   if (StringUtil.isNotEmpty(sNumDownloadThreads)) {
                	      System.out.println("[info] " + MESSAGE + "Setting download thread pool size: " + sNumDownloadThreads);
                	      this.numDownloadThreads = new Integer(sNumDownloadThreads).intValue();
            		}
        		} catch (Exception e) {
            		   System.err.println("[warn] " + MESSAGE + "Could not set download thread pool size: " + sNumDownloadThreads + "  - using default: " + this.numDownloadThreads);
        		}

			// purl
			m_purl = ingestConf.getString(matchPURL);
			if (!m_purl.endsWith("/")) m_purl += "/";

		} catch (TException tex) {
			tex.printStackTrace();
			throw tex;
		} catch (Exception ex) {
			ex.printStackTrace();
			String msg = MESSAGE + " Exception:" + ex;
			logger.logError(msg, 3);
			logger.logError(StringUtil.stackTrace(ex), 0);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Get count of number of active storage instances
	 * 
	 * @return int number of active storage instances
	 */
	public int getStorageCount() {
		return m_store.size();
	}

	/**
	 * Get the default storageID for defining what the current Storage service
	 * references
	 * 
	 * @return Integer default storageID
	 */
	public Integer getDefaultStorageID() {
		return this.defaultStorage;
	}

	public IngestServiceState getServiceState() throws TException {
		try {
			IngestServiceState ingestState = new IngestServiceState();
			URL storageInstance = null;
			for (Integer storageID : m_store.keySet()) {
				storageInstance = (URL) m_store.get(storageID);
				ingestState.addStorageInstance(storageInstance);
			}
			setIngestStateProperties(ingestState);
			return ingestState;

		} catch (TException te) {
			throw te;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);

		}
	}

	public Properties getProps(IngestRequest ingestRequest, String fileS) throws TException {
		try {
			File file = new File(ingestRequest.getQueuePath().getParentFile().getParentFile().getParentFile(), fileS);
			if (!file.exists()) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "file not found: " + file.getAbsolutePath());
			}

			return PropertiesUtil.loadFileProperties(file);
		} catch (TException te) {
			throw te;
		}
	}


	public JobState submit(IngestRequest ingestRequest, String state) throws Exception {
		ProfileState profileState = null;
		JobState jobState = null;
		try {
			// add ingest queue path to request
			// ingestRequest.setIngestQueuePath(ingestConf.getString("ingestQueuePath"));
	                try {
                	    ingestRequest.setIngestQueuePath(ingestConf.getString("ingestQueuePath"));
            	        } catch (org.json.JSONException je) {
                	    if (DEBUG) System.out.println("[debug] " + MESSAGE + "ingestQueuePath not set, no EFS shared disk defined.");
                	    ingestRequest.setIngestQueuePath(null);
            	        }

			// add service state properties to ingest request
			ingestRequest.setServiceState(getServiceState());

			// add download thread pool size
			ingestRequest.setNumDownloadThreads(numDownloadThreads);

			// assign preliminary job info
			jobState = ingestRequest.getJob();
			jobState.setSubmissionDate(new DateState(DateUtil.getCurrentDate()));

			if (ingestRequest.getRetainTargetURL()) {
				System.out.println("[info] Retain Target URL set: " + ingestRequest.getRetainTargetURL());
				jobState.setRetainTargetURL(ingestRequest.getRetainTargetURL());
			}

			if (ingestRequest.getUpdateFlag()) {
				System.out.println("[info] Update flag set: " + ingestRequest.getUpdateFlag());
				jobState.setUpdateFlag(ingestRequest.getUpdateFlag());
			}

			// assign profile
			profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
					ingestRequest.getQueuePath().getParentFile().getParentFile().getParent() + "/profiles"); // three
																												// levels
																												// down
																												// from
																												// home

			String profileStorageURL = profileState.getTargetStorage().getStorageLink().toString();
			// valid profile storage URL?
			Iterator iterator = m_store.keySet().iterator();
			boolean match = false;
			Integer intKey = null;
			while (iterator.hasNext()) {
				intKey = (Integer) iterator.next();
				String storeURL = m_store.get(intKey).toString();
				if (storeURL.equals(profileStorageURL)) {
					match = true;
					break;
				}
			}

			if (match) {
				// assign access URL
				if (m_access.get(intKey) != null) {
					System.out.println("Mapping store node to access node: " + m_access.get(intKey));
					profileState.setAccessURL(m_access.get(intKey));
				} else {
					System.err.println("No access node associated with  storage node: " + profileStorageURL);
				}
			} else {
				String msg = MESSAGE + "Exception: Profile storage node is not supported: " + profileStorageURL;
				// throw new TException.INVALID_CONFIGURATION(msg);
				System.err.println("[warn]" + msg);
			}

			if (m_localID != null) {
				profileState.setLocalIDURL(new URL(m_localID));
				System.out.println("Setting local ID URL: " + m_localID);
			}
			if (m_admin != null)
				profileState.setAdmin(m_admin);
			if (m_emailContact != null)
				profileState.setEmailContact(m_emailContact);
			if (m_emailReplyTo != null)
				profileState.setEmailReplyTo(m_emailReplyTo);
			if (m_ezid != null)
				profileState.setMisc(m_ezid);
			if (m_purl != null)
				profileState.setPURL(m_purl);

			if (profileState.getAccessURL() != null) {
				jobState.setTargetStorage(
						new StoreNode(profileState.getAccessURL(), profileState.getTargetStorage().getNodeID()));
			} else {
				jobState.setTargetStorage(profileState.getTargetStorage());
			}

			if (DEBUG)
				System.out.println("[debug] " + profileState.dump("profileState"));
			jobState.setObjectProfile(profileState);

			// link for ingest to expose manifest data
			ingestRequest.setLink(this.getServiceState().getAccessServiceURL().toString());

			// if we error during any one handler, skip remaining and update object
			boolean isError = false;
			boolean reQueue = false;
			BatchState batchState = new BatchState();
			HandlerResult handlerResult = null;

			// call appropriate handlers
			SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap()); // thread-safe
			if (state.matches("Estimate")) sortedMap = profileState.getEstimateHandlers();
			if (state.matches("Provision")) sortedMap = profileState.getProvisionHandlers();
			if (state.matches("Download")) sortedMap = profileState.getDownloadHandlers();
			if (state.matches("Process")) sortedMap = profileState.getProcessHandlers();
			if (state.matches("Record")) sortedMap = profileState.getRecordHandlers();
			if (state.matches("Notify")) sortedMap = profileState.getNotifyHandlers();
			for (Object key : sortedMap.keySet()) {
				String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
				Handler handler = (Handler) createObject(handlerS);
				
				if (handler == null) {
					throw new TException.INVALID_CONFIGURATION("[error] Could not find handler: " + handlerS);
				}
				StateInf stateClass = jobState;
				if (isError && (handler.getClass() != org.cdlib.mrt.ingest.handlers.notify.HandlerNotification.class) 
					    && (handler.getClass() != org.cdlib.mrt.ingest.handlers.notify.HandlerCallback.class)) {
					System.out.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
					continue;
				}
				if (handler.getClass() == org.cdlib.mrt.ingest.handlers.notify.HandlerNotification.class) {
					if (!isError) {
						jobState.setObjectState(jobState.grabTargetStorage().getStorageLink().toString() + "/state/"
								+ jobState.grabTargetStorage().getNodeID() + "/"
								+ URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"));
						batchState.setBatchStatus(BatchStatusEnum.COMPLETED);
						batchState.setCompletionDate(new DateState(DateUtil.getCurrentDate()));
					} else {
						jobState.setJobStatus(JobStatusEnum.FAILED);
						batchState.setBatchStatus(BatchStatusEnum.FAILED);
					}
					try {
						batchState = updateBatch(batchState, ingestRequest, jobState);
					} catch (Exception e) {
						System.out.println("Failed to update batch.  Assume this to be a requeued object and skipping Notification");
						reQueue = true;
					}
					if ( ! reQueue ) {
						stateClass = batchState;

						// update persistent URL if necessary
						jobState.setPersistentURL(profileState.getPURL() + jobState.getPrimaryID());
					} else {
						continue;
					}
				}

				if (handler.getClass() == org.cdlib.mrt.ingest.handlers.notify.HandlerCallback.class) {
					if (isError) {
						jobState.setJobStatus(JobStatusEnum.FAILED);
						// No batch for requeud jobs
						if (! reQueue) batchState.setBatchStatus(BatchStatusEnum.FAILED);
						stateClass = jobState;
					} else {
						// Not needed.  If successful, we do not populate message
                                               	// jobState.setJobStatusMessage(handlerResult.getDescription());
					}
				}

				if (handler.getClass() == org.cdlib.mrt.ingest.handlers.process.HandlerTransfer.class) {
					jobState.setObjectState(jobState.grabTargetStorage().getStorageLink().toString() + "/state/"
						+ jobState.grabTargetStorage().getNodeID() + "/"
						+ URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"));

					System.out.println("[info]" + MESSAGE + "Setting lock path prior to Transfer: " + ingestConf.getString("ingestLock"));
					jobState.setMisc(queueConf.getString("QueueService"));
					jobState.setExtra(ingestConf.getString("ingestLock"));
				}

				try {
					// Do some work
					handlerResult = handler.handle(profileState, ingestRequest, stateClass);

				} catch (Exception e) {
					e.printStackTrace();
					handlerResult.setSuccess(false);
				}

				// Abort if failure
				if (DEBUG) System.out.println("[debug] " + handler.getName() + ": " + handlerResult.getDescription());
				if (handlerResult.getSuccess()) {
					if (DEBUG) System.out.println("[debug] " + handler.getName() + " Success: " + handlerResult.getSuccess());
					//if (!isError) {
					jobState.setJobStatus(JobStatusEnum.COMPLETED);
					jobState.setJobStatusMessage(handlerResult.getDescription());
					//}
				} else {
					if (DEBUG) System.out.println("[debug] " + handler.getName() + " Failure: " + handlerResult.getSuccess());
					// do not abort, but skip all further processing and note exception
					jobState.setJobStatus(JobStatusEnum.FAILED);
					jobState.setJobStatusMessage(handlerResult.getDescription());
					isError = true;
				}
			} // end for

			if (jobState.getPrimaryID() != null) {
				jobState.setObjectState(jobState.grabTargetStorage().getStorageLink().toString() + "/state/"
						+ jobState.grabTargetStorage().getNodeID() + "/"
						+ URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"));
			}

			return jobState;

		} catch (TException me) {
			throw me;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			profileState = null;
		}
	}

	private boolean override(IngestRequest ingestRequest) throws Exception {
		try {
			if (new File(ingestRequest.getQueuePath().getParentFile(), "POST_COMPLETE").exists()) {
				System.out.println("[INFO] ProcessManager: POST_COMPLETE detected: "
						+ ingestRequest.getQueuePath().getParentFile());
				return true;
			}
		} catch (Exception e) {
		}
		return false;
	}

	protected void setIngestStateProperties(IngestServiceState ingestState) throws TException {
		String SERVICENAME = "name";
		String SERVICEID = "identifier";
		String TARGETID = "target";
		String SERVICEDESCRIPTION = "description";
		String SERVICESCHEME = "service-scheme";
		String NODESCHEME = "node-scheme";
		String ACCESSURI = "access-uri";
		String SUPPORTURI = "support-uri";
		String MAILHOST = "mail-host";

           ZooKeeper zooKeeper = null;
           try {
                zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());

		String serviceNameS = ingestConf.getString(SERVICENAME);
		if (serviceNameS != null) {
			ingestState.setServiceName(serviceNameS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICENAME + " parameter is not available");
		}

		String serviceIDS = ingestConf.getString(SERVICEID);
		if (serviceIDS != null) {
			ingestState.setServiceID(serviceIDS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICEID + " parameter is not available");
		}

		String targetIDS = ingestConf.getString(TARGETID);
		if (targetIDS == null) {
			targetIDS = "http://merritt.cdlib.org"; // default
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + TARGETID + " parameter is not available");
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + TARGETID + " using default value: " + targetIDS);
		}
		ingestState.setTargetID(targetIDS);

		String serviceScehmeS = ingestConf.getString(SERVICESCHEME);
		if (serviceScehmeS != null) {
			ingestState.setServiceVersion(serviceScehmeS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICESCHEME + " parameter is not available");
		}

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

		String supportServiceUrlS = ingestConf.getString(SUPPORTURI);
		if (supportServiceUrlS != null) {
			try {
				ingestState.setSupportServiceURL(new URL(supportServiceUrlS));
			} catch (MalformedURLException muex) {
				throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + "Support-uri parameter is not a valid URL");
			}
		} else {
			throw new TException.INVALID_CONFIGURATION(
					"[error] " + MESSAGE + SUPPORTURI + " parameter is not available");
		}
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
                // Submission state
                if (MerrittLocks.checkLockIngestQueue(zooKeeper)) {
                   onHold = "frozen";
                } else {
                   onHold = "thawed";
                }
                ingestState.setSubmissionState(onHold);

                // Collection submission state
                // String heldCollections = MerrittLocks.getHeldCollections(zooKeeper);
                // ingestState.setCollectionSubmissionState(heldCollections);

                // service start time
            	ingestState.setServiceStartTime(new DateState(jvmStartTime.longValue()));

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

	protected synchronized BatchState updateBatch(BatchState sourceBatchState, IngestRequest ingestRequest,
			JobState jobState) throws Exception {
		BatchState batchState = null;
		try {
			if (jobState.grabBatchID().getValue().equals(ProfileUtil.DEFAULT_BATCH_ID)) {
				// not a batch
				batchState = new BatchState(jobState.grabBatchID());
				// batchState.setBatchID(jobState.grabBatchID());
				batchState.addJob(jobState.getJobID().getValue(), jobState);
			} else {
				// update batch object on disk (must be synchronous)
				batchState = new BatchState(jobState.grabBatchID());

				try {
				   //batchState = BatchState.getBatchState(jobState.grabBatchID().getValue());
				   batchState.setBatchID(jobState.grabBatchID());
				} catch (Exception eee) {
				   // Recover from restart
				   System.out.println("[info]" + MESSAGE + "Batch not defined. Read from serialized object on disk: " + jobState.getJobID());
				   batchState = ProfileUtil.readFrom(batchState, ingestRequest.getQueuePath().getParentFile());
				   batchState.setBatchID(jobState.grabBatchID());
				}

				// remove old job and replace w/ new
				Map<String, JobState> jobStates = (HashMap<String, JobState>) batchState.getJobStates();
				JobState jobStateTemp = (JobState) jobStates.get(jobState.getJobID().getValue());
				System.out.println("[info]" + MESSAGE + "updating job: " + jobState.getJobID());
				batchState.removeJob(jobState.getJobID().getValue());
				batchState.addJob(jobState.getJobID().getValue(), jobState);
				if (batchState.getBatchStatus() == BatchStatusEnum.FAILED
						&& sourceBatchState.getBatchStatus() == BatchStatusEnum.COMPLETED) {
					// do not overwrite
				} else {
					batchState.setBatchStatus(sourceBatchState.getBatchStatus());
				}
				batchState.setCompletionDate(sourceBatchState.getCompletionDate());
				batchState.setBatchLabel(sourceBatchState.getBatchLabel());

				// Does not scale!!
				// ProfileUtil.writeTo(batchState,
				// ingestRequest.getQueuePath().getParentFile());
				//BatchState.putBatchState(jobState.grabBatchID().getValue(), batchState);
			}

			return batchState;
		} catch (Exception e) {
			System.out.println("-> Error updating batch: " + jobState.getJobID().getValue());
			e.printStackTrace(System.err);
			throw new Exception(e.getMessage());
		}
	}

	public IdentifierState requestIdentifier(IngestRequest ingestRequest) throws TException {
		ProfileState profileState = null;
		JobState jobState = null;

		try {

			profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
					ingestRequest.getQueuePath().getParentFile().getParentFile().getParent() + "/profiles"); // three
																												// levels
																												// down
																												// from
																												// home
			if (m_ezid != null)
				profileState.setMisc(m_ezid);
			if (m_purl != null)
				profileState.setPURL(m_purl);

			jobState = ingestRequest.getJob();

			String id = MintUtil.processObjectID(profileState, jobState, ingestRequest, true);

			IdentifierState identifierState = new IdentifierState(id);
			return identifierState;

		} catch (TException te) {
			throw te;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	protected static Object createObject(String className) {
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

        public static class Ignorer implements Watcher {
                public void process(WatchedEvent event) {
                }
        }
}
