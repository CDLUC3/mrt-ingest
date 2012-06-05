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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.IdentifierState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.JobStateInf;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.MintUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * Basic manager for Ingest Service
 * @author mreyes
 */
public class IngestManager
{

    private static final String NAME = "IngestManager";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private Properties ingestProperties = null;
    private Integer defaultStorage = null;
    private URL ingestLink = null;
    private boolean debugDump = false;
    private Hashtable<Integer, URL> m_store = new Hashtable<Integer, URL>(20);
    private Hashtable<Integer, URL> m_access = new Hashtable<Integer, URL>(20);
    private ArrayList<String> m_admin = new ArrayList<String>(20);
    private String m_ezid = null;
    private String ingestFileS = null;	// prop "IngestService"

    public String getIngestServiceProp() {
        return ingestFileS;
    }
    
    public Properties getIngestServiceProps() {
        return ingestProperties;
    }
    
    public URL getIngestLink() {
        return ingestLink;
    }
    
    protected IngestManager(LoggerInf logger, Properties conf)
        throws TException
    {
	try {
            this.logger = logger;
            this.conf = conf;
            init(conf);
	} catch (TException tex) {
	    throw tex;
	}
    }

    public static IngestManager getIngestManager(LoggerInf logger, Properties conf)
        throws TException
    {
        try {
            IngestManager ingestManager = new IngestManager(logger, conf);
            return ingestManager;

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + "IngestManager Exception:" + ex;
            logger.logError(msg, LoggerInf.LogLevel.SEVERE);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex),
                    LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }

    /**
     * <pre>
     * Initialize the IngestManager
     * Using a set of Properties identify all storage references.
     *
*!!!! -------------- May defer to use store node/node ID housed in Profile definitions ------ !!!!
     * Properties:
     * "Storage.nnn=value" identifies a storage reference. The nnn is the numeric serviceID.
     * The value is either a file name (local) or is a URL (remote)
     *
     * "IDDefault=" is the default serviceID used for accessing a storage service.
     * ingest -> ingest-info.txt - primarily the Access-uri is used in the  getVersion link manifest
     *
     * </pre>
     * @param prop system properties used to resolve Storage references
     * @throws TException process exceptions
     */
    public void init(Properties prop)
        throws TException
    {
        try {
            if (prop == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Exception MFrame properties not set");
            }

            String key = null;
            String value = null;
            String matchStorage = "store.";
            String matchAccess = "access.";
            String matchIngest = "ingestServicePath";
            String matchAdmin = "admin";
            String matchEZID = "ezid";
            String defaultIDKey = "IDDefault";
	    Integer id = null;

            ingestFileS = prop.getProperty(matchIngest);
            if (ingestFileS != null) {
		// load ingest-info.txt
                File ingestInfoTxt = new File(ingestFileS, "ingest-info.txt");
                if (!ingestInfoTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "ingest-info.txt file not found: " + ingestInfoTxt.getAbsolutePath());
                }
                ingestProperties = PropertiesUtil.loadFileProperties(ingestInfoTxt);

		// add stores.txt
                File storesTxt = new File(ingestFileS, "stores.txt");
                if (!storesTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "stores.txt file not found: " + storesTxt.getAbsolutePath());
                }
                Properties storesProperties = PropertiesUtil.loadFileProperties(storesTxt);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		storesProperties.store(out, null);
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		ingestProperties.load(in);	// append
	    } else {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "IngestService: ingest service path definition not found: " + matchIngest);
	    }

            Enumeration e = ingestProperties.propertyNames();
            while( e.hasMoreElements() ) {
                key = (String) e.nextElement();
                value = ingestProperties.getProperty(key);

		// store.1 .. store.n
                if (key.startsWith(matchStorage)) {
                    String storageS = key.substring(matchStorage.length());
                    id = Integer.parseInt(storageS);
                    URL urlValue = null;

                    try {
                        urlValue = new URL(value);
        	    } catch (MalformedURLException muex) {
	    		throw new TException.INVALID_CONFIGURATION("store.n parameter (stores.txt) is not a valid URL: " + value);
                    }

		    m_store.put(id, urlValue);
                }
                if (key.equals(defaultIDKey) && StringUtil.isNotEmpty(value)) {
                    try {
                        this.defaultStorage = Integer.parseInt(value);
                    } catch (Exception ex) {}
                }
		// access.1 .. access.n
                if (key.startsWith(matchAccess)) {
                    String accessS = key.substring(matchAccess.length());
                    id = Integer.parseInt(accessS);
                    URL urlValue = null;

                    try {
                        urlValue = new URL(value);
        	    } catch (MalformedURLException muex) {
	    		throw new TException.INVALID_CONFIGURATION("access.n parameter (stores.txt) is not a valid URL: " + value);
                    }

		    m_access.put(id, urlValue);
                }

		// admin notification
                if (key.startsWith(matchAdmin)) {
		    for (String recipient : value.split(";")) {
                        m_admin.add((String) recipient);
		    }
                }

		// ezid auth
                if (key.startsWith(matchEZID)) {
		    m_ezid = value;
                }
            }

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + " Exception:" + ex;
            logger.logError(msg, 3);
            logger.logError(StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(msg);
        }
    }

    /**
     * Get count of number of active storage instances
     * @return int number of active storage instances
     */
    public int getStorageCount()
    {
        return m_store.size();
    }

    /**
     * Get the default storageID for defining what the current Storage service
     * references
     * @return Integer default storageID
     */
    public Integer getDefaultStorageID()
    {
        return this.defaultStorage;
    }

    public IngestServiceState getServiceState()
        throws TException
    {
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
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);

        }
    }


    public Properties getProps(IngestRequest ingestRequest, String fileS)
        throws TException
    {
        try {
            File file = new File(ingestRequest.getQueuePath().getParentFile().getParentFile().getParentFile(), fileS);
            if (! file.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM( MESSAGE + "file not found: " + file.getAbsolutePath());
            }

            return PropertiesUtil.loadFileProperties(file);
	} catch (TException te) {
            throw te;
	}
    }


    public JobsState getStatus(String type)
        throws TException
    {
        try {
	    BatchState batchState = null;
	    JobsState jobsState = new JobsState();
	    jobsState.setIngestServer(this. getServiceState().getAccessServiceURL().toString());
	    File queueDir = new File(ingestFileS, "queue");
	    
	    File[] files = queueDir.listFiles();
	    Arrays.sort(files, new Comparator<File>() {
		 public int compare(File f1, File f2) {
	            // newest first
                    return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
		 }
    	    });

/*
            for (File queueFile : files) {
		try {
		    batchState = ProfileUtil.readFrom(batchState, queueFile);
		} catch (Exception e) {
		   continue;
		}
		if (batchState.getBatchStatus().getValue().equalsIgnoreCase(type)) {
		    for (JobState jobState : batchState.getJobStates()) {
		        if (jobState.getJobStatus().getValue().equalsIgnoreCase(type)) {
		            jobsState.addJob(jobState);
		            // System.out.println(jobState.dump("----"));
			}
		    }
		}
            }
*/

	    return jobsState;

        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);

        }
    }

    public BatchState updateStatus(String batchID, String jobID, String status)
        throws TException
    {
        try {
	    File queueDir = new File(ingestFileS, "queue");
	    File file = new File (queueDir, batchID);
	    if (! file.exists()) {
		System.out.println("[warn]" + MESSAGE + "Batch does not exist, can not update status: " + file.getAbsolutePath());
                throw new TException.GENERAL_EXCEPTION(MESSAGE + "Batch does not exist, can not update status: " + file.getAbsolutePath());
	    } 
            BatchState batchState = BatchState.getBatchState(batchID);

            Map<String, JobState> jobStates = (HashMap) batchState.getJobStates();
            JobState jobStateTemp = (JobState) jobStates.get(jobID);
            System.out.println("[info]" + MESSAGE + "updating job: " + jobStateTemp.getJobID());

	    // update status
	    jobStateTemp.setJobStatus(JobStatusEnum.RESOLVED);	// only option for now

            BatchState.removeBatchState(batchID);
            BatchState.putBatchState(batchID, batchState);

            return batchState;

        } catch (TException te) {
            throw te;
        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);
        }
    }

    public JobState submit(IngestRequest ingestRequest)
        throws Exception
    {
	ProfileState profileState = null;
	JobState jobState = null;
        try {
	    // add service state properties (ingest-info.txt) to ingest request
	    ingestRequest.setServiceState(getServiceState());

	    // assign preliminary job info
            jobState = ingestRequest.getJob();
	    jobState.setSubmissionDate(new DateState(DateUtil.getCurrentDate()));

	    // assign profile
	    profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
		 ingestRequest.getQueuePath().getParentFile().getParentFile().getParent() + "/profiles");	// three levels down from home

	    String profileStorageURL = profileState.getTargetStorage().getStorageLink().toString();
	    // valid profile storage URL?
	    Iterator iterator = m_store.keySet().iterator();
	    boolean match = false;
    	    Integer intKey = null;
	    while(iterator.hasNext()) {
    		intKey = (Integer) iterator.next();
		String storeURL = m_store.get(intKey).toString();
		if ( storeURL.equals(profileStorageURL)) {
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
                //throw new TException.INVALID_CONFIGURATION(msg);
                System.err.println("[warn]" + msg);
	    }

	    if (m_admin != null) profileState.setAdmin(m_admin);
	    if (m_ezid != null) profileState.setMisc(m_ezid);

	    if (profileState.getAccessURL() != null) {
	        jobState.setTargetStorage(new StoreNode(profileState.getAccessURL(), profileState.getTargetStorage().getNodeID()));
	    } else {
	        jobState.setTargetStorage(profileState.getTargetStorage());
	    }

	    if (DEBUG) System.out.println("[debug] "+ profileState.dump("profileState"));
	    jobState.setObjectProfile(profileState);

	    // wait until posting completes a) only for very large batches b) recovering after shutdown
	    if ( ! jobState.grabBatchID().getValue().equalsIgnoreCase(ProfileUtil.DEFAULT_BATCH_ID)) {
	        while (BatchState.getBatchReadiness(jobState.grabBatchID().getValue()) != 1) {
	            System.out.println("[info]" + MESSAGE + "waiting for posting to complete: " + jobState.getJobID());

/*
		    // are we recovering from a shutdown?
		    try {
			synchronized (this) {
		            if (BatchState.getBatchStates().size() == 0) {
		                if (BatchState.getBatchStates().size() == 0) {	// in case two objects pending, only let one through
		                    System.out.println("IngestManager [info] Accessing serialized object: " + ingestRequest.getQueuePath().getParentFile());
			            BatchState batchState = new BatchState();
			            batchState = ProfileUtil.readFrom(batchState, ingestRequest.getQueuePath().getParentFile());
			            BatchState.putBatchState(jobState.grabBatchID().getValue(), batchState);
			     
			            System.out.println(batchState.dump("------> recovering from shutdown"));
			            int completed = 0;

			            iterator = batchState.getJobStates().keySet().iterator();
		                    while(iterator.hasNext()) {
			                JobState jobStateTemp = (JobState) batchState.getJobState((String) iterator.next());
                		        if (jobStateTemp.getJobStatus() != JobStatusEnum.PENDING) completed++;
            		            }


			            System.out.println("-------> number of completed jobs: " + completed);
			            BatchState.putBatchCompletion(jobState.grabBatchID().getValue(), completed);
			            BatchState.putBatchReadiness(jobState.grabBatchID().getValue(), 1);
			            break;
		                }
		            }
			}
		    } catch (Exception e) {
		 	e.printStackTrace();
 	                System.err.println("IngestManager [error] accessing serialized object: " + ingestRequest.getQueuePath().getParentFile());
			break;		// prevent looping in recovery
		    }
*/
		
		    Thread.currentThread().sleep(30 * 1000);
	        }
	    }

	    // link for ingest to expose manifest data
	    ingestRequest.setLink(this. getServiceState().getAccessServiceURL().toString());

	    // if we error during any one handler, skip remaining and update object
	    boolean isError = false;	
	    BatchState batchState = new BatchState();
	    HandlerResult handlerResult = null;

	    // call appropriate handlers
	    SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap());    // thread-safe
            sortedMap = profileState.getIngestHandlers();
            for (Object key : sortedMap.keySet()) {
		String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
		Handler handler = (Handler) createObject(handlerS);
		if (handler == null) {
            	    throw new TException.INVALID_CONFIGURATION("[error] Could not find handler: " + handlerS);
		}
		StateInf stateClass = jobState;
	  	if (isError && (handler.getClass() != org.cdlib.mrt.ingest.handlers.HandlerNotification.class)) {
		    System.out.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
		    continue;
		}
                if (handler.getClass() == org.cdlib.mrt.ingest.handlers.HandlerNotification.class) {
		    if ( ! isError) {
	    	        jobState.setObjectState(jobState.grabTargetStorage().getStorageLink().toString() + "/state/" +
			    jobState.grabTargetStorage().getNodeID() + "/" + 
			    URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"));
		        batchState.setBatchStatus(BatchStatusEnum.COMPLETED);
			batchState.setCompletionDate(new DateState(DateUtil.getCurrentDate()));
		    } else {
            		jobState.setJobStatus(JobStatusEnum.FAILED);
		        batchState.setBatchStatus(BatchStatusEnum.FAILED);
		    }
		    try {
		       batchState = updateBatch(batchState, ingestRequest, jobState);
		    } catch (Exception e) {
			System.out.println("----> Failed to update batch");
		    }
		    stateClass = batchState;

		    BatchState.putBatchCompletion(jobState.grabBatchID().getValue(), 
			BatchState.getBatchCompletion(jobState.grabBatchID().getValue()) + 1);           // increment

            	    // update persistent URL if necessary
            	    jobState.setPersistentURL("http://" +
		        profileState.getObjectMinterURL().getHost() + "/" + jobState.getPrimaryID());
		}

                if (handler.getClass() == org.cdlib.mrt.ingest.handlers.HandlerCallback.class) {
		    // stateClass = batchState;
		}

		try {
                    if (handler.getClass() == org.cdlib.mrt.ingest.handlers.HandlerInventoryQueue.class) {
	    		if ( ! batchState.getBatchID().getValue().equalsIgnoreCase(ProfileUtil.DEFAULT_BATCH_ID)) {
			    jobState.setMisc(batchState.grabTargetQueue());
			} else {
			    // not a batch
			    jobState.setMisc(getProps(ingestRequest, "queue.txt").getProperty("QueueService"));
			}
                    }

		    // Do some work
                    handlerResult = handler.handle(profileState, ingestRequest, stateClass);

		} catch (Exception e) {
		    e.printStackTrace();
		    handlerResult.setSuccess(false);
		}

		// Abort if failure
                if (DEBUG) System.out.println("[debug] " + handler.getName() + ": " + handlerResult.getDescription());
		if (handlerResult.getSuccess()) {
		    if (! isError ) jobState.setJobStatus(JobStatusEnum.COMPLETED);
		} else {
		    // do not abort, but skip all further processing and note exception
		    jobState.setJobStatus(JobStatusEnum.FAILED);
	    	    jobState.setJobStatusMessage(handlerResult.getDescription());
		    isError = true;
		}
            } 	// end for 

	    if (jobState.getPrimaryID() != null) {
	        jobState.setObjectState(jobState.grabTargetStorage().getStorageLink().toString() + "/state/" +
			jobState.grabTargetStorage().getNodeID() + "/" + URLEncoder.encode(jobState.getPrimaryID().getValue(), "utf-8"));
	    }

	    // batch complete?
	    if ( ! batchState.getBatchID().getValue().equalsIgnoreCase(ProfileUtil.DEFAULT_BATCH_ID)) {
		try {
                    if (BatchState.getBatchCompletion(batchState.getBatchID().getValue()) == BatchState.getBatchState(batchState.getBatchID().getValue()).getJobStates().size()) {
                        if (DEBUG) System.out.println("[debug] " + MESSAGE + ": Batch is complete.");
		        BatchState.removeBatchState(jobState.grabBatchID().getValue());
		        BatchState.removeBatchReadiness(jobState.grabBatchID().getValue());
		        BatchState.removeBatchCompletion(jobState.grabBatchID().getValue());
		        BatchState.removeQueuePath(jobState.grabBatchID().getValue());
	            }
		} catch (Exception e) {
		    // ignore threads that may get here late
		}
	    }

            // update status if necessary
            if (profileState.getStatusURL() != null)
                if (! JSONUtil.updateJobState(profileState, jobState))
		    JSONUtil.notify(jobState, profileState, ingestRequest);

            return jobState;
          
        } catch (TException me) {
            throw me;
        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION( MESSAGE + "Exception:" + ex);
        } finally {
	    // if (profileState != null) profileState.getIngestHandlers().clear();
	    // this is causing a java.util.ConcurrentModificationException in notification handler

            // update status if necessary
	    try {
                if (profileState.getStatusURL() != null)
                    JSONUtil.updateJobState(profileState, jobState);
	    } catch (Exception e) { /* ignore */ }
	}
    }

    protected void setIngestStateProperties(IngestServiceState ingestState) 
	throws TException
    {
        String SERVICENAME = "name";
        String SERVICEID = "identifier";
        String TARGETID = "target";
        String SERVICEDESCRIPTION = "description";
        String SERVICESCHEME = "service-scheme";
        // String SERVICECUST = "customer-support";
        String NODESCHEME = "node-scheme";
        String ACCESSURI = "access-uri";
        String SUPPORTURI = "support-uri";

        String serviceNameS = ingestProperties.getProperty(SERVICENAME);
        if (serviceNameS != null) {
            ingestState.setServiceName(serviceNameS);
        } else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICENAME + " parameter is missing from ingest-info.txt");
	}

        String serviceIDS = ingestProperties.getProperty(SERVICEID);
        if (serviceIDS != null) {
            ingestState.setServiceID(serviceIDS);
        } else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICEID + " parameter is missing from ingest-info.txt");
	}

        String targetIDS = ingestProperties.getProperty(TARGETID);
        if (targetIDS == null) {
            targetIDS = "http://merritt.cdlib.org"; 	//default
	    if (DEBUG) System.err.println(MESSAGE + "[warn] " + TARGETID + " parameter is missing from ingest-info.txt");
	    if (DEBUG) System.err.println(MESSAGE + "[warn] " + TARGETID + " using default value: " + targetIDS);
	}
        ingestState.setTargetID(targetIDS);

        String serviceScehmeS = ingestProperties.getProperty(SERVICESCHEME);
        if (serviceScehmeS != null) {
            ingestState.setServiceVersion(serviceScehmeS);
        } else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICESCHEME + " parameter is missing from ingest-info.txt");
	}

        // ingestState.setServiceCustomerSupport(ingestProperties.getProperty(SERVICECUST));

        String accessServiceUrlS = ingestProperties.getProperty(ACCESSURI);
	if (accessServiceUrlS != null) {
            try {
                ingestState.setAccessServiceURL(new URL(accessServiceUrlS));
            } catch (MalformedURLException muex) {
	        throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + ACCESSURI + " parameter is not a valid URL");
            }
	} else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + ACCESSURI + " parameter is missing from ingest-info.txt");
	}

        String supportServiceUrlS = ingestProperties.getProperty(SUPPORTURI);
	if (supportServiceUrlS != null) {
            try {
                ingestState.setSupportServiceURL(new URL(supportServiceUrlS));
            } catch (MalformedURLException muex) {
	        throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + "Support-uri parameter is not a valid URL");
            }
	} else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + " parameter is missing from ingest-info.txt");
	}
    }


    protected synchronized BatchState updateBatch(BatchState sourceBatchState, IngestRequest ingestRequest, JobState jobState)
	throws Exception
    {
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

		// Does not scale!!
		//batchState = ProfileUtil.readFrom(batchState, ingestRequest.getQueuePath().getParentFile());

		batchState = BatchState.getBatchState(jobState.grabBatchID().getValue());
		batchState.setBatchID(jobState.grabBatchID());

		// remove old job and replace w/ new
		Map<String, JobState> jobStates = (HashMap) batchState.getJobStates();
		JobState jobStateTemp = (JobState) jobStates.get(jobState.getJobID().getValue());
		System.out.println("[info]" + MESSAGE + "updating job: " + jobState.getJobID());
		batchState.removeJob(jobState.getJobID().getValue());
		batchState.addJob(jobState.getJobID().getValue(), jobState);
		if (batchState.getBatchStatus() == BatchStatusEnum.FAILED && sourceBatchState.getBatchStatus() == BatchStatusEnum.COMPLETED) {
		    // do not overwrite
		} else {
		    batchState.setBatchStatus(sourceBatchState.getBatchStatus());
		}
		batchState.setCompletionDate(sourceBatchState.getCompletionDate());
		batchState.setBatchLabel(sourceBatchState.getBatchLabel());

		// Does not scale!!
                //ProfileUtil.writeTo(batchState, ingestRequest.getQueuePath().getParentFile());
		BatchState.putBatchState(jobState.grabBatchID().getValue(), batchState);
	    }


	    return batchState;
        } catch (Exception e) {
	   System.out.println("-> Error updating batch: " + jobState.getJobID().getValue());
           e.printStackTrace(System.err);
	   throw new Exception(e.getMessage());
        }
    }


    public IdentifierState requestIdentifier(IngestRequest ingestRequest)
        throws TException
    {
        ProfileState profileState = null;
        JobState jobState = null;

        try {

            profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
                 ingestRequest.getQueuePath().getParentFile().getParentFile().getParent() + "/profiles");       // three levels down from home
            if (m_ezid != null) profileState.setMisc(m_ezid);

            jobState = ingestRequest.getJob();

            String id = MintUtil.processObjectID(profileState, jobState, ingestRequest, true);

            IdentifierState identifierState = new IdentifierState(id);
            return identifierState;

        } catch (TException te) {
            throw te;
        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);
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
}

