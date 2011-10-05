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

import java.lang.IllegalArgumentException;
import java.lang.Thread;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.Date;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.handlers.queue.Handler;
import org.cdlib.mrt.ingest.handlers.queue.HandlerResult;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.JobStateInf;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.QueueState;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Basic manager for Queuing Service
 * @author mreyes
 */
public class QueueManager
{

    protected static final String NAME = "QueueManager";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;
    protected Properties ingestProperties = null;
    protected Properties queueProperties = null;
    protected String queueConnectionString = null;
    protected String queueNode = null;
    protected ArrayList<String> m_admin = new ArrayList<String>(20);

    protected boolean debugDump = false;
    protected String ingestFileS = null;	// prop "IngestService"

    public Properties getQueueServiceProps() {
        return queueProperties;
    }
    
    protected QueueManager(LoggerInf logger, Properties conf)
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

    public static QueueManager getQueueManager(LoggerInf logger, Properties conf)
        throws TException
    {
        try {
            QueueManager queueManager = new QueueManager(logger, conf);
            return queueManager;

        } catch (TException tex) {
	    throw tex;
        } catch (Exception ex) {
            String msg = MESSAGE + "QueueManager Exception:" + ex;
            logger.logError(msg, LoggerInf.LogLevel.SEVERE);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex),
                    LoggerInf.LogLevel.DEBUG);
            throw new TException.GENERAL_EXCEPTION( msg);
        }
    }

    /**
     * <pre>
     * Initialize the QueueManager
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
            String matchIngest = "ingestServicePath";
            String matchQueueService = "QueueService";
            String matchQueueNode = "QueueName";
            String matchAdmin = "admin";
            String defaultIDKey = "IDDefault";
	    Integer storageID = null;

            ingestFileS = prop.getProperty(matchIngest);
            if (ingestFileS != null) {
		// load ingest-info.txt
                File ingestInfoTxt = new File(ingestFileS, "ingest-info.txt");
                if (!ingestInfoTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "ingest-info.txt file not found: " + ingestInfoTxt.getAbsolutePath());
                }
                ingestProperties = PropertiesUtil.loadFileProperties(ingestInfoTxt);

                // add queue.txt
                File queueTxt = new File(ingestFileS, "queue.txt");
                if (!queueTxt.exists()) {
                    throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "queue.txt file not found: " + queueTxt.getAbsolutePath());
                }
                queueProperties = PropertiesUtil.loadFileProperties(queueTxt);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		queueProperties.store(out, null);
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		ingestProperties.load(in);	// append
	    } else {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "IngestService: ingest service path definition not found: " + matchIngest);
	    }

	    // Queue and Ingest properties (ingest_info.txt/queue.txt)
            Enumeration e = ingestProperties.propertyNames();
            while( e.hasMoreElements() ) {
                key = (String) e.nextElement();
                value = ingestProperties.getProperty(key);
                if (key.equals(matchQueueService)) {
		    this.queueConnectionString = value;
                }
                if (key.equals(matchQueueNode)) {
		    this.queueNode = value;
                }

                // admin notification
                if (key.startsWith(matchAdmin)) {
                    for (String recipient : value.split(";")) {
                        m_admin.add((String) recipient);
                    }
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

    public IngestServiceState getServiceState()
        throws TException
    {
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
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);
        }
    }

    public QueueState getQueueState()
        throws TException
    {
        ZooKeeper zooKeeper = null;
        try {
            QueueState queueState = new QueueState();

            // open a single connection to zookeeper for all queue posting
            // todo: create an interface
            zooKeeper = new ZooKeeper(queueConnectionString, 10000, new Ignorer());
            DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);    // default priority

            TreeMap<Long,String> orderedChildren;
            try {
                orderedChildren = distributedQueue.orderedChildren(null);
            } catch(KeeperException.NoNodeException e){
                throw new NoSuchElementException();
            }
            for (String headNode : orderedChildren.values()) {
                String path = String.format("%s/%s", distributedQueue.dir, headNode);
                try {
                    byte[] data = zooKeeper.getData(path, false, null);
                    Item item = Item.fromBytes(data);
		    if (item.getStatus() != Item.PENDING) continue;
            	    ByteArrayInputStream bis = new ByteArrayInputStream(item.getData());
            	    ObjectInputStream ois = new ObjectInputStream(bis);
            	    Properties p = (Properties) ois.readObject();

            	    QueueEntryState queueEntryState = new QueueEntryState();
            	    queueEntryState.setDate( item.getTimestamp().toString());
            	    queueEntryState.setStatus("Pending");
            	    queueEntryState.setID(headNode);
            	    queueEntryState.setJobID(p.getProperty("jobID"));
            	    queueEntryState.setBatchID(p.getProperty("batchID"));
            	    queueEntryState.setUser(p.getProperty("submitter"));
            	    queueEntryState.setProfile(p.getProperty("profile"));
            	    queueEntryState.setName(p.getProperty("name"));
            	    queueEntryState.setObjectCreator(p.getProperty("creator"));
            	    queueEntryState.setObjectTitle(p.getProperty("title"));
            	    queueEntryState.setObjectDate(p.getProperty("date"));
            	    queueEntryState.setLocalID(p.getProperty("localID"));
                    
                    queueState.addEntry(queueEntryState);
                } catch(KeeperException.NoNodeException e){}
            }
	    
            return queueState;

        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);
        } finally {
            try {
                zooKeeper.close();
            } catch (Exception e) {
            }
	}
    }

    public BatchState submit(IngestRequest ingestRequest)
        throws Exception
    {
	ProfileState profileState = null;
        try {
	    // add service state properties (ingest-info.txt) to ingest request
	    ingestRequest.setServiceState(getServiceState());

	    // assign preliminary batch info
            BatchState batchState = new BatchState();
	    batchState.clear();
	    batchState.setTargetQueue(queueConnectionString);
	    batchState.setTargetQueueNode(queueNode);
	    batchState.setUserAgent(ingestRequest.getJob().getUserAgent());
	    batchState.setSubmissionDate(new DateState(DateUtil.getCurrentDate()));
	    batchState.setBatchStatus(BatchStatusEnum.QUEUED);
	    batchState.setBatchID(ingestRequest.getJob().getBatchID());
            batchState.setUpdateFlag(ingestRequest.getUpdateFlag());

	    // assign profile
	    profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
		 ingestRequest.getQueuePath().getParentFile().getParent() + "/profiles");	// two levels down from home
            if (m_admin != null) profileState.setAdmin(m_admin);

	    if (DEBUG) System.out.println("[debug] "+ profileState.dump("profileState"));

	    batchState.setBatchProfile(profileState);

	    // link for ingest to expose manifest data
	    ingestRequest.setLink(this. getServiceState().getAccessServiceURL().toString());

	    // The work of posting is done asynchronously to prevent UI timeout
            Post post = new Post(batchState, ingestRequest, profileState);
            Thread postThread = new Thread(post);
	    postThread.start();
	    // undocumented synchronous request
	    if (ingestRequest.getSynchronousMode()) {
     		try {
         	    postThread.join();
	            if (DEBUG) System.out.println("[debug] Synchronous mode");
     		} catch (InterruptedException ignore) {
     		}
	    }

            return batchState;
          
        } catch (TException me) {
            throw me;
        } catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            logger.logError(MESSAGE + "Exception:" + ex, 0);
            throw new TException.GENERAL_EXCEPTION(
                    MESSAGE + "Exception:" + ex);
        } finally {
	    if (profileState != null) profileState.getQueueHandlers().clear();
	}
    }

    protected void setIngestStateProperties(IngestServiceState ingestState) 
	throws TException
    {
        String SERVICENAME = "name";
        String SERVICEID = "identifier";
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

        String serviceScehmeS = ingestProperties.getProperty(SERVICESCHEME);
        if (serviceScehmeS != null) {
            ingestState.setServiceVersion(serviceScehmeS);
        } else {
	    throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICESCHEME + " parameter is missing from ingest-info.txt");
	}

        // ingestState.setServiceCustomerSupport(queueProperties.getProperty(SERVICECUST));

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

   public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
   }


// Need to post (post to queueing service asyncrhonously due to UI timeout)
class Post implements Runnable
{

    private static final String NAME = "Post";
    private static final String MESSAGE = NAME + ": ";
    private BatchState batchState = null;
    private IngestRequest ingestRequest = null;
    private ProfileState profileState = null;
    private TreeMap<Integer,HandlerState> queueHandlers = null;
    private boolean isError = false;


    // Constructor
    public Post(BatchState batchState, IngestRequest ingestRequest, ProfileState profileState)
    {
        try {
	    this.batchState = batchState;
	    this.ingestRequest = ingestRequest;
	    this.profileState = profileState;
	    this.queueHandlers = new TreeMap(profileState.getQueueHandlers());
        } catch (Exception e) {
            e.printStackTrace(System.err);
	}
   }

    public void run()
    {
        try {
            // call appropriate handlers
            SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap());    // thread-safe
            sortedMap = queueHandlers;
            for (Object key : sortedMap.keySet()) {
                String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
                Handler handler = (Handler) createObject(handlerS);
                if (handler == null) {
                    throw new TException.INVALID_CONFIGURATION("[error] Could not find queue handler: " + handlerS);
                }
                StateInf stateClass = batchState;
                if (isError && (! handler.getName().equals("HandlerNotification"))) {
                    System.out.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
                    continue;
                }

                HandlerResult handlerResult = null;
		try {
                    handlerResult = handler.handle(profileState, ingestRequest, stateClass);
                } catch (Exception e) {
                    e.printStackTrace();
                    handlerResult.setSuccess(false);
                }


                // Abort if failure
                if (DEBUG) System.out.println("[debug] " + handler.getName() + ": " + handlerResult.getDescription());
                if (handlerResult.getSuccess()) {
                    batchState.setBatchStatus(BatchStatusEnum.QUEUED);

                } else {
                    batchState.setBatchStatus(BatchStatusEnum.FAILED);
                    batchState.setBatchStatusMessage(handlerResult.getDescription());
                    isError = true;
                }
            }

            // ready for consumer to start processing
            BatchState.putBatchReadiness(batchState.getBatchID().getValue(), 1);
            System.out.println(MESSAGE + "Completion of posting data to queue: " + batchState.getBatchID().getValue());

        } catch (Exception e) {
            System.out.println(MESSAGE + "Exception detected while posting data to queue.");
            e.printStackTrace(System.err);
        } finally {
        }
    }

}

}
