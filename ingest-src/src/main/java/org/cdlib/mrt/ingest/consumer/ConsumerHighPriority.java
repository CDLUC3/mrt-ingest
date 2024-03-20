/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

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
*********************************************************************/
package org.cdlib.mrt.ingest.consumer;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.NoSuchElementException;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.Long;
import java.lang.IllegalArgumentException;
import java.util.Iterator;

/**
 * High priority consume queue data and submit to ingest service
 * - zookeeper is the defined queueing service
 * 
 */
public class ConsumerHighPriority extends HttpServlet
{

    private static final String NAME = "Consumer";
    private static final String MESSAGE = NAME + ": ";
    private volatile Thread consumerHighPriorityThread = null;

    private String queueConnectionString = "localhost:2181";	// Default single server connection
    private String queueNode = "/server.1";	// Default queue
    private String queuePath = null;
    private int numThreads = 5;			// Default size
    private int pollingInterval = 2;		// Default interval (minutes)
    private int highPriorityThreshold = 5;	// Default threshold level ( HP <= highPriorityThreshold)

    public void init(ServletConfig servletConfig)
            throws ServletException {
        super.init(servletConfig);

	String queueConnectionString = null;
	String numThreads = null;
	String pollingInterval = null;
	String highPriorityThreshold = null;
        IngestServiceInit ingestServiceInit = null;
        IngestServiceInf ingestService = null;

	try {
            ingestServiceInit = IngestServiceInit.getIngestServiceInit(servletConfig);
            ingestService = ingestServiceInit.getIngestService();
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not create ingest service in daemon init. ");
	}

	try {
	    queueConnectionString = ingestService.getQueueServiceConf().getString("QueueService");
	    if (StringUtil.isNotEmpty(queueConnectionString)) {
	    	System.out.println("[info] " + MESSAGE + "Setting queue connection string: " + queueConnectionString);
		this.queueConnectionString = queueConnectionString;
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set queue connection string: " + queueConnectionString +
		 "  - using default: " + this.queueConnectionString);
	}

	try {
	    queueNode = ingestService.getQueueServiceConf().getString("QueueName");
	    if (StringUtil.isNotEmpty(queueNode)) {
	    	System.out.println("[info] " + MESSAGE + "Setting queue node: " + queueNode);
		this.queueNode = queueNode;
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set queue node: " + queueNode +
		 "  - using default: " + this.queueNode);
	}

	try {
	    queuePath = ingestService.getIngestServiceProp() + "/queue/";
	    if (StringUtil.isNotEmpty(queuePath)) {
	    	System.out.println("[info] " + MESSAGE + "Setting queue path: " + queuePath);
		this.queuePath = queuePath;
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set queue path: " + queuePath +
		 "  - using default: " + this.queuePath);
	}


	try {
	    numThreads = ingestService.getQueueServiceConf().getString("NumThreadsHighPriority");
	    if (StringUtil.isNotEmpty(numThreads)) {
	    	System.out.println("[info] " + MESSAGE + "Setting high priority thread pool size: " + numThreads);
		this.numThreads = new Integer(numThreads).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set high priority thread pool size: " + numThreads + "  - using default: " + this.numThreads);
	}

	try {
	    pollingInterval = ingestService.getQueueServiceConf().getString("PollingIntervalHighPriority");
	    if (StringUtil.isNotEmpty(pollingInterval)) {
	    	System.out.println("[info] " + MESSAGE + "Setting polling interval for high priority consumer daemon: " + pollingInterval);
		this.pollingInterval = new Integer(pollingInterval).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set polling interval for high priority consumer daemon: " + pollingInterval + "  - using default: " + this.pollingInterval);
	}

/*
	try {
	    highPriorityThreshold = ingestService.getQueueServiceConf().getString("HighPriorityThreshold");
	    if (StringUtil.isNotEmpty(highPriorityThreshold)) {
	    	System.out.println("[info] " + MESSAGE + "Setting high priortity threshold: " + highPriorityThreshold);
		this.highPriorityThreshold = new Integer(highPriorityThreshold).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set high priority threshold: " + highPriorityThreshold + "  - using default: " + this.highPriorityThreshold);
	}
*/

        try {
            // Start the Consumer high priority thread
            if (consumerHighPriorityThread == null) {
	    	System.out.println("[info] " + MESSAGE + "starting high priority consumer daemon");
		startConsumerHighPriorityThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start high priority consumer daemon");
        }

    }


    /**
     * Start consumer thread
     */
    private synchronized void startConsumerHighPriorityThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerHighPriorityThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            ConsumerDaemonHighPriority consumerDaemon = new ConsumerDaemonHighPriority(queueConnectionString, queueNode,
		servletConfig, pollingInterval, numThreads, highPriorityThreshold);

            consumerHighPriorityThread =  new Thread(consumerDaemon);
            consumerHighPriorityThread.setDaemon(true);                // Kill thread when servlet dies
            consumerHighPriorityThread.start();

	    System.out.println("[info] " + MESSAGE + "consumer high priority daemon started");

            return;

        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    public String getName() {
        return NAME;
    }

    public void destroy() {
	try {
	    System.out.println("[info] " + MESSAGE + "interrupting consumer high priority daemon");
            consumerHighPriorityThread.interrupt();
	    saveState();
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void saveState() {
	try {
            Iterator iterator = BatchState.getBatchStates().keySet().iterator();
            while(iterator.hasNext()){
                BatchState batchState = BatchState.getBatchState((String) iterator.next());
                System.out.println("[warning] " + MESSAGE + "Shutdown detected, writing to disk: " + batchState.getBatchID().getValue());
                System.out.println("[warning] " + MESSAGE + "queue path: " + queuePath + "/" + batchState.getBatchID().getValue());
                ProfileUtil.writeTo(batchState, new File(queuePath + "/" + batchState.getBatchID().getValue()));
            }
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

}

class ConsumerDaemonHighPriority implements Runnable
{
   
    private static final String NAME = "ConsumerDaemonHighPriority";
    private static final String MESSAGE = NAME + ": ";

    private IngestServiceInit ingestServiceInit = null;
    private IngestServiceInf ingestService = null;

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = null;
    private Integer poolSize = null;
    private Integer highPriorityThreshold = null;

    private ZooKeeper zooKeeper = null;
    private DistributedQueue distributedQueue = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public ConsumerDaemonHighPriority(String queueConnectionString, String queueNode, ServletConfig servletConfig, 
		Integer pollingInterval, Integer poolSize, Integer highPriorityThreshold)
    {
        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
	this.pollingInterval = pollingInterval;
	this.poolSize = poolSize;
	this.highPriorityThreshold = highPriorityThreshold;

	try {
            ingestServiceInit = IngestServiceInit.getIngestServiceInit(servletConfig);
            ingestService = ingestServiceInit.getIngestService();
	
            zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());

            distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);    // default priority
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<ConsumeDataHighPriority> workQueue = new ArrayBlockingQueue<ConsumeDataHighPriority>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) 5, TimeUnit.SECONDS, (BlockingQueue) workQueue);

	sessionID = zooKeeper.getSessionId();
	System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
	sessionAuth = zooKeeper.getSessionPasswd();
        Item item = null;

        try {
            long queueSize = workQueue.size();
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
                    init = false;
                }

                // Let's check to see if we are on hold
                if (onHold()) {
		    try {
                        distributedQueue.peek();
                        System.out.println(MESSAGE + "detected 'on hold' condition");
		    } catch (ConnectionLossException cle) {
			System.err.println("[warn] " + MESSAGE + "Queueing service is down.");
			cle.printStackTrace(System.err);
		    } catch (Exception e) {
		    }
                    continue;
                }

                // have we shutdown?
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(MESSAGE + "interruption detected.");
      		    throw new InterruptedException();
                }

		// Perform some work
		try {
		    long numActiveTasks = 0;

		    // To prevent long shutdown, no more than poolsize tasks queued.
		    while (true) {
		        numActiveTasks = executorService.getActiveCount();
			if (numActiveTasks < poolSize) {
			    String worker = getWorkerID();
			    System.out.println(MESSAGE + "Checking for additional High Priority tasks for Worker " + worker + "  -  Current tasks: " + numActiveTasks + " - Max: " + poolSize);
			    item = distributedQueue.consume(getWorkerID(), true);
			    System.out.println("Found a high Priority Zookeeper entry: " + item.getId());
                     	    executorService.execute(new ConsumeDataHighPriority(ingestService, item, distributedQueue, queueConnectionString, queueNode));
			} else {
			    System.out.println(MESSAGE + "Work queue is full, NOT checking for additional tasks: " + numActiveTasks + " - Max: " + poolSize);
			    break;
			}
		    }

        	} catch (ConnectionLossException cle) {
		    System.err.println("[error] " + MESSAGE + "Lost connection to queueing service.");
		    cle.printStackTrace(System.err);
		    String[] parse = cle.getMessage().split(queueNode + "/");	// ConnectionLoss for /q/qn-000000000970 
		    if (parse.length > 1) {
		        // attempt to requeue
			int i = 0;
			final int MAX_ATTEMPT = 3;
		        while (i < MAX_ATTEMPT) {
	    	            System.out.println("[info]" + MESSAGE +  i + ":Attempting to requeue: " + parse[1]);
		            if (requeue(parse[1])) {
			        break;
		    	    }
			    i++;
	    	            Thread.currentThread().sleep(30 * 1000);		// wait for ZK to recover
		        }
			if (i >= MAX_ATTEMPT){
	    	            System.out.println("[error]" + MESSAGE + "Could not requeue ITEM: " + parse[1]);
			    // session expired ?  If so, can we recover or just eror
			    // throw new org.apache.zookeeper.KeeperException.SessionExpiredException();
		            //zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Ignorer(), sessionID, sessionAuth);
			}
		    } else {
		        System.err.println("[info] " + MESSAGE + "Did not interrupt queue create.  We're OK.");
		    }   
        	} catch (SessionExpiredException see) {
		    see.printStackTrace(System.err);
		    System.err.println("[warn] " + MESSAGE + "Session expired.  Attempting to recreate session.");
            	    zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                    distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);  
		} catch (RejectedExecutionException ree) {
	            System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission, and requeuing: " + item.getId());
		    distributedQueue.requeue(item.getId());
        	    Thread.currentThread().sleep(5 * 1000);         // let thread pool relax a bit
		} catch (NoSuchElementException nsee) {
		    // no data in queue
		    System.out.println("[info] " + MESSAGE + "No data in queue to process");
		} catch (IllegalArgumentException iae) {
		    // no queue exists
		    System.out.println("[info] " + MESSAGE + "New queue does not yet exist: " + queueNode);
		} catch (Exception e) {
		    System.err.println("[warn] " + MESSAGE + "General exception.");
	            e.printStackTrace();
		}
	    }
        } catch (InterruptedException ie) {
	    try {
		try {
	    	    zooKeeper.close();
		} catch (Exception ze) {}
                System.out.println(MESSAGE + "shutting down consumer daemon.");
	        executorService.shutdown();

		int cnt = 0;
		while (! executorService.awaitTermination(15L, TimeUnit.SECONDS)) {
                    System.out.println(MESSAGE + "waiting for tasks to complete.");
		    cnt++;
		    if (cnt == 8) {	// 2 minutes
			// force shutdown
	        	executorService.shutdownNow();
		    }
		}
            } catch (Exception e) {
		e.printStackTrace(System.err);
            }
	} catch (Exception e) {
            System.out.println(MESSAGE + "Exception detected, shutting down consumer daemon.");
	    e.printStackTrace(System.err);
	    executorService.shutdown();
        } finally {
	}
    }

    // to do: make this a service call
    private boolean onHold()
    {
        try {
            File holdFile = new File(ingestService.getQueueServiceConf().getString("QueueHoldFile"));
            if (holdFile.exists()) {
                System.out.println("[info]" + NAME + ": hold file exists, not processing queue: " + holdFile.getAbsolutePath());
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private boolean requeue(String id)
    {
        try {

	    Item item = null;
	    try {
	        item = distributedQueue.updateStatus(id, Item.CONSUMED, Item.PENDING);
	    } catch (SessionExpiredException see) {
	        System.err.println("[error]" + MESSAGE +  "Session expired.  Attempting to recreate session while requeueing.");
                zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);

		return false;
	    }

	    if (item != null) {
		System.out.println("** [info] ** " + MESSAGE + "Successfully requeued: " + item.toString());
	    } else {
	        System.err.println("[error]" + MESSAGE +  "Could not requeue: " + id);
		return false;
	    }
        } catch (Exception e) {
	    e.printStackTrace();
            return false;
        }
        return true;
    }

    private String getWorkerID() {

        String workerID = "0";

        try {
            // Set in setenv.sh (e.g. ingest01-stg)
            String workerEnv = System.getenv("WORKERNAME");
            workerID = workerEnv.substring("ingest0".length(), "ingest0".length() + 1);
        } catch (Exception e ) {
            // System.out.println("[info] Can not calculate Ingest worker.  Setting to '0'.");
        }

        return workerID;
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}


class ConsumeDataHighPriority implements Runnable
{
   
    private static final String NAME = "ConsumeDataHighPriority";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private String queueNode = null;
    private ZooKeeper zooKeeper = null;
    private DistributedQueue distributedQueue = null;

    private Item item = null;
    private IngestServiceInf ingestService = null;
    private JobState jobState = null;

    // Constructor
    public ConsumeDataHighPriority(IngestServiceInf ingestService, Item item, DistributedQueue distributedQueue, String queueConnectionString, String queueNode)
    {
	this.distributedQueue = distributedQueue;
	this.item = item;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
    }

    public void run()
    {
        try {
            JSONObject jp = new JSONObject(new String(item.getData(), "UTF-8"));
            if (DEBUG) System.out.println("[info] START: consuming queue data:" + jp.toString());

            // Check if collection level hold
            if (onHold(JSONUtil.getValue(jp,"profile"))) {
                try {
                    zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                    distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
	            distributedQueue.holdConsumed(item.getId());
                    System.out.println(MESSAGE + "detected collection level hold.  Setting ZK entry state to 'held': " + item.getId());
                } catch (ConnectionLossException cle) {
                    System.err.println("[error] " + MESSAGE + "Queueing service is down.");
                    cle.printStackTrace(System.err);
                } catch (Exception e) {
                    System.err.println("[error] " + MESSAGE + "Exception while placing entry to 'held'");
                    e.printStackTrace(System.err);
                } finally {
                    zooKeeper.close();
                }

            } else {

            IngestRequest ingestRequest = new IngestRequest(JSONUtil.getValue(jp,"submitter"), JSONUtil.getValue(jp,"profile"),
                            JSONUtil.getValue(jp,"filename"), JSONUtil.getValue(jp,"type"), JSONUtil.getValue(jp,"size"),
                            JSONUtil.getValue(jp,"digestType"), JSONUtil.getValue(jp,"digestValue"),
                            JSONUtil.getValue(jp,"objectID"), JSONUtil.getValue(jp,"creator"),
                            JSONUtil.getValue(jp,"title"), JSONUtil.getValue(jp,"date"),
                            JSONUtil.getValue(jp,"responseForm"), JSONUtil.getValue(jp,"note"));
                            // jp.getString("retainTargetURL"), jp.getString("targetURL"));
            ingestRequest.getJob().setBatchID(new Identifier(JSONUtil.getValue(jp,"batchID")));
            ingestRequest.getJob().setJobID(new Identifier(JSONUtil.getValue(jp,"jobID")));
            ingestRequest.getJob().setLocalID(JSONUtil.getValue(jp,"localID"));

            try {
               if (JSONUtil.getValue(jp,"retainTargetURL") != null) {
                  if (JSONUtil.getValue(jp,"retainTargetURL").equalsIgnoreCase("true")) {
                     System.out.println("[info] Setting retainTargetURL to " + JSONUtil.getValue(jp,"retainTargetURL"));
                     ingestRequest.setRetainTargetURL(true);
                  }
               }
            } catch (Exception e) { }   // assigned with null value

	    try {
                ingestRequest.setNotificationFormat(JSONUtil.getValue(jp,"notificationFormat"));
	    } catch (Exception e) { } 	// assigned with null value
	    try {
                ingestRequest.setDataCiteResourceType(JSONUtil.getValue(jp,"DataCiteResourceType"));
	    } catch (Exception e) {}
	    try {
                ingestRequest.getJob().setAltNotification(JSONUtil.getValue(jp,"notification"));
	    } catch (Exception e) {}

            // process Dublin Core (optional)
            if (! jp.isNull("DCcontributor"))
                ingestRequest.getJob().setDCcontributor(jp.getString("DCcontributor"));
            if (! jp.isNull("DCcoverage"))
                ingestRequest.getJob().setDCcoverage(jp.getString("DCcoverage"));
            if (! jp.isNull("DCcreator"))
                ingestRequest.getJob().setDCcreator(jp.getString("DCcreator"));
            if (! jp.isNull("DCdate"))
                ingestRequest.getJob().setDCdate(jp.getString("DCdate"));
            if (! jp.isNull("DCdescription"))
                ingestRequest.getJob().setDCdescription(jp.getString("DCdescription"));
            if (! jp.isNull("DCformat"))
                ingestRequest.getJob().setDCformat(jp.getString("DCformat"));
            if (! jp.isNull("DCidentifier"))
                ingestRequest.getJob().setDCidentifier(jp.getString("DCidentifier"));
            if (! jp.isNull("DClanguage"))
                ingestRequest.getJob().setDClanguage(jp.getString("DClanguage"));
            if (! jp.isNull("DCpublisher"))
                ingestRequest.getJob().setDCpublisher(jp.getString("DCpublisher"));
            if (! jp.isNull("DCrelation"))
                ingestRequest.getJob().setDCrelation(jp.getString("DCrelation"));
            if (! jp.isNull("DCrights"))
                ingestRequest.getJob().setDCrights(jp.getString("DCrights"));
            if (! jp.isNull("DCsource"))
                ingestRequest.getJob().setDCsource(jp.getString("DCsource"));
            if (! jp.isNull("DCsubject"))
                ingestRequest.getJob().setDCsubject(jp.getString("DCsubject"));
            if (! jp.isNull("DCtitle"))
                ingestRequest.getJob().setDCtitle(jp.getString("DCtitle"));
            if (! jp.isNull("DCtype"))
                ingestRequest.getJob().setDCtype(jp.getString("DCtype"));

	    ingestRequest.getJob().setJobStatus(JobStatusEnum.CONSUMED);
            ingestRequest.getJob().setQueuePriority(JSONUtil.getValue(jp,"queuePriority"));
            Boolean update = new Boolean(jp.getBoolean("update"));
	    ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			"queue" + FS + ingestRequest.getJob().grabBatchID().getValue() + FS + 
		        ingestRequest.getJob().getJobID().getValue()));
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

            BatchState.putQueuePath(JSONUtil.getValue(jp,"batchID"), ingestRequest.getQueuePath().getAbsolutePath());

	    jobState = ingestService.submit(ingestRequest);

	    if (jobState.getJobStatus() == JobStatusEnum.COMPLETED) {
                if (DEBUG) System.out.println("[item]: COMPLETED queue data:" + item.getId());
	    	distributedQueue.complete(item.getId());
	    } else if (jobState.getJobStatus() == JobStatusEnum.FAILED) {
		System.out.println("[item]: FAILED queue data:" + item.getId());
		System.out.println("Consume Daemon - job message: " + jobState.getJobStatusMessage());
	    	distributedQueue.fail(item.getId());
	    } else {
		System.out.println("Consume Daemon - Undetermined STATE: " + jobState.getJobStatus().getValue() + " -- " + jobState.getJobStatusMessage());
	    }
	}	// end of else

	    // inform queue that we're done

        } catch (SessionExpiredException see) {
            see.printStackTrace(System.err);
            System.err.println("[warn] ConsumeDataHighPriority" + MESSAGE + "Session expired.  Attempting to recreate session.");
	    try {
                zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
	        distributedQueue.complete(item.getId());
	    } catch (Exception e) {
                e.printStackTrace(System.err);
                System.out.println("[error] Consuming queue data: Could not recreate session.");
	    }
        } catch (ConnectionLossException cle) {
            cle.printStackTrace(System.err);
            System.err.println("[warn] ConsumeDataHighPriority" + MESSAGE + "Connection loss.  Attempting to reconnect.");
	    try {
                zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
	        distributedQueue.complete(item.getId());
	    } catch (Exception e) {
                e.printStackTrace(System.err);
                System.out.println("[error] Consuming queue data: Could not reconnect.");
	    }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.out.println("[error] Consuming queue data");
        } finally {
	} 
    }

    // Support collection level hold
    private boolean onHold(String collection)
    {
        String append = "";
        try {
            if (collection != null) append = "_" + collection;
            File holdFile = new File(ingestService.getQueueServiceConf().getString("QueueHoldFile") + append);
            System.out.println("[info]" + NAME + ": Checking for collection hold: " + holdFile.getAbsolutePath());
            if (holdFile.exists()) {
                System.out.println("[info]" + NAME + ": Hold file exists for collection, not processing: " + holdFile.getAbsolutePath());
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }
}

