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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.ZKKey;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.json.JSONObject;

import java.nio.file.Path;
import java.nio.file.Paths;

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
import java.util.List;
import java.util.Iterator;

/**
 * Consume Batch queue data and create Jobs 
 * - zookeeper is the defined queueing service
 * 
 */
public class BatchConsumer extends HttpServlet
{

    private static final String NAME = "BatchConsumer";
    private static final String MESSAGE = NAME + ": ";
    private volatile Thread consumerThread = null;
    private volatile Thread cleanupThread = null;

    private String queueConnectionString = "localhost:2181";	// default single server connection
    private String queueNode = "/server.1";	// default queue
    private String queuePath = null;
    private int numThreads = 5;		// default size
    private int pollingInterval = 2;	// default interval (minutes)

    public void init(ServletConfig servletConfig)
            throws ServletException {
        super.init(servletConfig);

	String queueConnectionString = null;
	String numThreads = null;
	String pollingInterval = null;
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
	    numThreads = ingestService.getQueueServiceConf().getString("BatchNumThreads");
	    if (StringUtil.isNotEmpty(numThreads)) {
	    	System.out.println("[info] " + MESSAGE + "Setting thread pool size: " + numThreads);
		this.numThreads = new Integer(numThreads).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set thread pool size: " + numThreads + "  - using default: " + this.numThreads);
	}

	try {
	    pollingInterval = ingestService.getQueueServiceConf().getString("BatchPollingInterval");
	    if (StringUtil.isNotEmpty(pollingInterval)) {
	    	System.out.println("[info] " + MESSAGE + "Setting polling interval: " + pollingInterval);
		this.pollingInterval = new Integer(pollingInterval).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set polling interval: " + pollingInterval + "  - using default: " + this.pollingInterval);
	}

        try {
            // Start the Consumer thread
            if (consumerThread == null) {
	    	System.out.println("[info] " + MESSAGE + "starting consumer daemon");
		startBatchConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
	    	//System.out.println("[info] " + MESSAGE + "starting Queue cleanup daemon");
		//startCleanupThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not queue cleanup daemon");
        }
    }


    /**
     * Start consumer thread
     */
    private synchronized void startBatchConsumerThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            BatchConsumerDaemon consumerDaemon = new BatchConsumerDaemon(queueConnectionString, queueNode,
		servletConfig, pollingInterval, numThreads);

            consumerThread =  new Thread(consumerDaemon);
            consumerThread.setDaemon(true);                // Kill thread when servlet dies
            consumerThread.start();

	    System.out.println("[info] " + MESSAGE + "consumer daemon started");

            return;

        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    /**
     * Start Queue cleanup thread
     */
    private synchronized void startCleanupThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (cleanupThread != null) {
                System.out.println("[info] " + MESSAGE + "Queue cleanup daemon already started");
                return;
            }

            BatchCleanupDaemon cleanupDaemon = new BatchCleanupDaemon(queueConnectionString, queueNode, servletConfig);

            cleanupThread =  new Thread(cleanupDaemon);
            cleanupThread.setDaemon(true);                // Kill thread when servlet dies
            cleanupThread.start();

	    System.out.println("[info] " + MESSAGE + "cleanup daemon started");

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
	    System.out.println("[info] " + MESSAGE + "interrupting consumer daemon");
            consumerThread.interrupt();
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

}

class BatchConsumerDaemon implements Runnable
{
   
    private static final String NAME = "BatchConsumerDaemon";
    private static final String MESSAGE = NAME + ": ";

    private IngestServiceInit ingestServiceInit = null;
    private IngestServiceInf ingestService = null;

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = null;
    private Integer poolSize = null;
    public static int sessionTimeout = 40000;

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public BatchConsumerDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig, 
		Integer pollingInterval, Integer poolSize)
    {
        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
	this.pollingInterval = pollingInterval;
	this.poolSize = poolSize;

	try {
            ingestServiceInit = IngestServiceInit.getIngestServiceInit(servletConfig);
            ingestService = ingestServiceInit.getIngestService();
	
            zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());

	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<BatchConsumeData> workQueue = new ArrayBlockingQueue<BatchConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) 5, TimeUnit.SECONDS, (BlockingQueue) workQueue);

	sessionID = zooKeeper.getSessionId();
	System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
	sessionAuth = zooKeeper.getSessionPasswd();

        try {
            long queueSize = workQueue.size();
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    //System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
                    System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    init = false;
                }

                // Let's check to see if we are on hold
/*
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
*/

                // have we shutdown?
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(MESSAGE + "interruption detected.");
      		    throw new InterruptedException();
                }

		// Perform some work
		try {
		    long numActiveTasks = 0;

		    initPaths();
		    // To prevent long shutdown, no more than poolsize tasks queued.
		    while (true) {
		        numActiveTasks = executorService.getActiveCount();
			if (numActiveTasks < poolSize) {
			    System.out.println(MESSAGE + "Checking for additional tasks -  Current tasks: " + numActiveTasks + " - Max: " + poolSize);

			    Batch batch = null;
                            batch = Batch.acquirePendingBatch(zooKeeper);
			    if ( batch != null) { 
System.out.println(NAME + " =================> ACQUIRED PENDING batch status " + batch.status());
			    	System.out.println(MESSAGE + "Found pending batch data: " + batch.id());
                                executorService.execute(new BatchConsumeData(ingestService, batch, zooKeeper, queueConnectionString, queueNode));
			    } else {
				break;
		     	    }

			} else {
			    System.out.println(MESSAGE + "Work queue is full, NOT checking for additional tasks: " + numActiveTasks + " - Max: " + poolSize);
			    break;
			}
		    }

		} catch (RejectedExecutionException ree) {
        	    //Thread.currentThread().sleep(5 * 1000);         // let thread pool relax a bit
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

    public void create(String s, Object data) throws KeeperException, InterruptedException {
      create(Paths.get(s), data);
    }

    public void create(Path path, Object data) throws KeeperException, InterruptedException {
      Path par = path.getParent();
      if (!QueueItemHelper.exists(zooKeeper, par.toString())) {
        create(par, null);
      }
try {
      QueueItemHelper.create(zooKeeper, path.toString(), QueueItemHelper.serializeAsBytes(data));
} catch (Exception e) {}
    }

    public void initPaths() throws KeeperException, InterruptedException {
      create("/batches", null);
      create("/batch-uuids", null);
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}


class BatchConsumeData implements Runnable
{
   
    private static final String NAME = "BatchConsumeData";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private String queueNode = null;
    private ZooKeeper zooKeeper = null;

    private IngestServiceInf ingestService = null;
    private BatchState batchState = null;
    private Batch batch = null;

    // Constructor
    public BatchConsumeData(IngestServiceInf ingestService, Batch batch, ZooKeeper zooKeeper, String queueConnectionString, String queueNode)
    {
	this.zooKeeper = zooKeeper;
	this.batch = batch;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
    }

    public void run()
    {
        try {

	    // UTF-8 ??
	    JSONObject jp = batch.jsonProperty(zooKeeper, ZKKey.BATCH_SUBMISSION);
            if (DEBUG) System.out.println("[info] START: consuming batch queue " + batch.id() + " - " + jp.toString());

            // Check if collection level hold
            //if (onHold(JSONUtil.getValue(jp,"profile"))) {
		// Need this hold anymore??
            //} else {

	    IngestRequest ingestRequest = new IngestRequest(JSONUtil.getValue(jp,"submitter"), JSONUtil.getValue(jp,"profile"),
			    JSONUtil.getValue(jp,"filename"), JSONUtil.getValue(jp,"type"), JSONUtil.getValue(jp,"size"),
			    JSONUtil.getValue(jp,"digestType"), JSONUtil.getValue(jp,"digestValue"),
			    JSONUtil.getValue(jp,"objectID"), JSONUtil.getValue(jp,"creator"),
			    JSONUtil.getValue(jp,"title"), JSONUtil.getValue(jp,"date"),
			    JSONUtil.getValue(jp,"responseForm"), JSONUtil.getValue(jp,"note"));
			    // jp.getString("retainTargetURL"), jp.getString("targetURL"));
	    ingestRequest.getJob().setBatchID(new Identifier(JSONUtil.getValue(jp,"batchID")));
	    ingestRequest.getJob().setLocalID(JSONUtil.getValue(jp,"localID"));
	    try {
	       if (JSONUtil.getValue(jp,"retainTargetURL") != null) {
	          if (JSONUtil.getValue(jp,"retainTargetURL").equalsIgnoreCase("true")) {
		     System.out.println("[info] Setting retainTargetURL to " + JSONUtil.getValue(jp,"retainTargetURL"));
		     ingestRequest.setRetainTargetURL(true);
		  }
	       }
	    } catch (Exception e) { } 	// assigned with null value

	    try {
	        ingestRequest.setNotificationFormat(JSONUtil.getValue(jp,"notificationFormat"));
	    } catch (Exception e) { e.printStackTrace(); } 	// assigned with null value
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
	    ingestRequest.getJob().setUpdateFlag(update.booleanValue());
	    ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			"queue" + FS + ingestRequest.getJob().grabBatchID().getValue()));
	    //ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			//"queue" + FS + ingestRequest.getJob().grabBatchID().getValue() + FS + 
		        //ingestRequest.getJob().getJobID().getValue()));
            //new File(ingestRequest.getQueuePath(), "system").mkdir();
            //new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    //BatchState.putQueuePath(JSONUtil.getValue(jp,"batchID"), ingestRequest.getQueuePath().getAbsolutePath());

	    ingestRequest.setBatch(batch);
	    batchState = ingestService.submitBatch(ingestRequest);

	    batch.setStatus(zooKeeper, batch.status().stateChange(org.cdlib.mrt.zk.BatchState.Reporting));
	    System.out.println(NAME + " =================> Change batch state to: " + batch.status().name());
	    batch.unlock(zooKeeper);



//=========== Change state to Success here???  
//Little is known other than BID

/*
	    if (jobState.getJobStatus() == JobStatusEnum.COMPLETED) {
                if (DEBUG) System.out.println("[item]: COMPLETED queue data:" + jp.toString());
	    	distributedQueue.complete(item.getId());
	    } else if (jobState.getJobStatus() == JobStatusEnum.FAILED) {
		System.out.println("[item]: FAILED queue data:" + item.getId());
		System.out.println("Consume Daemon - job message: " + jobState.getJobStatusMessage());
	    	distributedQueue.fail(item.getId());
	    } else {
		System.out.println("Consume Daemon - Undetermined STATE: " + jobState.getJobStatus().getValue() + " -- " + jobState.getJobStatusMessage());
	    }
*/
	//}	// end of else

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


class BatchCleanupDaemon implements Runnable
{

    private static final String NAME = "BatchCleanupDaemon";
    private static final String MESSAGE = NAME + ": ";

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = 3600;	// seconds
    public static int sessionTimeout = 40000;

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public BatchCleanupDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig)
    {
        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;

        try {
            zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());

        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public void run()
    {
        boolean init = true;
        String status = null;

        sessionID = zooKeeper.getSessionId();
        System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
        sessionAuth = zooKeeper.getSessionPasswd();

        try {
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    //System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
                    System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    init = false;
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
                        System.out.println(MESSAGE + "Cleaning queue (COMPLETED states): " + queueConnectionString + " " + queueNode);
			try {
			    //distributedQueue.cleanup(Item.COMPLETED);
			} catch (NoSuchElementException nsee) {
			    // No more data
			} 
                        System.out.println(MESSAGE + "Cleaning queue (DELETED states): " + queueConnectionString + " " + queueNode);
			// Will throw NoSuchElementException to break tight loop
			//distributedQueue.cleanup(Item.DELETED);

                        Thread.currentThread().sleep(5 * 1000);		// wait a short amount of time
                    }

                } catch (RejectedExecutionException ree) {
                    System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission");
                } catch (NoSuchElementException nsee) {
                    // no data in queue
                    System.out.println("[info] " + MESSAGE + "No data in queue to clean");
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
                // zooKeeper.close();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        } catch (Exception e) {
            System.out.println(MESSAGE + "Exception detected, shutting down cleanup daemon.");
            e.printStackTrace(System.err);
        } finally {
	    sessionAuth = null;
        }
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}
