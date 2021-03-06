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

import org.cdlib.mrt.ingest.consumer.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.BatchState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.NoSuchElementException;

import javax.xml.soap.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.Long;
import java.lang.IllegalArgumentException;
import java.net.ConnectException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Consume queue data and submit to ingest service
 * - zookeeper is the defined queueing service
 * 
 */
public class Consumer extends HttpServlet
{

    private static final String NAME = "Consumer";
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
	    numThreads = ingestService.getQueueServiceConf().getString("NumThreads");
	    if (StringUtil.isNotEmpty(numThreads)) {
	    	System.out.println("[info] " + MESSAGE + "Setting thread pool size: " + numThreads);
		this.numThreads = new Integer(numThreads).intValue();
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set thread pool size: " + numThreads + "  - using default: " + this.numThreads);
	}

	try {
	    pollingInterval = ingestService.getQueueServiceConf().getString("PollingInterval");
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
		startConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
	    	System.out.println("[info] " + MESSAGE + "starting Queue cleanup daemon");
		startCleanupThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not queue cleanup daemon");
        }
    }


    /**
     * Start consumer thread
     */
    private synchronized void startConsumerThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            ConsumerDaemon consumerDaemon = new ConsumerDaemon(queueConnectionString, queueNode,
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

            CleanupDaemon cleanupDaemon = new CleanupDaemon(queueConnectionString, queueNode, servletConfig);

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

class ConsumerDaemon implements Runnable
{
   
    private static final String NAME = "ConsumerDaemon";
    private static final String MESSAGE = NAME + ": ";

    private IngestServiceInit ingestServiceInit = null;
    private IngestServiceInf ingestService = null;

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = null;
    private Integer poolSize = null;

    private ZooKeeper zooKeeper = null;
    private DistributedQueue distributedQueue = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public ConsumerDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig, 
		Integer pollingInterval, Integer poolSize)
    {
        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
	this.pollingInterval = pollingInterval;
	this.poolSize = poolSize;

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
        ArrayBlockingQueue<ConsumeData> workQueue = new ArrayBlockingQueue<ConsumeData>(poolSize);
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
			    System.out.println(MESSAGE + "Checking for additional tasks.  Current tasks: " + numActiveTasks + " - Max: " + poolSize);
			    item = distributedQueue.consume();
                            executorService.execute(new ConsumeData(ingestService, item, distributedQueue, queueConnectionString, queueNode));
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
	            System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission, and requeuing: " + item.toString());
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
		// reconnect may be necessary
                zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer(), sessionID, sessionAuth);
                distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);

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

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}


class ConsumeData implements Runnable
{
   
    private static final String NAME = "ConsumeData";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private String queueNode = null;
    private ZooKeeper zooKeeper = null;
    private DistributedQueue distributedQueue = null;

    private Item item = null;
    private IngestServiceInf ingestService = null;

    // Constructor
    public ConsumeData(IngestServiceInf ingestService, Item item, DistributedQueue distributedQueue, String queueConnectionString, String queueNode)
    {
	this.distributedQueue = distributedQueue;
	this.item = item;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
    }

    public void run()
    {
        ObjectInputStream ois = null;
        try {
            if (DEBUG) System.out.println("[info] START: consuming queue data:" + item.toString());

            ois = new ObjectInputStream(new ByteArrayInputStream(item.getData()));
            Properties p = (Properties) ois.readObject();
	    IngestRequest ingestRequest = new IngestRequest(p.getProperty("submitter"), p.getProperty("profile"),
			    p.getProperty("filename"), p.getProperty("type"), p.getProperty("size"), p.getProperty("digestType"),
			    p.getProperty("digestValue"), p.getProperty("objectID"), p.getProperty("creator"), p.getProperty("title"),
			    p.getProperty("date"), p.getProperty("responseForm"), p.getProperty("note"));
			    // p.getProperty("retainTargetURL"), p.getProperty("targetURL"));
	    ingestRequest.getJob().setBatchID(new Identifier(p.getProperty("batchID")));
	    ingestRequest.getJob().setJobID(new Identifier(p.getProperty("jobID")));
	    ingestRequest.getJob().setLocalID(p.getProperty("localID"));
	    try {
	       if (p.getProperty("retainTargetURL") != null) {
	          if (p.getProperty("retainTargetURL").equalsIgnoreCase("true")) {
		     System.out.println("[info] Setting retainTargetURL to " + p.getProperty("retainTargetURL"));
		     ingestRequest.setRetainTargetURL(true);
		  }
	       }
	    } catch (Exception e) { } 	// assigned with null value

	    try {
	        ingestRequest.setNotificationFormat(p.getProperty("notificationFormat"));
	    } catch (Exception e) { } 	// assigned with null value
	    try {
	        ingestRequest.setDataCiteResourceType(p.getProperty("DataCiteResourceType"));
	    } catch (Exception e) {}
	    try {
	        ingestRequest.getJob().setAltNotification(p.getProperty("notification"));
	    } catch (Exception e) {}

            // process Dublin Core (optional)
            if (p.getProperty("DCcontributor") != null)
                ingestRequest.getJob().setDCcontributor(p.getProperty("DCcontributor"));
            if (p.getProperty("DCcoverage") != null)
                ingestRequest.getJob().setDCcoverage(p.getProperty("DCcoverage"));
            if (p.getProperty("DCcreator") != null)
                ingestRequest.getJob().setDCcreator(p.getProperty("DCcreator"));
            if (p.getProperty("DCdate") != null)
                ingestRequest.getJob().setDCdate(p.getProperty("DCdate"));
            if (p.getProperty("DCdescription") != null)
                ingestRequest.getJob().setDCdescription(p.getProperty("DCdescription"));
            if (p.getProperty("DCformat") != null)
                ingestRequest.getJob().setDCformat(p.getProperty("DCformat"));
            if (p.getProperty("DCidentifier") != null)
                ingestRequest.getJob().setDCidentifier(p.getProperty("DCidentifier"));
            if (p.getProperty("DClanguage") != null)
                ingestRequest.getJob().setDClanguage(p.getProperty("DClanguage"));
            if (p.getProperty("DCpublisher") != null)
                ingestRequest.getJob().setDCpublisher(p.getProperty("DCpublisher"));
            if (p.getProperty("DCrelation") != null)
                ingestRequest.getJob().setDCrelation(p.getProperty("DCrelation"));
            if (p.getProperty("DCrights") != null)
                ingestRequest.getJob().setDCrights(p.getProperty("DCrights"));
            if (p.getProperty("DCsource") != null)
                ingestRequest.getJob().setDCsource(p.getProperty("DCsource"));
            if (p.getProperty("DCsubject") != null)
                ingestRequest.getJob().setDCsubject(p.getProperty("DCsubject"));
            if (p.getProperty("DCtitle") != null)
                ingestRequest.getJob().setDCtitle(p.getProperty("DCtitle"));
            if (p.getProperty("DCtype") != null)
                ingestRequest.getJob().setDCtype(p.getProperty("DCtype"));

	    ingestRequest.getJob().setJobStatus(JobStatusEnum.CONSUMED);
	    ingestRequest.getJob().setQueuePriority(p.getProperty("queuePriority"));
	    ingestRequest.getJob().setUpdateFlag(((Boolean) p.get("update")).booleanValue());
	    ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			"queue" + FS + ingestRequest.getJob().grabBatchID().getValue() + FS + 
		        ingestRequest.getJob().getJobID().getValue()));
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    BatchState.putQueuePath(p.getProperty("batchID"), ingestRequest.getQueuePath().getAbsolutePath());

	    ingestService.submit(ingestRequest);

	    // inform queue that we're done
	    distributedQueue.complete(item.getId());

            if (DEBUG) System.out.println("[item] END: completed queue data:" + item.toString());
        } catch (SessionExpiredException see) {
            see.printStackTrace(System.err);
            System.err.println("[warn] ConsumeData" + MESSAGE + "Session expired.  Attempting to recreate session.");
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
            System.err.println("[warn] ConsumeData" + MESSAGE + "Connection loss.  Attempting to reconnect.");
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
	    try {
	        ois.close();
	    } catch (IOException ioe) {
	    }
	} 
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }
}


class CleanupDaemon implements Runnable
{

    private static final String NAME = "CleanupDaemon";
    private static final String MESSAGE = NAME + ": ";

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = 3600;	// seconds

    private ZooKeeper zooKeeper = null;
    private DistributedQueue distributedQueue = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public CleanupDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig)
    {
        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;

        try {
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

        sessionID = zooKeeper.getSessionId();
        System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
        sessionAuth = zooKeeper.getSessionPasswd();

        try {
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
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
                        System.out.println(MESSAGE + "Cleaning queue: " + queueConnectionString + " " + queueNode);
			distributedQueue.cleanup(Item.COMPLETED);

                        Thread.currentThread().sleep(5 * 1000);		// wait a short amount of time
                    }

                } catch (ConnectionLossException cle) {
                    System.err.println("[error] " + MESSAGE + "Lost connection to queueing service.");
                    cle.printStackTrace(System.err);
                } catch (SessionExpiredException see) {
                    see.printStackTrace(System.err);
                    System.err.println("[warn] " + MESSAGE + "Session expired.  Attempting to recreate session.");
                    zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                    distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
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
        }
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}
