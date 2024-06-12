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
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.zk.Job;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.ZKKey;
import org.cdlib.mrt.zk.MerrittJsonKey;
import org.cdlib.mrt.zk.MerrittStateError;
import org.cdlib.mrt.zk.QueueItemHelper;
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
import java.util.List;
import java.util.Properties;

/**
 * Consume process state queue data and submit to ingest service
 * - zookeeper is the defined queueing service
 * 
 */
public class EstimateConsumer extends HttpServlet
{

    private static final String NAME = "EstimateConsumer";
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
		startEstimateConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
	    	//System.out.println("[info] " + MESSAGE + "starting Batch/Job cleanup daemon");
		//startEstimateleanupThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not Batch/Job cleanup daemon");
        }
    }


    /**
     * Start consumer thread
     */
    private synchronized void startEstimateConsumerThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            EstimateConsumerDaemon jobConsumerDaemon = new EstimateConsumerDaemon(queueConnectionString, queueNode,
		servletConfig, pollingInterval, numThreads);

            consumerThread =  new Thread(jobConsumerDaemon);
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
    private synchronized void startEstimateCleanupThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (cleanupThread != null) {
                System.out.println("[info] " + MESSAGE + "Queue cleanup daemon already started");
                return;
            }

            EstimateCleanupDaemon cleanupDaemon = new EstimateCleanupDaemon(queueConnectionString, queueNode, servletConfig);

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

class EstimateConsumerDaemon implements Runnable
{
   
    private static final String NAME = "EstimateConsumerDaemon";
    private static final String MESSAGE = NAME + ": ";

    private IngestServiceInit ingestServiceInit = null;
    private IngestServiceInf ingestService = null;

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = null;
    private Integer poolSize = null;

    private ZooKeeper zooKeeper = null;
    public static int sessionTimeout = 300000;  //5 minutes

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public EstimateConsumerDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig, 
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
        ArrayBlockingQueue<EstimateConsumeData> workQueue = new ArrayBlockingQueue<EstimateConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) 5, TimeUnit.SECONDS, (BlockingQueue) workQueue);

	sessionID = zooKeeper.getSessionId();
	System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
	sessionAuth = zooKeeper.getSessionPasswd();
        //Item item = null;

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
                if (onHold()) {
                    System.out.println(MESSAGE + "detected 'on hold' condition");
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
			    System.out.println(MESSAGE + "Checking for additional Job tasks for Worker: Current tasks: " + numActiveTasks + " - Max: " + poolSize);
                            Job job = null;
                            job = Job.acquireJob(zooKeeper, org.cdlib.mrt.zk.JobState.Pending);
                            if ( job != null) {
                                System.out.println(MESSAGE + "Found estimating job data: " + job.id());

				try {
            			   job.setStatus(zooKeeper, job.status().stateChange(org.cdlib.mrt.zk.JobState.Estimating));
				} catch (MerrittStateError mse) {
				   mse.printStackTrace();
            			   job.setStatus(zooKeeper, job.status().stateChange(org.cdlib.mrt.zk.JobState.Estimating));
				}

                                executorService.execute(new EstimateConsumeData(ingestService, job, zooKeeper, queueConnectionString, queueNode));
                            } else {
                                break;
                            }


			} else {
			    System.out.println(MESSAGE + "Work queue is full, NOT checking for additional tasks: " + numActiveTasks + " - Max: " + poolSize);
			    break;
			}
		    }

		} catch (RejectedExecutionException ree) {
	            //System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission, and requeuing: " + item.getId());
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

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}


class EstimateConsumeData implements Runnable
{
   
    private static final String NAME = "EstimateConsumeData";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private String queueNode = null;
    private ZooKeeper zooKeeper = null;
    // public static int sessionTimeout = 40000;
    public static int sessionTimeout = 360000;         // hour^M

    private Job job = null;
    private IngestServiceInf ingestService = null;
    private JobState jobState = null;

    // Constructor
    public EstimateConsumeData(IngestServiceInf ingestService, Job job, ZooKeeper zooKeeper, String queueConnectionString, String queueNode)
    {
        this.zooKeeper = zooKeeper;
	this.job = job;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
        this.queueNode = queueNode;
    }

    public void run()
    {
        try {

            JSONObject jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
            if (DEBUG) System.out.println("[info] START: consuming job queue " + job.id() + " - " + jp.toString());

            // Check if collection level hold
            if (onHold(JSONUtil.getValue(jp,"profile"))) {
                //try {
                    //zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                    //distributedQueue = new DistributedQueue(zooKeeper, queueNode, null);
	            //distributedQueue.holdConsumed(item.getId());
                    //System.out.println(MESSAGE + "detected collection level hold.  Setting ZK entry state to 'held': " + item.getId());
                //} catch (ConnectionLossException cle) {
                    //System.err.println("[error] " + MESSAGE + "Queueing service is down.");
                    //cle.printStackTrace(System.err);
                //} catch (Exception e) {
                    //System.err.println("[error] " + MESSAGE + "Exception while placing entry to 'held'");
                    //e.printStackTrace(System.err);
                //} finally {
		    //zooKeeper.close();
		//}
            } else {

            IngestRequest ingestRequest = JSONUtil.populateIngestRequest(jp);

	    ingestRequest.getJob().setJobStatus(JobStatusEnum.CONSUMED);
	    ingestRequest.getJob().setQueuePriority(JSONUtil.getValue(jp,"queuePriority"));
	    Boolean update = new Boolean(jp.getBoolean("update"));
	    ingestRequest.getJob().setUpdateFlag(update.booleanValue());
	    ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			"queue" + FS + ingestRequest.getJob().grabBatchID().getValue() + FS + 
		        ingestRequest.getJob().getJobID().getValue()));
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    //BatchState.putQueuePath(JSONUtil.getValue(jp,"batchID"), ingestRequest.getQueuePath().getAbsolutePath());

            System.out.println(NAME + " --------------- ABOUT TO SUBMIT JOB: " + job.status().name());
	    String process = "Estimate";
	    jobState = ingestService.submitProcess(ingestRequest, process);

            System.out.println(NAME + " =================> Current job state to: " + job.status().name());
	    System.out.println("-----> unlock status " + job.unlock(zooKeeper));
	    if (jobState.getJobStatus() == JobStatusEnum.COMPLETED) {
                if (DEBUG) System.out.println("[item]: EstimateConsumer Daemon COMPLETED queue data:" + jp.toString());
                job.setStatus(zooKeeper, job.status().success(), "Success");
	    } else if (jobState.getJobStatus() == JobStatusEnum.FAILED) {
                System.out.println("[item]: EstimateConsume Daemon - FAILED job message: " + jobState.getJobStatusMessage());
                job.setStatus(zooKeeper, job.status().fail(), jobState.getJobStatusMessage());
	    } else {
		System.out.println("EstimateConsume Daemon - Undetermined STATE: " + jobState.getJobStatus().getValue() + " -- " + jobState.getJobStatusMessage());
	    }
	}	// end of else

        } catch (SessionExpiredException see) {
            see.printStackTrace(System.err);
            System.err.println("[warn] EstimateConsumeData" + MESSAGE + "Session expired.  Attempting to recreate session.");
	    try {
                zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
	    } catch (Exception e) {
                e.printStackTrace(System.err);
                System.out.println("[error] Consuming queue data: Could not recreate session.");
	    }
        } catch (ConnectionLossException cle) {
            cle.printStackTrace(System.err);
            System.err.println("[warn] EstimateConsumeData" + MESSAGE + "Connection loss.  Attempting to reconnect.");
	    try {
                zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
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




class EstimateCleanupDaemon implements Runnable
{

    private static final String NAME = "EstimateCleanupDaemon";
    private static final String MESSAGE = NAME + ": ";

    private String queueConnectionString = null;
    private String queueNode = null;
    private Integer pollingInterval = 15;	// seconds

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;
    public static int sessionTimeout = 360000;


    // Constructor
    public EstimateCleanupDaemon(String queueConnectionString, String queueNode, ServletConfig servletConfig)
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
                    //Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                    Thread.currentThread().sleep(60 * 1000);
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
		    Job job = null;
		    Batch batch = null;

		    // COMPLETED JOBS
/*
                    while (true) {
                        System.out.println(MESSAGE + "Cleaning JOB queue (COMPLETED states): " + queueConnectionString + " " + queueNode);
                        job = null;
                        try {
                           job = Job.acquireJob(zooKeeper, org.cdlib.mrt.zk.JobState.Completed);
			   if (job != null) {
			       System.out.println(NAME + " Found completed job.  Removing: " + job.id() + " - " + job.primaryId());
			       job.delete(zooKeeper);
			   } else {
			       break;
			   }
                        } catch (org.apache.zookeeper.KeeperException ke) {
                           System.out.println(MESSAGE + "Lock exists, someone already acquired data");
                        }
                        System.out.println(MESSAGE + "Cleaning queue (DELETED states): " + queueConnectionString + " " + queueNode);

                        Thread.currentThread().sleep(5 * 1000);		// wait a short amount of time
                    }

		   
		    // DELETED JOBS
                    while (true) {
                        System.out.println(MESSAGE + "Cleaning Job queue (DELETED states): " + queueConnectionString + " " + queueNode);
                        job = null;
                        try {
                           job = Job.acquireJob(zooKeeper, org.cdlib.mrt.zk.JobState.Deleted);
                           if (job != null) {
                               System.out.println(NAME + " Found deleted job.  Removing: " + job.id() + " - " + job.primaryId());
                               job.delete(zooKeeper);
			   } else {
			       break;
			   }
                        } catch (org.apache.zookeeper.KeeperException ke) {
                           System.out.println(MESSAGE + "Lock exists, someone already acquired data");
                        }

                        Thread.currentThread().sleep(5 * 1000);         // wait a short amount of time
                    }
*/

		    // COMPLETED BATCHES
                    while (true) {
                        System.out.println(MESSAGE + "Cleaning Batch queue (Completed states): " + queueConnectionString + " " + queueNode);
                        List<String> batches = null;
                        try {
                           batches = Batch.deleteCompletedBatches(zooKeeper);
			   for (String batchName: batches) {
                               System.out.println(NAME + " Found completed batch.  Removing: " + batchName);
			   } 
                        } catch (org.apache.zookeeper.KeeperException ke) {
                           System.out.println(MESSAGE + "Error removing completed batches: " + ke.toString());
                        }

                        //Thread.currentThread().sleep(5 * 1000);         // wait a short amount of time
			break;
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
