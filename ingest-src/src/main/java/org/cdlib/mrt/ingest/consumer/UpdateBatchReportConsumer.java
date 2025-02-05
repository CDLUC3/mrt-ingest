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
import org.cdlib.mrt.ingest.utility.ZookeeperUtil;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.Job;
import org.cdlib.mrt.zk.ZKKey;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.cdlib.mrt.zk.MerrittJsonKey;
import org.cdlib.mrt.zk.MerrittStateError;
import org.cdlib.mrt.zk.MerrittLocks;

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
import java.util.Iterator;
import java.util.List;

/**
 * Consume Update Batch Report
 * - zookeeper is the defined queueing service
 * 
 */
public class UpdateBatchReportConsumer extends HttpServlet
{

    private static final String NAME = "UpdateBatchReportConsumer";
    private static final String MESSAGE = NAME + ": ";
    private volatile Thread consumerThread = null;
    private volatile Thread cleanupThread = null;

    private String queueConnectionString = "localhost:2181";	// default single server connection
    private String queuePath = null;
    private int numThreads = 5;		// default size
    private int pollingInterval = 15;	// default interval (seconds)

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
		startUpdateBatchReportConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
                System.out.println("[info] " + MESSAGE + "NOT starting Batch Queue cleanup daemon.  Cleanup is performed in final Batch Daemon");
	    	// System.out.println("[info] " + MESSAGE + "starting Update Batch Report cleanup daemon");
		// startCleanupThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not queue cleanup daemon");
        }
    }


    /**
     * Start consumer thread
     */
    private synchronized void startUpdateBatchReportConsumerThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            UpdateBatchReportConsumerDaemon consumerDaemon = new UpdateBatchReportConsumerDaemon(queueConnectionString,
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
                System.out.println("[info] " + MESSAGE + "Update Batch Queue cleanup daemon already started");
                return;
            }

            UpdateBatchReportCleanupDaemon cleanupDaemon = new UpdateBatchReportCleanupDaemon(queueConnectionString, servletConfig);

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

class UpdateBatchReportConsumerDaemon implements Runnable
{
   
    private static final String NAME = "UpdateBatchReportConsumerDaemon";
    private static final String MESSAGE = NAME + ": ";

    private IngestServiceInit ingestServiceInit = null;
    private IngestServiceInf ingestService = null;

    private String queueConnectionString = null;
    private Integer pollingInterval = null;
    private int keepAliveTime = 60;     // when poolSize is exceeded
    private Integer poolSize = null;

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;

    // Constructor
    public UpdateBatchReportConsumerDaemon(String queueConnectionString, ServletConfig servletConfig, 
		Integer pollingInterval, Integer poolSize)
    {
        this.queueConnectionString = queueConnectionString;
	this.pollingInterval = pollingInterval;
	this.poolSize = poolSize;

	try {
            ingestServiceInit = IngestServiceInit.getIngestServiceInit(servletConfig);
            ingestService = ingestServiceInit.getIngestService();
	
            zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());

	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<UpdateBatchReportConsumeData> workQueue = new ArrayBlockingQueue<UpdateBatchReportConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) keepAliveTime, TimeUnit.SECONDS, (BlockingQueue) workQueue);

	// Refresh connection. if necessary
	try {
	    // Test connection
	    zooKeeper.exists("/",false);
	} catch (KeeperException ke) {
	    ke.printStackTrace();
	    System.out.println(MESSAGE + "[WARN] Session expired.  Reconnecting...");
	    try {
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
	    } catch (IOException ioe){}
	} catch (Exception e) {}

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

		    try {
                       Job.initNodes(zooKeeper);
                    } catch (KeeperException ke) {
                       ke.printStackTrace();
                       System.out.println(MESSAGE + "[WARN] Session expired or Connection loss.  Reconnecting...");
                       try {
               		   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                           zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                           Job.initNodes(zooKeeper);
                       } catch (IOException ioe){}
                    } catch (Exception e) {}

		    // To prevent long shutdown, no more than poolsize tasks queued.
		    while (true) {
		        numActiveTasks = executorService.getActiveCount();
			if (numActiveTasks < poolSize) {
			    System.out.println(MESSAGE + "Checking for additional tasks -  Current tasks: " + numActiveTasks + " - Max: " + poolSize);

			    Batch batch = null;
			    try {
			        batch = Batch.acquireUpdateBatchForReporting(zooKeeper);
                            } catch (Exception e) {
                                System.err.println(MESSAGE + "[WARN] error acquiring job: " + e.getMessage());
                                //e.printStackTrace();
                                try {
               			   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                                   if (batch != null) batch.unlock(zooKeeper);
				   break;
                                } catch (Exception e2) {}
                            }

			    if ( batch != null) { 
			    	System.out.println(MESSAGE + "Found reporting batch data: " + batch.id());
                                executorService.execute(new UpdateBatchReportConsumeData(ingestService, batch, queueConnectionString));
                                Thread.currentThread().sleep(5 * 1000);
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
	    try {
		zooKeeper.close();
	    } catch (Exception ze) {}
	}
    }

    // to do: make this a service call
    private boolean onHold()
    {
        try {
            if (MerrittLocks.checkLockIngestQueue(zooKeeper)) {
                System.out.println("[info]" + NAME + ": hold exists, not processing queue.");
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


class UpdateBatchReportConsumeData implements Runnable
{
   
    private static final String NAME = "UpdateBatchReportConsumeData";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private ZooKeeper zooKeeper = null;

    private IngestServiceInf ingestService = null;
    private BatchState batchState = null;
    private Batch batch = null;

    // Constructor
    public UpdateBatchReportConsumeData(IngestServiceInf ingestService, Batch batch, String queueConnectionString)
    {
	this.zooKeeper = zooKeeper;
	this.batch = batch;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
    }

    public void run()
    {
        try {

	    // UTF-8 ??
            JSONObject jp = null;
            JSONObject ji = null;
            zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            try {
	       jp = batch.jsonProperty(zooKeeper, ZKKey.BATCH_SUBMISSION);
            } catch (SessionExpiredException see) {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
	       jp = batch.jsonProperty(zooKeeper, ZKKey.BATCH_SUBMISSION);
            }
            ji = Job.createJobIdentifiers(JSONUtil.getValue(jp,"objectID"), JSONUtil.getValue(jp,"localID"));

            if (DEBUG) System.out.println(NAME + " [info] START: consuming batch report queue " + batch.id() + " - " + jp.toString() + " - " + ji.toString());

            IngestRequest ingestRequest = JSONUtil.populateIngestRequest(jp, ji);

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
	    batchState = ingestService.submitPost(ingestRequest, "Report");

	    if (! batch.hasFailure()) {
	       batch.setStatus(zooKeeper, batch.status().success());
	    } else {
	      batch.setStatus(zooKeeper, batch.status().fail(),"Batch failure");
	    }
	    batch.unlock(zooKeeper);

        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.out.println("[error] Consuming queue data");
        } finally {
	    try {
		zooKeeper.close();
	    } catch(Exception ze) {}
	} 
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }
}


class UpdateBatchReportCleanupDaemon implements Runnable
{

    private static final String NAME = "UpdateBatchReportCleanupDaemon";
    private static final String MESSAGE = NAME + ": ";

    private String queueConnectionString = null;
    private Integer pollingInterval = 3600;	// seconds

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public UpdateBatchReportCleanupDaemon(String queueConnectionString, ServletConfig servletConfig)
    {
        this.queueConnectionString = queueConnectionString;

        try {
            zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
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
		   
*/

		    // COMPLETED BATCHES
                    while (true) {
                        System.out.println(MESSAGE + "Cleaning Batch queue (Completed states): " + queueConnectionString);
                        List<String> batches = null;
                        try {
			   try {
                              batches = Batch.deleteCompletedBatches(zooKeeper);
			   } catch (SessionExpiredException see) {
               		      Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               		      zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                              System.out.println(MESSAGE + "Error removing completed batches, retrying: " + see.getMessage());
                              batches = Batch.deleteCompletedBatches(zooKeeper);
			   }

			   for (String batchName: batches) {
                               System.out.println(NAME + " Found completed batch.  Removing: " + batchName);
			   } 
                        } catch (MerrittStateError mse) {
                           System.out.println(MESSAGE + "Found items but in failed/processing state: " + mse.getMessage());
                        } catch (Exception e) {
			   e.printStackTrace();
                           System.out.println(MESSAGE + "Error removing completed batches: " + e.getMessage());
                        }

                        //Thread.currentThread().sleep(5 * 1000);         // wait a short amount of time
			break;
                    }

		    // DELETED JOBS that are orphaned
/*
                    while (true) {
                        System.out.println(MESSAGE + "Cleaning Job queue (DELETED states): " + queueConnectionString);
                        List<String> jobs = null;
                        try {
                           try {
                              jobs = Batch.deleteDeletedJobs(zooKeeper);
                           } catch (SessionExpiredException see) {
                              zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                              System.out.println(MESSAGE + "Error removing deleted Jobs, retrying: " + see.getMessage());
                              jobs = Batch.deleteDeletedJobs(zooKeeper);
                           }

                           for (String jobName: jobs) {
                               System.out.println(NAME + " Found deleted job.  Removing: " + jobName);
                           }
                        } catch (MerrittStateError mse) {
                           System.out.println(MESSAGE + "Found items but in failed/processing state: " + mse.getMessage());
                        } catch (Exception e) {
                           e.printStackTrace();
                           System.out.println(MESSAGE + "Error removing deleted jobs: " + e.getMessage());
                        }

                        //Thread.currentThread().sleep(5 * 1000);         // wait a short amount of time
                        break;
                    }
*/

                } catch (RejectedExecutionException ree) {
                    System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission");
                } catch (NoSuchElementException nsee) {
                    // no data in queue
                    System.out.println("[info] " + MESSAGE + "No data in queue to clean");
                } catch (IllegalArgumentException iae) {
                    // no queue exists
                } catch (Exception e) {
                    System.err.println("[warn] " + MESSAGE + "General exception.");
                    e.printStackTrace();
                } finally {
		    try {
			zooKeeper.close();
		    } catch (Exception ze) {}
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
