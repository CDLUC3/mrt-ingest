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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private int interruptDelay = 1;     // delay before interrupting daemon^M

    protected static final Logger log4j = LogManager.getLogger();

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
	    log4j.error("[error] " + MESSAGE + "Could not create ingest service in daemon init. ");
	}

	try {
	    queueConnectionString = ingestService.getQueueServiceConf().getString("QueueService");
	    if (StringUtil.isNotEmpty(queueConnectionString)) {
	    	log4j.info("[info] " + MESSAGE + "Setting queue connection string: " + queueConnectionString);
		this.queueConnectionString = queueConnectionString;
	    }
	} catch (Exception e) {
	    log4j.warn("[warn] " + MESSAGE + "Could not set queue connection string: " + queueConnectionString +
		 "  - using default: " + this.queueConnectionString);
	}

	try {
	    queuePath = ingestService.getIngestServiceProp() + "/queue/";
	    if (StringUtil.isNotEmpty(queuePath)) {
	    	log4j.info("[info] " + MESSAGE + "Setting queue path: " + queuePath);
		this.queuePath = queuePath;
	    }
	} catch (Exception e) {
	    log4j.warn("[warn] " + MESSAGE + "Could not set queue path: " + queuePath +
		 "  - using default: " + this.queuePath);
	}


	try {
	    numThreads = ingestService.getQueueServiceConf().getString("BatchNumThreads");
	    if (StringUtil.isNotEmpty(numThreads)) {
	    	log4j.info("[info] " + MESSAGE + "Setting thread pool size: " + numThreads);
		this.numThreads = Integer.valueOf(numThreads);
	    }
	} catch (Exception e) {
	    log4j.warn("[warn] " + MESSAGE + "Could not set thread pool size: " + numThreads + "  - using default: " + this.numThreads);
	}

	try {
	    pollingInterval = ingestService.getQueueServiceConf().getString("BatchPollingInterval");
	    if (StringUtil.isNotEmpty(pollingInterval)) {
	    	log4j.info("[info] " + MESSAGE + "Setting polling interval: " + pollingInterval);
		this.pollingInterval = Integer.valueOf(pollingInterval);
	    }
	} catch (Exception e) {
	    log4j.warn("[warn] " + MESSAGE + "Could not set polling interval: " + pollingInterval + "  - using default: " + this.pollingInterval);
	}

        try {
            // Start the Consumer thread
            if (consumerThread == null) {
	    	log4j.info("[info] " + MESSAGE + "starting consumer daemon");
		startUpdateBatchReportConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
                log4j.info("[info] " + MESSAGE + "NOT starting Batch Queue cleanup daemon.  Cleanup is performed in final Batch Daemon");
	    	// log4j.info("[info] " + MESSAGE + "starting Update Batch Report cleanup daemon");
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
                log4j.warn("[warn] " + MESSAGE + "consumer daemon already started");
                return;
            }

            UpdateBatchReportConsumerDaemon consumerDaemon = new UpdateBatchReportConsumerDaemon(queueConnectionString,
		servletConfig, pollingInterval, numThreads);

            consumerThread =  new Thread(consumerDaemon);
            consumerThread.setDaemon(true);                // Kill thread when servlet dies
            consumerThread.start();

	    log4j.info("[info] " + MESSAGE + "consumer daemon started");

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
                log4j.info("[info] " + MESSAGE + "Update Batch Queue cleanup daemon already started");
                return;
            }

            UpdateBatchReportCleanupDaemon cleanupDaemon = new UpdateBatchReportCleanupDaemon(queueConnectionString, servletConfig);

            cleanupThread =  new Thread(cleanupDaemon);
            cleanupThread.setDaemon(true);                // Kill thread when servlet dies
            cleanupThread.start();

	    log4j.info("[info] " + MESSAGE + "cleanup daemon started");

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
            log4j.info("[info] " + MESSAGE + "destroy() " +   consumerThread.activeCount());
            log4j.info("[info] " + MESSAGE + "Waiting " + interruptDelay + " seconds before interrupt");
            Thread.sleep(interruptDelay * 1000);
            log4j.info("[info] " + MESSAGE + "Wait complete, interrupting daemon");
            consumerThread.interrupt();
        } catch (Exception e) {
	    log4j.error("Exception:" + e, e);
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

    protected static final Logger log4j = LogManager.getLogger();

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
	
            if (! ZookeeperUtil.validateZK(zooKeeper)) {
                try {
                   // Refresh ZK connection
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               } catch  (Exception e ) {
		 log4j.error("Exception:" + e, e);
               }
            }

	} catch (Exception e) {
	    log4j.error("Exception:" + e, e);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<UpdateBatchReportConsumeData> workQueue = new ArrayBlockingQueue<UpdateBatchReportConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) keepAliveTime, TimeUnit.SECONDS, (BlockingQueue) workQueue);

        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            } catch  (Exception e ) {
	       log4j.error("Exception:" + e, e);
            }
        }

        try {
            long queueSize = workQueue.size();
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    //log4j.info(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
                    log4j.debug(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    init = false;
                }


                // Let's check to see if we are on hold
                if (onHold()) {
                    log4j.info(MESSAGE + "detected 'on hold' condition");
                    continue;
                }

                // have we shutdown?
                if (Thread.currentThread().isInterrupted()) {
                    log4j.info(MESSAGE + "interruption detected.");
      		    throw new InterruptedException();
                }

		// Perform some work
		try {
		    long numActiveTasks = 0;

		    try {
                       Job.initNodes(zooKeeper);
                    } catch (KeeperException ke) {
		       log4j.error("Exception:" + ke, ke);
                       log4j.warn(MESSAGE + "[warn] Session expired or Connection loss.  Reconnecting...");
                       try {
               		   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);

            		   if (! ZookeeperUtil.validateZK(zooKeeper)) {
                	       try {
                   		   // Refresh ZK connection
                   		   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               		       } catch  (Exception e ) {
				   log4j.error("Exception:" + e, e);
               		       }
            		   }

                           Job.initNodes(zooKeeper);
                       } catch (Exception ioe){}
                    } catch (Exception e) {}

		    // To prevent long shutdown, no more than poolsize tasks queued.
		    while (true) {
		        numActiveTasks = executorService.getActiveCount();
			if (numActiveTasks < poolSize) {
			    log4j.debug(MESSAGE + "Checking for additional tasks -  Current tasks: " + numActiveTasks + " - Max: " + poolSize);

            		    if (! ZookeeperUtil.validateZK(zooKeeper)) {
                	        try {
                   		    // Refresh ZK connection
                   		    zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               		        } catch  (Exception e ) {
				    log4j.error("Exception:" + e, e);
               		        }
            		    }

			    Batch batch = null;
			    try {
                                if (Thread.currentThread().isInterrupted()) {
                                   log4j.info(MESSAGE + "interruption detected.  Acquiring halted.");
                                } else {
			           batch = Batch.acquireUpdateBatchForReporting(zooKeeper);
				}
                            } catch (Exception e) {
                                log4j.warn(MESSAGE + "[warn] error acquiring job: " + e.getMessage());
                                //log4j.error("Exception:" + e, e);
                                try {
               			   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                                } catch (Exception e4) {
                                } finally {
                                   if (batch != null) batch.unlock(zooKeeper);
                                   break;
                                }
                            }

			    if ( batch != null) { 
			    	log4j.info(MESSAGE + "Found reporting batch data: " + batch.id());
                                executorService.execute(new UpdateBatchReportConsumeData(ingestService, batch, queueConnectionString));
                                Thread.currentThread().sleep(5 * 1000);
			    } else {
				break;
		     	    }

			} else {
			    log4j.debug(MESSAGE + "Work queue is full, NOT checking for additional tasks: " + numActiveTasks + " - Max: " + poolSize);
			    break;
			}
		    }

		} catch (RejectedExecutionException ree) {
        	    //Thread.currentThread().sleep(5 * 1000);         // let thread pool relax a bit
		} catch (NoSuchElementException nsee) {
		    // no data in queue
		    log4j.info("[info] " + MESSAGE + "No data in queue to process");
		} catch (IllegalArgumentException iae) {
		    // no queue exists
		} catch (Exception e) {
		    log4j.error("[error] " + MESSAGE + "General exception.");
		    log4j.error("Exception:" + e, e);
		}
	    }
        } catch (InterruptedException ie) {
            try {

                long numActive = executorService.getActiveCount();
                log4j.info(MESSAGE + "Still active tasks: " + numActive + " -  Forcing failure.");
                executorService.shutdownNow();

            } catch (Exception e) {
		log4j.error("Exception:" + e, e);
            }
	} catch (Exception e) {
            log4j.error(MESSAGE + "Exception detected, shutting down consumer daemon.");
	    log4j.error("Exception:" + e, e);
	    executorService.shutdown();
        } finally {
	    try {
		zooKeeper.close();
		zooKeeper = null;
	    } catch (Exception ze) {}
	}
    }

    // to do: make this a service call
    private boolean onHold()
    {
        
        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            } catch  (Exception e ) {
	       log4j.error("Exception:" + e, e);
            }
        }

        try {
            if (MerrittLocks.checkLockIngestQueue(zooKeeper)) {
                log4j.info("[info]" + NAME + ": hold exists, not processing queue.");
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
               log4j.error("Disconnected: " + event.toString());
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

    protected static final Logger log4j = LogManager.getLogger();

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
            // JSONObject jpr = new JSONObject();

            if (! ZookeeperUtil.validateZK(zooKeeper)) {
                try {
                   // Refresh ZK connection
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               } catch  (Exception e ) {
		 log4j.error("Exception:" + e, e);
               }
            }

            try {
	       jp = batch.jsonProperty(zooKeeper, ZKKey.BATCH_SUBMISSION);
            } catch (Exception e) {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
	       jp = batch.jsonProperty(zooKeeper, ZKKey.BATCH_SUBMISSION);
            }
            ji = Job.createJobIdentifiers(JSONUtil.getValue(jp,"objectID"), JSONUtil.getValue(jp,"localID"));
            // jpr = jpr.put(ZKKey.JOB_PRIORITY.key(), 0); // Not defined at batch level

            log4j.info(NAME + " [info] START: consuming batch queue " + batch.id() + " - "
                + jp.toString() + " - " + ji.toString());
                // + jp.toString() + " - " + ji.toString() + " - " + jpr.toString());


            IngestRequest ingestRequest = JSONUtil.populateIngestRequest(jp, ji, 0, 0L);

	    ingestRequest.getJob().setJobStatus(JobStatusEnum.CONSUMED);
	    // ingestRequest.getJob().setQueuePriority(JSONUtil.getValue(jp,"queuePriority"));
	    Boolean update = Boolean.valueOf(jp.getBoolean("update"));
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
	    log4j.error("Exception:" + e, e);
            log4j.error("[error] Consuming queue data");
        } finally {
	    try {
		zooKeeper.close();
		zooKeeper = null;
	    } catch(Exception ze) {}
	} 
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               log4j.error("Disconnected: " + event.toString());
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

    protected static final Logger log4j = LogManager.getLogger();

    // Constructor
    public UpdateBatchReportCleanupDaemon(String queueConnectionString, ServletConfig servletConfig)
    {
        this.queueConnectionString = queueConnectionString;

        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            } catch  (Exception e ) {
	       log4j.error("Exception:" + e, e);
            }
        }

    }



    public void run()
    {
        boolean init = true;
        String status = null;


        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
            } catch  (Exception e ) {
	       log4j.error("Exception:" + e, e);
            }
        }

        sessionID = zooKeeper.getSessionId();
        log4j.info("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
        sessionAuth = zooKeeper.getSessionPasswd();

        try {
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    log4j.debug(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
                } else {
                    log4j.debug(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    init = false;
                }

                // have we shutdown?
                if (Thread.currentThread().isInterrupted()) {
                    log4j.info(MESSAGE + "interruption detected.");
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
                        log4j.info(MESSAGE + "Cleaning JOB queue (COMPLETED states): " + queueConnectionString + " " + queueNode);
                        job = null;
                        try {
                           job = Job.acquireJob(zooKeeper, org.cdlib.mrt.zk.JobState.Completed);
			   if (job != null) {
			       log4j.info(NAME + " Found completed job.  Removing: " + job.id() + " - " + job.primaryId());
			       job.delete(zooKeeper);
			   } else {
			       break;
			   }
                        } catch (org.apache.zookeeper.KeeperException ke) {
                           log4j.info(MESSAGE + "Lock exists, someone already acquired data");
                        }
                        log4j.info(MESSAGE + "Cleaning queue (DELETED states): " + queueConnectionString + " " + queueNode);

                        Thread.currentThread().sleep(5 * 1000);		// wait a short amount of time
                    }
		   
*/

		    // COMPLETED BATCHES
                    while (true) {
                        log4j.info(MESSAGE + "Cleaning Batch queue (Completed states): " + queueConnectionString);
                        List<String> batches = null;

            		if (! ZookeeperUtil.validateZK(zooKeeper)) {
                	    try {
                   		// Refresh ZK connection
                   		zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               		    } catch  (Exception e ) {
				log4j.error("Exception:" + e, e);
               		    }
            		}

                        try {
			   try {
                              batches = Batch.deleteCompletedBatches(zooKeeper);
			   } catch (Exception e) {
               		      Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               		      zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                              log4j.warn(MESSAGE + "Error removing completed batches, retrying: " + e.getMessage());
                              batches = Batch.deleteCompletedBatches(zooKeeper);
			   }

			   for (String batchName: batches) {
                               log4j.info(NAME + " Found completed batch.  Removing: " + batchName);
			   } 
                        } catch (MerrittStateError mse) {
                           log4j.error(MESSAGE + "Found items but in failed/processing state: " + mse.getMessage());
                        } catch (Exception e) {
			   log4j.error("Exception:" + e, e);
                           log4j.error(MESSAGE + "Error removing completed batches: " + e.getMessage());
                        }

                        //Thread.currentThread().sleep(5 * 1000);         // wait a short amount of time
			break;
                    }

                } catch (RejectedExecutionException ree) {
                    log4j.info("[info] " + MESSAGE + "Thread pool limit reached. no submission");
                } catch (NoSuchElementException nsee) {
                    // no data in queue
                    log4j.info("[info] " + MESSAGE + "No data in queue to clean");
                } catch (IllegalArgumentException iae) {
                    // no queue exists
                } catch (Exception e) {
                    log4j.error("[error] " + MESSAGE + "General exception.");
		    log4j.error("Exception:" + e, e);
                } finally {
		    try {
			zooKeeper.close();
		    } catch (Exception ze) {}
		}
            }
        } catch (Exception e) {
            log4j.info(MESSAGE + "Exception detected, shutting down cleanup daemon.");
	    log4j.error("Exception:" + e, e);
        } finally {
	    sessionAuth = null;
		    try {
			zooKeeper.close();
		    } catch (Exception ze) {}
        }
    }


   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               log4j.error("Disconnected: " + event.toString());
       }
   }

}
