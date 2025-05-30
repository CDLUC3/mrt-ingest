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
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.service.IngestServiceInf;
import org.cdlib.mrt.ingest.app.IngestServiceInit;
import org.cdlib.mrt.ingest.utility.JobStatusEnum;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.ingest.utility.ZookeeperUtil;
import org.cdlib.mrt.ingest.utility.JSONUtil;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.zk.Job;
import org.cdlib.mrt.zk.Batch;
import org.cdlib.mrt.zk.ZKKey;
import org.cdlib.mrt.zk.MerrittJsonKey;
import org.cdlib.mrt.zk.MerrittStateError;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.cdlib.mrt.zk.MerrittLocks;

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

            EstimateConsumerDaemon jobConsumerDaemon = new EstimateConsumerDaemon(queueConnectionString,
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
    private Integer pollingInterval = null;
    private Integer poolSize = null;
    private int keepAliveTime = 60;     // when poolSize is exceeded

    private ZooKeeper zooKeeper = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;


    // Constructor
    public EstimateConsumerDaemon(String queueConnectionString, ServletConfig servletConfig, 
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
                 e.printStackTrace(System.err);
               }
            }

	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<EstimateConsumeData> workQueue = new ArrayBlockingQueue<EstimateConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) keepAliveTime, TimeUnit.SECONDS, (BlockingQueue) workQueue);

        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
           } catch  (Exception e ) {
             e.printStackTrace(System.err);
           }
        }

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

        		    if (! ZookeeperUtil.validateZK(zooKeeper)) {
            		        try {
               		            // Refresh ZK connection
               		            zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
           		        } catch  (Exception e ) {
             		            e.printStackTrace(System.err);
           		        }
        		    }

			    try {
                                job = Job.acquireJob(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating);
                            } catch (Exception e) {
                                System.err.println(MESSAGE + "[WARN] error acquiring job: " + e.getMessage());
                                try {
        	    		   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY); 
               			   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                                } catch (Exception e4) {
                                } finally {
                                   if (job != null) job.unlock(zooKeeper);
                                   break;
                                }

                            }

                            if ( job != null) {
                                System.out.println(MESSAGE + "Found estimating job data: " + job.id());
                                if (job.status() != org.cdlib.mrt.zk.JobState.Estimating) {
                                   System.err.println(MESSAGE + "Job already processed by Estimate Consumer: " + job.id());
                                   try {
                                     job.unlock(zooKeeper);
                                   } catch (Exception el) {}
                                   break;
                                }

			        JSONObject jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
			        String profile = JSONUtil.getValue(jp,"profile");
			        System.out.println("[info]: Checking if profile is held: " + job.id() + " - " + profile);

			        // Check if collection level hold
			        if (onHold(profile)) {
			           try {
				      System.out.println(MESSAGE + "detected collection level hold.  Setting ZK entry state to 'held' state: " + job.id() + " - " + profile);
				      job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Held);
                		      job.unlock(zooKeeper);
				      break;
			           } catch (Exception e) {
				      System.err.println("[error] " + MESSAGE + "Exception while placing entry to 'held': " + job.id());
				      e.printStackTrace(System.err);
			           } finally {
			           }
			        } 

				//try {
            			   // job.setStatusWithPriority(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating, job.priority());
            			   //job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating);
				//} catch (MerrittStateError mse) {
				   //mse.printStackTrace();
            			   // job.setStatusWithPriority(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating, job.priority());
				//}

                                executorService.execute(new EstimateConsumeData(ingestService, job, queueConnectionString));
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
	            //System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission, and requeuing: " + item.getId());
        	    Thread.currentThread().sleep(5 * 1000);         // let thread pool relax a bit
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

    private boolean onHold()
    {
        
        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
           } catch  (Exception e ) {
             e.printStackTrace(System.err);
           }
        }

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

    // Support collection level hold
    private boolean onHold(String collection)
    {

        if (! ZookeeperUtil.validateZK(zooKeeper)) {
            try {
               // Refresh ZK connection
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
           } catch  (Exception e ) {
             e.printStackTrace(System.err);
           }
        }

        try {
            if (StringUtil.isEmpty(collection)) {
                System.out.println("[warn]" + NAME + ": Collection hold check not valid: " + collection);
	        return false;
	    }
            System.out.println("[info]" + NAME + ": Checking for collection hold: " + collection);
	    return MerrittLocks.checkLockCollection(zooKeeper, collection);
        } catch (Exception e) {
            return false;
        }
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
    private ZooKeeper zooKeeper = null;

    private Job job = null;
    private IngestServiceInf ingestService = null;
    private JobState jobState = null;
    private int penalizeForNoContentLength = 10;	// Increase priority if no size data not provided

    // Constructor
    public EstimateConsumeData(IngestServiceInf ingestService, Job job, String queueConnectionString)
    {
        this.zooKeeper = zooKeeper;
	this.job = job;
	this.ingestService = ingestService;

        this.queueConnectionString = queueConnectionString;
    }

    public void run()
    {
        try {

            JSONObject jp = null;
            JSONObject ji = null;
            long spaceNeeded = 0L;
            int priority = 0;

            if (! ZookeeperUtil.validateZK(zooKeeper)) {
                try {
                   // Refresh ZK connection
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               } catch  (Exception e ) {
                 e.printStackTrace(System.err);
               }
            }

	    try {
               jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
               ji = job.jsonProperty(zooKeeper, ZKKey.JOB_IDENTIFIERS);
               spaceNeeded = job.longProperty(zooKeeper, ZKKey.JOB_SPACE_NEEDED);
               priority = job.intProperty(zooKeeper, ZKKey.JOB_PRIORITY);
	    } catch (Exception e) {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
               ji = job.jsonProperty(zooKeeper, ZKKey.JOB_IDENTIFIERS);
               spaceNeeded = job.longProperty(zooKeeper, ZKKey.JOB_SPACE_NEEDED);
               priority = job.intProperty(zooKeeper, ZKKey.JOB_PRIORITY);
	    }
            if (DEBUG) System.out.println(NAME + " [info] START: consuming job queue " + job.id() + " - " + jp.toString() + " - " + ji.toString()
                + " - " + "priority: "  + priority +  " - " + "spaceNeeded: "  + spaceNeeded);

            IngestRequest ingestRequest = JSONUtil.populateIngestRequest(jp, ji, priority, spaceNeeded);

	    ingestRequest.getJob().setJobStatus(JobStatusEnum.CONSUMED);
	    ingestRequest.getJob().setQueuePriority(String.format("%02d", priority));
	    Boolean update = new Boolean(jp.getBoolean("update"));
	    ingestRequest.getJob().setUpdateFlag(update.booleanValue());
            ingestRequest.getJob().setSubmissionSize(spaceNeeded);
	    ingestRequest.setQueuePath(new File(ingestService.getIngestServiceProp() + FS +
			"queue" + FS + ingestRequest.getJob().grabBatchID().getValue() + FS + 
		        ingestRequest.getJob().getJobID().getValue()));
            new File(ingestRequest.getQueuePath(), "system").mkdir();
            new File(ingestRequest.getQueuePath(), "producer").mkdir();

	    //BatchState.putQueuePath(JSONUtil.getValue(jp,"batchID"), ingestRequest.getQueuePath().getAbsolutePath());

	    String process = "Estimate";
            if (FileUtilAlt.quickFailure(ingestRequest.getQueuePath().getParentFile().getParentFile(), process + "_FAIL")) {
                System.out.println("[item]: ProcessConsume Daemon - FAIL file exists: " + ingestRequest.getQueuePath().getParentFile().getParentFile().toString() + "/" + process + "_FAIL");
                System.out.println("[item]: ProcessConsume Daemon - Forcing a failure.");
                job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Failed, "FAIL file exists: " + ingestRequest.getQueuePath().getParentFile().getParentFile() + "/" + process + "_FAIL  -  Forcing a failure.");
                job.unlock(zooKeeper);
                return;
            }

	    jobState = ingestService.submitProcess(ingestRequest, process);


            if (! ZookeeperUtil.validateZK(zooKeeper)) {
                try {
                   // Refresh ZK connection
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               } catch  (Exception e ) {
                 e.printStackTrace(System.err);
               }
            }

	    // Set submission size
	    long submissionSize = jobState.grabSubmissionSize();
            try {
               if (DEBUG) System.out.println(NAME + " [info] Setting Job submission size to: " + job.id() + " - " + submissionSize);
               job.setData(zooKeeper, ZKKey.JOB_SPACE_NEEDED, submissionSize);
            } catch (Exception e) {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               job.setData(zooKeeper, ZKKey.JOB_SPACE_NEEDED, submissionSize);
            }

	    // Alter priority if no content-length provided
	    int newPriority = 0;
	    if (submissionSize  < 0) {
                try {
	    	   priority = job.intProperty(zooKeeper, ZKKey.JOB_PRIORITY);
		   newPriority = priority + penalizeForNoContentLength;
                   if (DEBUG) System.out.println(NAME + " [info] Penalizing for no Content-Length: " + job.id() + " - " + newPriority);
		   job.setStatusWithPriority(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating, newPriority);
                   ingestRequest.getJob().setQueuePriority(Integer.toString(newPriority));
                } catch (Exception e) {
                   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
		   job.setStatusWithPriority(zooKeeper, org.cdlib.mrt.zk.JobState.Estimating, newPriority);
                   ingestRequest.getJob().setQueuePriority(Integer.toString(newPriority));
                }
	    }
	    
            try {
               jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
               ji = job.jsonProperty(zooKeeper, ZKKey.JOB_IDENTIFIERS);
               spaceNeeded = job.longProperty(zooKeeper, ZKKey.JOB_SPACE_NEEDED);
               priority = job.intProperty(zooKeeper, ZKKey.JOB_PRIORITY);
            } catch (Exception e) {
               Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
               zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
               jp = job.jsonProperty(zooKeeper, ZKKey.JOB_CONFIGURATION);
               ji = job.jsonProperty(zooKeeper, ZKKey.JOB_IDENTIFIERS);
               spaceNeeded = job.longProperty(zooKeeper, ZKKey.JOB_SPACE_NEEDED);
               priority = job.intProperty(zooKeeper, ZKKey.JOB_PRIORITY);
            }

	    if (jobState.getJobStatus() == JobStatusEnum.COMPLETED) {
                if (DEBUG) System.out.println("[item]: EstimateConsumer Daemon COMPLETED queue data:" 
			+ jp.toString() + " --- " + ji.toString() + " - " + "priority: "  + priority +  " - " + "spaceNeeded: "  + spaceNeeded);
                try {
                   job.setStatus(zooKeeper, job.status().success(), "Success");
                } catch (MerrittStateError mse) {
                   System.err.println(MESSAGE + "[WARN] error changing job status: " + mse.getMessage());
                   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                   job.setStatus(zooKeeper, job.status().success(), "Success");
                }
	    } else if (jobState.getJobStatus() == JobStatusEnum.FAILED) {
		try {
                   System.out.println("[item]: EstimateConsume Daemon - FAILED job message: " + jobState.getJobStatusMessage());
                   job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Failed, jobState.getJobStatusMessage());
                } catch (Exception see) {
                   System.err.println(MESSAGE + "[WARN] error changing job status: " + see.getMessage());
                   Thread.currentThread().sleep(ZookeeperUtil.SLEEP_ZK_RETRY);
                   zooKeeper = new ZooKeeper(queueConnectionString, ZookeeperUtil.ZK_SESSION_TIMEOUT, new Ignorer());
                   job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Failed, jobState.getJobStatusMessage());
                }

	    } else {
		System.out.println("EstimateConsume Daemon - Undetermined STATE: " + jobState.getJobStatus().getValue() + " -- " + jobState.getJobStatusMessage());
	    }
	    // boolean stat = job.unlock(zooKeeper);

        } catch (SessionExpiredException see) {
            see.printStackTrace(System.err);
	    System.out.println(NAME + "[error] Consuming queue data: Could not recreate session.");
        } catch (ConnectionLossException cle) {
            cle.printStackTrace(System.err);
	    System.out.println(NAME + "[error] Consuming queue data: Could not reconnect.");
        } catch (Exception e) {
            e.printStackTrace(System.err);
            try {
                job.setStatus(zooKeeper, org.cdlib.mrt.zk.JobState.Failed, e.getMessage());
                job.unlock(zooKeeper);
           } catch (Exception ex) { System.out.println("Exception [error] Error failing job: " + job.id());}
           System.out.println("Exception [error] Consuming queue data");
        } finally {
	   try {
		job.unlock(zooKeeper);
	   } catch(Exception ze) {}
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
