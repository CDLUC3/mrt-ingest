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
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.JobStateInf;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.LockState;
import org.cdlib.mrt.ingest.QueueState;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.queue.DistributedLock;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.queue.LockItem;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.ZooCodeUtil;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Basic manager for Queuing Service
 * 
 * @author mreyes
 */
public class QueueManager {

	private static final String NAME = "QueueManager";
	private static final String MESSAGE = NAME + ": ";
	private static final boolean DEBUG = true;
	private LoggerInf logger = null;
	private JSONObject queueConf = null;
	private JSONObject ingestConf = null;
	private String queueConnectionString = null;
	private String queueNode = null;
	private String ingestQNames = null;
	private String ingestLName = null;
	private String inventoryNode = "/inv"; // default
	private String accessSmallNode = "/accessSmall.1"; // hard-coded.  Keep in synv with access code
	private String accessLargeNode = "/accessLarge.1"; // hard-coded.  Keep in synv with access code
	private ArrayList<String> m_admin = new ArrayList<String>(20);

	private boolean debugDump = false;
	private String ingestFileS = null; // prop "IngestService"

	public JSONObject getQueueServiceConf() {
		return queueConf;
	}

	protected QueueManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			this.logger = logger;
			this.queueConf = queueConf;
			this.ingestConf = ingestConf;
			init(queueConf, ingestConf);
		} catch (TException tex) {
			throw tex;
		}
	}

	public static QueueManager getQueueManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			QueueManager queueManager = new QueueManager(logger, queueConf, ingestConf);
			return queueManager;

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + "QueueManager Exception:" + ex;
			logger.logError(msg, LoggerInf.LogLevel.SEVERE);
			logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Initialize the QueueManager
	 * Using a set of Properties identify all storage references.
	 *
	 * @param configs system properties used to resolve Storage references
	 * @throws TException process exceptions
	 */
	public void init(JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			if (queueConf == null) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Queue Config properties not set");
			}
			if (ingestConf == null) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Ingest Config properties not set");
			}

			String key = null;
			String value = null;
			String matchIngest = "ingestServicePath";
			String matchQueueService = "QueueService";
			String matchQueueNode = "QueueName";
			String matchIngestQNames = "IngestQNames";
			String matchIngestLName = "ingestLock";
			String matchInventoryNode = "InventoryName";
			String matchAdmin = "admin";
			String defaultIDKey = "IDDefault";
			Integer storageID = null;

			this.ingestFileS = ingestConf.getString(matchIngest);

			// QueueService - host1:2181,host2:2181
			this.queueConnectionString = queueConf.getString(matchQueueService);
			// QueueName - /ingest.ingest01.1
			this.queueNode = queueConf.getString(matchQueueNode);
			// InventoryName - /mrt.inventory.full
			this.inventoryNode = queueConf.getString(matchInventoryNode);
			// All Ingest Queue Names - "ingest01,ingest02"
			this.ingestQNames = queueConf.getString(matchIngestQNames);
			// All Ingest Lock Names - "zkLock,..."
			this.ingestLName = ingestConf.getString(matchIngestLName);

			// email list
			value = ingestConf.getString(matchAdmin);
			for (String recipient : value.split(";")) {
				m_admin.add((String) recipient);
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

	public IngestServiceState getServiceState() throws TException {
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
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public QueueState getQueueState(String queue) throws TException {
		ZooKeeper zooKeeper = null;
		try {
			QueueState queueState = new QueueState();
			if (queue == null) queue = queueNode;

			// open a single connection to zookeeper for all queue posting
			// todo: create an interface
			zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
			DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

			TreeMap<Long, String> orderedChildren;
			try {
				orderedChildren = distributedQueue.orderedChildren(null);
			} catch (KeeperException.NoNodeException e) {
				orderedChildren = null;
				// throw new NoSuchElementException();
			}
			if ( orderedChildren != null) {
			   for (String headNode : orderedChildren.values()) {
				   String path = String.format("%s/%s", distributedQueue.dir, headNode);
				   try {
				 	byte[] data = zooKeeper.getData(path, false, null);
				   	Item item = Item.fromBytes(data);
					ByteArrayInputStream bis = new ByteArrayInputStream(item.getData());
					ObjectInputStream ois = new ObjectInputStream(bis);
					Properties p = (Properties) ois.readObject();

					QueueEntryState queueEntryState = new QueueEntryState();
					queueEntryState.setDate(item.getTimestamp().toString());
					if (item.getStatus() == Item.PENDING) 
						queueEntryState.setStatus("Pending");
					else if (item.getStatus() == Item.CONSUMED) 
						queueEntryState.setStatus("Consumed");
					else if (item.getStatus() == Item.FAILED) 
						queueEntryState.setStatus("Failed");
					else if (item.getStatus() == Item.COMPLETED) 
						queueEntryState.setStatus("Completed");
					else if (item.getStatus() == Item.DELETED) 
						queueEntryState.setStatus("Deleted");
					queueEntryState.setID(headNode);
					queueEntryState.setJobID(p.getProperty("jobID"));
					queueEntryState.setBatchID(p.getProperty("batchID"));
					queueEntryState.setFileType(p.getProperty("type"));
					queueEntryState.setUser(p.getProperty("submitter"));
					queueEntryState.setProfile(p.getProperty("profile"));
					queueEntryState.setName(p.getProperty("filename"));
					queueEntryState.setObjectCreator(p.getProperty("creator"));
					queueEntryState.setObjectTitle(p.getProperty("title"));
					queueEntryState.setObjectDate(p.getProperty("date"));
					queueEntryState.setLocalID(p.getProperty("localID"));
					queueEntryState.setQueueNode(queue);

					queueState.addEntry(queueEntryState);
				} catch (KeeperException.NoNodeException e) {
					System.out.println("KeeperException.NoNodeException");
					System.out.println(StringUtil.stackTrace(e));
				} catch (Exception ex) { 
					System.out.println("Exception");
					System.out.println(StringUtil.stackTrace(ex));
				}
			    }
			}

			return queueState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			try {
				zooKeeper.close();
			} catch (Exception e) {
			}
		}
	}

        public QueueState getAccessQueueState(String queue) throws TException {
                ZooKeeper zooKeeper = null;
                try {
                        QueueState accessQueueState = new QueueState();

                        // open a single connection to zookeeper for all queue posting
                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

                        TreeMap<Long, String> orderedChildren;
                        try {
                                orderedChildren = distributedQueue.orderedChildren(null);
                        } catch (KeeperException.NoNodeException e) {
                                orderedChildren = null;
                                // throw new NoSuchElementException();
                        }
                        if ( orderedChildren != null) {
                           for (String headNode : orderedChildren.values()) {
                                   String path = String.format("%s/%s", distributedQueue.dir, headNode);
                                   try {
                                        byte[] data = zooKeeper.getData(path, false, null);
                                        Item item = Item.fromBytes(data);
					JSONObject jo = new JSONObject(new String(item.getData()));

                                        QueueEntryState queueEntryState = new QueueEntryState();
                                        queueEntryState.setDate(item.getTimestamp().toString());
                                        if (item.getStatus() == Item.PENDING)
                                                queueEntryState.setStatus("Pending");
                                        else if (item.getStatus() == Item.CONSUMED)
                                                queueEntryState.setStatus("Consumed");
                                        else if (item.getStatus() == Item.FAILED)
                                                queueEntryState.setStatus("Failed");
                                        else if (item.getStatus() == Item.COMPLETED)
                                                queueEntryState.setStatus("Completed");
                                        else if (item.getStatus() == Item.DELETED)
                                                queueEntryState.setStatus("Deleted");
                                        queueEntryState.setQueueNode(queue);
                                        queueEntryState.setID(headNode);
                                        queueEntryState.setToken(jo.getString("token"));
                                        queueEntryState.setCloudContentByte(String.valueOf(jo.getLong("cloud-content-byte")));
                                        queueEntryState.setDeliveryNode(String.valueOf(jo.getLong("delivery-node")));
                                        queueEntryState.setQueueStatus(String.valueOf(jo.getLong("status")));

                                        accessQueueState.addEntry(queueEntryState);
                                } catch (KeeperException.NoNodeException e) {
                                        System.out.println("KeeperException.NoNodeException");
                                        System.out.println(StringUtil.stackTrace(e));
                                } catch (Exception ex) {
                                        System.out.println("Exception");
                                        System.out.println(StringUtil.stackTrace(ex));
                                }
                            }
                        }

                        return accessQueueState;

                } catch (Exception ex) {
                        System.out.println(StringUtil.stackTrace(ex));
                        logger.logError(MESSAGE + "Exception:" + ex, 0);
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }
        }

        public QueueState getInventoryQueueState(String queue) throws TException {
                ZooKeeper zooKeeper = null;
                try {
                        QueueState inventoryQueueState = new QueueState();

                        // open a single connection to zookeeper for all queue posting
                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

                        TreeMap<Long, String> orderedChildren;
                        try {
                                orderedChildren = distributedQueue.orderedChildren(null);
                        } catch (KeeperException.NoNodeException e) {
                                orderedChildren = null;
                                // throw new NoSuchElementException();
                        }
                        if ( orderedChildren != null) {
                           for (String headNode : orderedChildren.values()) {
                                   String path = String.format("%s/%s", distributedQueue.dir, headNode);
                                   try {
                                        byte[] data = zooKeeper.getData(path, false, null);
                                        Item item = Item.fromBytes(data);
					Properties entry = ZooCodeUtil.decodeItem(item.getData());
                                        String manifestURL = entry.getProperty("manifestURL");

                                        QueueEntryState queueEntryState = new QueueEntryState();
                                        queueEntryState.setDate(item.getTimestamp().toString());
                                        if (item.getStatus() == Item.PENDING)
                                                queueEntryState.setStatus("Pending");
                                        else if (item.getStatus() == Item.CONSUMED)
                                                queueEntryState.setStatus("Consumed");
                                        else if (item.getStatus() == Item.FAILED)
                                                queueEntryState.setStatus("Failed");
                                        else if (item.getStatus() == Item.COMPLETED)
                                                queueEntryState.setStatus("Completed");
                                        else if (item.getStatus() == Item.DELETED)
                                                queueEntryState.setStatus("Deleted");
                                        queueEntryState.setManifestURL(manifestURL);
                                        queueEntryState.setQueueNode(queue);
                                        queueEntryState.setID(headNode);

                                        inventoryQueueState.addEntry(queueEntryState);
                                } catch (KeeperException.NoNodeException e) {
                                        System.out.println("KeeperException.NoNodeException");
                                        System.out.println(StringUtil.stackTrace(e));
                                } catch (Exception ex) {
                                        System.out.println("Exception");
                                        System.out.println(StringUtil.stackTrace(ex));
                                }
                            }
                        }

                        return inventoryQueueState;

                } catch (Exception ex) {
                        System.out.println(StringUtil.stackTrace(ex));
                        logger.logError(MESSAGE + "Exception:" + ex, 0);
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }
        }


        public LockState getIngestLockState(String lockNode) throws TException {
                ZooKeeper zooKeeper = null;
                try {
                        LockState ingestLockState = new LockState(); 
			String pathID = null;  // Not needed for tree listing

                        // open a single connection to zookeeper for all lock posting
                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedLock.sessionTimeout, new Ignorer());
                        DistributedLock distributedLock = new DistributedLock(zooKeeper, lockNode, pathID, null);

                        TreeMap<Long, String> orderedChildren;
                        try {
                                orderedChildren = distributedLock.orderedChildren(null);
                        } catch (KeeperException.NoNodeException e) {
                                orderedChildren = null;
                                // throw new NoSuchElementException();
                        }
                        if ( orderedChildren != null) {
                           for (String headNode : orderedChildren.values()) {
                                   String path = String.format("%s/%s", distributedLock.node, headNode);
                                   try {
                                        byte[] data = zooKeeper.getData(path, false, null);
                                        LockItem lockItem = LockItem.fromBytes(data);

                                        LockEntryState lockEntryState = new LockEntryState();
                                        lockEntryState.setDate(lockItem.getTimestamp().toString());
                                        lockEntryState.setJobID(lockItem.getData());
                                        lockEntryState.setID(headNode);

                                        ingestLockState.addEntry(lockEntryState);
                                } catch (KeeperException.NoNodeException e) {
                                        System.out.println("KeeperException.NoNodeException");
                                        System.out.println(StringUtil.stackTrace(e));
                                } catch (Exception ex) {
                                        System.out.println("Exception");
                                        System.out.println(StringUtil.stackTrace(ex));
                                }
                            }
                        }

                        return ingestLockState;

                } catch (Exception ex) {
                        System.out.println(StringUtil.stackTrace(ex));
                        logger.logError(MESSAGE + "Exception:" + ex, 0);
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }
        }


	public IngestLockNameState getIngestLockState() throws TException {
		try {
			IngestLockNameState ingestLockNameState = new IngestLockNameState();
			// comma delimiter if multiple ingest ZK locks
			String[] nodes = ingestLName.split(",");
			for (String node: nodes) {
			   ingestLockNameState.addEntry(node);
			}
			return ingestLockNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public IngestQueueNameState getIngestQueueState() throws TException {
		try {
			IngestQueueNameState ingestQueueNameState = new IngestQueueNameState();
			// comma delimiter if multiple ingest ZK queues
			String[] nodes = ingestQNames.split(",");
			for (String node: nodes) {
			   ingestQueueNameState.addEntry(node);
			}
			return ingestQueueNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public IngestQueueNameState getInventoryQueueState() throws TException {
		try {
			IngestQueueNameState inventoryQueueNameState = new IngestQueueNameState();
			// Assume a single Inventory ZK queue
			inventoryQueueNameState.addEntry(inventoryNode);

			return inventoryQueueNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public IngestQueueNameState getAccessQueueState() throws TException {
		try {
			IngestQueueNameState accessQueueNameState = new IngestQueueNameState();
			// get small and large queue
			accessQueueNameState.addEntry(accessSmallNode);
			accessQueueNameState.addEntry(accessLargeNode);

			return accessQueueNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		}
	}

	public BatchState submit(IngestRequest ingestRequest) throws Exception {
		ProfileState profileState = null;
		try {
			// add service state properties to ingest request
			ingestRequest.setServiceState(getServiceState());

			// assign preliminary batch info
			BatchState batchState = new BatchState();
			batchState.clear();
			batchState.setTargetQueue(queueConnectionString);
			batchState.setTargetQueueNode(queueNode);
			batchState.setTargetInventoryNode(inventoryNode);
			batchState.setUserAgent(ingestRequest.getJob().grabUserAgent());
			batchState.setSubmissionDate(new DateState(DateUtil.getCurrentDate()));
			batchState.setBatchStatus(BatchStatusEnum.QUEUED);
			batchState.setBatchID(ingestRequest.getJob().grabBatchID());
			batchState.setUpdateFlag(ingestRequest.getUpdateFlag());

			// assign profile
			profileState = ProfileUtil.getProfile(ingestRequest.getProfile(),
					ingestFileS + "/profiles"); // two levels down from
																								// home
			if (m_admin != null)
				profileState.setAdmin(m_admin);

			if (DEBUG)
				System.out.println("[debug] " + profileState.dump("profileState"));

			batchState.setBatchProfile(profileState);

			// link for ingest to expose manifest data
			ingestRequest.setLink(this.getServiceState().getAccessServiceURL().toString());

			// The work of posting is done asynchronously to prevent UI timeout
			Post post = new Post(batchState, ingestRequest, profileState);
			Thread postThread = new Thread(post);
			postThread.start();
			// undocumented synchronous request
			if (ingestRequest.getSynchronousMode()) {
				try {
					postThread.join();
					if (DEBUG)
						System.out.println("[debug] Synchronous mode");
				} catch (InterruptedException ignore) {
				}
			}

			return batchState;

		} catch (TException me) {
			throw me;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			if (profileState != null)
				profileState.getQueueHandlers().clear();
		}
	}

        public IngestServiceState postSubmissionAction(String action, String collection) throws TException {
                try {
			String append = "";
			if (StringUtil.isNotEmpty(collection)) append = "_" + collection;
			FileUtilAlt.modifyHoldFile(action, new File(queueConf.getString("QueueHoldFile") + append));

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
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                }
        }

        public QueueEntryState postRequeue(String queue, String id, String fromState) throws TException {
                ZooKeeper zooKeeper = null;
                QueueEntryState queueEntryState = new QueueEntryState();
                try {
	    		Item item = null;
			if ( ! queue.startsWith("/")) queue = "/" + queue;

                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null);
			if (fromState.contains("fail")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Requeue from Fail state: " + queue + ":" + id);
				item = distributedQueue.requeuef(id);
			} else if (fromState.contains("consume")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Requeue from Consume state: " + queue + ":" + id);
				item = distributedQueue.requeue(id);
			} else if (fromState.contains("complete")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Requeue from Complete state: " + queue + ":" + id);
				item = distributedQueue.requeuec(id);
			} else {
	        		System.err.println("[ERROR]" + MESSAGE +  "Requeue input not valid: " + queue + ":" + id);
				return queueEntryState;
			}


                        queueEntryState.setDate(item.getTimestamp().toString());
                        if (item.getStatus() == Item.PENDING)
                        	queueEntryState.setStatus("Pending");
                        else if (item.getStatus() == Item.CONSUMED)
                        	queueEntryState.setStatus("Consumed");
                        else if (item.getStatus() == Item.FAILED)
                        	queueEntryState.setStatus("Failed");
                        else if (item.getStatus() == Item.COMPLETED)
                        	queueEntryState.setStatus("Completed");
                        else if (item.getStatus() == Item.DELETED)
                        	queueEntryState.setStatus("Deleted");
                        queueEntryState.setID(id);
			queueEntryState.setQueueNode(queue);

	    		if (item != null) {
				System.out.println("** [info] ** " + MESSAGE + "Successfully requeued: " + item.toString());
	    		} else {
	        		System.err.println("[error]" + MESSAGE +  "Could not requeue: " + queue + ":" + id);
			}
        	} catch (Exception ex) {
            		System.out.println(StringUtil.stackTrace(ex));
            		logger.logError(MESSAGE + "Exception:" + ex, 0);
            		throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }
        	return queueEntryState;
    	}

        public QueueEntryState postHoldRelease(String action, String queue, String id) throws TException {
                ZooKeeper zooKeeper = null;
                QueueEntryState queueEntryState = new QueueEntryState();
                try {
	    		Item item = null;
			if ( ! queue.startsWith("/")) queue = "/" + queue;

                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null);

			if (action.matches("release")) {
	        	    System.out.println("[INFO]" + MESSAGE +  "Release: " + queue + ":" + id);
			    item = distributedQueue.release(id);
			} else if (action.matches("hold")) {
	        	    System.out.println("[INFO]" + MESSAGE +  "Hold: " + queue + ":" + id);
			    item = distributedQueue.holdPending(id);
			}

                        queueEntryState.setDate(item.getTimestamp().toString());
                        if (item.getStatus() == Item.PENDING)
                        	queueEntryState.setStatus("Pending");
                        if (item.getStatus() == Item.HELD)
                        	queueEntryState.setStatus("Held");
                        queueEntryState.setID(id);
			queueEntryState.setQueueNode(queue);

	    		if (item != null) {
				System.out.println("** [info] ** " + MESSAGE + "Successfully released: " + item.toString());
	    		} else {
	        		System.err.println("[error]" + MESSAGE +  "Could not released: " + queue + ":" + id);
			}
        	} catch (Exception ex) {
            		System.out.println(StringUtil.stackTrace(ex));
            		logger.logError(MESSAGE + "Exception:" + ex, 0);
            		throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }
        	return queueEntryState;
    	}

        public QueueEntryState postDeleteq(String queue, String id, String fromState) throws TException {
                ZooKeeper zooKeeper = null;
                QueueEntryState queueEntryState = new QueueEntryState();
                try {
	    		Item item = null;
			if ( ! queue.startsWith("/")) queue = "/" + queue;

                        zooKeeper = new ZooKeeper(queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null);
			if (fromState.contains("fail")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Delete from Fail state: " + queue + ":" + id);
				item = distributedQueue.deletef(id);
			} else if (fromState.contains("consume")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Delete from Consume state: " + queue + ":" + id);
				item = distributedQueue.delete(id);
			} else if (fromState.contains("complete")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Delete from Complete state: " + queue + ":" + id);
				item = distributedQueue.deletec(id);
			} else if (fromState.contains("pending")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Delete from Pending state: " + queue + ":" + id);
				item = distributedQueue.deletep(id);
			} else {
	        		System.err.println("[ERROR]" + MESSAGE +  "Delete input not valid: " + queue + ":" + id);
				return queueEntryState;
			}


                        queueEntryState.setDate(item.getTimestamp().toString());
                        if (item.getStatus() == Item.PENDING)
                        	queueEntryState.setStatus("Pending");
                        else if (item.getStatus() == Item.CONSUMED)
                        	queueEntryState.setStatus("Consumed");
                        else if (item.getStatus() == Item.FAILED)
                        	queueEntryState.setStatus("Failed");
                        else if (item.getStatus() == Item.COMPLETED)
                        	queueEntryState.setStatus("Completed");
                        else if (item.getStatus() == Item.DELETED)
                        	queueEntryState.setStatus("Deleted");
                        queueEntryState.setID(id);
			queueEntryState.setQueueNode(queue);

	    		if (item != null) {
				System.out.println("** [info] ** " + MESSAGE + "Successfully deleted: " + item.toString());
	    		} else {
	        		System.err.println("[error]" + MESSAGE +  "Could not delete: " + queue + ":" + id);
			}
        	} catch (Exception ex) {
            		System.out.println(StringUtil.stackTrace(ex));
            		logger.logError(MESSAGE + "Exception:" + ex, 0);
            		throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
                }

        	return queueEntryState;
    	}

	protected void setIngestStateProperties(IngestServiceState ingestState) throws TException {
	   try {
		String SERVICENAME = "name";
		String SERVICEID = "identifier";
		String SERVICEDESCRIPTION = "description";
		String SERVICESCHEME = "service-scheme";
		String SERVICECUST = "customer-support";
		String NODESCHEME = "node-scheme";
		String ACCESSURI = "access-uri";
		String SUPPORTURI = "support-uri";
		String MAILHOST = "mail-host";
                String QUEUEHOLDFILE = "QueueHoldFile";

		// name
		String serviceNameS = ingestConf.getString(SERVICENAME);
		if (serviceNameS != null) {
			ingestState.setServiceName(serviceNameS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICENAME + " parameter is not available");
		}

		// identifier
		String serviceIDS = ingestConf.getString(SERVICEID); 
		if (serviceIDS != null) {
			ingestState.setServiceID(SERVICEID);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICEID + " parameter is not available");
		}

		// service-scheme
		String serviceScehmeS = ingestConf.getString(SERVICESCHEME);
		if (serviceScehmeS != null) {
			ingestState.setServiceVersion(serviceScehmeS);
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SERVICESCHEME + " parameter is not available");
		}

		// access-uri
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

		// support-uri
		String supportServiceUrlS = ingestConf.getString(SUPPORTURI);
		if (supportServiceUrlS != null) {
			try {
				ingestState.setSupportServiceURL(new URL(supportServiceUrlS));
			} catch (MalformedURLException muex) {
				throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + "Support-uri parameter is not a valid URL");
			}
		} else {
			throw new TException.INVALID_CONFIGURATION("[error] " + MESSAGE + SUPPORTURI + " parameter is not available");
		}

		// mail-host
		String mailHost = ingestConf.getString(MAILHOST);
		if (mailHost == null) {
			mailHost = "localhost"; // default
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + MAILHOST + " parameter is not available");
			if (DEBUG)
				System.err.println(MESSAGE + "[warn] " + MAILHOST + " using default value: " + mailHost);
		}
		ingestState.setMailHost(mailHost);

                // submission state
                String queueHoldString = queueConf.getString(QUEUEHOLDFILE);
                File queueHoldFile = new File(queueHoldString);
		String onHold = null;
		if (queueHoldFile.exists()) {
		   onHold = "frozen";
		} else {
		   onHold = "thawed";
		}
		ingestState.setSubmissionState(onHold);

                // collection submission state
		File parent = queueHoldFile.getParentFile();
		String regex = queueHoldFile.getName() + "_*";
                String heldCollections = FileUtilAlt.getHeldCollections(parent, regex);
		ingestState.setCollectionSubmissionState(heldCollections);

            } catch (TException me) {
                    throw me;

            } catch (Exception ex) {
                    System.out.println(StringUtil.stackTrace(ex));
                    logger.logError(MESSAGE + "Exception:" + ex, 0);
                    throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
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
		public void process(WatchedEvent event) {
		}
	}

// Need to post (post to queueing service asyncrhonously due to UI timeout)
	class Post implements Runnable {

		private static final String NAME = "Post";
		private static final String MESSAGE = NAME + ": ";
		private BatchState batchState = null;
		private IngestRequest ingestRequest = null;
		private ProfileState profileState = null;
		private TreeMap<Integer, HandlerState> queueHandlers = null;
		private boolean isError = false;

		// Constructor
		public Post(BatchState batchState, IngestRequest ingestRequest, ProfileState profileState) {
			try {
				this.batchState = batchState;
				this.ingestRequest = ingestRequest;
				this.profileState = profileState;
				this.queueHandlers = new TreeMap<Integer, HandlerState>(profileState.getQueueHandlers());
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}

		public void run() {
			try {
				// call appropriate handlers
				SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap()); // thread-safe
				sortedMap = queueHandlers;
				for (Object key : sortedMap.keySet()) {
					String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
					Handler handler = (Handler) createObject(handlerS);
					if (handler == null) {
						throw new TException.INVALID_CONFIGURATION("[error] Could not find queue handler: " + handlerS);
					}
					StateInf stateClass = batchState;
					if (isError && (!handler.getName().equals("HandlerNotification"))) {
						System.out
								.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
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
					if (DEBUG)
						System.out.println("[debug] " + handler.getName() + ": " + handlerResult.getDescription());
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
				System.out.println(
						MESSAGE + "Completion of posting data to queue: " + batchState.getBatchID().getValue());

			} catch (Exception e) {
				System.out.println(MESSAGE + "Exception detected while posting data to queue.");
				e.printStackTrace(System.err);
			} finally {
			}
		}

	}

}
