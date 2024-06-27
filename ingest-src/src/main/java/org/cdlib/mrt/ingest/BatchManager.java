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

import java.lang.Thread;
import java.util.Properties;
import java.util.TreeMap;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.lang.ArrayIndexOutOfBoundsException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.NoSuchElementException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.ingest.handlers.Handler;
import org.cdlib.mrt.ingest.handlers.HandlerResult;
import org.cdlib.mrt.ingest.utility.FileUtilAlt;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.BatchStatusEnum;
import org.cdlib.mrt.queue.DistributedLock;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.queue.Item;
import org.cdlib.mrt.queue.LockItem;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.ZooCodeUtil;
import org.cdlib.mrt.zk.MerrittLocks;

import org.json.JSONObject;
import org.json.JSONException;

/**
 * Basic manager for Queuing Service
 * 
 * @author mreyes
 */
public class BatchManager {

	private static final String NAME = "BatchManager";
	private static final String MESSAGE = NAME + ": ";
	private static final boolean DEBUG = true;
    	private static int sessionTimeout = 300000;  //5 minutes
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
	private String highPriorityThreshold = null;
	private ArrayList<String> m_admin = new ArrayList<String>(20);
        private String emailContact = null;
        private String emailReplyTo = null;

	private boolean debugDump = false;
	private String ingestFileS = null; // prop "IngestService"

	public JSONObject getQueueServiceConf() {
		return queueConf;
	}

	protected BatchManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			this.logger = logger;
			this.queueConf = queueConf;
			this.ingestConf = ingestConf;
			init(queueConf, ingestConf);
		} catch (TException tex) {
			throw tex;
		}
	}

	public static BatchManager getBatchManager(LoggerInf logger, JSONObject queueConf, JSONObject ingestConf) throws TException {
		try {
			BatchManager batchManager = new BatchManager(logger, queueConf, ingestConf);
			return batchManager;

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + "batchManager Exception:" + ex;
			logger.logError(msg, LoggerInf.LogLevel.SEVERE);
			logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Initialize the BatchManager
	 * Using a set of Properties identify all storage references.
	 *
	 * @param configs system properties used to resolve Storage references
	 * @throws TException process exceptions
	 */
	public void init(JSONObject queueConf, JSONObject ingestConf) throws TException {
		ZooKeeper zooKeeper = null;
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
			String matchIngestLName = "ingestLock";
			String matchInventoryNode = "InventoryName";
			String matchHighPriorityThreshold = "HighPriorityThreshold";
			String matchAdmin = "admin";
                        String matchEmailContact = "mail-contact";
                        String matchEmailReplyTo = "mail-replyto";
			String defaultIDKey = "IDDefault";
			Integer storageID = null;

			this.ingestFileS = ingestConf.getString(matchIngest);

			// QueueService - host1:2181,host2:2181
			this.queueConnectionString = queueConf.getString(matchQueueService);
			// QueueName - /ingest.ingest01.1
			this.queueNode = queueConf.getString(matchQueueNode);
			// InventoryName - /mrt.inventory.full
			this.inventoryNode = queueConf.getString(matchInventoryNode);
			// Priority Threshold
			this.highPriorityThreshold = queueConf.getString(matchHighPriorityThreshold);
			// All Ingest Lock Names - "zkLock,..."
			this.ingestLName = ingestConf.getString(matchIngestLName);

			// email list
			value = ingestConf.getString(matchAdmin);
			for (String recipient : value.split(";")) {
				m_admin.add((String) recipient);
			}
			// email contact
			emailContact = ingestConf.getString(matchEmailContact);
                        System.out.println("[info] " + MESSAGE + "Contact email: " + emailContact);

                        // email reply-to
                        emailReplyTo = ingestConf.getString(matchEmailReplyTo);
                        System.out.println("[info] " + MESSAGE + "Repy To email: " + emailReplyTo);

			// Initialize ZK locks
			zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                        System.out.println("[info] " + MESSAGE + "Initializing Zookeeper Locks");
			MerrittLocks.initLocks(zooKeeper);

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + " Exception:" + ex;
			logger.logError(msg, 3);
			logger.logError(StringUtil.stackTrace(ex), 0);
			throw new TException.GENERAL_EXCEPTION(msg);
                } finally {
                        try {
                                zooKeeper.close();
                        } catch (Exception e) {
                        }
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
		byte[] data = null;
		try {
			QueueState queueState = new QueueState();
			if (queue == null) queue = queueNode;

			// open a single connection to zookeeper for all queue posting
			// todo: create an interface
			zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
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
				 	data = zooKeeper.getData(path, false, null);
				   	Item item = Item.fromBytes(data);
					JSONObject jp = new JSONObject(new String(item.getData(), "UTF-8"));

					QueueEntryState queueEntryState = new QueueEntryState();
					queueEntryState.setDate(item.getTimestamp().toString());
					queueEntryState.setStatus(capFirst(item.getStatusStr()));
					queueEntryState.setID(headNode);
            				if (! jp.isNull("jobID"))
						queueEntryState.setJobID(jp.getString("jobID"));
            				if (! jp.isNull("batchID"))
						queueEntryState.setBatchID(jp.getString("batchID"));
            				if (! jp.isNull("type"))
						queueEntryState.setFileType(jp.getString("type"));
            				if (! jp.isNull("submitter"))
						queueEntryState.setUser(jp.getString("submitter"));
            				if (! jp.isNull("profile"))
						queueEntryState.setProfile(jp.getString("profile"));
            				if (! jp.isNull("filename"))
						queueEntryState.setName(jp.getString("filename"));
            				if (! jp.isNull("creator"))
						queueEntryState.setObjectCreator(jp.getString("creator"));
            				if (! jp.isNull("title"))
						queueEntryState.setObjectTitle(jp.getString("title"));
            				if (! jp.isNull("date"))
						queueEntryState.setObjectDate(jp.getString("date"));
            				if (! jp.isNull("localID"))
						queueEntryState.setLocalID(jp.getString("localID"));
					queueEntryState.setQueueNode(queue);

					queueState.addEntry(queueEntryState);
				} catch (KeeperException.NoNodeException e) {
					System.out.println("KeeperException.NoNodeException");
					System.out.println(StringUtil.stackTrace(e));
				} catch (Exception ex) { 
					System.out.println("Exception");
					System.out.println(StringUtil.stackTrace(ex));
				} finally {
					data = null;
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


	public QueueState postReleaseAll(String queue, String profile) throws TException {
		ZooKeeper zooKeeper = null;
		TreeMap<Long, String> orderedChildren;
                byte[] data = null;
		try {
			QueueState queueState = new QueueState();

			if (queue == null) queue = queueNode;
                        if ( ! queue.startsWith("/")) queue = "/" + queue;

			// open a single connection to zookeeper for all queue posting
			zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
			DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

			try {
				orderedChildren = distributedQueue.orderedChildren(null);
			} catch (KeeperException.NoNodeException e) {
				orderedChildren = null;
				// throw new NoSuchElementException();
			}
			if ( orderedChildren != null) {
			   for (String headNode : orderedChildren.values()) {
				   String path = String.format("%s/%s", distributedQueue.dir, headNode);
				   System.out.println("[INFO]" + MESSAGE + "Checking queue entry: " + path);
				   try {

				 	data = zooKeeper.getData(path, false, null);
				   	Item item = Item.fromBytes(data);
					JSONObject jp = new JSONObject(new String(item.getData(), "UTF-8"));

					QueueEntryState queueEntryState = new QueueEntryState();
					queueEntryState.setDate(item.getTimestamp().toString());
					if (item.getStatus() == Item.HELD && jp.getString("profile").matches(profile)) {
						String id = headNode;
						System.out.println("[INFO]" + MESSAGE + "Release held collection entry: " + id);
						item = distributedQueue.release(id);

                        			queueEntryState.setDate(item.getTimestamp().toString());
                        			if (item.getStatus() == Item.PENDING)
                        				queueEntryState.setStatus("Pending");
                        			queueEntryState.setID(item.getId());
						queueEntryState.setQueueNode(queue);
	    					if (item != null) {
							System.out.println("** [info] ** " + MESSAGE + "Successfully released: " + item.toString());
	    					} else {
	        					System.err.println("[error]" + MESSAGE +  "Could not released: " + queue + ":" + id);
						}
						queueEntryState.setID(headNode);
            					if (! jp.isNull("jobID"))
							queueEntryState.setJobID(jp.getString("jobID"));
            					if (! jp.isNull("batchID"))
							queueEntryState.setBatchID(jp.getString("batchID"));
            					if (! jp.isNull("type"))
							queueEntryState.setFileType(jp.getString("type"));
            					if (! jp.isNull("submitter"))
							queueEntryState.setUser(jp.getString("submitter"));
            					if (! jp.isNull("profile"))
							queueEntryState.setProfile(jp.getString("profile"));
            					if (! jp.isNull("filename"))
							queueEntryState.setName(jp.getString("filename"));
            					if (! jp.isNull("creator"))
							queueEntryState.setObjectCreator(jp.getString("creator"));
            					if (! jp.isNull("title"))
							queueEntryState.setObjectTitle(jp.getString("title"));
            					if (! jp.isNull("date"))
							queueEntryState.setObjectDate(jp.getString("date"));
            					if (! jp.isNull("localID"))
							queueEntryState.setLocalID(jp.getString("localID"));
						queueEntryState.setQueueNode(queue);

						queueState.addEntry(queueEntryState);
					}

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
				orderedChildren = null;
				data = null;
			} catch (Exception e) {
			}
		}
	}

        public QueueState getAccessQueueState(String queue) throws TException {
                ZooKeeper zooKeeper = null;
                TreeMap<Long, String> orderedChildren;
                byte[] data = null;
                try {
                        QueueState accessQueueState = new QueueState();

                        // open a single connection to zookeeper for all queue posting
                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

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
                                        data = zooKeeper.getData(path, false, null);
                                        Item item = Item.fromBytes(data);
					JSONObject jo;
                                        try {
						jo = new JSONObject(new String(item.getData()));
                                        } catch (JSONException jex) {
                                                String msg = "[Error] getAccessQueueState: Request for access queue not valid: " + queue;
                                                System.err.println(msg);
                                                throw new TException.REQUEST_INVALID(MESSAGE + msg);
                                        }

                                        QueueEntryState queueEntryState = new QueueEntryState();
                                        queueEntryState.setDate(item.getTimestamp().toString());
					queueEntryState.setStatus(capFirst(item.getStatusStr()));
                                        queueEntryState.setQueueNode(queue);
                                        queueEntryState.setID(headNode);
                                        queueEntryState.setToken(jo.getString("token"));
                                        queueEntryState.setCloudContentByte(String.valueOf(jo.getLong("cloud-content-byte")));
                                        queueEntryState.setDeliveryNode(String.valueOf(jo.getLong("delivery-node")));
                                        queueEntryState.setQueueStatus(String.valueOf(jo.getLong("status")));

                                        accessQueueState.addEntry(queueEntryState);
                                } catch (TException tex) {
                                        throw tex;
                                } catch (KeeperException.NoNodeException e) {
                                        System.out.println("KeeperException.NoNodeException");
                                        System.out.println(StringUtil.stackTrace(e));
                                } catch (Exception ex) {
                                        System.out.println("Exception");
                                        System.out.println(StringUtil.stackTrace(ex));
                                } finally {
					data = null;
				}
                            }
                        }

                        return accessQueueState;

                } catch (TException me) {
                        throw me;
                } catch (Exception ex) {
                        System.out.println(StringUtil.stackTrace(ex));
                        logger.logError(MESSAGE + "Exception:" + ex, 0);
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
				orderedChildren = null;
                        } catch (Exception e) {
                        }
                }
        }

        public QueueState getInventoryQueueState(String queue) throws TException {
                ZooKeeper zooKeeper = null;
                TreeMap<Long, String> orderedChildren;
                byte[] data = null;
                try {
                        QueueState inventoryQueueState = new QueueState();

                        // open a single connection to zookeeper for all queue posting
                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null); // default priority

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
                                        data = zooKeeper.getData(path, false, null);
                                        Item item = Item.fromBytes(data);
					Properties entry;
					try {
						entry = ZooCodeUtil.decodeItem(item.getData());
                                	} catch (TException tex) {
                                        	String msg = "[Error] getInventoryQueueState: Request for inventory queue not valid: " + queue;
						System.err.println(msg);
                                        	throw new TException.REQUEST_INVALID(MESSAGE + msg);
					}
                                        String manifestURL = entry.getProperty("manifestURL");

                                        QueueEntryState queueEntryState = new QueueEntryState();
                                        queueEntryState.setDate(item.getTimestamp().toString());
					queueEntryState.setStatus(capFirst(item.getStatusStr()));
                                        queueEntryState.setManifestURL(manifestURL);
                                        queueEntryState.setQueueNode(queue);
                                        queueEntryState.setID(headNode);

                                        inventoryQueueState.addEntry(queueEntryState);
                                } catch (TException tex) {
					throw tex;
                                } catch (KeeperException.NoNodeException e) {
                                        System.out.println("KeeperException.NoNodeException");
                                        System.out.println(StringUtil.stackTrace(e));
                                } catch (Exception ex) {
                                        System.out.println("Exception");
                                        System.out.println(StringUtil.stackTrace(ex));
                                } finally {
					data = null;
				}
                            }
                        }

                        return inventoryQueueState;

                } catch (TException me) {
                        throw me;
                } catch (Exception ex) {
                        System.out.println(StringUtil.stackTrace(ex));
                        logger.logError(MESSAGE + "Exception:" + ex, 0);
                        throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
                } finally {
                        try {
                                zooKeeper.close();
                                orderedChildren = null;
                        } catch (Exception e) {
                        }
                }
        }


        public LockState getIngestLockState(String lockNode) throws TException {
                ZooKeeper zooKeeper = null;
                TreeMap<Long, String> orderedChildren;
                byte[] data = null;
                try {
                        LockState ingestLockState = new LockState(); 
			String pathID = null;  // Not needed for tree listing

                        // open a single connection to zookeeper for all lock posting
                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                        DistributedLock distributedLock = new DistributedLock(zooKeeper, lockNode, pathID, null);

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
                                        data = zooKeeper.getData(path, false, null);
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
                                } finally {
					data = null;
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
                                orderedChildren = null;
                        } catch (Exception e) {
                        }
                }
        }


	public IngestLockNameState getIngestLockState() throws TException {
		String[] nodes;
		try {
			IngestLockNameState ingestLockNameState = new IngestLockNameState();
			// comma delimiter if multiple ingest ZK locks
			nodes = ingestLName.split(",");
			for (String node: nodes) {
			   ingestLockNameState.addEntry(node);
			}
			return ingestLockNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			nodes = null;
		}
	}

	public IngestQueueNameState getIngestQueueState() throws TException {
		String[] nodes;
		try {
			IngestQueueNameState ingestQueueNameState = new IngestQueueNameState();
			// comma delimiter if multiple ingest ZK queues
			nodes = queueNode.split(",");
			for (String node: nodes) {
			   ingestQueueNameState.addEntry(node);
			}
			return ingestQueueNameState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
			nodes = null;
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

	public BatchState submit(IngestRequest ingestRequest, String state) throws Exception {
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
			if (highPriorityThreshold != null)
				profileState.setPriorityThreshold(highPriorityThreshold);
			if (emailContact != null)
				profileState.setEmailContact(emailContact);
                        if (emailReplyTo != null)
                                profileState.setEmailReplyTo(emailReplyTo);

			if (DEBUG)
				System.out.println("[debug] " + profileState.dump("profileState"));

			batchState.setBatchProfile(profileState);

			// link for ingest to expose manifest data
			ingestRequest.setLink(this.getServiceState().getAccessServiceURL().toString());

			// The work of posting is done asynchronously to prevent UI timeout
			Post post = new Post(batchState, ingestRequest, profileState, state);
			Thread postThread = new Thread(post);
 			postThread.start();
                        // Async for initial posting of data only
                        //if ( ! state.matches("Process")) {
                                try {
                                        postThread.join();
                                        if (DEBUG) System.out.println(NAME + "[debug] Synchronous mode processing: " + state);
                                } catch (InterruptedException ignore) {
                                }
                        //}

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


        public QueueEntryState postRequeue(String queue, String id, String fromState) throws TException {
                ZooKeeper zooKeeper = null;
                QueueEntryState queueEntryState = new QueueEntryState();
                try {
	    		Item item = null;
			if ( ! queue.startsWith("/")) queue = "/" + queue;

                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
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
			queueEntryState.setStatus(capFirst(item.getStatusStr()));
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

                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
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

                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
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
			} else if (fromState.contains("held")) {
	        		System.out.println("[INFO]" + MESSAGE +  "Delete from Held state: " + queue + ":" + id);
				item = distributedQueue.deleteh(id);
			} else {
	        		System.err.println("[ERROR]" + MESSAGE +  "Delete input not valid: " + queue + ":" + id);
				return queueEntryState;
			}


                        queueEntryState.setDate(item.getTimestamp().toString());
			queueEntryState.setStatus(capFirst(item.getStatusStr()));
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

        public QueueState postCleanupq(String queue) throws TException {
                ZooKeeper zooKeeper = null;
                QueueState queueState = new QueueState();
                try {
	    		Item item = null;
			if ( ! queue.startsWith("/")) queue = "/" + queue;

                        zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());
                        DistributedQueue distributedQueue = new DistributedQueue(zooKeeper, queue, null);

                        System.out.println(MESSAGE + "Cleaning queue (COMPLETED states): " + queueConnectionString + " " + queue);
                        try {
                            distributedQueue.cleanup(Item.COMPLETED);
                        } catch (NoSuchElementException nsee) {
                            // No more data
                        }
                        System.out.println(MESSAGE + "Cleaning queue (DELETED states): " + queueConnectionString + " " + queue);
                        try {
                            distributedQueue.cleanup(Item.DELETED);
                        } catch (NoSuchElementException nsee) {
                                // No more data
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

	protected void setIngestStateProperties(IngestServiceState ingestState) throws TException {
           ZooKeeper zooKeeper = null;
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
                // String QUEUEHOLDFILE = "QueueHoldFile";

                zooKeeper = new ZooKeeper(queueConnectionString, sessionTimeout, new Ignorer());

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

                String onHold = null;
                // submission state
                if (MerrittLocks.checkLockIngestQueue(zooKeeper)) {
                   onHold = "frozen";
                } else {
                   onHold = "thawed";
                }
                ingestState.setSubmissionState(onHold);

                // Collection submission state
                // String heldCollections = MerrittLocks.getHeldCollections(zooKeeper);
                // ingestState.setCollectionSubmissionState(heldCollections);
            } catch (TException me) {
                    throw me;

            } catch (Exception ex) {
                    System.out.println(StringUtil.stackTrace(ex));
                    logger.logError(MESSAGE + "Exception:" + ex, 0);
                    throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            } finally {
		try {
		   zooKeeper.close();
		} catch(Exception ze) {}
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

	public String capFirst(String str) {
		return str.substring(0, 1).toUpperCase() + str.substring(1);
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
		//private TreeMap<Integer, HandlerState> batchHandlers = null;
		private String state = null;
		private boolean isError = false;

		// Constructor
		public Post(BatchState batchState, IngestRequest ingestRequest, ProfileState profileState, String state) {
			try {
				this.batchState = batchState;
				this.ingestRequest = ingestRequest;
				this.profileState = profileState;
				this.state = state;
				//this.batchHandlers = new TreeMap<Integer, HandlerState>(profileState.getBatchHandlers());
				//this.batchHandlers = new TreeMap<Integer, HandlerState>();
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}

		public void run() {
			try {
				// call appropriate handlers
				SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap()); // thread-safe
	                        if (state.matches("Process")) sortedMap = profileState.getBatchProcessHandlers();
                        	if (state.matches("Report")) sortedMap = profileState.getBatchReportHandlers();

				//sortedMap = batchHandlers;
				for (Object key : sortedMap.keySet()) {
					String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
					Handler handler = (Handler) createObject(handlerS);
					if (handler == null) {
						throw new TException.INVALID_CONFIGURATION("[error] Could not find queue handler: " + handlerS);
					}
					StateInf stateClass = batchState;
/*
					if (isError && (!handler.getName().equals("HandlerNotification"))) {
						System.out
								.println("[info]" + MESSAGE + "error detected, skipping handler: " + handler.getName());
						continue;
					}

*/
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
				//BatchState.putBatchReadiness(batchState.getBatchID().getValue(), 1);
				System.out.println(MESSAGE + "Completion of posting data to queue: " + batchState.getBatchID().getValue());

			} catch (Exception e) {
				System.out.println(MESSAGE + "Exception detected while posting data to queue.");
				e.printStackTrace(System.err);
			} finally {
				//batchHandlers = null;
			}
		}

	}

}
