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
import java.util.Date;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.log4j.Logger;

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

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Basic manager for Admin Service
 * 
 * @author mreyes
 */
public class AdminManager {

	private static final String NAME = "AdminManager";
	private static final String MESSAGE = NAME + ": ";
	private static final boolean DEBUG = true;
	private LoggerInf logger = null;
	private JSONObject ingestConf = null;
	private ArrayList<String> m_admin = new ArrayList<String>(20);

	private boolean debugDump = false;
	private String ingestFileS = null; // prop "IngestService"

	protected AdminManager(LoggerInf logger, JSONObject ingestConf) throws TException {
		try {
			this.logger = logger;
			this.ingestConf = ingestConf;
			init(ingestConf);
		} catch (TException tex) {
			throw tex;
		}
	}

	public static AdminManager getAdminManager(LoggerInf logger, JSONObject ingestConf) throws TException {
		try {
			AdminManager adminManager = new AdminManager(logger, ingestConf);
			return adminManager;

		} catch (TException tex) {
			throw tex;
		} catch (Exception ex) {
			String msg = MESSAGE + "AdminManager Exception:" + ex;
			logger.logError(msg, LoggerInf.LogLevel.SEVERE);
			logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), LoggerInf.LogLevel.DEBUG);
			throw new TException.GENERAL_EXCEPTION(msg);
		}
	}

	/**
	 * Initialize the AdminManager
	 * Using a set of Properties identify all storage references.
	 *
	 * @param configs system properties used to resolve Storage references
	 * @throws TException process exceptions
	 */
	public void init(JSONObject ingestConf) throws TException {
		try {
			if (ingestConf == null) {
				throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Ingest Config properties not set");
			}

			String key = null;
			String value = null;
			String matchIngest = "ingestServicePath";
			String matchAdmin = "admin";
			String defaultIDKey = "IDDefault";
			Integer storageID = null;

			this.ingestFileS = ingestConf.getString(matchIngest);

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

	public ProfileState getProfileState(String profile) throws TException {
		try {
			ProfileState profileState = new ProfileState();
			Identifier profileID = new Identifier(profile, Identifier.Namespace.Local);
			profileState = ProfileUtil.getProfile(profileID, ingestFileS + "/profiles");

			return profileState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public ProfilesState getProfilesState() throws TException {
		try {
			ProfilesState profilesState = new ProfilesState();
			profilesState = ProfileUtil.getProfiles(ingestFileS + "/profiles");

			return profilesState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public ProfilesFullState getProfilesFullState() throws TException {
		try {
			ProfilesFullState profilesFullState = new ProfilesFullState();
			profilesFullState = ProfileUtil.getProfilesFull(ingestFileS + "/profiles");

			return profilesFullState;

		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public BatchFileState getBatchFileState(String batchID) throws TException {
		try {
			BatchFileState batchFileState = new BatchFileState();

			File batchDir = new File(ingestConf.getString("ingestServicePath") + "/queue/" + batchID);
			if ( ! batchDir.isDirectory()) { 
                    	    throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + ": Unable to find Batch directory: " + batchDir);
			}
		
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

                	File[] files = batchDir.listFiles();
                	for (File file: files) {
			   String filename = file.getName();

			   // Add jobs within batch
			   if (file.isDirectory() && filename.startsWith("jid")) {
				Date date = new Date(file.lastModified());
				//batchFileState.addBatchFile(filename, dateFormat.parse(date.toString()).toString());
				batchFileState.addBatchFile(filename, dateFormat.format(date));
			   // Add manifest if present
			   } else if (file.isFile() && filename.endsWith(".checkm")) {
				batchFileState.setBatchManifestName(filename);
				batchFileState.setBatchManifestData(FileUtil.file2String(file));
			   }
			}				

			return batchFileState;
                } catch (TException tex) {
                        throw tex;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public BatchFileState getQueueFileState(Integer days) throws TException {
		try {
			BatchFileState batchFileState = new BatchFileState();

			File queueDir = new File(ingestConf.getString("ingestServicePath") + "/queue" );
			if ( ! queueDir.isDirectory()) { 
                    	    throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + ": Unable to find Queue directory: " + queueDir);
			}
		
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
			long daysMilli = days.longValue() * 86400 * 1000;
			long nowMilli = System.currentTimeMillis();

                	File[] files = queueDir.listFiles();
                	for (File file: files) {
			   String filename = file.getName();

			   // filter data
			   if (! file.isDirectory()) continue; 
			   if (! filename.startsWith("bid")) continue; 
			   if (file.lastModified() <= (nowMilli - daysMilli)) continue;

			   Date date = new Date(file.lastModified());
			   batchFileState.addBatchFile(filename, dateFormat.format(date));
			}				

			return batchFileState;
                } catch (TException tex) {
                        throw tex;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public JobFileState getJobFileState(String batchID, String jobID) throws TException {
		try {
			JobFileState jobFileState = new JobFileState();

			// Fixed location of ERC file
			File jobFile = new File(ingestConf.getString("ingestServicePath") + "/queue/" + batchID + "/" + jobID + "/system/mrt-erc.txt");
			if ( ! jobFile.exists()) { 
                    	    throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + ": Unable to find Job file: " + jobFile.getAbsolutePath());
			}
		
                	String[] lines = FileUtil.getLinesFromFile(jobFile);
			boolean primaryID = true;
                	for (String line: lines) {
			   String parts[] = line.split(":", 2);

			   String key = parts[0];
			   String value = parts[1];
			   if (key.startsWith("erc")) continue;

			   if (key.startsWith("where")) {
				// Primary ID is alsways listed first
				// All subsequent entries are local IDs
				if (primaryID) {
				   key += "-primary";
			           primaryID = false;
				} else {
				   key += "-local";
				}
			   } 
			   jobFileState.addEntry(key, value);
			}				

			return jobFileState;
                } catch (TException tex) {
                        throw tex;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public BatchFileState getJobViewState(String batchID, String jobID) throws TException {
		try {
			BatchFileState batchFileState = new BatchFileState();
			Vector<File> jobFiles = new Vector<File>();

			// Fixed location of ERC file
			File jobDir = new File(ingestConf.getString("ingestServicePath") + "/queue/" + batchID + "/" + jobID);
			if ( ! jobDir.exists()) { 
                    	    throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + ": Unable to find Job file: " + jobDir.getAbsolutePath());
			}

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
			FileUtil.getDirectoryFiles(jobDir, jobFiles);
                	for (File file: jobFiles.toArray(new File[0])) {
			   if (file.isDirectory()) continue;

                           Date date = new Date(file.lastModified());
                           batchFileState.addBatchFile(file.getAbsolutePath(), dateFormat.format(date));
			}				

			return batchFileState;
                } catch (TException tex) {
                        throw tex;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
	}

	public ManifestsState getJobManifestState(String batchID, String jobID) throws TException {
		try {

			// Fixed location of Manifest file
			File jobFile = new File(ingestConf.getString("ingestServicePath") + "/queue/" + batchID + "/" + jobID + "/system/mrt-manifest.txt");
			if ( ! jobFile.exists()) { 
                    	    throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + ": Unable to find Job manifest: " + jobFile.getAbsolutePath());
			}
		
			ManifestsState manifestsState = new ManifestsState();

                	String[] lines = FileUtil.getLinesFromFile(jobFile);
			// File URL | sha256 | 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 | 4 |  | producer/TDR Acc+H2O.md | text/x-web-markdown
			for (String line: lines) {
			   // skip headers and footers
			   if (! line.startsWith("http:")) continue;
			   String parts[] = line.split("\\|", 7);
			
			   // skip system files
			   if ( parts[5].contains("system/")) continue;

			   ManifestEntryState manifestEntryState = new ManifestEntryState();
			   manifestEntryState.setFileName(parts[5]);
			   manifestEntryState.setFileSize(parts[3]);
			   manifestEntryState.setHashAlgorithm(parts[1]);
			   manifestEntryState.setHashValue(parts[2]);
			   manifestEntryState.setMimeType(parts[6]);

			   manifestsState.addManifestInstance(manifestEntryState);
			}				

			return manifestsState;
                } catch (TException tex) {
                        throw tex;
		} catch (Exception ex) {
			System.out.println(StringUtil.stackTrace(ex));
			logger.logError(MESSAGE + "Exception:" + ex, 0);
			throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
		} finally {
		}
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

            } catch (TException me) {
                    throw me;

            } catch (Exception ex) {
                    System.out.println(StringUtil.stackTrace(ex));
                    logger.logError(MESSAGE + "Exception:" + ex, 0);
                    throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
            }
	}


}
