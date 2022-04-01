/*
Copyright (c) 2005-2010, Regents of the University of California
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

import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import java.util.LinkedHashMap;

import org.cdlib.mrt.tools.SSMConfigResolver;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

import org.json.JSONObject;

/**
 *
 * @author mreyes
 */
public class IngestConfig
{
    protected static final String NAME = "IngestConfig";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    
    protected LoggerInf logger = null;
    protected JSONObject loggerConf = null;
    protected JSONObject ingestConf = null;
    protected JSONObject queueConf = null;
    protected JSONObject storeConf = null;
    protected String ingestServicePath = null;
    protected String ingestQueuePath = null;

    // ingest-info
    protected String ingestName = null;
    protected String ingestIdentifier = null;
    protected String ingestTarget = null;
    protected String ingestDescription = null;
    protected String ingestAcessURI = null;
    protected String ingestEZID = null;
    protected String ingestServiceScheme = null;
    protected String ingestSupportURI = null;
    protected String ingestAdmin = null;
    protected String ingestPurl = null;
    protected String ingestLock = null;
    // store-info
    protected String storeStore = null;
    protected String storeAccess = null;
    protected String storeLocalID = null;
    // queue-info
    protected String QueueService = null;
    protected String QueueName = null;
    protected String IngestQNames = null;
    protected String QueueInventoryName = null;
    protected String QueueHoldFile = null;
    protected Integer QueuePollingInterval = null;
    protected Integer QueueNumThreads = null;
    // config-info
    protected Integer messageMaximumLevel = null;
    protected Integer messageMaximumError = null;
    protected String logPath = null;
    protected String logName = null;
    protected String logQualifier = null;
    protected Integer logTrace = null;
    protected String servicePath = null;
    private static class Test{ }; 

    public static IngestConfig useYaml()
        throws TException
    {
        try {
            IngestConfig ingestConfig = new IngestConfig();

            JSONObject jIngInfo = getYamlJson();

            // System.out.println("***getYamlJson:\n" + jIngInfo.toString(3));
	    // Config logger object (config-info)
            JSONObject loggerConf = jIngInfo.getJSONObject("logger-info");
            LoggerInf logger = ingestConfig.setLogger(loggerConf);
            ingestConfig.setLogger(logger);

	    // Ingest config object (ingest-info)
            JSONObject ingestConf = jIngInfo.getJSONObject("ingest-info");
            ingestConfig.setIngestConf(ingestConf);
	    // ingestServicePath var
	    ingestConfig.setServicePath(ingestConf.getString("ingestServicePath"));
	    // ingestQueuePath var
	    try {
	        ingestConfig.setIngestQueuePath(ingestConf.getString("ingestQueuePath"));
	    } catch (org.json.JSONException je) {
	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "ingestQueuePath not set, no EFS shared disk defined.");
	        ingestConfig.setIngestQueuePath(null);
	    }

	    // Store config object (store-info)
            JSONObject storeConf = jIngInfo.getJSONObject("store-info");
            ingestConfig.setStoreConf(storeConf);
	    
	    // Queue config object (queue-info)
            JSONObject queueConf = jIngInfo.getJSONObject("queue-info");
            ingestConfig.setQueueConf(queueConf);

            return ingestConfig;

        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }

    }

    protected static JSONObject getYamlJson()
       throws TException
    {
        try {
            String propName = "resources/ingestConfig.yaml";
            Test test=new Test();
            InputStream propStream =  test.getClass().getClassLoader().getResourceAsStream(propName);
            String ingestYaml = StringUtil.streamToString(propStream, "utf8");
               // System.out.println("ingestYaml:\n" + ingestYaml);
            String ingInfoConfig = getYamlInfo();
            String rootPath = System.getenv("SSM_ROOT_PATH");
            System.out.println("SSM_ROOT_PATH:" + rootPath);
            SSMConfigResolver ssmResolver = new SSMConfigResolver();
            YamlParser yamlParser = new YamlParser(ssmResolver);
            System.out.println("Ingest Table:" + ingInfoConfig);
            System.out.println("Ingest Yaml:\n" + ingestYaml);
            LinkedHashMap<String, Object> map = yamlParser.parseString(ingestYaml);
            LinkedHashMap<String, Object> lmap = (LinkedHashMap<String, Object>)map.get(ingInfoConfig);
            if (lmap == null) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "Unable to locate configuration");
            }
            yamlParser.loadConfigMap(lmap);
            yamlParser.resolveValues();
            return yamlParser.getJson();

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.INVALID_CONFIGURATION(MESSAGE + "Unable to locate configuration");
        }
    }

    protected static String getYamlInfo()
       throws TException
    {
        String ingInfoConfig = System.getenv("which-ingest");
        if (ingInfoConfig == null) {
            ingInfoConfig = System.getenv("MERRITT_INGEST");
        }
        if (ingInfoConfig == null) {
            ingInfoConfig = "ingest-info";
        }
        return ingInfoConfig;
    }



    /**
     * set local logger to node/log/...
     * @param path String path to node
     * @return Node logger
     * @throws Exception process exception
     */
    protected LoggerInf setLogger(JSONObject fileLogger)
        throws Exception
    {
        String qualifier = fileLogger.getString("qualifier");
        String path = fileLogger.getString("path");
        Properties logprop = new Properties();
        logprop.setProperty("fileLogger.message.maximumLevel", "" + fileLogger.getInt("messageMaximumLevel"));
        logprop.setProperty("fileLogger.error.maximumLevel", "" + fileLogger.getInt("messageMaximumError"));
        logprop.setProperty("fileLogger.name", fileLogger.getString("name"));
        logprop.setProperty("fileLogger.trace", "" + fileLogger.getInt("trace"));
        logprop.setProperty("fileLogger.qualifier", fileLogger.getString("qualifier"));
        if (StringUtil.isEmpty(path)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "setCANLog: path not supplied");
        }

        File canFile = new File(path);
        File log = new File(canFile, "logs");
        if (!log.exists()) log.mkdir();
        String logPath = log.getCanonicalPath() + '/';

        if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("LOG", logprop)
            + "\npath:" + path
            + "\nlogpath:" + logPath
        );
        LoggerInf logger = LoggerAbs.getTFileLogger(qualifier, log.getCanonicalPath() + '/', logprop);
        return logger;
    }


    // GETTERS
    public LoggerInf getLogger() {
        return logger;
    }

    public String getServicePath() {
        return servicePath;
    }

    public String getIngestQueuePath() {
        return ingestQueuePath;
    }

    public JSONObject getIngestConf() {
        return ingestConf;
    }

    public JSONObject getLoggerConf() {
        return loggerConf;
    }

    public JSONObject getStoreConf() {
        return storeConf;
    }

    public JSONObject getQueueConf() {
        return queueConf;
    }

    // SETTERS
    public void setLogger(LoggerInf logger) {
        this.logger = logger;
    }

    public void setServicePath(String servicePath) {
        this.servicePath = servicePath;
    }
    
    public void setIngestQueuePath(String ingestQueuePath) {
        this.ingestQueuePath = ingestQueuePath;
    }
    
    public void setIngestConf(JSONObject ingestConf) {
        this.ingestConf = ingestConf;
    }

    public void setLoggerConf(JSONObject loggerConf) {
        this.loggerConf = loggerConf;
    }

    public void setStoreConf(JSONObject storeConf) {
        this.storeConf = storeConf;
    }

    public void setQueueConf(JSONObject queueConf) {
        this.queueConf = queueConf;
    }

}
