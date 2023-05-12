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
package org.cdlib.mrt.ingest.utility;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.ManifestXML;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;
/**
 * Storage wrapper methods
 * @author mreyes
 */
public class StorageUtil
{

    private static final String NAME = "StorageUtil";
    private static final String MESSAGE = NAME + ": ";
    private LoggerInf logger = null;
    private static final boolean DEBUG = true;
    private static StoreNode storeNode = null;
    public static final int STORAGE_READ_TIMEOUT_SHORT = (15 * 60 * 1000); 		// 15 minutes for short requests
    public static final int STORAGE_CONNECT_TIMEOUT_SHORT = (15 * 60 * 1000);		// 15 minutes for short requests
    public static final int STORAGE_READ_TIMEOUT = (48 * 60 * 60 * 1000); 		// 48 hours for long requests
    public static final int STORAGE_CONNECT_TIMEOUT = (48 * 60 * 60 * 1000);		// 48 hours for long requests


    public static String getStorageManifest(ProfileState profileState, String objectID)
        throws TException
    {

        try {
	    String manifestString = getStorageString(profileState, objectID, null);
	    if (manifestString == null) 
		return null;
	    else 
		return manifestString;

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in accessing Storage Manifest - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("Error in accessing Storage Manifest");
	}
    }


    public static File getStorageFile(ProfileState profileState, String objectID, String filename)
        throws TException
    {
        try {
	    File tempFile = File.createTempFile(objectID + filename, "txt");
	    String s = getStorageString(profileState, objectID, filename);
	    if (s == null) return tempFile;	// No existing data 

	    FileUtil.string2File(tempFile, getStorageString(profileState, objectID, filename));
	    return tempFile;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in accessing Storage file - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("Error in accessing Storage file");
	}
    }

    public static String getStorageString(ProfileState profileState, String objectID, String filename)
        throws TException
    {
       HttpResponse clientResponse = null;
       String storageURL = null;

        try {
	    String type = "/content/";
            storeNode = profileState.getTargetStorage();

	    // assume we want a manifest
	    if (StringUtil.isEmpty(filename)) {
		type = "/manifest/";
		filename = "";
	    } else {
		// version and filename
		filename = "/0/" + URLEncoder.encode(filename, "UTF-8");
	    }

            // build REST url 
            storageURL = storeNode.getStorageLink().toString() + type + storeNode.getNodeID() + "/" + URLEncoder.encode(objectID, "UTF-8") + filename;

            HttpClient httpClient = HTTPUtil.getHttpClient(storageURL, StorageUtil.STORAGE_READ_TIMEOUT_SHORT);
            HttpGet httpget = new HttpGet(storageURL);
            httpget.setHeader("Content-Type", MediaType.APPLICATION_FORM_URLENCODED);
            if (DEBUG) System.out.println("[debug] " + MESSAGE + " storage URL: " + storageURL);

            // make service request
            try {
            	clientResponse = httpClient.execute(httpget);
            } catch (Exception e) {
		e.printStackTrace();
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + storageURL);
            }

            int responseCode = clientResponse.getStatusLine().getStatusCode();
            String responseMessage = clientResponse.getStatusLine().getReasonPhrase();
            String responseBody = StringUtil.streamToString(clientResponse.getEntity().getContent(), "UTF-8");
            if (DEBUG) System.out.println("[debug] " + MESSAGE + " response code " + responseCode);

            if (responseCode == 404) return null;
            if (responseCode != 200) {
                try {
                    // most likely exception
                    // can only call once, as stream is not reset
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(responseMessage);
                } catch (TException te) {
                    throw te;
                } catch (Exception e) {
                    // let's report something
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("[error] " + NAME + ": storage service: " + storageURL);
                }
            }

            if (DEBUG) System.out.println("[debug] " + MESSAGE + " storage URL response: " + responseMessage);
	    return responseBody;

	} catch (TException tex) {
	    throw tex;
	} catch (Exception ex) {
            System.out.println(StringUtil.stackTrace(ex));
            String err = MESSAGE + "error in accessing Storage file - Exception:" + ex;

            throw new TException.GENERAL_EXCEPTION("Error in accessing Storage file");
	}
    }


    public static VersionMap getVersionMap(String objectID, String storageManifest)
        throws TException
    {
       InputStream inputstream = null;
       Identifier identifier = null;

        try {
            inputstream = new ByteArrayInputStream(storageManifest.getBytes());
            identifier = new Identifier(objectID);

            return ManifestXML.getVersionMap(identifier, null, inputstream);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create version map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
                identifier = null;
                inputstream = null;
            } catch (Exception e) {}
        }
    }


    public static List<File> getCurrentFiles(VersionMap versionMap, int version)
        throws TException
    {
       List<File> files = null;
       List<FileComponent> fileComponents = null;

        try {
            fileComponents = versionMap.getVersionComponents(version);

            for (FileComponent fileComponent: fileComponents)
                files.add(fileComponent.getComponentFile());

            return files;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create version map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
            } catch (Exception e) {}
        }
    }

}
