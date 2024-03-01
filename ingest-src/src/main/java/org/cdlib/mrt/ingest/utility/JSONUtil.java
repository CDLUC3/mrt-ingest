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

import java.net.URL;
import java.util.Iterator;

import java.security.SecureRandom;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

import org.apache.commons.mail.MultiPartEmail;

import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.json.JSONObject;


/**
 * simple json tools and couchDB interface routines
 * @author mreyes
 */
public class JSONUtil
{

    private static final String NAME = "JSONUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String DELIMITER = "\t";
    private LoggerInf logger = null;

    /**
     * create json 
     *
     * @param jsonObject object to update
     * @param key string 
     * @param value json string
     * @return JSON object
     */
    public static JSONObject string2json(String jsonObjectString)
        throws TException
    {
	JSONObject jsonObject = null;
	try {
	    jsonObject = new JSONObject(jsonObjectString);
	} catch (Exception e) { }

	return jsonObject;
    }


    /**
     * read json 
     *
     * @param jsonObject object
     * @return jsonobject as string
     */
    public static String json2string(JSONObject jsonObject)
        throws TException
    {
	
	try {
	    return jsonObject.toString();
	} catch (Exception e) { }

	return null;
    }

    public static String getName() {
        return JSONUtil.class.toString();
    }

    public static String getValue(JSONObject jo, String key) {
	if (jo.isNull(key)) return "";
	try {
           return jo.getString(key);
	} catch (Exception e) {
	   System.err.print("[ERROR] Could not find value in JSONObject: " + key);
	   return null;
	}
    }

    private static String removeNamespaceJobState(JobState jobState) {

	String jobStateString = "";
        FormatterUtil formatterUtil = new FormatterUtil();
	try {

	    jobStateString = formatterUtil.doStateFormatting(jobState, FormatType.json).replaceAll("job:","").
        	    replaceFirst("\"xmlns:job\":\"http://uc3.cdlib.org/ontology/mrt/ingest/job\",","");
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	   formatterUtil = null;
	}

	return jobStateString;
    }

}
