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
package org.cdlib.mrt.ingest.handlers;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.cdlib.mrt.ingest.HandlerState;
import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * create manifest based on current queue path
 * @author mreyes
 */
public class HandlerDocument extends Handler<JobState>
{

    private static final String NAME = "HandlerDocument";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private LoggerInf logger = null;
    private Properties conf = null;
    private Integer defaultStorage = null;

    /**
     * document handlers used in processing
     *
     * @param profileState target storage service info
     * @param ingestRequest ingest request info
     * @param jobState 
     * @return HandlerResult result in documenting
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	String value = "";

	try {

            // get all handlers
            SortedMap sortedMap = Collections.synchronizedSortedMap(new TreeMap());    // thread-safe
            sortedMap = profileState.getIngestHandlers();
            for (Object key : sortedMap.keySet()) {
		if (StringUtil.isNotEmpty(value)) value += ",";
                String handlerS = ((HandlerState) sortedMap.get((Integer) key)).getHandlerName();
                Handler handler = (Handler) createObject(handlerS);

		// String version = handler.getVersion();	// NOT YET SUPPORTED
		String version = "not-available";

		value += handlerS + "/" + version;
	    }



            // metadata file in ANVL format
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File ingestFile = new File(systemTargetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, value)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to append metadata file: " + ingestFile.getAbsolutePath());
            }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " documented handlers", 0);
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "documenting handlers: " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }
    }

    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param handlers list of handlers used in processing this object
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String handlers)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("handlers", handlers);

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
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

    public String getName() {
	return NAME;
    }

}
