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

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.vocabulary.*;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.ResourceMapUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * move package to staging area "producer" directory
 * @author mreyes
 */
public class HandlerAccept extends Handler<JobState>
{

    private static final String NAME = "HandlerAccept";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String FS = System.getProperty("file.separator");
    private LoggerInf logger = null;
    private Properties conf = null;

    /**
     * copy package to staging area
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	try {

	    boolean result;
	    File sourceDir = ingestRequest.getQueuePath();
	    File targetDir = new File(ingestRequest.getQueuePath(), "producer");
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            PackageTypeEnum packageType = ingestRequest.getPackageType();

	    for (String fileS : ingestRequest.getQueuePath().list()) {
	    	if ( ! isComponent(fileS)) continue;

	        if (DEBUG) System.out.println("[debug] " + MESSAGE + "copying file to stage dir: " + fileS);
		File sourceFile = new File(sourceDir, fileS);
		File targetFile = new File(targetDir, fileS);
		if (sourceFile.isHidden()) continue;		// process after disaggregate
		FileUtil.copyFile(sourceFile.getName(), sourceDir, targetDir);

		sourceFile.delete();
		if (! targetFile.exists()) {
	            if (DEBUG) System.out.println("[error] " + MESSAGE + "unable to copying file to stage dir: " + fileS);
	    	    // return new HandlerResult(false, "[error]: " + MESSAGE + "unable to copying file to stage dir: " + fileS);
		    throw new TException.REQUESTED_ITEM_NOT_FOUND("[error] " 
			+ MESSAGE + ": unable to copying file to stage dir: " + fileS);
		}
	    }

            // update resource map if necessary
            if (packageType == PackageTypeEnum.file) {
                System.out.println("[info] " + MESSAGE + "file parm specified, updating resource map.");
                File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");
                if ( ! updateResourceMap(profileState, ingestRequest, mapFile, targetDir)) {
                    System.err.println("[warn] " + MESSAGE + "Failure to update resource map.");
                }
	    }

	    return new HandlerResult(true, "SUCCESS: " + NAME + " has copied data to staging area", 0);
	} catch (TException te) {
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
	    String msg = "[error] " + MESSAGE + "copying file to staging directory: " + e.getMessage();
            return new HandlerResult(false, msg);
	} finally {
	    // cleanup?
	}
    }

    /**
     * write aggregates references to resource map
     *
     * @param profileState profile state
     * @param ingestRequest ingest request
     * @param resourceMapFile target file (usually "mrt-object-map.ttl")
     * @param sourceDir source directory 
     * @return successful in updating resource map
     */
    private boolean updateResourceMap(ProfileState profileState, IngestRequest ingestRequest, File mapFile, File sourceDir)
        throws TException {
        try {
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath() + " - " + sourceDir.getAbsolutePath());

            Model model = updateModel(profileState, ingestRequest, mapFile, sourceDir);
            if (DEBUG) ResourceMapUtil.dumpModel(model);
            ResourceMapUtil.writeModel(model, mapFile);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to create resource map: " + e.getMessage();
            System.err.println(msg);
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
        }
    }
   
    public Model updateModel(ProfileState profileState, IngestRequest ingestRequest, File mapFile, File sourceDir)
        throws Exception
    {
        try {

            // read in existing model
            InputStream inputStream = FileManager.get().open(mapFile.getAbsolutePath());
            if (inputStream == null) {
                String msg = "[error] " + MESSAGE + "failed to update resource map: " + mapFile.getAbsolutePath();
                throw new TException.GENERAL_EXCEPTION(msg);
            }
            Model model = ModelFactory.createDefaultModel();
            model.read(inputStream, null, "TURTLE");

            String versionID = "0";             // current
            String objectIDS = null;
            String ore = "http://www.openarchives.org/ore/terms#";

            try {
                objectIDS = ingestRequest.getJob().getPrimaryID().getValue();
            } catch (Exception e) {
                objectIDS = "OID_UNKNOWN";          // replace when known
            }
            String objectURI = profileState.getTargetStorage().getStorageLink().toString() + "/content/" +
                        profileState.getTargetStorage().getNodeID() + "/" +
                        URLEncoder.encode(objectIDS, "utf-8");

            String resourceMapURI = objectURI + "/" + versionID + "/" + URLEncoder.encode("system/mrt-object-map.ttl", "utf-8");

            // add each component file
            Vector<File> files = new Vector();

            FileUtil.getDirectoryFiles(sourceDir, files);
            for (File file : files) {
                if (file.isDirectory()) continue;
                // Turtle will not handle whitespace in URL, must encode
                String component = objectURI + "/" + versionID + URLEncoder.encode(file.getPath().substring(file.getPath().indexOf("/producer")), "utf-8");
                model.add(ResourceFactory.createResource(objectURI),
                    ResourceFactory.createProperty(ore + "aggregates"),
                    ResourceFactory.createResource(component));
            }

            return model;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update model: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }


    public boolean isComponent(String file) {
	if (file.equals("producer")) return false;
	if (file.equals("system")) return false;
	return true;
    }

    public String getName() {
	return NAME;
    }

}
