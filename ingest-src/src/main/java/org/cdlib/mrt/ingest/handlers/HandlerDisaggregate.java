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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.zip.*;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import org.cdlib.mrt.ingest.IngestRequest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.ingest.utility.MetadataUtil;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.ingest.utility.PackageTypeEnum;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * unpack container (if needed) and move to appropriate processing location
 * @author mreyes
 */
public class HandlerDisaggregate extends Handler<JobState>
{

    protected static final String NAME = "HandlerDisaggregate";
    protected static final String MESSAGE = NAME + ": ";
    protected static final int BUFFERSIZE = 4096;
    protected static final boolean DEBUG = true;
    protected LoggerInf logger = null;
    protected Properties conf = null;


    /**
     * Unpack container
     *
     * @param profileState contains target storage service info
     * @param ingestRequest contains ingest request info
     * @param Object stateInf based class
     * @return HandlerResult object containing processing status 
     */
    public HandlerResult handle(ProfileState profileState, IngestRequest ingestRequest, JobState jobState) 
	throws TException 
    {

	File file = null;
	String status = null;

	PackageTypeEnum packageType = ingestRequest.getPackageType();
	if (packageType == PackageTypeEnum.batchManifest) {
	    System.out.println("batch manifest detected. resetting type to object manifest");
	    ingestRequest.setPackageType("manifest");	// eliminate when ingestRequest is OBSOLETE!!!
	    packageType = ingestRequest.getPackageType();
	}
	try {

	    boolean result;
	    File targetDir = new File(ingestRequest.getQueuePath(), "producer");
	    for (String fileS : targetDir.list()) {
	        file = new File(targetDir, fileS);
	    	if (packageType == PackageTypeEnum.container) {
			System.out.println("[info] " + MESSAGE + "container parm specified, uncompression/un-archiving processing: " + fileS);
			status = "valid";

			// uncompress
			file = decompress(file, targetDir);
			if (file == null) {
				throw new TException.INVALID_OR_MISSING_PARM("[error] " 
					+ MESSAGE + "uncompressing: " + file.getAbsolutePath());
			}
			if (! file.getName().endsWith(".tar")) {
			    file.delete();
			    continue;
			}

			// untar 
			if (! untar(file, targetDir)) {
	    		    System.out.println("[error] " + MESSAGE + "processing tar container: " + file.getAbsolutePath());
	    		    throw new TException.INVALID_OR_MISSING_PARM("[error] " 
				+ MESSAGE + "processing tar container: " + file.getAbsolutePath());
			}
		        file.delete();

                        // If updating and we have a container
            		File existingProducerDir = new File(ingestRequest.getQueuePath() + 
			        System.getProperty("file.separator") + ".producer" + System.getProperty("file.separator"));
	    		if (existingProducerDir.exists()) {
			    System.out.println("[debug] " + MESSAGE + "Found existing producer data, processing.");
			    FileUtil.updateDirectory(existingProducerDir, new File(ingestRequest.getQueuePath(), "producer"));
			    FileUtil.deleteDir(existingProducerDir);
	    		}

	    	} else if (packageType == PackageTypeEnum.file) {
			System.out.println("[info] " + MESSAGE + "file parm specified, no uncompression/un-archiving needed: " + fileS);
			status = "n/a";
	    	} else if (packageType == PackageTypeEnum.manifest) {
			System.out.println("[info] " + MESSAGE + "manifest parm specified, no uncompression/un-archiving needed: " + fileS);
			status = "n/a";
		} else {
			System.out.println("[error] " + MESSAGE + "specified package type not recognized (file/container/manifest): " + packageType + " - " + fileS);
			status = "not-valid";
	    		throw new Exception("[error] " + MESSAGE + "specified package type not recognized (file/container/manifest): " + packageType + " - " + fileS);
		}
            }

            // metadata file in ANVL format
            File systemTargetDir = new File(ingestRequest.getQueuePath(), "system");
            File ingestFile = new File(systemTargetDir, "mrt-ingest.txt");
            if ( ! createMetadata(ingestFile, status)) {
                throw new TException.GENERAL_EXCEPTION("[error] "
                    + MESSAGE + ": unable to build metadata file: " + ingestFile.getAbsolutePath());
            }

            // update resource map
	    if (! status.equals("n/a")) {	// we did some work here
                File mapFile = new File(systemTargetDir, "mrt-object-map.ttl");
                if ( ! updateResourceMap(profileState, ingestRequest, mapFile, targetDir)) {
                    System.err.println("[warn] " + MESSAGE + "Failure to update resource map.");
                }
	    }
	
	    return new HandlerResult(true, "SUCCESS: " + NAME + " completed successfully", 0);
	} catch (TException te) {
            te.printStackTrace(System.err);
            return new HandlerResult(false, "[error]: " + MESSAGE + te.getDetail());
	} catch (Exception e) {
            e.printStackTrace(System.err);
            String msg = "[error] " + MESSAGE + "processing container: " + file.getAbsolutePath() + " : " + e.getMessage();
            return new HandlerResult(false, msg);
        } finally {
            // cleanup?
        }
    }
   
    /**
     * untar container
     *
     * @param container compressed data
     * @param target target unpacking location
     * @return boolean decompression status
     */
    private boolean untar(File container, File target) {
	FileInputStream in = null;
	FileOutputStream out = null;
	TarInputStream tarIn = null;

	String name = container.getName();
	boolean compressed = false;

	try {

	    tarIn = new TarInputStream(new FileInputStream(container));
	    TarEntry tarEntry = tarIn.getNextEntry();
	    while (tarEntry != null) {
	        File destFile = new File(container.getParent() + System.getProperty("file.separator") + tarEntry.getName());
	        if (DEBUG) System.out.println("[info] " + MESSAGE + "creating tar entry: " + destFile.getAbsolutePath());
	        if (tarEntry.isDirectory()){
		    destFile.mkdirs();
	        } else {
		    // tar file without directory entries
		    if ( ! destFile.getParentFile().exists()) {
			 destFile.getParentFile().mkdirs();
		    }
		    out = new FileOutputStream(destFile);
		    tarIn.copyEntryContents(out);
	        }
	        tarEntry = tarIn.getNextEntry();
	    }
	    container.delete();
    	    return true;
	} catch (Exception e) {
    	    e.printStackTrace();
    	    System.out.println("[error] " + MESSAGE + "error decompressing/expanding file: " + container.getAbsolutePath());
    	    return false;
	} finally {
            try {
	        tarIn.close();
	        in.close();
	        out.close();
	        container.delete();
            } catch (Exception e) { }
	}
    }

    /**
     * decompress container
     * supported extensions (tgz|gz, zip, bz)
     *
     * @param container compressed data
     * @param target target unpacking location
     * @return expanded file (or directory for zip)
     */
    private File decompress(File container, File target) 
	throws TException
    {
	InputStream in = null;
	FileInputStream fileIn = null;
	FileOutputStream fileOut = null;
	TarInputStream tarIn = null;
	ZipInputStream zipIn = null;
	BZip2CompressorInputStream bzIn = null;

	String name = container.getName();

	try {
	    // gzip
	    if (name.endsWith("gz") || name.endsWith("tarz")) {
		fileIn = new FileInputStream(container);
		String newName = getOutputName(container, name);

		// is this a tar file?
		// if so then do not move to target just yet.
		String isTar = "/producer/";
		if (newName.endsWith(".tar")) isTar = "/";

		File file = new File (container.getParent() + isTar + newName);
		fileOut = new FileOutputStream(file);
                if (DEBUG) System.out.println("[info] " + MESSAGE + "creating gzip entry: " + file.getAbsolutePath());

	        GzipCompressorInputStream gzIn = new GzipCompressorInputStream(fileIn);
	        final byte[] buffer = new byte[BUFFERSIZE];
	        int n = 0;
	        while (-1 != (n = gzIn.read(buffer))) {
    		        fileOut.write(buffer, 0, n);
	        }
		gzIn.close();
		fileIn.close();
	        fileOut.close();

		container.delete();
		container = new File(container.getParent() + "/" + file.getName());

	    // bzip
	    } else if (name.endsWith("bz2")) {
		fileIn = new FileInputStream(container);
		String newName = getOutputName(container, name);
		File file = new File (container.getParent() + "/" + newName);
		fileOut = new FileOutputStream(file);
                if (DEBUG) System.out.println("[info] " + MESSAGE + "creating bzip2 entry: " + file.getAbsolutePath());
		bzIn = new BZip2CompressorInputStream(fileIn);
		final byte[] buffer = new byte[BUFFERSIZE];
		int n = 0;
		while (-1 != (n = bzIn.read(buffer))) {
    		    fileOut.write(buffer, 0, n);
		}
		fileOut.close();
		bzIn.close();

	    // zip
	    } else if (name.toLowerCase().endsWith("zip")) {
		in = new BufferedInputStream(new FileInputStream(container));
		zipIn = new ZipInputStream(in); 
		ZipEntry zipEntry; 

	        final byte[] buffer = new byte[BUFFERSIZE];
                while ((zipEntry = zipIn.getNextEntry()) != null) {
                     File destFile = new File(container.getParent() + "/" + zipEntry.getName());
                     if (DEBUG) System.out.println("[info] " + MESSAGE + "creating zip entry: " + destFile.getAbsolutePath());
                     if (zipEntry.isDirectory()){
                        destFile.mkdirs();
                     } else {
			try {
                            fileOut = new FileOutputStream(destFile);
			} catch (Exception e) {
			    destFile.getParentFile().mkdirs();
                            fileOut = new FileOutputStream(destFile);
			}
			int length = 0;
			while((length = zipIn.read(buffer)) != -1) {
			    fileOut.write(buffer, 0, length);
			}
    			fileOut.close();
                     }
                 }
                 container.delete();
		 in.close();
		 zipIn.close();

	    } else if (! name.endsWith(".tar")) {
		if (DEBUG) System.out.println("[error] " + MESSAGE + "file extension not supported as a container: " + container.getAbsolutePath());
		throw new TException.INVALID_OR_MISSING_PARM("[error] File extension not supported as a container: " + container.getAbsolutePath());
	    }

	    return container;

	} catch (TException te) {
	    throw te;
	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println("[error] + " + MESSAGE + "error decompressing file: " + container.getAbsolutePath());
	    return null;
	} finally {
	    try {
		in.close();
		tarIn.close();
		fileIn.close();
		fileOut.close();
	    } catch (Exception e) { }
	}
   }

    public String getOutputName(File input, String name) {
	String inS = input.getName();
	String outParentS = input.getParent();
	String outNameS = null;
	
	if (name.endsWith(".gz")) {
	    outNameS = inS.substring(0, inS.length() - ".gz".length());
	} else if (name.endsWith(".tgz")) {
	    outNameS = inS.substring(0, inS.length() - ".tgz".length()) + ".tar";
	} else if (name.endsWith("tarz")) {
	    outNameS = inS.substring(0, inS.length() - "z".length());
	} else if (name.endsWith("bz2")) {
	    outNameS = inS.substring(0, inS.length() - ".bz2".length());
	} 

	return outNameS;
    }

    /**
     * append results to metadata file
     *
     * @param ingestFile metadata file
     * @param status package integrity
     * @return successful in appending metadata
     */
    private boolean createMetadata(File ingestFile, String status)
        throws TException
    {
        if (DEBUG) System.out.println("[debug] " + MESSAGE + "appending metadata: " + ingestFile.getAbsolutePath());
        Map<String, Object> ingestProperties = new LinkedHashMap();   // maintains insertion order

        ingestProperties.put("containerValidity", status);

        return MetadataUtil.writeMetadataANVL(ingestFile, ingestProperties, true);
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
            if (DEBUG) System.out.println("[debug] " + MESSAGE + "updating resource map: " + mapFile.getAbsolutePath());

            Model model = updateModel(profileState, ingestRequest, mapFile, sourceDir);
            if (DEBUG) dumpModel(model);
            writeModel(model, mapFile);

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
                objectIDS = "OID_UNKNOWN";	// replace when known
            }
            String objectURI = profileState.getTargetStorage().getStorageLink().toString() + "/content/" +
                        profileState.getTargetStorage().getNodeID() + "/" +
                        URLEncoder.encode(objectIDS, "utf-8");

            String resourceMapURI = objectURI + "/" + versionID + "/system" + "/mrt-object-map.ttl";

	    // add each component file
	    Vector<File> files = new Vector();


	    FileUtil.getDirectoryFiles(sourceDir, files);
	    for (File file : files) {
		if (file.isDirectory()) continue;
		if (file.getName().equals("mrt-erc.txt")) continue;
		String component = objectURI + "/" + versionID + 
			URLEncoder.encode(file.getPath().substring(file.getPath().indexOf("/producer")), "utf-8");
                model.add(ResourceFactory.createStatement(ResourceFactory.createResource(objectURI),
                    ResourceFactory.createProperty(ore + "aggregates"),
                    ResourceFactory.createResource(component)));
	    }

            return model;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to update model: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }


    public static void writeModel(Model model, File mapFile)
        throws TException
    {
        FileOutputStream fos = null;
        try {
            String [] formats = { "RDF/XML", "RDF/XML-ABBREV", "N-TRIPLE", "TURTLE", "TTL", "N3"};
            String format = formats[4]; // Turtle

            fos = new FileOutputStream(mapFile);
            model.write(fos, format);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "[error] " + MESSAGE + "failed to write resource map: " + e.getMessage();
            throw new TException.GENERAL_EXCEPTION(msg);
        } finally {
            try {
                fos.flush();
            } catch (Exception e) {}
        }
    }


    public static void dumpModel(Model model)
    {
        System.out.println( "[debug] dump resource map - START");

        // list the statements in the graph
        StmtIterator iter = model.listStatements();

        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt      = iter.nextStatement();         // get next statement
            Resource  subject   = stmt.getSubject();   // get the subject
            Property  predicate = stmt.getPredicate(); // get the predicate
            RDFNode   object    = stmt.getObject();    // get the object

            System.out.print(subject.toString());
            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                System.out.print(object.toString());
            } else {
                // object is a literal
                System.out.print(" \"" + object.toString() + "\"");
            }
            System.out.println(" .");
        }
        System.out.println( "[debug] dump resource map - END");
    }


    public String getName() {
	return NAME;
    }

}
