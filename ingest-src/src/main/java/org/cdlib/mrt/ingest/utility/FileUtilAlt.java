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

package org.cdlib.mrt.ingest.utility;
import java.io.IOException;
import java.io.File;
import org.cdlib.mrt.utility.TException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import java.io.FileFilter;


/**
 * Generalized file utilities
 * @author dloy
 */
public class FileUtilAlt {
    protected static final String NAME = "FileUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static final int BUFSIZE = 32768;

    /**
     * Build list of files for a directory
     * @param sourceLocation start directory for extraction
     * @param files list of file contents
     * @throws org.cdlib.mrt.utility.MException
     */
    public static void getDirectoryFiles(File sourceLocation , Vector<File> files)
        throws TException
    {
        try {
            File [] children = null;
	    int cnt = 0;
	    BasicFileAttributes attrs;
	    boolean isDir;

	    try {
               // Can throw I/O exception
	       attrs = Files.readAttributes(sourceLocation.toPath(), BasicFileAttributes.class);
	    } catch (IOException ioe) {
	       // Try a second and last time
	       attrs = Files.readAttributes(sourceLocation.toPath(), BasicFileAttributes.class);
	    }
	    try {
               // Can throw exception
               isDir = attrs.isDirectory();		
	    } catch (Exception e) {
	       // Try a second and last time
               isDir = attrs.isDirectory();		
	    }

            // if (sourceLocation.isDirectory()) {	// I/O exception returns false
            if (isDir) {
		while (children == null && cnt < 3) {
		    // I/O exception returns null
                    children = sourceLocation.listFiles();
		    cnt++;
		}
		if (children == null) throw new Exception("I/O error: " + sourceLocation.getAbsolutePath());
                for (int i=0; i<children.length; i++) {
                    File child = children[i];
                    getDirectoryFiles(child, files);
                }

            } else {
                files.add(sourceLocation);
            }

        } catch(TException mfe) {
	    mfe.printStackTrace();
            throw mfe;
        } catch(Exception ex) {
	    ex.printStackTrace();
            String err = MESSAGE + "getDirectoryFiles() - Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION( err);
        }

    }

    /**
     * Build list of files for a directory
     * @param sourceLocation target directory for testing
     * @throws org.cdlib.mrt.utility.MException
     */
    public static boolean isDirectory(File sourceLocation)
        throws Exception
    {

        try {
	    BasicFileAttributes attrs;
	    boolean isDir;

	    try {
               // Can throw I/O exception
	       attrs = Files.readAttributes(sourceLocation.toPath(), BasicFileAttributes.class);
	    } catch (IOException ioe) {
	       // Try a second and last time
	       attrs = Files.readAttributes(sourceLocation.toPath(), BasicFileAttributes.class);
	    }
	    try {
               // Can throw exception
               isDir = attrs.isDirectory();		
	    } catch (Exception e) {
	       // Try a second and last time
               isDir = attrs.isDirectory();		
	    }

            if (isDir) {
		return true;
	    } else {
		return false;
	    }
        } catch(Exception ex) {
	    ex.printStackTrace();
            String err = MESSAGE + "isDirectory() - Exception:" + ex;
            throw new TException.GENERAL_EXCEPTION( err);
        }

    }


    /**
     * Quick failure if file exists
     * @param File directory
     * @param String daemon process name
     * @throws org.cdlib.mrt.utility.MException
     */
    public static boolean quickFailure(File failDir, String daemonName)
        throws Exception
    {
        try {

	    File daemonFile = new File(failDir, "/" + daemonName);
            return daemonFile.exists();
        } catch(Exception ex) {
	    ex.printStackTrace();
            String err = MESSAGE + "quickFailure() - Exception:" + ex;
            // throw new TException.GENERAL_EXCEPTION(err);
	    return false;
        }
    }

}
