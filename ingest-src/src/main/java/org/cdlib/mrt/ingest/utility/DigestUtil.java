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


import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.net.URL;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.ingest.JobState;
import org.cdlib.mrt.ingest.ProfileState;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFileLogger;



/**
 * Simple fixity checking
 * @author mreyes
 */
public class DigestUtil
{

    protected static final String NAME = "DigestUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String EOL = "%0A";
    protected static final String NL =  System.getProperty("line.separator");
    protected LoggerInf logger = null;
    protected Properties conf = null;
    protected Properties ingestProperties = null;

    public DigestUtil() {
        logger = new TFileLogger("DigestUtil", 10, 10);
    }
    
    /**
     * Run fixity on this file
     * Originally seen in org.cdlib.mrt.store.dflat.DflatManager (to do: move to core-lib)
     * @param componentFile file to be tested
     * @param fileState state information for file including remote extract
     * information
     * @throws Exception
     */
    public void doFileFixity(
            File componentFile,
            FileComponent fileState)
        throws Exception
    {
        MessageDigest messageDigest = fileState.getMessageDigest();
        MessageDigestType algorithm = messageDigest.getAlgorithm();
        FixityTests fixity = new FixityTests(componentFile, algorithm.toString(), logger);
        String checksum = fileState.getMessageDigest().getValue();
        long size = fileState.getSize();
	FixityTests.FixityResult fixityResult = fixity.validateSizeChecksum(checksum, 
		algorithm.toString(), componentFile.length());    // ignore size
        if (!fixityResult.checksumMatch) {
            String msg = MESSAGE + "Fixity check fails: " + fixityResult.dump(componentFile.getName());
            System.out.println("[ERROR] Checksum error:" + NL
                    + " - File checksum=" + fixity.getChecksum() + NL
                    + " - In   checksum=" + checksum + NL
                    + " - File algorithm=" + algorithm.toString() + NL);
            throw new TException.INVALID_DATA_FORMAT(msg + " ]");
        }
    }
}
