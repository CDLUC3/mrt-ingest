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
package org.cdlib.mrt.ingest.app;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;
/**
 * General validation methods command line info
 * @author mreyes
 */
public class ValidateCmdParms
{

    /**
     * Validate and convert Object Identifier
     * @param objectIDS String form of Identifier
     * @return Identifier form
     * @throws TException not present
     */
    public static Identifier validateObjectID(String objectIDS)
        throws TException
    {
        if (StringUtil.isEmpty(objectIDS)) {
            throw  setException("fileName missing");
        }
        Identifier objectID = null;
        try {
            objectID = new Identifier(objectIDS);
        } catch (Exception ex) {}
        if (objectID == null) {
            throw  setException("objectID not valid:" + objectIDS);
        }
        return objectID;
    }

    /**
     * Convert String to numeric and throw exception if invalid
     * @param header Exception display header
     * @param value String to convert to numeric
     * @return int form of value
     * @throws TException invalid numeric
     */
    protected static int getInteger(String header, String value)
        throws TException
    {
        int retVal = -1;
        try {
            retVal = Integer.parseInt(value);
            return retVal;

        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(header + ": Non-numeric value:" + value);
        }
    }


    public static  void testObjectID(Identifier objectID)
        throws TException
    {
        if ((objectID == null) || (objectID.toString() == null)) {
            throw setException("ObjectID required but not found");
        }
    }

    public static  void testArchiveType(String archiveType)
        throws TException
    {
        if (StringUtil.isEmpty(archiveType)) {
            throw setException("Archive type required but not found");
        }
        archiveType = archiveType.toLowerCase();
        if ("*zip*tar*targz*".indexOf("*" + archiveType + "*") < 0) {
            throw setException("Archive type not supported");
        }
    }

    public static TException setException(String header)
    {
        return new TException.REQUEST_INVALID(
                header);
    }
}
