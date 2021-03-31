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

import java.net.URL;
import java.util.SortedMap;
import java.util.Vector;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.HandlerState;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.StateInf;

/**
 * Profile State information
 * @author mreyes
 */
public interface ProfileStateInf
        extends StateInf
{

    /**
     * Get profile identifier
     * @return Identifier profile identifier
     */
    public Identifier getProfileID();

    /**
     * Get profile description
     * @return String profile description
     */
    public String getProfileDescription();

    /**
     * Get object minter URL
     * @return URL object minter
     */
    public URL getObjectMinterURL();

    /**
     * Get characterization URL
     * @return URL characterization
     */
    public URL getCharacterizationURL();

    /**
     * Get Fixity URL
     * @return URL fixity
     */
    public URL getFixityURL();

    /**
     * Get target storage service
     * @return StoreNode storage service endpoint
     */
    public StoreNode getTargetStorage();

    /**
     * Get object content model
     * @return String (string,enum ????) object content model
     */
    public String getContentModel();

    /**
     * Get contact endpoints (email)
     * @return ContactEmail email contacts
     */
    public Vector<Notification> getContactsEmail();

    /**
     * Get handlers
     * @return Vector<Handler> handlers
     */
    public SortedMap<Integer,HandlerState> getIngestHandlers();

    /**
     * Get profile creation date-time 
     * @return DateState creation time
     */
    public DateState getCreationDate();

    /**
     * Get profile modification date-time 
     * @return DateState modification time
     */
    public DateState getModificationDate();

    /**
     * Get profile identifier scheme
     * @return 
     */
    public Identifier.Namespace getIdentifierScheme();

    /**
     * Get profile identifier namespace
     * @return String namespace
     */
    public String getIdentifierNamespace();

    /**
     * Get collection 
     * @return Vector collection
     */
    public Vector<String> getCollection();

    /**
     * Get collection Name
     * @return String collection
     */
    public String getCollectionName();

}
