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
import java.util.Collection;
import java.util.Date;
import java.util.SortedMap;
import java.util.Vector;
import java.io.File;
import java.io.Serializable;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.ingest.Notification;
import org.cdlib.mrt.ingest.StoreNode;
import org.cdlib.mrt.utility.StateInf;

/**
 * Profile State information
 * @author mreyes
 */
public class ProfileState
        implements ProfileStateInf, StateInf, Serializable
{

    private static final String NAME = "ProfileState";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;


    private Identifier profileID = null;
    private String profileDescription = null;
    private StoreNode storeNode = null;
    private URL accessURL = null;
    private String contentModel = null;
    private String scheme = null;
    private URL objectMinterURL = null;
    private URL localIDURL = null;
    private URL characterizationURL = null;
    private URL fixityURL = null;
    private URL dataONEURL = null;
    private URL coordinatingNodeURL = null;
    private URL callbackURL = null;
    private URL statusURL = null;
    private File statusView = null;
    private Vector<Notification> contactsEmail = new Vector<Notification>();
    private SortedMap<Integer,HandlerState> ingestHandlers = null;
    private SortedMap<Integer,HandlerState> queueHandlers = null;
    private DateState creationDate = null;
    private DateState modificationDate = null;
    private Identifier.Namespace objectScheme = null;
    private String objectNamespace = null;
    private Vector<String> collection = new Vector<String>();
    private String objectType = null;
    private String objectRole = null;
    private String aggregateType = null;
    private String owner = null;
    private Collection<String> admin = null;
    private String context = null;
    private String misc = null;
    private String purl = null;
    private String dataoneNodeID = null;
    private String ezidCoowner = null;
    private FormatType notificationFormat = null;	// response type
    private String notificationType = null;		// human readable notification 
    private boolean suppressDublinCoreLocalID = false;	// opt-in policy

    final String[] OBJECTTYPE = { "MRT-curatorial", "MRT-system" };
    final String[] OBJECTROLE = { "MRT-content", "MRT-class" };
    final String[] AGGREGATETYPE = { "MRT-collection", "MRT-owner", "MRT-service-level-agreement" };

    public Identifier getProfileID() {
        return profileID;
    }

    /**
     * Set object type
     * @param String object type
     */
    public boolean setObjectType(String objectType) {
	boolean valid = false;
	for (String types : OBJECTTYPE) {
	    if (types.equals(objectType)) {
		valid = true;
		break;
	    }
	}
        this.objectType = objectType;
	return valid;
    }

    /**
     * Set object role
     * @param String object role
     */
    public boolean setObjectRole(String objectRole) {
	boolean valid = false;
	for (String roles : OBJECTROLE) {
	    if (roles.equals(objectRole)) {
		valid = true;
		break;
	    }
	}
        this.objectRole = objectRole;
	return valid;
    }

    /**
     * Set aggregate type
     * @param String aggregate type
     */
    public boolean setAggregateType(String aggregateType) {
	boolean valid = false;
	for (String types : AGGREGATETYPE) {
	    if (types.equals(aggregateType)) {
		valid = true;
		break;
	    }
	}
        this.aggregateType = aggregateType;
	return valid;
    }

    /**
     * Set owner identifier
     * @param objectID owner identifier
     */
    public boolean setOwner(String owner) {
	try {
	    String scheme = "ark:/";
	    if (! owner.contains(scheme)) return false;
	    this.owner = owner;
	    return true;
	} catch (Exception e) { 
	    return false;
	}
    }

    /**
     * Set profile identifier
     * @param Identifier profile identifier
     */
    public void setProfileID(Identifier profileID) {
        this.profileID = profileID;
    }

    /**
     * Set profile description
     * @param String profile description
     */
    public void setProfileDescription(String profileDescription) {
        this.profileDescription = profileDescription;
    }

    /**
     * Get profile description
     * @return profile description
     */
    public String getProfileDescription() {
        return this.profileDescription;
    }

    /**
     * Set target store node
     * @param StoreNode store node
     */
    public void setTargetStorage(StoreNode storeNode) {
        this.storeNode = storeNode;
    }

    /**
     * Set object minter URL
     * @param URL object minter URL
     */
    public void setObjectMinterURL(URL objectMinterURL) {
        this.objectMinterURL = objectMinterURL;
    }

    /**
     * Set localID service URL
     * @param URL localID service URL
     */
    public void setLocalIDURL(URL localIDURL) {
        this.localIDURL = localIDURL;
    }

    /**
     * Set characterization URL
     * @param URL characterization URL
     */
    public void setCharacterizationURL(URL characterizationURL) {
        this.characterizationURL = characterizationURL;
    }

    /**
     * Set fixity URL
     * @param URL fixity URL
     */
    public void setFixityURL(URL fixityURL) {
        this.fixityURL = fixityURL;
    }

    /**
     * Set dataONE member node URL
     * @param URL dataONE MN URL
     */
    public void setDataoneURL(URL dataONEURL) {
        this.dataONEURL = dataONEURL;
    }

    /**
     * Set dataONE coordinating node URL
     * @param URL dataONE CN URL
     */
    public void setCoordinatingNodeURL(URL coordinatingNodeURL) {
        this.coordinatingNodeURL = coordinatingNodeURL;
    }

    /**
     * Set callback URL
     * @param URL callback URL
     */
    public void setCallbackURL(URL callbackURL) {
        this.callbackURL = callbackURL;
    }

    /**
     * Set status URL
     * @param URL status URL
     */
    public void setStatusURL(URL statusURL) {
        this.statusURL = statusURL;
    }

    /**
     * Set status view
     * @param File status view
     */
    public void setStatusView(File statusView) {
        this.statusView = statusView;
    }

    /**
     * Get object minter URL
     * @return object minter URL
     */
    public URL getObjectMinterURL() {
        return this.objectMinterURL; 
    }

    /**
     * Get localID service URL
     * @return localID service URL
     */
    public URL getLocalIDURL() {
        return this.localIDURL; 
    }

    /**
     * Get characterization URL
     * @return characterization URL
     */
    public URL getCharacterizationURL() {
        return this.characterizationURL; 
    }

    /**
     * Get fixity URL
     * @return fixity URL
     */
    public URL getFixityURL() {
        return this.fixityURL; 
    }

    /**
     * Get dataONE member node URL
     * @return dataONE MN URL
     */
    public URL getDataoneURL() {
        return this.dataONEURL; 
    }

    /**
     * Get dataONE coordinating node URL
     * @return dataONE CN URL
     */
    public URL getCoordinatingNodeURL() {
        return this.coordinatingNodeURL; 
    }

    /**
     * Get callback URL
     * @return callback URL
     */
    public URL getCallbackURL() {
        return this.callbackURL; 
    }

    /**
     * Get status URL
     * @return status URL
     */
    public URL getStatusURL() {
        return this.statusURL; 
    }

    /**
     * Get status view
     * @return status view as string
     */
    public File getStatusView() {
        return this.statusView; 
    }

    /**
    /**
     * Set access URL
     * @param URL access URL
     */
    public void setAccessURL(URL accessURL) {
        this.accessURL = accessURL;
    }

    /**
     * Get access URL
     * @return access URL
     */
    public URL getAccessURL() {
        return this.accessURL; 
    }

    public String getContentModel() {
        return contentModel;
    }

    /**
     * Set content model
     * @param String object content model
     */
    public void setContentModel(String contentModel) {
        this.contentModel = contentModel;
    }

    /**
     * Get contacts email
     * @return notification object
     */
    public Vector<Notification> getContactsEmail() {
        return contactsEmail;
    }

    /**
     * Set contacts email
     * @param ContactsEmail set notification
     */
    public void setContactsEmail(Notification notification) {
        this.contactsEmail.add(notification);
    }

    public SortedMap<Integer,HandlerState> getIngestHandlers() {
        return ingestHandlers;
    }

    public SortedMap<Integer,HandlerState> getQueueHandlers() {
        return queueHandlers;
    }

    /**
     * Add object handlers
     * @param HandlerState object handler
     */
    //public void setHandlers(HandlerState handler) {
        //this.handlers.put(new Integer(), (HandlerState) handler);
    //}

    /**
     * Set object ingest handlers
     * @param Vector objects handler
     */
    public void setIngestHandlers(SortedMap<Integer,HandlerState> handlers) {
        this.ingestHandlers = handlers;
    }

    /**
     * Set batch queue handlers
     * @param Vector queue handler
     */
    public void setQueueHandlers(SortedMap<Integer,HandlerState> handlers) {
        this.queueHandlers = handlers;
    }

    public DateState getCreationDate() {
        return creationDate;
    }

    /**
     * Set creation date
     * @param DateState creation date-time
     */
    public void setCreationDate(DateState creationDate) {
        this.creationDate = creationDate;
    }

    public DateState getModificationDate() {
        return modificationDate;
    }

    /**
     * Set collection
     * @param String collection
     */
    public void setCollection(String collection) {
        this.collection.add(collection);
    }

    /**
     * Get collection
     * @return Vector collection
     */
    public Vector<String> getCollection() {
        return collection;
    }

    /**
     * Set modification date
     * @param DateState modification date
     */
    public void setModificationDate(DateState modificationDate) {
        this.modificationDate = modificationDate;
    }

    /**
     * Set identifier scheme
     * @param objectScheme identifier scheme
     */
    public void setIdentifierScheme(String objectScheme) {
        for (Identifier.Namespace n: Identifier.Namespace.values()) {
            if (n.toString().equals(objectScheme)) {
	    	this.objectScheme = n;
	    }
        }
    }

    public Identifier.Namespace getIdentifierScheme() {
        return objectScheme;
    }

    /**
     * Set identifier namespace
     * @param objectNamespace identifier namespace
     */
    public void setIdentifierNamespace(String objectNamespace) {
        this.objectNamespace = objectNamespace;
    }

    public String getIdentifierNamespace() {
        return objectNamespace;
    }

    public String getOwner() {
        return owner;
    }

    public String getObjectType() {
        return objectType;
    }

    public String getObjectRole() {
        return objectRole;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public StoreNode getTargetStorage() {
        return storeNode;
    }

    public Collection<String> getAdmin() {
        return admin;
    }

    public void setAdmin(Collection<String> admin) {
        this.admin = admin;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getMisc() {
        return misc;
    }

    public String getPURL() {
        return purl;
    }

    public void setMisc(String misc) {
        this.misc = misc;
    }

    public void setPURL(String purl) {
        this.purl = purl;
    }

    public String getDataoneNodeID() {
        return dataoneNodeID;
    }

    public void setDataoneNodeID(String dataoneNodeID) {
        this.dataoneNodeID = dataoneNodeID;
    }

    public FormatType getNotificationFormat() {
        return notificationFormat;
    }

    public String getNotificationType() {
        return notificationType;
    }

    public String getEzidCoowner() {
        return ezidCoowner;
    }

    public void setEzidCoowner(String ezidCoowner) {
        this.ezidCoowner = ezidCoowner;
    }

    public boolean grabSuppressDublinCoreLocalID() {
        return suppressDublinCoreLocalID;
    }

    public void setSuppressDublinCoreLocalID(boolean suppressDublinCoreLocalID) {
        this.suppressDublinCoreLocalID = suppressDublinCoreLocalID;
    }

    public void setNotificationFormat(String notificationFormat) {
        try {
            this.notificationFormat = FormatType.valueOf(notificationFormat);
        } catch (Exception e) {
            // default
	    e.printStackTrace();
            if (DEBUG) System.out.println("[warn] ProfileState: Could not assign format type: " + notificationFormat);
            this.notificationFormat = null;
        }
    }

    public void setNotificationType(String notificationType) {
	this.notificationType = notificationType;
    }

    public String dump(String header)
    {
        return header
                + " - profileID=" + profileID
                + " - target storage node =" + storeNode.dump(profileID.toString())
                + " - object content model =" + contentModel
                + " - email contacts =" + contactsEmail.toString()
                + " - ingestHandlers =" + ingestHandlers.toString()
                + " - queueHandlers =" + queueHandlers.toString()
                + " - context =" + context
                + " - creationDate=" + creationDate
                + " - modificationDate=" + modificationDate;
    }
}
