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

import java.io.Serializable;
import java.util.Iterator;
import java.lang.String;
import java.util.Vector;

/**
 * Profiles State information
 * @author mreyes
 */
public class ProfilesFullState
        implements ProfilesFullStateInf, Serializable

{

    private static final String NAME = "ProfilesFullState";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;


    private Vector<ProfileState> profilesFull = new Vector<ProfileState>();


    /**
     * Get profile 
     * @return Vector profile states
     */
    public Vector<ProfileState> getProfilesFull() {
        return this.profilesFull;
    }

    /**
     * Set profile states
     * @param ProfileState profiles
     */
    public void setProfiles(Vector<ProfileState> profilesFull) {
	this.profilesFull = profilesFull;
    }

    /**
     * Add a profile instance to list
     * @param File profile  to be added
     */
    public void addProfileInstance(ProfileState profileState)
    {
        if (profileState == null) return;
        profilesFull.add(profileState);
    }

    public String dump(String header)
    {
        // gather profile names
        String outProfile = "\n\n";
        Iterator<ProfileState> iterator = profilesFull.iterator();
        while(iterator.hasNext()) {
            ProfileState profile = iterator.next();
            outProfile = outProfile + profile.toString() + "\n";
        }
        outProfile = outProfile.substring(1, outProfile.length() - 1 );

        return header  + "\n\n"
                + " profileStates: " + outProfile + "\n";
    }

}
