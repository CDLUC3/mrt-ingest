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

import java.util.Iterator;
import java.util.Vector;
import java.io.Serializable;
import org.cdlib.mrt.utility.StateInf;

/**
 * Lock State information
 * @author mreyes
 */
public class LockState
        implements LockStateInf, StateInf, Serializable
{

    protected Vector<LockEntryState> lockEntries = new Vector<LockEntryState>();


    /**
     * Add entry to lock
     * @param String entry
     */
    public void addEntry(LockEntryState entry) {
        this.lockEntries.add(entry);
    }


    /**
     * retrieve all entries
     * @return lock entries
     */
    public Vector<LockEntryState> getLockEntries() {
        return this.lockEntries;
    }

    /**
     * add lock entry
     * @return lock entries
     */
    public void addLockEntry(LockEntryState lockEntry)
    {
        if (lockEntry == null) return;
        lockEntries.add(lockEntry);
    }


    public String dump(String header)
    {
	// gather lock entries
	String lockEntriesS = "\n\n";
        Iterator<LockEntryState> iterator = lockEntries.iterator();
        while(iterator.hasNext()) {
            LockEntryState lockEntry = iterator.next();
	    lockEntriesS = lockEntriesS + lockEntry + "\n";
	}
	lockEntriesS = lockEntriesS.substring(1, lockEntriesS.length() - 1 );

        return header  + "\n\n"
                + " - lock entries: " + lockEntriesS + "\n";
    }
}
