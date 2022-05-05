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
 * Queue State information
 * @author mreyes
 */
public class QueueState
        implements QueueStateInf, StateInf, Serializable
{

    protected Vector<QueueEntryState> queueEntries = new Vector<QueueEntryState>();


    /**
     * Add entry to queue
     * @param String entry
     */
    public void addEntry(QueueEntryState entry) {
        this.queueEntries.add(entry);
    }

    /**
     * Remove entry from queue
     * @param String entry
     */
    public void removeEntry(int index) {
        this.queueEntries.remove(index);
    }

    /**
     * retrieve all entries
     * @return queue entries
     */
    public Vector<QueueEntryState> getQueueEntries() {
        return this.queueEntries;
    }

    /**
     * add queue entry
     * @return queue entries
     */
    public void addQueueEntry(QueueEntryState queueEntry)
    {
        if (queueEntry == null) return;
        queueEntries.add(queueEntry);
    }


    public String dump(String header)
    {
	// gather queue entries
	String queueEntriesS = "\n\n";
        Iterator<QueueEntryState> iterator = queueEntries.iterator();
        while(iterator.hasNext()) {
            QueueEntryState queueEntry = iterator.next();
	    queueEntriesS = queueEntriesS + queueEntry + "\n";
	}
	queueEntriesS = queueEntriesS.substring(1, queueEntriesS.length() - 1 );

        return header  + "\n\n"
                + " - queue entries: " + queueEntriesS + "\n";
    }
}
