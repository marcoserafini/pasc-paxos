/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.pasc.paxos.state;

import java.io.Serializable;
import java.util.ArrayList;

import com.yahoo.aasc.Introspect;
import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

@Introspect
public final class InstanceRecord implements Serializable, EqualsDeep<InstanceRecord>, CloneableDeep<InstanceRecord> {

    private static final long serialVersionUID = -9144513788670934858L;

    long iid;
    int ballot;
    int arraySize;
    ArrayList<ClientTimestamp> clientTimestamps;

    public InstanceRecord(long iid, int ballot, int bufferSize) {
        this.iid = iid;
        this.ballot = ballot;
        clientTimestamps = new ArrayList<ClientTimestamp>(bufferSize);
        arraySize = 0;
    }

    public InstanceRecord(long iid, int ballot, ArrayList<ClientTimestamp> clientTimestamps, int arraySize) {
        this.iid = iid;
        this.ballot = ballot;
        this.clientTimestamps = clientTimestamps;
        this.arraySize = arraySize;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    public int getBallot() {
        return ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public ArrayList<ClientTimestamp> getClientTimestamps() {
        return clientTimestamps;
    }

    public void setClientTimestamps(ArrayList<ClientTimestamp> cts) {
        clientTimestamps = cts;
    }

    public void setClientTimestamp(ClientTimestamp ct, int index) {
    	while(clientTimestamps.size() < index + 1){
    		clientTimestamps.add(null);
    	}
        clientTimestamps.set(index, ct);
    }

    public ClientTimestamp getClientTimestamp(int index) {
        return clientTimestamps.get(index);
    }

    public int getArraySize() {
        return arraySize;
    }

    public void setArraySize(int newSize) {
        arraySize = newSize;
    }

    public int size(){
        // 16 bytes for internal fields and 12 for each clientTimestamp
        return 16 + 12 * arraySize;
    }

    @Override
    public String toString() {
        return String
                .format("{iid:%d bl:%d sz:%d array:%s}", iid, ballot, arraySize, clientTimestamps);
    }

    public InstanceRecord cloneDeep() {
        ArrayList<ClientTimestamp> resClientTimestamps = new ArrayList<ClientTimestamp>(this.clientTimestamps.size());
        for (int i = 0; i < this.arraySize; i++) {
            resClientTimestamps.add(this.clientTimestamps.get(i).cloneDeep());
        }
        return new InstanceRecord(this.iid, this.ballot, resClientTimestamps, this.arraySize);
    }

    public boolean equalsDeep(InstanceRecord other) {
        if (this.iid != other.iid || this.ballot != other.ballot || this.arraySize != other.arraySize) {
            return false;
        }
        for (int i = 0; i < this.arraySize; i++) {
            if (!this.clientTimestamps.get(i).equalsDeep(other.clientTimestamps.get(i))) {
                return false;
            }
        }
        return true;
    }
}
