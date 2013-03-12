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

import java.util.ArrayList;
import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.aasc.Introspect;
import com.yahoo.aasc.ReadOnly;
import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

@Introspect
public class DigestStore implements CloneableDeep<DigestStore>, EqualsDeep<DigestStore> {

    @ReadOnly 
    private static final Logger LOG = LoggerFactory.getLogger(DigestStore.class);

    private long digestId;
    private ArrayList<Long> digests;
    private ArrayList<Integer> counts;
    private boolean haveMine;
    private int size;
    BitSet senders;
    
    private boolean recovered;
    
    public DigestStore(long digestId, int servers) {
        this.digestId = digestId;
        this.digests = new ArrayList<Long>(servers);
        this.counts = new ArrayList<Integer>(servers);
        while(this.digests.size() < servers){
        	this.digests.add(null);
        	this.counts.add(null);
        }
        this.senders = new BitSet(servers);
        this.size = 0;
        this.recovered = false;
        this.haveMine = false;
    }
    
    public void setRecovered(long digest){
        this.recovered = true;
        this.size = 1;
        this.digests.set(0, digest);
    }

    public boolean isRecovered (){
        return this.recovered;
    }

    public boolean matches(int quorum) {
        if (!haveMine) return false;
        return counts.get(0) >= quorum;
    }

    public void addRemote(int senderId, long digest) {
        if (! isRecovered() && ! senders.get(senderId)){
            senders.set(senderId);
            int i = 0;
            for (; i<size; ++i) {
                if (digests.get(i) == digest) {
                    counts.set(i, counts.get(i) + 1);
                    return;
                } else {
                    LOG.warn("State divergence adding remote. \n Stored: {} \n Received: {}", digests.get(i), digest);
                }
            }
            if (!haveMine) {
                digests.set(i, digest);
                counts.set(i, 1);
                size++;
            }
        }
    }

    public void addMine(long digest) {
        if (! isRecovered ()){
            int i = 0;
            haveMine = true;
            int count = 0;
            for (; i<size; ++i) {
                if (digests.get(i) == digest) {
                    count = counts.get(i);
                    break;
                } else {
                    LOG.warn("State divergence adding mine. \n Stored: {} \n Received: {}", digests.get(i), digest);
                }
            }
            size = 1;
            counts.set(0, count + 1);
            digests.set(0, digest);
        }
    }
    
    public long getDigestId() {
        return digestId;
    }

    public long getStableDigest(int quorum){
        if (matches(quorum) || isRecovered()){
            return digests.get(0);
        } else {
            return -1;
        }
    }

    @Override
    public DigestStore cloneDeep() {
        DigestStore clone = new DigestStore(digestId, digests.size());
        clone.haveMine = this.haveMine;
        clone.size = this.size;
        clone.recovered = this.recovered;
        clone.senders = new BitSet();
        clone.senders.or(senders);
        for (int i = 0; i < size; ++i) {
            clone.digests.add(this.digests.get(i));
            clone.counts.add(this.counts.get(i));
        }
        return clone;
    }
    
    @Override
    public boolean equalsDeep(DigestStore other) {
        if (this.digestId != other.digestId) return false;
        if (this.haveMine != other.haveMine) return false;
        if (this.size != other.size) return false;
        if (this.recovered != other.recovered) return false;
        if (!this.senders.equals(other.senders)) return false;
        for (int i = 0; i < this.size; ++i) {
            if (this.digests.get(i) != other.digests.get(i)) return false;
            if (this.counts.get(i) != other.counts.get(i)) return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("{DigestStore id:%d mine:%s recovered:%s size:%d digests:%s counts:%s",
                digestId, haveMine ? "yes" : "no", recovered ? "yes" : "no", size, digests, counts);
    }
}
