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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.aasc.Introspect;
import com.yahoo.aasc.ReadOnly;
import com.yahoo.pasc.ProcessState;

@Introspect
public class PaxosState implements ProcessState {

//    @SuppressWarnings("unused")
    @ReadOnly 
    private static final Logger LOG = LoggerFactory.getLogger(PaxosState.class);

    private static final int MAX_CLIENTS = 4096;
    // this field is just for testing. will be replaced when the learner will
    // use the state machine
    public final int REPLY_SIZE = 1;

    public PaxosState(int maxInstances, int bufferSize, int serverId, int quorum, int digestQuorum,
            int servers, int congestionWindow, int maxDigests, int requestThreshold, 
            int checkpointPeriod, boolean leaderReplies) {
        this.maxInstances = maxInstances;
        this.instances = new ArrayList<InstanceRecord>(maxInstances);
        while(this.instances.size() < maxInstances) this.instances.add(null);
        this.accepted = new ArrayList<IidAcceptorsCounts>(maxInstances);
        while(this.accepted.size() < maxInstances) this.accepted.add(null);
        this.requests = new ArrayList<ArrayList<IidRequest>>(MAX_CLIENTS);
        while(this.requests.size() < MAX_CLIENTS) this.requests.add(null);
        this.bufferSize = bufferSize;
        this.serverId = serverId;
        this.quorum = quorum;
        this.digestQuorum = digestQuorum;
        this.servers = servers;
        this.congestionWindow = congestionWindow;
        this.maxDigests = maxDigests;
        this.digestStore = new ArrayList<DigestStore>(maxDigests);
        while(this.digestStore.size() < maxDigests) this.digestStore.add(null); 
        this.maxExecuted = -1;
        this.leaderId = -1;
        this.replyCache = new ArrayList<TimestampReply>(MAX_CLIENTS);
        while(this.replyCache.size() < MAX_CLIENTS) this.replyCache.add(null);
        this.inProgress = new ArrayList<Long>(MAX_CLIENTS);
        Long minusOne = new Long(-1);
        while(this.inProgress.size() < MAX_CLIENTS) this.inProgress.add(minusOne);
        this. prepared = new PreparedMessages(servers);
        this.requestThreshold = requestThreshold;
        this.checkpointPeriod = checkpointPeriod;
        this.leaderReplies = leaderReplies;
    }
    
    // Digests
    final int digestQuorum;
    final ArrayList<DigestStore> digestStore;
    long firstDigestId;
    int maxDigests;
    final int checkpointPeriod;

    // All
    final int serverId;
    int leaderId;
    final int quorum;
    final int servers;
    final int bufferSize;

    /*
     * Proposer
     */
    final int congestionWindow;
    /** Execution pending instances */
    int pendingInstances;
    final ArrayList<ArrayList<IidRequest>> requests;
    boolean isLeader;
    int ballotProposer;
    /** Size under which request are forwarded through the leader */
    final int requestThreshold;
    final PreparedMessages prepared;

    /*
     * Proposer & Acceptor
     */

    final ArrayList<InstanceRecord> instances;
    long firstInstanceId;
    int maxInstances;
    long currIid;

    /*
     * Acceptor
     */
    int ballotAcceptor;

    /*
     * Learner
     */
    final ArrayList<IidAcceptorsCounts> accepted;
    long maxExecuted;

    // Generate messages
    final ArrayList<TimestampReply> replyCache;
    final ArrayList<Long> inProgress;
    final boolean leaderReplies;
    boolean completedPhaseOne;

    // ------------------------------

    public int getBallotProposer() {
        return ballotProposer;
    }

    public void setBallotProposer(int ballotProposer) {
        this.ballotProposer = ballotProposer;
    }

    public boolean getIsLeader() {
        return isLeader;
    }

    public void setIsLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public ArrayList<InstanceRecord> getInstances() {
        return instances;
    }

    public int getBallotAcceptor() {
        return ballotAcceptor;
    }

    public void setBallotAcceptor(int ballotAcceptor) {
        this.ballotAcceptor = ballotAcceptor;
    }

    public ArrayList<IidAcceptorsCounts> getAccepted() {
        return accepted;
    }

    public long getReplyCacheTimestampElement(int clientId){
        TimestampReply reply = replyCache.get(clientId);
        if (reply != null)
            return reply.getTimestamp();
        return -1;
    }
    
    public void setReplyCacheTimestampElement(int clientId, long newVal){
    }
    
    public TimestampReply getReplyCacheElement(int clientId){
        return replyCache.get(clientId);
    }
    
    public void setReplyCacheElement(int clientId, TimestampReply newVal){
        replyCache.set(clientId, newVal);
    }
    
    public long getInProgressElement(int clientId){
        return inProgress.get(clientId);
    }
    
    public void setInProgressElement(int clientId, long timestamp){
        inProgress.set(clientId, timestamp);
    }

    public void setClientTimestampBufferElem(IndexIid index, ClientTimestamp ct) {
        instances.get((int) (index.getIid() % maxInstances)).setClientTimestamp(ct, index.getIndex());
    }

    public ClientTimestamp getClientTimestampBufferElem(IndexIid index) {
        return instances.get((int) (index.getIid() % maxInstances)).getClientTimestamp(index.getIndex());
    }

    public int getInstanceBufferSize(long iid) {
        return instances.get((int) (iid % maxInstances)).getArraySize();
    }

    public void setInstanceBufferSize(long iid, int newSize) {
        instances.get((int) (iid % maxInstances)).setArraySize(newSize);
    }

    public InstanceRecord getInstancesElement(long iid) {
        return instances.get((int) (iid % maxInstances));
    }

    public long getInstancesIid (long iid) {
        return instances.get((int) (iid % maxInstances)).getIid();
    }

    public void setInstancesElement(long iid, InstanceRecord instancesElement) {
        this.instances.set((int) (iid % maxInstances), instancesElement);
    }

    public int getInstancesBallot(long iid){
        return instances.get((int) (iid % maxInstances)).getBallot();
    }

    public IidAcceptorsCounts getAcceptedElement(long iid) {
        return accepted.get((int) (iid % maxInstances));
    }

    public boolean getIsAcceptedElement(long iid) {
        return accepted.get((int) (iid % maxInstances)).isAccepted();
    }

    public void setAcceptedElement(long iid, IidAcceptorsCounts acceptedElement) {
        this.accepted.set((int) (iid % maxInstances), acceptedElement);
    }

    public long getCurrIid() {
        return currIid;
    }

    public void setCurrIid(long firstInstancesElement) {
        this.currIid = firstInstancesElement;
    }

    public long getReceivedRequestIid(ClientTimestamp element) {
        ArrayList<IidRequest> clientArray = requests.get(element.clientId);
        if (clientArray == null || clientArray.size() == 0) {
            return -1;
        }
        IidRequest request = clientArray.get((int) (element.timestamp % maxInstances));
        if (request != null)
            return request.getIid();
        return -1;
    }

    public void setReceivedRequestIid(ClientTimestamp element, long value) {
    }

    public IidRequest getReceivedRequest(ClientTimestamp element) {
        ArrayList<IidRequest> clientArray = requests.get(element.clientId);
        if (clientArray == null || clientArray.size() == 0) {
            return null;
        }
        return clientArray.get((int) (element.timestamp % maxInstances));
    }

    public void setReceivedRequest(ClientTimestamp element, IidRequest value) {
        ArrayList<IidRequest> clientArray = requests.get(element.clientId);
        if (clientArray == null || clientArray.size() == 0) {
            clientArray = new ArrayList<IidRequest>(maxInstances);
            while(clientArray.size() < maxInstances) clientArray.add(null);
            requests.set(element.clientId, clientArray);
        }
        clientArray.set((int) (element.timestamp % maxInstances), value);
    }

    public int getMaxInstances() {
        return maxInstances;
    }

    public void setMaxInstances(int maxInstances) {
        this.maxInstances = maxInstances;
    }

    public int getServerId() {
        return serverId;
    }

    public int getLeaderId() {
        return leaderId;
    }
    
    public void setLeaderId(int leader){
    	leaderId = leader;
    }

    public int getQuorum() {
        return quorum;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getServers() {
        return servers;
    }

    public int getCongestionWindow() {
        return congestionWindow;
    }

    public int getPendingInstances() {
        return pendingInstances;
    }

    public void setPendingInstances(int pendingInstances) {
        this.pendingInstances = pendingInstances;
    }

//    public int getClientTimestampBufferSize() {
//        return clientTimestampBufferSize;
//    }
//
//    public void setClientTimestampBufferSize(int clientTimestampBufferSize) {
//        this.clientTimestampBufferSize = clientTimestampBufferSize;
//    }

//    public ClientTimestamp getClientTimestampBufferElement(int index) {
//        return clientTimestampBuffer[index];
//    }
//
//    public void setClientTimestampBufferElement(int index, ClientTimestamp clientTimestampBuffer) {
//        this.clientTimestampBuffer[index] = clientTimestampBuffer;
//    }
//
//    public ClientTimestamp[] getClientTimestampBuffer() {
//        return clientTimestampBuffer;
//    }
//
//    public void setClientTimestampBuffer(ClientTimestamp[] clientTimestampBuffer) {
//        this.clientTimestampBuffer = clientTimestampBuffer;
//    }

    public int getRequestThreshold() {
        return requestThreshold;
    }

    public ArrayList<DigestStore> getDigestStore() {
        return digestStore;
    }

    public DigestStore getDigestStoreElement(long digestId) {
        return digestStore.get((int) (digestId % maxDigests));
    }

    public void setDigestStoreElement(long digestId, DigestStore digestStoreElement) {
        this.digestStore.set((int) (digestId % maxDigests), digestStoreElement);
    }

    public int getCheckpointPeriod() {
        return checkpointPeriod;
    }

    public long getMaxExecuted() {
        return maxExecuted;
    }

    public void setMaxExecuted(long maxExecuted) {
        this.maxExecuted = maxExecuted;
    }

    public long getFirstDigestId() {
        return firstDigestId;
    }

    public void setFirstDigestId(long firstDigestId) {
        this.firstDigestId = firstDigestId;
    }

    public int getMaxDigests() {
        return maxDigests;
    }

    public void setMaxDigests(int maxDigests) {
        this.maxDigests = maxDigests;
    }

    public long getFirstInstanceId() {
        return firstInstanceId;
    }

    public void setFirstInstanceId(long firstInstanceId) {
        this.firstInstanceId = firstInstanceId;
    }

    public int getDigestQuorum() {
        return digestQuorum;
    }

    public boolean getLeaderReplies() {
        return leaderReplies;
    }

    public PreparedMessages getPrepared() {
        return prepared;
    }

    public void setCompletedPhaseOne(boolean completedPhaseOne) {
        this.completedPhaseOne = completedPhaseOne;
    }

    public boolean getCompletedPhaseOne() {
        return this.completedPhaseOne;
    }
}
