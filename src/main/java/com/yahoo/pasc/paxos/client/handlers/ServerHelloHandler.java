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

package com.yahoo.pasc.paxos.client.handlers;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.aasc.ReadOnly;
import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.client.ClientState;
import com.yahoo.pasc.paxos.client.Connected;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.ServerHello;

public class ServerHelloHandler implements MessageHandler<ServerHello, Connected> {
    @ReadOnly 
    private static final Logger LOG = LoggerFactory.getLogger(ServerHelloHandler.class);
    
    private ClientState state;
    
    public ServerHelloHandler (ClientState state){
    	this.state = state;
    }

    @Override
    public boolean guardPredicate(ServerHello receivedMessage) {
        return true;
    }

    @Override
    public List<Connected> processMessage(ServerHello hello) {
        List<Connected> descriptors = null;
        if (!matches(hello, state)) {
            return null;
        }
        int connected = state.getConnected();
        connected++;
        state.setConnected(connected);
        if (connected == state.getQuorum()  ) {
            LOG.debug("Connected");
            state.setClientId(hello.getClientId());
            // Send the first message if connected to all servers
            descriptors = Arrays.asList(new Connected());
        }
        return descriptors;
    }

    private boolean matches(ServerHello serverHello, ClientState state) {
        Hello hello = state.getPendingHello();
        if (hello == null)
            return false;
        return hello.getClientId() == serverHello.getClientId();
    }

    @Override
    public List<Message> getOutputMessages(List<Connected> descriptors) {
        if (descriptors != null && descriptors.size() > 0) {
            return Arrays.<Message> asList(new Connected());
        }
        return null;
    }
}
