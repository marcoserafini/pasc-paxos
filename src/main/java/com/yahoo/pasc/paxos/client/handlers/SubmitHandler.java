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

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.client.ClientState;
import com.yahoo.pasc.paxos.client.ReplyStore;
import com.yahoo.pasc.paxos.client.messages.Submit;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.Request;

public class SubmitHandler implements MessageHandler<Submit, Request> {

	private ClientState state;
	
	public SubmitHandler (ClientState state){
		this.state = state;
	}
	
    @Override
    public boolean guardPredicate(Submit receivedMessage) {
        return true;
    }

    @Override
    public List<Request> processMessage(Submit submit) {
        long timestamp = state.getTimestamp();
        timestamp++;
        Request request;
        if (submit.getRequest().length > state.getInlineThreshold()) {
            request = new Request(state.getClientId(), timestamp, submit.getRequest());
        } else {
            request = new InlineRequest(state.getClientId(), timestamp, submit.getRequest());
        }
        state.setPendingRequest(request);
        state.setTimestamp(timestamp);
        state.setReplyStore(new ReplyStore(state.getServers()));
        return Arrays.asList(request);
    }

    @Override
    public List<Message> getOutputMessages(List<Request> messages) {
        return Arrays.<Message>asList(messages.get(0));
    }
}
