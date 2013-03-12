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
import com.yahoo.pasc.paxos.messages.Prepared;

@Introspect
public final class PreparedMessages implements Serializable, EqualsDeep<PreparedMessages>, CloneableDeep<PreparedMessages> {

    private static final long serialVersionUID = 7587497611602103466L;

    ArrayList<Prepared> messages;
    
    public PreparedMessages(int acceptorsNumber) {
    	messages = new ArrayList<Prepared>(acceptorsNumber);
    	while(messages.size() < acceptorsNumber){
    		messages.add(null);
    	}
    }

    public int getCardinality(int servers) {
        int count = 0;
        for (int i = 0; i < servers; ++i) {
            if (messages.get(i) != null)
                ++count;
        }
        return count;
    }
    
    public void setPreparedMessage (Prepared message, int senderId){
    	messages.set(senderId, message); 
    }
    
    public ArrayList<Prepared> getPreparedMessages (){
    	return messages;
    }
    
    public void clear(){
    	messages.clear();
    }

    public PreparedMessages cloneDeep() {
        PreparedMessages res = new PreparedMessages(this.messages.size());
        for (int i = 0; i < this.messages.size(); i++){
            if (this.messages.get(i) != null) {
                res.messages.set(i, this.messages.get(i).cloneDeep());
            }
        }
        return res;
    }

    public boolean equalsDeep(PreparedMessages other) {
        if (other.messages.size() != this.messages.size()){
        	return false;
        }
        for (int i = 0; i < this.messages.size(); i++){
            if (this.messages.get(i) == null) {
                if (other.messages.get(i) != null) {
                    return false;
                }
            } else {
            	if(!this.messages.get(i).equalsDeep(other.messages.get(i))){
            		return false;
            	}
            }
        }
        return true;
    }
}
