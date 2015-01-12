/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeStack;


public final class InsertTaskSync extends InsertTask {

	private final AerospikeClient client; 

	public InsertTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
	}
	
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.put(args.writePolicy, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(args.writePolicy, key, bins);
			counters.write.count.getAndIncrement();			
		}
	}

	protected void largeListAdd(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeListAdd(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeListAdd(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}

	private void largeListAdd(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Add entry
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker", null);
		list.add(Value.getAsMap(entry));
	}
		
	protected void largeStackPush(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeStackPush(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeStackPush(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}
	
	private void largeStackPush(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Push entry
		LargeStack lstack = client.getLargeStack(args.writePolicy, key, "stackltracker", null);
		lstack.push(Value.getAsMap(entry));
	}
        protected void smallListAdd(Key key, Value value) throws AerospikeException {
                ArrayList<Object> Slist = null;
                int itemsInList = 3;
            		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}
		Slist = new ArrayList<Object>();
		for(int i = 0; i < itemsInList; i++) {
			Slist.add(value);
		}
		Bin listBin = Bin.asList("binTest", Slist);
                
		if (counters.write.latency != null) {		
                        long begin = System.currentTimeMillis();
                        client.put(args.writePolicy, key, listBin);
                        long elapsed = System.currentTimeMillis() - begin;
                        counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(args.writePolicy, key, listBin);
                        counters.write.count.getAndIncrement();			

		}
            
        }
        protected void smallMapAdd(Key key, Value value) throws AerospikeException{
                            ArrayList<Object> Slist = null;
                int mapSize = 3;
            		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}
		HashMap<String,Value> map = new HashMap<String,Value>();
		for(int i = 0; i < mapSize; i++) {
			map.put(Integer.toString(i+1),value);
		}
		Bin mapBin = Bin.asMap("binTest", map);
                
		if (counters.write.latency != null) {		
                        long begin = System.currentTimeMillis();
                        client.put(args.writePolicy, key, mapBin);
                        long elapsed = System.currentTimeMillis() - begin;
                        counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(args.writePolicy, key, mapBin);
                        counters.write.count.getAndIncrement();			

		}
        }
}
