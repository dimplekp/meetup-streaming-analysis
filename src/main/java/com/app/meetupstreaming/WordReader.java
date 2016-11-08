package com.app.meetupstreaming;

import java.io.IOException;
import java.util.Iterator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

// extracts words from input line
public class WordReader extends BaseOperator {
	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {

		@Override
		public void process(String line) {
			
			// line; split it into words and emit them
			try {
				JSONObject obj = new JSONObject(line);

				JSONObject jobj = new JSONObject(obj.get("group").toString().trim());
				Iterator<?> keys = jobj.keys();
				JSONArray urlkeys = null;
				while (keys.hasNext()) {
					String key = (String) keys.next();
					if (key.equals("group_topics")) {
						urlkeys = (JSONArray) jobj.get(key);
					}
				}
				for (int i = 0; i < urlkeys.length(); i++) {
					JSONObject rec = urlkeys.getJSONObject(i);
					String loc = rec.getString("urlkey");
					output.emit(loc);
				}

			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	};

	@Override
	public void setup(OperatorContext context) {
	}

}
