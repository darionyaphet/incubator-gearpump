/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.examples.wordcountjava;

import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.javaapi.Task;
import org.apache.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class Sum extends Task {

  private Logger LOG = super.LOG();
  private HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

  public Sum(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
  }

  @Override
  public void onStart(Instant startTime) {
    //skip
  }

  @Override
  public void onNext(Message message) {
    String word = (String) (message.value());
    Integer current = wordCount.get(word);
    if (current == null) {
      current = 0;
    }
    Integer newCount = current + 1;
    wordCount.put(word, newCount);
  }

  @Override
  public void onStop() {
    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
      LOG.info(entry.getKey() + " : " + entry.getValue());
    }
  }
}
