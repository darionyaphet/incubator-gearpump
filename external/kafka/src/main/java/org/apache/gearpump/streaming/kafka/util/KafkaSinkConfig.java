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

package org.apache.gearpump.streaming.kafka.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * kafka sink configs
 */
public class KafkaSinkConfig extends AbstractConfig implements Serializable {

  private static final ConfigDef CONFIG;

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  private static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for "
            + "establishing the initial connection to the Kafka cluster. "
            + "The client will make use of all servers irrespective of which servers are specified "
            + "here for bootstrapping&mdash;this list only impacts the initial hosts used to discover "
            + "the full set of servers. This list should be in the form "
            + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the "
            + "initial connection to discover the full cluster membership (which may change dynamically),"
            + " this list need not contain the full set of servers (you may want more than one, though, "
            + "in case a server is down).";


  public static final String ACKS_CONFIG = "acks";
  private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader" +
            "to have received before considering a request complete. " +
            "This controls the durability of records that are sent. ";

  public static final String CLIENT_ID_CONFIG = "client.id";
  private static final String CLIENT_ID_DOC = "An id string to pass to the server when making "
            + "requests. The purpose of this is to be able to track the source of requests beyond just "
            + "ip/port by allowing a logical application name to be included in server-side request "
            + "logging.";


  public KafkaSinkConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public KafkaSinkConfig(Properties props) {
    super(CONFIG, props);
  }

  static {
    CONFIG = new ConfigDef()
        .define(BOOTSTRAP_SERVERS_CONFIG, // required with no default value
            ConfigDef.Type.LIST,
            ConfigDef.Importance.HIGH,
            BOOTSTRAP_SERVERS_DOC)
        .define(CLIENT_ID_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            CLIENT_ID_DOC);
  }

  public Properties toProducerConfig() {
    Properties props = new Properties();
    for(String key:this.originals().keySet()){
      props.setProperty(key,this.originals().get(key).toString());
    }
    return props;
  }

  public static class KafkaSinkConfigFactory implements Serializable {
    public KafkaSinkConfig getKafkaSinkConfig(Properties props) {
      return new KafkaSinkConfig(props);
    }
  }
}
