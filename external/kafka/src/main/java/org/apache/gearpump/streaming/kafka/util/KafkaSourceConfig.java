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

import kafka.api.OffsetRequest;
import kafka.consumer.ConsumerConfig;
import org.apache.gearpump.streaming.kafka.lib.source.DefaultKafkaMessageDecoder;
import org.apache.gearpump.streaming.kafka.lib.source.grouper.DefaultPartitionGrouper;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * kafka source configs
 */
public class KafkaSourceConfig extends AbstractConfig implements Serializable {

  private static final ConfigDef CONFIG;

  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  private static final String ZOOKEEPER_CONNECT_DOC =
      "Zookeeper connect string for Kafka topics management.";

  public static final String CLIENT_ID_CONFIG = "client.id";
  private static final String CLIENT_ID_DOC = "An id string to pass to the server when making "
      + "requests. The purpose of this is to be able to track the source of requests beyond just "
      + "ip/port by allowing a logical application name to be included in server-side request "
      + "logging.";

  public static final String GROUP_ID_CONFIG = "group.id";
  private static final String GROUP_ID_DOC =
      "A string that uniquely identifies a set of consumers within the same consumer group";

  public static final String ENABLE_AUTO_COMMIT_CONFIG = "auto.commit.enable";
  public static final String ENABLE_AUTO_COMMIT_DOC =
      "If true the consumer's offset will be periodically committed in the background.";

  public static final String CONSUMER_START_OFFSET_CONFIG = "consumer.start.offset";
  private static final String CONSUMER_START_OFFSET_DOC = "Kafka offset to start consume from. "
      + "This will be overwritten when checkpoint recover takes effect.";

  public static final String FETCH_THRESHOLD_CONFIG = "fetch.threshold";
  private static final String FETCH_THRESHOLD_DOC = "Kafka messages are fetched asynchronously "
      + "and put onto a internal queue. When the number of messages in the queue hit the threshold,"
      + "the fetch thread stops fetching, and goes to sleep. It starts fetching again when the"
      + "number falls below the threshold";

  public static final String FETCH_SLEEP_MS_CONFIG = "fetch.sleep.ms";
  private static final String FETCH_SLEEP_MS_DOC =
    "The amount of time to sleep when hitting fetch.threshold.";

  public static final String MESSAGE_DECODER_CLASS_CONFIG = "message.decoder.class";
  private static final String MESSAGE_DECODER_CLASS_DOC =
            "Message decoder class that implements the <code>MessageDecoder</code> interface.";

  public static final String PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
  private static final String PARTITION_GROUPER_CLASS_DOC =
            "Partition grouper class that implements the <code>KafkaGrouper</code> interface.";


  public KafkaSourceConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public KafkaSourceConfig(Properties props) {
    super(CONFIG, props);
  }

  static {
    CONFIG = new ConfigDef()
        .define(ZOOKEEPER_CONNECT_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            ZOOKEEPER_CONNECT_DOC)
        .define(CLIENT_ID_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            CLIENT_ID_DOC)
        .define(GROUP_ID_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            GROUP_ID_DOC)
        .define(ENABLE_AUTO_COMMIT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            ENABLE_AUTO_COMMIT_DOC)
        .define(FETCH_THRESHOLD_CONFIG,
            ConfigDef.Type.INT,
            10000,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.LOW,
            FETCH_THRESHOLD_DOC)
        .define(FETCH_SLEEP_MS_CONFIG,
            ConfigDef.Type.LONG,
            100,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.LOW,
            FETCH_SLEEP_MS_DOC)
        .define(CONSUMER_START_OFFSET_CONFIG,
            ConfigDef.Type.LONG,
            OffsetRequest.EarliestTime(),
            ConfigDef.Range.atLeast(-2),
            ConfigDef.Importance.MEDIUM,
            CONSUMER_START_OFFSET_DOC)
        .define(MESSAGE_DECODER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DefaultKafkaMessageDecoder.class.getName(),
            ConfigDef.Importance.MEDIUM,
            MESSAGE_DECODER_CLASS_DOC)
        .define(PARTITION_GROUPER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DefaultPartitionGrouper.class.getName(),
            ConfigDef.Importance.MEDIUM,
            PARTITION_GROUPER_CLASS_DOC);
  }

  public ConsumerConfig toConsumerConfig() {
    Properties properties = new Properties();
    properties.putAll(this.originals());
    return new ConsumerConfig(properties);
  }

  public static class KafkaSourceConfigFactory implements Serializable {
    public KafkaSourceConfig getKafkaSourceConfig(Properties props) {
        return new KafkaSourceConfig(props);
    }
  }
}
