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

import org.apache.gearpump.streaming.kafka.lib.source.DefaultKafkaMessageDecoder;
import org.apache.gearpump.streaming.kafka.lib.source.grouper.DefaultPartitionGrouper;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * kafka store configs
 */
public class KafkaStoreConfig extends AbstractConfig implements Serializable {

  private static final ConfigDef CONFIG;

  public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
  private static final String REPLICATION_FACTOR_DOC =
            "The replication factor for checkpoint store topic.";

  public static final String CHECKPOINT_STORE_NAME_PREFIX_CONFIG = "checkpoint.store.name.prefix";
  private static final String CHECKPOINT_STORE_NAME_PREFIX_DOC = "Name prefix for checkpoint "
            + "store whose name will be of the form, namePrefix-sourceTopic-partitionId";


  static {
    CONFIG = new ConfigDef()
        .define(REPLICATION_FACTOR_CONFIG,
            ConfigDef.Type.INT,
            1,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.MEDIUM,
            REPLICATION_FACTOR_DOC)
        .define(CHECKPOINT_STORE_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            CHECKPOINT_STORE_NAME_PREFIX_DOC);
  }

  public KafkaStoreConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public KafkaStoreConfig(Properties props) {
    super(CONFIG, props);
  }

  public static class KafkaStoreConfigFactory implements Serializable {
    public KafkaStoreConfig getKafkaStoreConfig(Properties props) {
      return new KafkaStoreConfig(props);
    }
  }
}
