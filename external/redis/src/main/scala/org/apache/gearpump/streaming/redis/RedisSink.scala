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
package org.apache.gearpump.streaming.redis

import io.gearpump.Message
import io.gearpump.google.common.base.Strings
import RedisMessage.PublishMessage
import io.gearpump.streaming.sink.DataSink
import io.gearpump.util.LogUtil
import org.apache.gearpump.streaming.redis.RedisMessage.PublishMessage
import redis.clients.jedis.{Jedis, Protocol}

class RedisSink(host: String = Protocol.DEFAULT_HOST,
                port: Int = Protocol.DEFAULT_PORT,
                timeout: Int = Protocol.DEFAULT_TIMEOUT,
                password: String = "",
                channel: Array[Byte]) extends DataSink {

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Jedis(host, port, timeout)

  def this(channel: String) = this(channel = channel.getBytes())

  override def open(context: _root_.io.gearpump.streaming.task.TaskContext): Unit = {
    if (!Strings.isNullOrEmpty(password)) {
      client.auth(password)
    }
  }

  override def write(message: Message): Unit = {
    val msg = message.msg
    msg match {
      case publish: PublishMessage => client.publish(channel, publish.message)
      case _ => LOG.error("Error Message ")
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
