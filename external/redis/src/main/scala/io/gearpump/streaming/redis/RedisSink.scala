package io.gearpump.streaming.redis

import io.gearpump.Message
import io.gearpump.google.common.base.Strings
import io.gearpump.streaming.redis.RedisMessage.PublishMessage
import io.gearpump.streaming.sink.DataSink
import io.gearpump.util.LogUtil
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
