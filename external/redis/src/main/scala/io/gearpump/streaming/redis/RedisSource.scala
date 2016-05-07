package io.gearpump.streaming.redis

import java.util.Arrays

import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.LogUtil
import io.gearpump.{Message, TimeStamp}
import redis.clients.jedis.Protocol.Keyword.{MESSAGE, PMESSAGE, SUBSCRIBE}
import redis.clients.jedis.{Client, Protocol}
import redis.clients.util.SafeEncoder

class RedisSource(
                   host: String = Protocol.DEFAULT_HOST,
                   port: Int = Protocol.DEFAULT_PORT,
                   timeout: Int = Protocol.DEFAULT_TIMEOUT,
                   patterned: Boolean = false,
                   channels: Array[String]) extends DataSource {

  def this(channel: String) = this(channels = Array(channel))

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Client(host, port)

  override def open(context: TaskContext, startTime: Long): Unit = {
    client.setTimeoutInfinite()

    if (!patterned) {
      for (channel <- channels) {
        client.subscribe(channel)
      }
    } else {
      for (channel <- channels) {
        client.psubscribe(channel)
      }
    }

    val reply = client.getObjectMultiBulkReply()
    val response = reply.get(0).asInstanceOf[Array[Byte]]

    if (Arrays.equals(SUBSCRIBE.raw, response)) {
      val channel = SafeEncoder.encode(reply.get(1).asInstanceOf[Array[Byte]]);
      LOG.info(s"Subscribed on ${channel}")
    }
  }

  override def read(): Message = {
    val reply = client.getRawObjectMultiBulkReply()
    val response = reply.get(0).asInstanceOf[Array[Byte]];

    val msg: Option[String] = if (Arrays.equals(MESSAGE.raw, response)) {
      Some(SafeEncoder.encode(reply.get(2).asInstanceOf[Array[Byte]]))
    } else if (Arrays.equals(PMESSAGE.raw, response)) {
      Some(SafeEncoder.encode(reply.get(3).asInstanceOf[Array[Byte]]))
    } else {
      None
    }

    LOG.info(s"Message ${msg.get}")
    new Message(msg, current())
  }

  private def current(): TimeStamp = System.currentTimeMillis()

  override def close(): Unit = {
    client.close()
  }
}
