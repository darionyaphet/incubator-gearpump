package io.gearpump.streaming.redis

import io.gearpump.Message
import io.gearpump.google.common.base.Strings
import io.gearpump.streaming.redis.RedisMessage.{GEOAdd, HSetMessage, LPushMessage, PFAdd, RPushMessage, SAddMessage, SetMessage, ZAddMessage}
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.LogUtil
import redis.clients.jedis.{Jedis, Protocol}

class RedisStorage(
                    host: String = Protocol.DEFAULT_HOST,
                    port: Int = Protocol.DEFAULT_PORT,
                    timeout: Int = Protocol.DEFAULT_TIMEOUT,
                    database: Int = Protocol.DEFAULT_DATABASE,
                    password: String = "") extends DataSink {

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Jedis(host, port, timeout)

  override def open(context: TaskContext): Unit = {
    client.select(database)

    if (!Strings.isNullOrEmpty(password)) {
      client.auth(password)
    }
  }

  override def write(message: Message): Unit = {
    val msg = message.msg

    msg match {
      case set: SetMessage => client.set(set.key, set.value)
      case lpush: LPushMessage => client.lpush(lpush.key, lpush.value)
      case rpush: RPushMessage => client.rpush(rpush.key, rpush.value)
      case hset: HSetMessage => client.hset(hset.key, hset.field, hset.value)
      case sadd: SAddMessage => client.sadd(sadd.key, sadd.member)
      case zadd: ZAddMessage => client.zadd(zadd.key, zadd.score, zadd.member)
      case pfAdd: PFAdd => client.pfadd(pfAdd.key, pfAdd.member)
      case geoAdd: GEOAdd => client.geoadd(geoAdd.key, geoAdd.longitude, geoAdd.latitude, geoAdd.member)
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
