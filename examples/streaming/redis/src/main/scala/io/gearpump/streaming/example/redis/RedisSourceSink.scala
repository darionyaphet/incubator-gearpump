package io.gearpump.streaming.example.redis

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.embedded.EmbeddedCluster
import io.gearpump.cluster.main.ArgumentsParser
import io.gearpump.streaming.redis.RedisMessage.PublishMessage
import io.gearpump.streaming.redis.{RedisSink, RedisSource}
import io.gearpump.streaming.sink.DataSinkProcessor
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.streaming.task.{Task, TaskContext}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.Graph._
import io.gearpump.util.{AkkaApp, Graph}

class RedisSourceSinkUpperProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.output

  override def onNext(message: Message): Unit = {
    val msg = message.msg.asInstanceOf[Option[String]]

    if (!msg.isEmpty) {
      val upper = msg.get.toUpperCase
      LOG.info("to Upper : " + upper)
      output(new Message(new PublishMessage(upper), message.timestamp))
    }
  }
}

object RedisSourceSink extends AkkaApp with ArgumentsParser {
  override def main(akkaConf: RedisSourceSink.Config, args: Array[String]): Unit = {
    val cluster = new EmbeddedCluster(akkaConf: Config)
    cluster.start()

    val context = cluster.newClientContext
    implicit val actorSystem = context.system

    val source = DataSourceProcessor(new RedisSource(channel = "channel.in"), 1)
    val upper = Processor[RedisSourceSinkUpperProcessor](1)
    val sink = DataSinkProcessor(new RedisSink(channel = "channel.out"), 1)
    val dag = source ~> upper ~> sink
    val app = StreamApplication("RedisSourceSink", Graph(dag), UserConfig.empty)

    context.submit(app)
    Thread.sleep(600 * 1000)
    context.close()
    cluster.stop()
  }
}
