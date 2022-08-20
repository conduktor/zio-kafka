package zio.kafka.consumer

import org.apache.kafka.clients.consumer.{ Consumer => Jconsumer, OffsetAndMetadata }
import org.apache.kafka.common.TopicPartition
import zio.Task

import scala.jdk.CollectionConverters._

/**
 * A subset of Consumer methods available during rebalances.
 */
trait RebalanceConsumer {
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Task[Unit]
}

object RebalanceConsumer {
  final case class Live(blocking: zio.blocking.Blocking.Service, consumer: Jconsumer[Array[Byte], Array[Byte]])
      extends RebalanceConsumer {
    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Task[Unit] =
      blocking.effectBlocking(consumer.commitSync(offsets.asJava))
  }
}
