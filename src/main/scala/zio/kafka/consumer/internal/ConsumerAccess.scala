package zio.kafka.consumer.internal

import org.apache.kafka.clients.consumer.{ Consumer => JConsumer, KafkaConsumer }
import org.apache.kafka.common.errors.WakeupException
import zio._
import zio.kafka.consumer.ConsumerSettings
import zio.kafka.consumer.internal.ConsumerAccess.ByteArrayKafkaConsumer

private[consumer] class ConsumerAccess(
  private[consumer] val consumer: ByteArrayKafkaConsumer,
  access: Semaphore
) {
  def withConsumer[A](f: ByteArrayKafkaConsumer => A): Task[A] =
    withConsumerM[Any, A](c => ZIO.attempt(f(c)))

  def withConsumerM[R, A](f: ByteArrayKafkaConsumer => RIO[R, A]): RIO[R, A] =
    access.withPermit(withConsumerNoPermit(f))

  private[consumer] def withConsumerNoPermit[R, A](
    f: ByteArrayKafkaConsumer => RIO[R, A]
  ): RIO[R, A] =
    ZIO
      .blocking(ZIO.suspend(f(consumer)))
      .catchSome { case _: WakeupException =>
        ZIO.interrupt
      }
      .fork
      .flatMap(fib => fib.join.onInterrupt(ZIO.effectTotal(consumer.wakeup()) *> fib.interrupt))

}

private[consumer] object ConsumerAccess {
  type ByteArrayKafkaConsumer = JConsumer[Array[Byte], Array[Byte]]

  def fromJavaConsumer(
    javaConsumer: => ByteArrayKafkaConsumer,
    closeTimeout: Duration
  ): RManaged[Blocking, ConsumerAccess] =
    for {
      access   <- Semaphore.make(1).toManaged_
      blocking <- ZManaged.service[Blocking.Service]
      consumer <- blocking
                    .effectBlocking(javaConsumer)
                    .toManaged(c => blocking.blocking(access.withPermit(UIO(c.close(closeTimeout)))))
    } yield new ConsumerAccess(consumer, access, blocking)
}
