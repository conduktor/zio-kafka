package zio.kafka.utils

import zio.{ Task, ZIO }

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

object SslHelper {
  // https://issues.apache.org/jira/browse/KAFKA-4090
  def validateEndpoint(bootstrapServers: List[String], props: Map[String, AnyRef]): Task[Unit] =
    ZIO
      .unless(
        props
          .get("security.protocol")
          .collect { case x: String =>
            x
          }
          .exists(_.toLowerCase().contains("SSL"))
      ) {
        ZIO.foreachParDiscard(bootstrapServers) { str =>
          ZIO.scoped {
            for {
              address <- ZIO.attempt {
                           val arr  = str.split(":")
                           val host = arr(0)
                           val port = arr(1).toInt
                           new InetSocketAddress(host, port)
                         }
              channel <- ZIO.acquireRelease(
                           ZIO.attemptBlocking(SocketChannel.open(address))
                         )(channel => ZIO.attempt(channel.close()).orDie)
              tls <- ZIO.attemptBlocking {
                       val buf = ByteBuffer.allocate(5)
                       channel.write(buf)
                       buf.position(0)
                       channel.read(buf)
                       buf.position(0)
                       isTls(buf)
                     }
              _ <-
                ZIO.when(tls)(
                  ZIO.fail(
                    new IllegalArgumentException(
                      s"Received an unexpected SSL packet from the server. Please ensure the client is properly configured with SSL enabled"
                    )
                  )
                )
            } yield ()
          }

        }
      }
      .unit

  private def isTls(buf: ByteBuffer): Boolean = {
    val tlsMessageType = buf.get()
    tlsMessageType match {
      case 20 | 21 | 22 | 23 | 255 =>
        true
      case _ => tlsMessageType >= 128
    }
  }
}
