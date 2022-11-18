package zio.kafka.admin.acl

import org.apache.kafka.clients.admin.{ CreateAclsOptions => JCreateAclOptions }
import zio.duration.Duration

final case class CreateAclOptions(timeout: Option[Duration]) {
  def asJava: JCreateAclOptions = {
    val jopts = new JCreateAclOptions()

    timeout.fold(jopts)(timeout => jopts.timeoutMs(timeout.toMillis.toInt))
  }
}
