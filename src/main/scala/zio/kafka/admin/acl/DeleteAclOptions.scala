package zio.kafka.admin.acl

import org.apache.kafka.clients.admin.{ DeleteAclsOptions => JDeleteAclsOptions }
import zio.duration.Duration

final case class DeleteAclsOptions(timeout: Option[Duration]) {
  def asJava: JDeleteAclsOptions = {
    val jopts = new JDeleteAclsOptions()

    timeout.fold(jopts)(timeout => jopts.timeoutMs(timeout.toMillis.toInt))
  }
}
