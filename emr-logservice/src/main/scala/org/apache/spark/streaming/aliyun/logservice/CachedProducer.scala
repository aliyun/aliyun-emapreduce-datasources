package org.apache.spark.streaming.aliyun.logservice

import com.aliyun.openservices.aliyun.log.producer._
import com.aliyun.openservices.log.common.LogItem
import org.apache.commons.cli.MissingArgumentException


class ProducerAdapter(project: String,
                      logstore: String,
                      producer: Producer) {
  def send(topic: String,
           source: String,
           record: LogItem,
           callback: Callback): Unit = {
    producer.send(project, logstore, topic, source, record, callback)
  }

  def close(): Unit = {
    producer.close()
  }
}

object CachedProducer {
  @transient private var producer: ProducerAdapter = _

  def getOrCreate[K, V](producerConfig: Map[String, String]): ProducerAdapter = {
    if (producer == null) {
      val logProject = producerConfig.getOrElse("sls.project",
        throw new MissingArgumentException("Missing logService project (='sls.project')."))
      val logstore = producerConfig.getOrElse("sls.logstore",
        throw new MissingArgumentException("Missing logService project (='sls.logstore')."))
      val accessKeyId = producerConfig.getOrElse("access.key.id",
        throw new MissingArgumentException("Missing access key id (='access.key.id')."))
      val accessKeySecret = producerConfig.getOrElse("access.key.secret",
        throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
      val endpoint = producerConfig.getOrElse("sls.endpoint",
        throw new MissingArgumentException("Missing log store endpoint (='sls.endpoint')."))
      val config = new ProducerConfig()
      val maxIOThread = producerConfig.getOrElse("sls.ioThreadCount", "1").toInt
      config.setIoThreadCount(maxIOThread)
      val logProducer = new LogProducer(config)
      logProducer.putProjectConfig(new ProjectConfig(logProject, endpoint, accessKeyId, accessKeySecret))
      producer = new ProducerAdapter(logProject, logstore, logProducer)
    }
    producer
  }

  /**
   * Flush and close the [[Producer]] in the cache associated with this config
   *
   * @param producerConfig producer configuration associated to a [[Producer]]
   */
  def close(producerConfig: Map[String, Object]): Unit = {
    producer.close()
  }
}
