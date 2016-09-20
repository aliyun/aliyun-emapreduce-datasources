package org.apache.spark.streaming.aliyun.logservice

import com.aliyun.openservices.loghub.client.interfaces.{ILogHubProcessor, ILogHubProcessorFactory}

class SimpleLogHubProcessorFactory(receiver: LoghubReceiver)
    extends ILogHubProcessorFactory {
  override def generatorProcessor(): ILogHubProcessor =
    new SimpleLogHubProcessor(receiver)
}
